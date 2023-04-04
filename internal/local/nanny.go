// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

// Provides an implementation of a Nanny server for a gke-local deployment,
// which hosts a number of Service Weaver management services, such as:
//   - Controller  - controls the rollout of new application versions;
//     accessed via the "controller/" URL path prefix.
//   - Distributor - controls traffic distribution across multiple application
//     versions in the same deployment location (e.g., cloud
//     region); accessed via the "distributor/" URL path prefix.
//   - Manager     - controls various runtime tasks for Service Weaver
//     ReplicaSets; accessed via the "manager/" URL path prefix.
//   - Proxy       - proxies requests to the applications' network listeners.
//
// A given Nanny server may host any subset of the above servers, depending
// on the deployment environment.  For example, in GKE, we start a single
// "super-nanny" server that hosts only the controller service, and N Nanny
// servers (i.e., one per cloud region) that host distributor and manager
// services.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/local/proxy"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/uuid"
	"golang.org/x/exp/slog"
)

// URL on the controller where the metrics are exported in the Prometheus format.
const PrometheusURL = "/metrics"

// LogDir is where weaver-gke-local deployed applications store their logs.
var LogDir = filepath.Join(logging.DefaultLogDir, "weaver-gke-local")

// Nanny returns the nanny address, along with the HTTP client that can be
// used to reach it.
func Nanny(context.Context) (string, *http.Client, error) {
	addr := fmt.Sprintf("http://localhost:%d", controllerPort)
	return addr, http.DefaultClient, nil
}

// NannyOptions configure a nanny.
type NannyOptions struct {
	Region           string // simulated GKE region (e.g., us-central1)
	StartController  bool   // start a controller?
	StartDistributor bool   // start a distributor/manager?
	Port             int    // listen on localhost:port
}

// RunNanny creates and runs a nanny.
func RunNanny(ctx context.Context, opts NannyOptions) error {
	if !opts.StartController && !opts.StartDistributor {
		return fmt.Errorf("RunNanny: nothing to start")
	}

	// Nanny logs will be written to the same log-store to which we write
	// gke-local application logs.
	id := uuid.New()
	ls, err := logging.NewFileStore(LogDir)
	if err != nil {
		return fmt.Errorf("cannot create log storage: %w", err)
	}
	defer ls.Close()

	makeLogger := func(component string) *slog.Logger {
		return slog.New(&logging.LogHandler{
			Opts: logging.Options{
				App:        "nanny",
				Deployment: id.String(),
				Component:  component,
				Weavelet:   id.String(),
				Attrs:      []string{"serviceweaver/system", ""},
			},
			Write: ls.Add,
		})
	}

	addr := fmt.Sprintf("localhost:%d", opts.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		// Whenever an application version is deployed, we run this code to
		// start a nanny. If a nanny is already running, we will fail to bind
		// to the nanny address.
		logger := makeLogger("")
		logger.Error("nanny listen", err, "address", addr)
		return fmt.Errorf("nanny cannot listen on %q: %w", addr, err)
	}

	// Use a region-scoped storage dir.
	s, err := Store(opts.Region)
	if err != nil {
		return fmt.Errorf("cannot retrieve the store: %w", err)
	}

	mux := http.NewServeMux()
	managerAddr := fmt.Sprintf("http://%s", lis.Addr())
	proxyAddr := fmt.Sprintf("http://localhost:%d", proxyPort)
	babysitterConstructor := func(addr string) clients.BabysitterClient {
		return &babysitter.HttpClient{Addr: internal.ToHTTPAddress(addr)}
	}

	metricDB, err := metricdb.Open(ctx)
	if err != nil {
		return err
	}

	if opts.StartController {
		logger := makeLogger("controller")
		if _, err := controller.Start(ctx,
			mux,
			store.WithMetrics("controller", id, s),
			logger,
			10*time.Second, // actuationDelay
			func(addr string) clients.DistributorClient {
				return &distributor.HttpClient{Addr: addr}
			},
			10*time.Second, // fetchAssignmentsInterval
			10*time.Second, // applyAssignmentInterval
			5*time.Second,  // manageAppInterval,
			func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
				return applyTraffic(ctx, proxyAddr, assignment, opts.Region)
			},
		); err != nil {
			return fmt.Errorf("cannot start controller: %w", err)
		}

		// Add the Prometheus metric handler.
		mux.Handle("/metrics", NewPrometheusHandler(metricDB, logger))
	}

	if opts.StartDistributor {
		dstore := store.WithMetrics("distributor", id, s)
		if _, err := distributor.Start(ctx,
			mux,
			dstore,
			makeLogger("distributor-"+opts.Region),
			&manager.HttpClient{Addr: managerAddr},
			opts.Region,
			babysitterConstructor,
			10*time.Second, // manageAppsInterval
			10*time.Second, // computeTrafficInterval
			10*time.Second, // applyTrafficInterval
			10*time.Second, // detectAppliedTrafficInterval
			func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
				return applyTraffic(ctx, fmt.Sprintf("http://localhost:%d", proxyPort), assignment, opts.Region)
			},
			func(ctx context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
				return getListeners(ctx, dstore, cfg)
			},
			func(ctx context.Context, metric string, labels ...string) ([]distributor.MetricCount, error) {
				return getMetricCounts(ctx, metricDB, time.Now().Add(-2*time.Minute), time.Now().Add(-time.Minute), time.Now(), metric, labels...)
			},
		); err != nil {
			return fmt.Errorf("cannot start distributor: %w", err)
		}
		mstore := store.WithMetrics("manager", id, s)
		starter := NewStarter(mstore)
		if err := manager.Start(ctx,
			mux,
			mstore,
			makeLogger("manager-"+opts.Region),
			managerAddr,
			babysitterConstructor,
			func(ctx context.Context, podName string) (bool, error) {
				// For GKE local, the babysitter is aware when a weavelet is killed,
				// so we don't need any external signal to check whether a weavelet still exists.
				return true, nil
			},
			func(context.Context, *config.GKEConfig, string, string) (int, error) {
				return 0, nil
			},
			func(ctx context.Context, cfg *config.GKEConfig, _ string, lis *nanny.Listener) (*protos.ExportListenerReply, error) {
				if err := RecordListener(ctx, mstore, cfg, lis); err != nil {
					return &protos.ExportListenerReply{}, err
				}
				return &protos.ExportListenerReply{ProxyAddress: proxyAddr}, nil
			},
			func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) error {
				return starter.Start(ctx, cfg, replicaSet)
			},
			func(_ context.Context, _ string, appVersions []string) error {
				return starter.Stop(ctx, appVersions)
			},
			func(context.Context, string, []string) error {
				// Nothing to do.
				return nil
			},
		); err != nil {
			return fmt.Errorf("cannot start manager: %w", err)
		}
	}

	makeLogger("nanny").Info("Nanny listening", "address", lis.Addr())
	server := http.Server{Handler: mux}
	errs := make(chan error, 1)
	go func() {
		errs <- server.Serve(lis)
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return server.Shutdown(ctx)
	}
}

// RunProxy creates and runs a proxy listening on ":port".
func RunProxy(ctx context.Context, port int) error {
	id := uuid.New()
	ls, err := logging.NewFileStore(LogDir)
	if err != nil {
		return fmt.Errorf("cannot create log storage: %w", err)
	}
	defer ls.Close()
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        "nanny",
			Deployment: id.String(),
			Component:  "proxy",
			Weavelet:   id.String(),
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: ls.Add,
	})

	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("proxy listen", err, "address", addr)
		return err
	}

	proxy := proxy.NewProxy(logger)
	logger.Debug("Proxy listenening", "address", lis.Addr())
	return proxy.Serve(ctx, lis, fmt.Sprintf("localhost:%d", port))
}
