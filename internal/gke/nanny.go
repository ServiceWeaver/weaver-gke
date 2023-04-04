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

package gke

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/uuid"
	"golang.org/x/exp/slog"
)

// NannyOptions configure a GKE nanny.
type NannyOptions struct {
	StartController  bool // start a controller?
	StartDistributor bool // start a distributor?
	StartManager     bool // start a manager?
	Port             int  // listen on :port
}

// RunNanny creates and runs a nanny.
func RunNanny(ctx context.Context, opts NannyOptions) error {
	if !opts.StartController && !opts.StartDistributor && !opts.StartManager {
		return fmt.Errorf("RunNanny: nothing to start")
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	cluster, err := inClusterInfo(ctx)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", opts.Port))
	if err != nil {
		return err
	}
	store := Store(cluster)
	id := uuid.New()

	// Parse container metadata.
	for _, v := range []string{
		containerMetadataEnvKey,
		nodeNameEnvKey,
		podNameEnvKey,
	} {
		if _, ok := os.LookupEnv(v); !ok {
			return fmt.Errorf("environment variable %q not set\n", v)
		}
	}
	meta := &ContainerMetadata{}
	metaStr := os.Getenv(containerMetadataEnvKey)
	if err := proto.FromEnv(metaStr, meta); err != nil {
		return err
	}
	meta.NodeName = os.Getenv(nodeNameEnvKey)
	meta.PodName = os.Getenv(podNameEnvKey)

	logClient, err := newCloudLoggingClient(ctx, meta)
	if err != nil {
		return err
	}
	defer logClient.Close()

	makeLogger := func(service string) *slog.Logger {
		return logClient.Logger(logging.Options{
			App:       "nanny",
			Component: service,
			Weavelet:  id.String(),
			Attrs:     []string{"serviceweaver/system", ""},
		})
	}

	// Launch a goroutine to export metrics.
	exporter, err := newMetricExporter(ctx, meta)
	if err != nil {
		return err
	}
	go exportNannyMetrics(ctx, exporter, makeLogger("metric_exporter"))

	// Launch the nanny.
	return run(ctx, opts, cluster, lis, store, id, makeLogger)
}

func exportNannyMetrics(ctx context.Context, exporter *metricExporter, logger *slog.Logger) {
	const metricExportInterval = 15 * time.Second
	ticker := time.NewTicker(metricExportInterval)
	for {
		select {
		case <-ticker.C:
			snaps := metrics.Snapshot()
			if err := exporter.Export(ctx, snaps); err != nil {
				logger.Error("exporting nanny metrics", err)
			}

		case <-ctx.Done():
			logger.Debug("exportMetrics cancelled")
			return
		}
	}
}

func run(ctx context.Context, opts NannyOptions, cluster *ClusterInfo, lis net.Listener, s store.Store, id uuid.UUID, makeLogger func(service string) *slog.Logger) error {
	babysitterConstructor := func(addr string) clients.BabysitterClient {
		return &babysitter.HttpClient{Addr: internal.ToHTTPAddress(addr)}
	}
	mux := http.NewServeMux()
	managerAddr := fmt.Sprintf("http://manager.%s.svc.%s-%s:80", namespaceName, applicationClusterName, cluster.Region)
	if opts.StartController {
		logger := makeLogger("controller")
		if _, err := controller.Start(ctx,
			mux,
			store.WithMetrics("controller", id, s),
			logger,
			10*time.Minute, // actuationDelay
			func(addr string) clients.DistributorClient {
				return &distributor.HttpClient{Addr: addr}
			},
			10*time.Second, // fetchAssignmentsInterval
			10*time.Second, // applyAssignmentInterval
			5*time.Second,  // manageAppInterval
			func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
				return updateGlobalExternalTrafficRoutes(ctx, logger, cluster, assignment)
			},
		); err != nil {
			return fmt.Errorf("cannot start controller: %w", err)
		}
	}
	if opts.StartDistributor {
		logger := makeLogger(fmt.Sprintf("distributor-%s", cluster.Region))
		if _, err := distributor.Start(ctx,
			mux,
			store.WithMetrics("distributor", id, s),
			logger,
			&manager.HttpClient{Addr: managerAddr},
			cluster.Region,
			babysitterConstructor,
			5*time.Second,  // manageAppsInterval
			10*time.Second, // computeTrafficInterval
			10*time.Second, // applyTrafficInterval
			10*time.Second, // detectAppliedTrafficInterval
			func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
				return updateRegionalInternalTrafficRoutes(ctx, cluster, logger, assignment)
			},
			func(ctx context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
				dep := cfg.Deployment
				return getListeners(ctx, cluster.Clientset, dep.App.Name, dep.Id)
			},
			func(ctx context.Context, metric string, labels ...string) ([]distributor.MetricCount, error) {
				return getMetricCounts(ctx, cluster.CloudConfig, cluster.Region, metric, labels...)
			},
		); err != nil {
			return fmt.Errorf("cannot start distributor: %w", err)
		}
	}
	if opts.StartManager {
		logger := makeLogger("manager")
		s = store.WithMetrics("manager", id, s)
		if err := manager.Start(ctx,
			mux,
			s,
			logger,
			managerAddr,
			babysitterConstructor,
			func(ctx context.Context, podName string) (bool, error) {
				return podExists(ctx, cluster, podName)
			},
			func(ctx context.Context, cfg *config.GKEConfig, replicaSet string, lis string) (int, error) {
				port, err := getListenerPort(ctx, s, logger, cluster, cfg, replicaSet, lis)
				if err != nil {
					return -1, err
				}
				return port, nil
			},
			func(ctx context.Context, cfg *config.GKEConfig, replicaSet string, lis *nanny.Listener) (*protos.ExportListenerReply, error) {
				if err := ensureListenerService(ctx, cluster, logger, cfg, replicaSet, lis); err != nil {
					return nil, err
				}

				// TODO(spetrovic): use the global load-balancer's address here.
				const proxyAddr = ""
				return &protos.ExportListenerReply{ProxyAddress: proxyAddr}, nil
			},
			func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) error {
				return deploy(ctx, cluster, logger, cfg, replicaSet)
			},
			func(_ context.Context, app string, versions []string) error {
				for _, version := range versions {
					if err := stop(ctx, cluster, logger, app, version); err != nil {
						return fmt.Errorf("stop %q: %w", version, err)
					}
				}
				return nil
			},
			func(_ context.Context, app string, versions []string) error {
				for _, version := range versions {
					if err := kill(ctx, cluster, app, version); err != nil {
						return fmt.Errorf("kill %q: %w", version, err)
					}
				}
				return nil
			},
		); err != nil {
			return fmt.Errorf("cannot start manager: %w", err)
		}
	}

	// Run the nanny server.
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

// getListenerPort returns the port the network listener should listen on
// inside the Kubernetes ReplicaSet, and creates a service to forward traffic to
// that
// port.
func getListenerPort(ctx context.Context, s store.Store, logger *slog.Logger, cluster *ClusterInfo, cfg *config.GKEConfig, replicaSet string, listener string) (int, error) {
	dep := cfg.Deployment
	id, err := uuid.Parse(dep.Id)
	if err != nil {
		return -1, fmt.Errorf("invalid deployment version %q: %w", dep.Id, err)
	}

	key := store.AppKey(dep.App.Name, "ports")
	histKey := store.DeploymentKey(dep.App.Name, id, store.HistoryKey)
	err = store.AddToSet(ctx, s, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		// Track the key in the store under histKey.
		return -1, fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	targetPort, err := pickPort(ctx, s, key, listener)
	if err != nil {
		return -1, fmt.Errorf("error picking port for listener %q: %w", listener, err)
	}
	return targetPort, nil
}

// pickPort assigns to a listener a unique port, or returns the assigned port if
// one has already been picked. More formally, pickPort provides the
// following guarantees:
//
// - It returns the same port for the same listener, even if called concurrently
// from different processes (potentially on different machines). Coordination is
// performed using the store.
//
// - It returns different port numbers for different listeners.
func pickPort(ctx context.Context, s store.Store, key string, listener string) (int, error) {
	const defaultPortStartNumber = 10000
	index, err := store.Sequence(ctx, s, key, listener)
	return defaultPortStartNumber + index, err
}
