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
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/local/proxy"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
)

// URL on the controller where the metrics are exported in the Prometheus format.
const PrometheusURL = "/metrics"

// Controller returns the address the tool should use to connect to the
// controller, as well as the HTTP client that can be used to reach it.
func Controller(context.Context) (string, *http.Client, error) {
	caCert, caKey, err := ensureCACert()
	if err != nil {
		return "", nil, err
	}
	selfCertPEM, selfKeyPEM, err := ensureToolCert(caCert, caKey)
	if err != nil {
		return "", nil, err
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return selfCertPEM, selfKeyPEM, nil
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, "controller"),
		},
	}
	addr := fmt.Sprintf("https://localhost:%d", controllerPort)
	return addr, client, nil
}

func makeNannyLogger(id, service string) (*slog.Logger, func() error, error) {
	ls, err := logging.NewFileStore(LogDir)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create log storage: %w", err)
	}
	return slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        "nanny",
			Deployment: id,
			Component:  service,
			Weavelet:   id,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: ls.Add,
	}), ls.Close, nil
}

func runNannyServer(ctx context.Context, server *http.Server, lis net.Listener) error {
	errs := make(chan error, 1)
	go func() {
		errs <- server.ServeTLS(lis, "", "")
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return server.Shutdown(ctx)
	}
}

// RunController creates and runs a controller.
func RunController(ctx context.Context, id string, region string, port int) error {
	logger, close, err := makeNannyLogger(id, "controller")
	if err != nil {
		return err
	}
	defer close()

	// Try to create a listener: if a controller is already running, we will
	// fail to bind to the nanny address.
	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("controller listen", "err", err, "address", addr)
		return fmt.Errorf("controller cannot listen on %q: %w", addr, err)
	}

	// Generate a controller certificate.
	caCert, selfCertPEM, selfKeyPEM, err := generateNannyCert("controller")
	if err != nil {
		return fmt.Errorf("cannot generate controller cert: %w", err)
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return selfCertPEM, selfKeyPEM, nil
	}

	s, err := Store(region)
	if err != nil {
		return fmt.Errorf("cannot get the store: %w", err)
	}
	s = store.WithMetrics("controller", id, s)
	proxyAddr := fmt.Sprintf("http://localhost:%d", proxyPort)
	mux := http.NewServeMux()
	if _, err := controller.Start(ctx,
		mux,
		s,
		logger,
		10*time.Second, // actuationDelay
		func(addr string) endpoints.Distributor {
			return &distributor.HttpClient{
				Addr:      addr,
				TLSConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, "distributor"),
			}
		},
		10*time.Second, // fetchAssignmentsInterval
		10*time.Second, // applyAssignmentInterval
		5*time.Second,  // manageAppInterval,
		func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
			return ApplyTraffic(ctx, proxyAddr, assignment, region)
		},
	); err != nil {
		return fmt.Errorf("cannot start controller: %w", err)
	}

	// Add the Prometheus metric handler.
	metricDB, err := metricdb.Open(ctx, MetricsFile)
	if err != nil {
		return err
	}
	mux.Handle("/metrics", NewPrometheusHandler(metricDB, logger))

	logger.Info("Controller listening", "address", lis.Addr())
	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(projectName, caCert, getSelfCert, "tool"),
	}
	return runNannyServer(ctx, server, lis)
}

// RunDistributor creates and runs a distributor.
func RunDistributor(ctx context.Context, id, region string, port, managerPort int) error {
	name := "distributor-" + region
	logger, close, err := makeNannyLogger(id, name)
	if err != nil {
		return err
	}
	defer close()

	// Try to create a listener: if a distributor is already running, we will
	// fail to bind to the port, and that's okay.
	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		logger.Error("distributor listen", "err", err, "address", addr)
		return fmt.Errorf("distributor cannot listen on %q: %w", addr, err)
	}

	// Generate a distributor certificate.
	caCert, selfCertPEM, selfKeyPEM, err := generateNannyCert("distributor")
	if err != nil {
		return fmt.Errorf("cannot generate distributor cert: %w", err)
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return selfCertPEM, selfKeyPEM, nil
	}

	metricDB, err := metricdb.Open(ctx, MetricsFile)
	if err != nil {
		return err
	}
	s, err := Store(region)
	if err != nil {
		return fmt.Errorf("cannot get the store: %w", err)
	}
	s = store.WithMetrics(name, id, s)
	mux := http.NewServeMux()
	managerAddr := fmt.Sprintf("https://localhost:%d", managerPort)
	if _, err := distributor.Start(ctx,
		mux,
		s,
		logger,
		&manager.HttpClient{
			Addr:      managerAddr,
			TLSConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, "manager"),
		},
		region,
		func(cfg *config.GKEConfig, replicaSet, addr string) (endpoints.Babysitter, error) {
			replicaSetIdentity, ok := cfg.ComponentIdentity[replicaSet]
			if !ok { // should never happen
				return nil, fmt.Errorf("unknown identity for replica set %q", replicaSet)
			}
			return &babysitter.HttpClient{
				Addr:      addr,
				TLSConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, replicaSetIdentity),
			}, nil
		},
		10*time.Second, // manageAppsInterval
		10*time.Second, // computeTrafficInterval
		10*time.Second, // applyTrafficInterval
		10*time.Second, // detectAppliedTrafficInterval
		func(ctx context.Context, assignment *nanny.TrafficAssignment) error {
			return ApplyTraffic(ctx, fmt.Sprintf("http://localhost:%d", proxyPort), assignment, region)
		},
		func(ctx context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
			return GetListeners(ctx, s, cfg)
		},
		func(ctx context.Context, metric string, labels ...string) ([]distributor.MetricCount, error) {
			return getMetricCounts(ctx, metricDB, time.Now().Add(-2*time.Minute), time.Now().Add(-time.Minute), time.Now(), metric, labels...)
		},
	); err != nil {
		return fmt.Errorf("cannot start distributor: %w", err)
	}

	logger.Info("Distributor listening", "address", lis.Addr())
	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(projectName, caCert, getSelfCert, "controller"),
	}
	return runNannyServer(ctx, server, lis)
}

// RunManager creates and runs a manager.
func RunManager(ctx context.Context, id, region string, port, proxyPort int) error {
	name := "manager-" + region
	logger, close, err := makeNannyLogger(id, name)
	if err != nil {
		return err
	}
	defer close()

	logger.Info("Starting manager")

	// Try to create a listener: if a manager is already running, we will
	// fail to bind to the port, and that's okay.
	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("manager listen", "err", err, "address", addr)
		return fmt.Errorf("manager cannot listen on %q: %w", addr, err)
	}

	// Generate the manager certificate.
	caCert, selfCertPEM, selfKeyPEM, err := generateNannyCert("manager")
	if err != nil {
		return fmt.Errorf("cannot generate controller cert: %w", err)
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return selfCertPEM, selfKeyPEM, nil
	}

	s, err := Store(region)
	if err != nil {
		return fmt.Errorf("cannot get the store: %w", err)
	}
	s = store.WithMetrics(name, id, s)
	starter, err := NewStarter(logger)
	if err != nil {
		return fmt.Errorf("cannot create starter: %w", err)
	}
	proxyAddr := fmt.Sprintf("http://localhost:%d", proxyPort)
	m := manager.NewManager(ctx,
		s,
		logger,
		fmt.Sprintf("https://%s", lis.Addr()),
		2*time.Second, /*updateRoutingInterval*/
		// getHealthyPods
		func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error) {
			replicaSetIdentity, ok := cfg.ComponentIdentity[replicaSet]
			if !ok { // should never happen
				return nil, fmt.Errorf("unknown identity for replica set %q", replicaSet)
			}
			newBabysitter := func(addr string) endpoints.Babysitter {
				return &babysitter.HttpClient{
					Addr:      addr,
					TLSConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, replicaSetIdentity),
				}
			}
			return starter.getHealthyPods(ctx, cfg, replicaSet, newBabysitter)
		},
		// getListenerPort
		func(context.Context, *config.GKEConfig, string, string) (int, error) {
			return 0, nil
		},
		// exportListener
		func(ctx context.Context, cfg *config.GKEConfig, _ string, lis *nanny.Listener) (*protos.ExportListenerReply, error) {
			if err := RecordListener(ctx, s, cfg, lis); err != nil {
				return &protos.ExportListenerReply{}, err
			}
			return &protos.ExportListenerReply{ProxyAddress: proxyAddr}, nil
		},
		// startReplicaSet
		func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) error {
			return starter.Start(ctx, cfg, replicaSet)
		},
		// stopAppVersions
		func(_ context.Context, _ string, appVersions []*config.GKEConfig) error {
			return starter.Stop(ctx, appVersions)
		},
		// deleteAppVersions
		func(context.Context, string, []*config.GKEConfig) error {
			// We don't delete state for the older app versions.
			return nil
		},
	)

	logger.Info("Manager listening", "address", lis.Addr())
	verifyPeerCert := func(peer []*x509.Certificate) (string, error) {
		return mtls.VerifyCertificateChain(projectName, caCert, peer)
	}
	return manager.RunHTTPServer(m, logger, lis, getSelfCert, verifyPeerCert)
}

// RunProxy creates and runs a proxy listening on ":port".
func RunProxy(ctx context.Context, id string, port int) error {
	ls, err := logging.NewFileStore(LogDir)
	if err != nil {
		return fmt.Errorf("cannot create log storage: %w", err)
	}
	defer ls.Close()
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        "nanny",
			Deployment: id,
			Component:  "proxy",
			Weavelet:   id,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: ls.Add,
	})

	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	proxy := proxy.NewProxy(logger)
	logger.Debug("Proxy listenening", "address", lis.Addr())
	return proxy.Serve(ctx, lis, fmt.Sprintf("localhost:%d", port))
}
