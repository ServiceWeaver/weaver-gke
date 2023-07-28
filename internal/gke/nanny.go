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
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
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

func getNannyContainerMetadata() (*ContainerMetadata, error) {
	for _, v := range []string{
		containerMetadataEnvKey,
		nodeNameEnvKey,
		podNameEnvKey,
	} {
		if _, ok := os.LookupEnv(v); !ok {
			return nil, fmt.Errorf("environment variable %q not set\n", v)
		}
	}
	meta := &ContainerMetadata{}
	metaStr := os.Getenv(containerMetadataEnvKey)
	if err := proto.FromEnv(metaStr, meta); err != nil {
		return nil, err
	}
	meta.NodeName = os.Getenv(nodeNameEnvKey)
	meta.PodName = os.Getenv(podNameEnvKey)
	return meta, nil
}

func getNannyLogger(ctx context.Context, meta *ContainerMetadata, id, service string) (*slog.Logger, func() error, error) {
	lc, err := newCloudLoggingClient(ctx, meta)
	if err != nil {
		return nil, nil, err
	}
	return lc.Logger(logging.Options{
		App:       "nanny",
		Component: service,
		Weavelet:  id,
		Attrs:     []string{"serviceweaver/system", ""},
	}), lc.Close, nil
}

func startNannyMetricExporter(ctx context.Context, logger *slog.Logger, meta *ContainerMetadata) (context.CancelFunc, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	exporter, err := newMetricExporter(ctx, meta)
	if err != nil {
		cancel()
		return nil, err
	}
	go func() {
		const metricExportInterval = 15 * time.Second
		ticker := time.NewTicker(metricExportInterval)
		for {
			select {
			case <-ticker.C:
				snaps := metrics.Snapshot()
				if err := exporter.Export(ctx, snaps); err != nil {
					logger.Error("exporting nanny metrics", "err", err)
				}

			case <-ctx.Done():
				logger.Debug("exportMetrics cancelled")
				return
			}
		}
	}()
	return cancel, nil
}

func runNannyServer(ctx context.Context, server *http.Server, lis net.Listener) error {
	errs := make(chan error, 1)
	go func() {
		if server.TLSConfig != nil {
			errs <- server.ServeTLS(lis, "", "")
		} else {
			errs <- server.Serve(lis)
		}
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return server.Shutdown(ctx)
	}
}

// Controller returns the HTTP address of the controller and an HTTP client
// that can be used to contact the controller.
func Controller(ctx context.Context, config CloudConfig) (string, *http.Client, error) {
	// Generate a new CA-signed certificate for the tool.
	caCert, toolCert, toolKey, err := createToolCertificate(ctx, config)
	if err != nil {
		return "", nil, fmt.Errorf("cannot create tool certificate: %w", err)
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return toolCert, toolKey, nil
	}

	ipAddr, err := ensureControllerIPAddress(ctx, config)
	if err != nil {
		return "", nil, err
	}
	addr := fmt.Sprintf("https://%s:%d", ipAddr, nannyServingPort)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: mtls.ClientTLSConfig(config.Project, caCert, getSelfCert, "controller"),
		},
	}
	return addr, client, nil
}

// RunController creates and runs a controller.
func RunController(ctx context.Context) error {
	id := uuid.New().String()
	cluster, err := inClusterInfo(ctx)
	if err != nil {
		return err
	}
	s := store.WithMetrics("controller", id, Store(cluster))
	meta, err := getNannyContainerMetadata()
	if err != nil {
		return err
	}
	logger, close, err := getNannyLogger(ctx, meta, id, "controller")
	if err != nil {
		return err
	}
	defer close()

	cancel, err := startNannyMetricExporter(ctx, logger, meta)
	if err != nil {
		return err
	}
	defer cancel()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nannyServingPort))
	if err != nil {
		return err
	}
	caCert, getSelfCert, err := getPodCerts()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	if _, err := controller.Start(ctx,
		mux,
		s,
		logger,
		10*time.Minute, // actuationDelay
		func(addr string) endpoints.Distributor {
			return &distributor.HttpClient{
				Addr:      addr,
				TLSConfig: mtls.ClientTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, "distributor"),
			}
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

	logger.Info("Controller listening", "address", lis.Addr())
	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, "tool"),
	}
	return runNannyServer(ctx, server, lis)
}

// RunDistributor creates and runs a distributor.
func RunDistributor(ctx context.Context) error {
	id := uuid.New().String()
	cluster, err := inClusterInfo(ctx)
	if err != nil {
		return err
	}
	name := "distributor-" + cluster.Region
	s := store.WithMetrics(name, id, Store(cluster))
	meta, err := getNannyContainerMetadata()
	if err != nil {
		return err
	}
	logger, close, err := getNannyLogger(ctx, meta, id, name)
	if err != nil {
		return err
	}
	defer close()

	cancel, err := startNannyMetricExporter(ctx, logger, meta)
	if err != nil {
		return err
	}
	defer cancel()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nannyServingPort))
	if err != nil {
		return err
	}
	caCert, getSelfCert, err := getPodCerts()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	managerAddr := fmt.Sprintf("https://manager.%s.svc.%s-%s:%d", namespaceName, applicationClusterName, cluster.Region, nannyServingPort)
	if _, err := distributor.Start(ctx,
		mux,
		s,
		logger,
		&manager.HttpClient{
			Addr:      managerAddr,
			TLSConfig: mtls.ClientTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, "manager"),
		},
		cluster.Region,
		func(cfg *config.GKEConfig, replicaSet, addr string) (endpoints.Babysitter, error) {
			replicaSetIdentity, ok := cfg.ComponentIdentity[replicaSet]
			if !ok { // should never happen
				return nil, fmt.Errorf("unknown identity for replica set %q", replicaSet)
			}
			return &babysitter.HttpClient{
				Addr:      addr,
				TLSConfig: mtls.ClientTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, replicaSetIdentity),
			}, nil
		},
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

	logger.Info("Distributor listening", "address", lis.Addr())
	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, "controller"),
	}
	return runNannyServer(ctx, server, lis)
}

// RunManager creates and runs a manager.
func RunManager(ctx context.Context) error {
	id := uuid.New().String()
	cluster, err := inClusterInfo(ctx)
	if err != nil {
		return err
	}
	name := "manager-" + cluster.Region
	s := store.WithMetrics(name, id, Store(cluster))
	meta, err := getNannyContainerMetadata()
	if err != nil {
		return err
	}
	logger, close, err := getNannyLogger(ctx, meta, id, name)
	if err != nil {
		return err
	}
	defer close()

	cancel, err := startNannyMetricExporter(ctx, logger, meta)
	if err != nil {
		return err
	}
	defer cancel()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nannyServingPort))
	if err != nil {
		return err
	}
	caCert, getSelfCert, err := getPodCerts()
	if err != nil {
		return err
	}

	s = store.WithMetrics("manager", id, s)
	m := manager.NewManager(ctx,
		s,
		logger,
		fmt.Sprintf("https://manager.%s.svc.%s-%s:%d", namespaceName, applicationClusterName, cluster.Region, nannyServingPort),
		func(cfg *config.GKEConfig, replicaSet, addr string) (endpoints.Babysitter, error) {
			replicaSetIdentity, ok := cfg.ComponentIdentity[replicaSet]
			if !ok { // should never happen
				return nil, fmt.Errorf("unknown identity for replica set %q", replicaSet)
			}
			return &babysitter.HttpClient{
				Addr:      addr,
				TLSConfig: mtls.ClientTLSConfig(cluster.CloudConfig.Project, caCert, getSelfCert, replicaSetIdentity),
			}, nil
		},
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
		func(_ context.Context, app string, versions []*config.GKEConfig) error {
			for _, version := range versions {
				id := version.Deployment.Id
				if err := stop(ctx, cluster, logger, app, id); err != nil {
					return fmt.Errorf("stop %q: %w", version, err)
				}
			}
			return nil
		},
		func(_ context.Context, app string, versions []*config.GKEConfig) error {
			for _, version := range versions {
				id := version.Deployment.Id
				if err := kill(ctx, cluster, logger, app, id); err != nil {
					return fmt.Errorf("kill %q: %w", version, err)
				}
			}
			return nil
		},
	)

	logger.Info("Manager listening", "address", lis.Addr())
	verifyPeerCert := func(peer []*x509.Certificate) (string, error) {
		return mtls.VerifyCertificateChain(cluster.CloudConfig.Project, caCert, peer)
	}
	return manager.RunHTTPServer(m, logger, lis, getSelfCert, verifyPeerCert)
}

// getListenerPort returns the port the network listener should listen on
// inside the Kubernetes ReplicaSet, and creates a service to forward traffic to
// that
// port.
func getListenerPort(ctx context.Context, s store.Store, logger *slog.Logger, cluster *ClusterInfo, cfg *config.GKEConfig, replicaSet string, listener string) (int, error) {
	dep := cfg.Deployment
	key := store.AppKey(dep.App.Name, "ports")
	histKey := store.DeploymentKey(cfg, store.HistoryKey)
	err := store.AddToSet(ctx, s, histKey, key)
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
