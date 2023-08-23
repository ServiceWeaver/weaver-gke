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

// Package manager handles the management of new application versions in a
// particular deployment environment (e.g., starting colocation groups,
// register replicas, get components to start, etc.).

package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/assigner"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// manager manages applications' deployments (e.g., starting Service Weaver groups)
type manager struct {
	ctx      context.Context
	store    store.Store
	assigner *assigner.Assigner
	logger   *slog.Logger

	// Address on which the manager may be reached from a Service Weaver group.
	selfAddr string

	// Returns the port the network listener should listen on inside the
	// Kubernetes ReplicaSet.
	getListenerPort func(context.Context, *config.GKEConfig, string, string) (int, error)

	// Export the network listener that runs inside the given Kubernetes
	// ReplicaSet.
	exportListener func(context.Context, *config.GKEConfig, string, *nanny.Listener) (*protos.ExportListenerReply, error)

	// Start running the Kubernetes ReplicaSet.
	startReplicaSet func(context.Context, *config.GKEConfig, string) error

	// Stops all the groups for a list of applications versions.
	stopAppVersions func(ctx context.Context, app string, versions []*config.GKEConfig) error

	// Delete all the groups for a list of applications versions.
	deleteAppVersions func(ctx context.Context, app string, versions []*config.GKEConfig) error
}

var _ endpoints.Manager = &manager{}

// NewManager returns a new manager instance.
func NewManager(ctx context.Context,
	store store.Store,
	logger *slog.Logger,
	dialAddr string,
	babysitterConstructor func(cfg *config.GKEConfig, replicaSet, addr string) (endpoints.Babysitter, error),
	replicaExists func(context.Context, string) (bool, error),
	getListenerPort func(context.Context, *config.GKEConfig, string, string) (int, error),
	exportListener func(context.Context, *config.GKEConfig, string, *nanny.Listener) (*protos.ExportListenerReply, error),
	startReplicaSet func(context.Context, *config.GKEConfig, string) error,
	stopAppVersions func(context.Context, string, []*config.GKEConfig) error,
	deleteAppVersions func(context.Context, string, []*config.GKEConfig) error) endpoints.Manager {
	return &manager{
		ctx:               ctx,
		store:             store,
		assigner:          assigner.NewAssigner(ctx, store, logger, assigner.EqualDistributionAlgorithm, babysitterConstructor, replicaExists),
		logger:            logger,
		selfAddr:          dialAddr,
		getListenerPort:   getListenerPort,
		exportListener:    exportListener,
		startReplicaSet:   startReplicaSet,
		stopAppVersions:   stopAppVersions,
		deleteAppVersions: deleteAppVersions,
	}
}

// Deploy deploys a new version of the given application.
func (m *manager) Deploy(ctx context.Context, req *nanny.ApplicationDeploymentRequest) error {
	versionStrs := make([]string, len(req.Versions))
	for i, cfg := range req.Versions {
		versionStrs[i] = cfg.Deployment.Id
	}
	m.logger.Info("Starting", "versions", versionStrs, "app", req.AppName)
	var errs []error
	for _, cfg := range req.Versions {
		if err := m.deploy(ctx, cfg); err != nil {
			errs = append(errs, err)
			continue
		}
	}
	if err := errors.Join(errs...); err != nil {
		m.logger.Error("Error starting", "err", err, "versions", versionStrs, "app", req.AppName)
		return err
	}
	m.logger.Info("Success starting", "versions", versionStrs, "app", req.AppName, "err", errs)
	return nil
}

func (m *manager) deploy(ctx context.Context, cfg *config.GKEConfig) error {
	// Update the manager address, so the babysitters in this deployment can
	// reach the local manager.
	cfg.ManagerAddr = m.selfAddr

	// Activate the main component.
	return m.ActivateComponent(ctx, &nanny.ActivateComponentRequest{
		Component: runtime.Main,
		Routed:    false,
		Config:    cfg,
	})
}

// Stop stops all of the Kubernetes ReplicaSets associated with a list of
// applications versions.
//
// Note that it is the responsibility of the stopAppVersions() method to stop
// all of the ReplicaSets. stopAppVersions() might be blocking and may not
// finish before the request from the distributor times out. However, the
// distributor will keep sending the list of versions to delete as long as it
// didn't get a successful reply. Eventually, the stopAppVersions() will return
// immediately once all versions are deleted, in which case the reply to the
// distributor will be successful.
func (m *manager) Stop(_ context.Context, req *nanny.ApplicationStopRequest) error {
	m.logger.Info("Stopping versions", "versions", req.Versions, "app", req.AppName)
	if err := m.stop(req); err != nil {
		return fmt.Errorf("cannot stop versions %v of application %q: %w", req.Versions, req.AppName, err)
	}
	m.logger.Info("Successfully stopped", "versions", req.Versions, "app", req.AppName)
	return nil
}

func (m *manager) stop(req *nanny.ApplicationStopRequest) error {
	if err := m.stopAppVersions(m.ctx, req.AppName, req.Versions); err != nil {
		return err
	}
	if err := m.assigner.UnregisterReplicaSets(m.ctx, req.AppName, req.Versions); err != nil {
		return err
	}

	// Garbage collect store entries.
	//
	// TODO(mwhittaker): This garbage collection races any operation that
	// writes to the store. We should make sure that every operation that could
	// potentially write to the store first checks to see if the version has
	// been stopped. This is complicated by the fact that the store writes are
	// spread between the assigner, manager, the gke deployer, and the
	// gke-local deployer.
	var errs []error
	for _, version := range req.Versions {
		if err := m.gcVersion(m.ctx, version); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// gcVersion garbage collects all the store entries for the provided version.
func (m *manager) gcVersion(_ context.Context, cfg *config.GKEConfig) error {
	// Get all keys recorded under histKey in the store.
	histKey := store.DeploymentKey(cfg, store.HistoryKey)
	keys, _, err := store.GetSet(m.ctx, m.store, histKey, nil)
	if err != nil {
		return fmt.Errorf("cannot get history for %q: %v", cfg.Deployment.Id, err)
	}

	for _, key := range keys {
		if err := m.store.Delete(m.ctx, key); err != nil {
			return fmt.Errorf("cannot delete key %q for %q: %v", key, cfg.Deployment.Id, err)
		}
	}

	// Don't forget to delete the history as well. We make sure to delete
	// the history last so that if any previous delete fails, the history
	// remains.
	if err := m.store.Delete(m.ctx, histKey); err != nil {
		return fmt.Errorf("cannot delete key %q for %q: %v", histKey, cfg.Deployment.Id, err)
	}
	return nil
}

// Delete deletes all the groups associated with a list of applications
// versions.
func (m *manager) Delete(_ context.Context, req *nanny.ApplicationDeleteRequest) error {
	if err := m.deleteAppVersions(m.ctx, req.AppName, req.Versions); err != nil {
		return fmt.Errorf("cannot delete versions %v: %w", req.Versions, err)
	}
	return nil
}

// GetReplicaSetState returns ReplicaSet information for an application version
// or a set of application versions.
func (m *manager) GetReplicaSetState(ctx context.Context, req *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error) {
	reply, err := m.assigner.GetReplicaSetState(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cannot get group state: %w", err)
	}
	return reply, nil
}

// ActivateComponent ensures that the given component is hosted by a running
// Kubernetes ReplicaSet.
func (m *manager) ActivateComponent(ctx context.Context, req *nanny.ActivateComponentRequest) error {
	// Register the component with the assigner, which will generate
	// routing information for the component.
	if err := m.assigner.RegisterActiveComponent(ctx, req); err != nil {
		return fmt.Errorf("cannot register component to start for deployment version %q of application %q: %w",
			req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}

	// Ensure that the Kubernetes ReplicaSet, which hosts the component, is
	// started.
	rs := nanny.ReplicaSetForComponent(req.Component, req.Config)
	if err := m.startReplicaSet(m.ctx, req.Config, rs); err != nil {
		return fmt.Errorf("cannot start ReplicaSet %q in version %q: %w", rs, req.Config.Deployment.Id, err)
	}
	return nil
}

func (m *manager) RegisterReplica(ctx context.Context, req *nanny.RegisterReplicaRequest) error {
	if err := m.assigner.RegisterReplica(ctx, req); err != nil {
		return fmt.Errorf(
			"cannot register pod %q in version %v of application %q: %w",
			req.PodName, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return nil
}

func (m *manager) ReportLoad(ctx context.Context, req *nanny.LoadReport) error {
	if err := m.assigner.OnNewLoadReport(ctx, req); err != nil {
		return fmt.Errorf("cannot handle load report of pod %q in version %v of application %q: %w", req.PodName, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return nil
}

func (m *manager) GetListenerAddress(ctx context.Context, req *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	port, err := m.getListenerPort(ctx, req.Config, req.ReplicaSet, req.Listener)
	if err != nil {
		return nil, fmt.Errorf("cannot get address for ReplicaSset %s listener %s in version %v of application %q: %w", req.ReplicaSet, req.Listener, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return &protos.GetListenerAddressReply{Address: fmt.Sprintf(":%d", port)}, nil
}

func (m *manager) ExportListener(ctx context.Context, req *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	lis := req.Listener
	if err := m.assigner.RegisterListener(ctx, req); err != nil {
		return nil, fmt.Errorf("cannot register listener %q in version %v of application %q: %w", lis.Name, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return m.exportListener(ctx, req.Config, req.ReplicaSet, req.Listener)
}

func (m *manager) GetRoutingInfo(_ context.Context, req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	reply, err := m.assigner.GetRoutingInfo(req)
	if err != nil {
		return nil, fmt.Errorf("cannot get routing info for component %q in version %v of application %q: %v",
			req.Component, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return reply, nil
}

func (m *manager) GetComponentsToStart(_ context.Context, req *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	reply, err := m.assigner.GetComponentsToStart(req)
	if err != nil {
		return nil, fmt.Errorf("cannot get components to start for deployment version %q of application %q: %w",
			req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return reply, nil
}
