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
	"time"

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
	updateRoutingInterval time.Duration,
	getHealthyPods func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error),
	getListenerPort func(context.Context, *config.GKEConfig, string, string) (int, error),
	exportListener func(context.Context, *config.GKEConfig, string, *nanny.Listener) (*protos.ExportListenerReply, error),
	startReplicaSet func(context.Context, *config.GKEConfig, string) error,
	stopAppVersions func(context.Context, string, []*config.GKEConfig) error,
	deleteAppVersions func(context.Context, string, []*config.GKEConfig) error) endpoints.Manager {
	return &manager{
		ctx:               ctx,
		store:             store,
		assigner:          assigner.NewAssigner(ctx, store, logger, assigner.EqualDistributionAlgorithm, updateRoutingInterval, getHealthyPods),
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
	m.logger.Debug("Deploy", "versions", versionStrs, "app", req.AppName)
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
	return nil
}

func (m *manager) deploy(ctx context.Context, cfg *config.GKEConfig) error {
	// Update the manager address, so the babysitters in this deployment can
	// reach the local manager.
	cfg.ManagerAddr = m.selfAddr

	// Find the replica set that hosts the main component.
	rs := config.ReplicaSetForComponent(runtime.Main, cfg)

	// Activate the main component.
	return m.ActivateComponent(ctx, &nanny.ActivateComponentRequest{
		Component:  runtime.Main,
		ReplicaSet: rs,
		Routed:     false,
		Config:     cfg,
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
	m.logger.Debug("Stop", "versions", req.Versions, "app", req.AppName)
	if err := m.stop(req); err != nil {
		m.logger.Error("Stop", "versions", req.Versions, "app", req.AppName, "error", err)
		return err
	}
	return nil
}

func (m *manager) stop(req *nanny.ApplicationStopRequest) error {
	if err := m.stopAppVersions(m.ctx, req.AppName, req.Versions); err != nil {
		return err
	}
	var errs []error
	for _, version := range req.Versions {
		if err := m.assigner.StopAppVersion(m.ctx, req.AppName, version.Deployment.Id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Delete deletes all replica sets associated with a given list of application
// versions.
func (m *manager) Delete(_ context.Context, req *nanny.ApplicationDeleteRequest) error {
	versionStrs := make([]string, len(req.Versions))
	for i, cfg := range req.Versions {
		versionStrs[i] = cfg.Deployment.Id
	}
	m.logger.Info("Delete", "versions", versionStrs, "app", req.AppName)
	if err := m.deleteAppVersions(m.ctx, req.AppName, req.Versions); err != nil {
		m.logger.Error("Delete", "versions", versionStrs, "app", req.AppName, "error", err)
		return err
	}
	return nil
}

// GetReplicaSets returns ReplicaSet information for an application version
// or a set of application versions.
func (m *manager) GetReplicaSets(ctx context.Context, req *nanny.GetReplicaSetsRequest) (*nanny.GetReplicaSetsReply, error) {
	m.logger.Debug("GetReplicaSets", "app", req.AppName, "version", req.VersionId)
	reply, err := m.assigner.GetReplicaSets(ctx, req)
	if err != nil {
		m.logger.Error("GetReplicaSets", "app", req.AppName, "version", req.VersionId, "error", err)
		return nil, err
	}
	return reply, nil
}

// ActivateComponent ensures that the given component is hosted by a running
// Kubernetes ReplicaSet.
func (m *manager) ActivateComponent(ctx context.Context, req *nanny.ActivateComponentRequest) error {
	m.logger.Debug("ActivateComponent", "component", req.Component, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id)
	// Register the component with the assigner, which will generate
	// routing information for the component.
	if err := m.assigner.RegisterComponent(ctx, req); err != nil {
		m.logger.Error("ActivateComponent", "component", req.Component, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return err
	}

	// Ensure that the Kubernetes ReplicaSet, which hosts the component, is
	// started.
	if err := m.startReplicaSet(m.ctx, req.Config, req.ReplicaSet); err != nil {
		m.logger.Error("ActivateComponent", "component", req.Component, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return err
	}
	return nil
}

func (m *manager) GetListenerAddress(ctx context.Context, req *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	m.logger.Debug("GetListenerAddress", "listener", req.Listener, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id)
	port, err := m.getListenerPort(ctx, req.Config, req.ReplicaSet, req.Listener)
	if err != nil {
		m.logger.Error("GetListenerAddress", "listener", req.Listener, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return nil, err
	}
	return &protos.GetListenerAddressReply{Address: fmt.Sprintf(":%d", port)}, nil
}

func (m *manager) ExportListener(ctx context.Context, req *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	m.logger.Debug("ExportListener", "listener", req.Listener, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id)
	if err := m.assigner.RegisterListener(ctx, req); err != nil {
		m.logger.Error("ExportListener", "listener", req.Listener, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return nil, err
	}
	return m.exportListener(ctx, req.Config, req.ReplicaSet, req.Listener)
}

func (m *manager) GetRoutingInfo(_ context.Context, req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	m.logger.Debug("GetRoutingInfo", "component", req.Component, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id)
	reply, err := m.assigner.GetRoutingInfo(req)
	if err != nil {
		m.logger.Error("GetRoutingInfo", "component", req.Component, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return nil, err
	}
	return reply, nil
}

func (m *manager) GetComponentsToStart(_ context.Context, req *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	m.logger.Debug("GetComponentsToStart", "replica_set", req.ReplicaSet, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id)
	reply, err := m.assigner.GetComponentsToStart(req)
	if err != nil {
		m.logger.Error("GetComponentsToStart", "replica_set", req.ReplicaSet, "app", req.Config.Deployment.App.Name, "version", req.Config.Deployment.Id, "error", err)
		return nil, err
	}
	return reply, nil
}
