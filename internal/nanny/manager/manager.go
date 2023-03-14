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
	"fmt"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/errlist"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/assigner"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/uuid"
)

const (
	// URL suffixes for various HTTP endpoints exported by the manager.
	deployURL               = "/manager/deploy"
	stopURL                 = "/manager/stop"
	deleteURL               = "/manager/delete"
	getGroupStateURL        = "/manager/get_group_state"
	startComponentURL       = "/manager/start_component"
	startColocationGroupURL = "/manager/start_colocation_group"
	registerReplicaURL      = "/manager/register_replica"
	reportLoadURL           = "/manager/report_load"
	getListenerAddressURL   = "/manager/get_listener_address"
	exportListenerURL       = "/manager/export_listener"
	getRoutingInfoURL       = "/manager/get_routing_info"
	getComponentsToStartURL = "/manager/get_components_to_start"
)

// manager manages applications' deployments (e.g., starting Service Weaver groups)
type manager struct {
	ctx      context.Context
	store    store.Store
	assigner *assigner.Assigner
	logger   *logging.FuncLogger

	// Address on which the manager may be reached from a Service Weaver group.
	selfAddr string

	// Returns the port the network listener should listen on inside the
	// colocation group.
	getListenerPort func(context.Context, *config.GKEConfig, *protos.ColocationGroup, string) (int, error)

	// Record the network listener exported by an application version's colocation group.
	exportListener func(context.Context, *config.GKEConfig, *protos.ColocationGroup, *protos.Listener) (*protos.ExportListenerReply, error)

	// Start running an application version's colocation group.
	startColocationGroup func(context.Context, *config.GKEConfig, *protos.ColocationGroup) error

	// Stops all the groups for a list of applications versions.
	stopAppVersions func(ctx context.Context, app string, versions []string) error

	// Delete all the groups for a list of applications versions.
	deleteAppVersions func(ctx context.Context, app string, versions []string) error
}

var _ clients.ManagerClient = &manager{}

// Start starts the manager service and registers its handlers with the given request multiplexer.
func Start(ctx context.Context,
	mux *http.ServeMux,
	store store.Store,
	logger *logging.FuncLogger,
	dialAddr string,
	babysitterConstructor func(string) clients.BabysitterClient,
	replicaExists func(context.Context, string) (bool, error),
	getListenerPort func(context.Context, *config.GKEConfig, *protos.ColocationGroup, string) (int, error),
	exportListener func(context.Context, *config.GKEConfig, *protos.ColocationGroup, *protos.Listener) (*protos.ExportListenerReply, error),
	startColocationGroup func(context.Context, *config.GKEConfig, *protos.ColocationGroup) error,
	stopAppVersions func(context.Context, string, []string) error,
	deleteAppVersions func(context.Context, string, []string) error) error {
	m := &manager{
		ctx:                  ctx,
		store:                store,
		assigner:             assigner.NewAssigner(ctx, store, logger, assigner.EqualDistributionAlgorithm, babysitterConstructor, replicaExists),
		logger:               logger,
		selfAddr:             dialAddr,
		getListenerPort:      getListenerPort,
		exportListener:       exportListener,
		startColocationGroup: startColocationGroup,
		stopAppVersions:      stopAppVersions,
		deleteAppVersions:    deleteAppVersions,
	}
	m.addHandlers(mux) // keeps a ref on "manager"
	return nil
}

// addHandlers adds network handlers exported by this manager.
func (m *manager) addHandlers(mux *http.ServeMux) {
	mux.HandleFunc(deployURL, protomsg.HandlerDo(m.logger, m.Deploy))
	mux.HandleFunc(stopURL, protomsg.HandlerDo(m.logger, m.Stop))
	mux.HandleFunc(deleteURL, protomsg.HandlerDo(m.logger, m.Delete))
	mux.HandleFunc(getGroupStateURL, protomsg.HandlerFunc(m.logger, m.GetGroupState))
	mux.HandleFunc(startComponentURL, protomsg.HandlerDo(m.logger, m.StartComponent))
	mux.HandleFunc(startColocationGroupURL, protomsg.HandlerDo(m.logger, m.StartColocationGroup))
	mux.HandleFunc(registerReplicaURL, protomsg.HandlerDo(m.logger, m.RegisterReplica))
	mux.HandleFunc(reportLoadURL, protomsg.HandlerDo(m.logger, m.ReportLoad))
	mux.HandleFunc(getListenerAddressURL, protomsg.HandlerFunc(m.logger, m.GetListenerAddress))
	mux.HandleFunc(exportListenerURL, protomsg.HandlerFunc(m.logger, m.ExportListener))
	mux.HandleFunc(getRoutingInfoURL, protomsg.HandlerFunc(m.logger, m.GetRoutingInfo))
	mux.HandleFunc(getComponentsToStartURL, protomsg.HandlerFunc(m.logger, m.GetComponentsToStart))
}

func (m *manager) Deploy(ctx context.Context, req *nanny.ApplicationDeploymentRequest) error {
	versionStrs := make([]string, len(req.Versions))
	for i, cfg := range req.Versions {
		versionStrs[i] = cfg.Deployment.Id
	}
	m.logger.Info("Starting", "versions", versionStrs, "app", req.AppName)
	var errs errlist.ErrList
	for _, cfg := range req.Versions {
		if err := m.deploy(ctx, cfg); err != nil {
			errs = append(errs, err)
			continue
		}
	}
	if err := errs.ErrorOrNil(); err != nil {
		m.logger.Error("Error starting", err, "versions", versionStrs, "app", req.AppName)
		return err
	}
	m.logger.Info("Success starting", "versions", versionStrs, "app", req.AppName, "err", errs)
	return nil
}

func (m *manager) deploy(ctx context.Context, cfg *config.GKEConfig) error {
	// Update the manager address, so the groups in this deployment can
	// reach the local manager.
	cfg.ManagerAddr = m.selfAddr

	// Register the main group to be started.
	dep := cfg.Deployment
	if err := m.assigner.RegisterComponentToStart(ctx,
		&protos.ComponentToStart{
			App:             dep.App.Name,
			DeploymentId:    dep.Id,
			ColocationGroup: "main",
			Component:       "main",
			IsRouted:        false,
		}); err != nil {
		return err
	}

	// Start the main colocation group in this deployment.
	group := &protos.ColocationGroup{Name: "main"}
	if err := m.startColocationGroup(m.ctx, cfg, group); err != nil {
		return fmt.Errorf("cannot start %q: %v", dep.Id, err)
	}
	return nil
}

// Stop stops all the groups associated with a list of applications
// versions.
//
// Note that it is the responsibility of the stopAppVersions() method to stop all
// groups. stopAppVersions() might be blocking and may not finish before the
// request from the distributor timeouts. However, the distributor will keep
// sending the list of versions to delete as long as it didn't get a successful
// reply. Eventually, the stopAppVersions() will return immediately once all
// versions are deleted, in which case the reply to the distributor will be successful.
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
	if err := m.assigner.UnregisterGroups(m.ctx, req.AppName, req.Versions); err != nil {
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
	var errs errlist.ErrList
	for _, version := range req.Versions {
		if err := m.gcVersion(m.ctx, req.AppName, version); err != nil {
			errs = append(errs, err)
		}
	}
	return errs.ErrorOrNil()
}

// gcVersion garbage collects all the store entries for the provided version.
func (m *manager) gcVersion(_ context.Context, app, version string) error {
	id, err := uuid.Parse(version)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", version, err)
	}

	// Get all keys recorded under histKey in the store.
	histKey := store.DeploymentKey(app, id, store.HistoryKey)
	keys, _, err := store.GetSet(m.ctx, m.store, histKey, nil)
	if err != nil {
		return fmt.Errorf("cannot get history for %q: %v", version, err)
	}

	for _, key := range keys {
		if err := m.store.Delete(m.ctx, key); err != nil {
			return fmt.Errorf("cannot delete key %q for %q: %v", key, version, err)
		}
	}

	// Don't forget to delete the history as well. We make sure to delete
	// the history last so that if any previous delete fails, the history
	// remains.
	if err := m.store.Delete(m.ctx, histKey); err != nil {
		return fmt.Errorf("cannot delete key %q for %q: %v", histKey, version, err)
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

// GetGroupState returns group information for an application version
// or a set of application versions.
func (m *manager) GetGroupState(ctx context.Context, req *nanny.GroupStateRequest) (*nanny.GroupState, error) {
	reply, err := m.assigner.GetGroupState(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cannot get group state: %w", err)
	}
	return reply, nil
}

// StartComponent registers a component to start for a group. Every group
// has a key in the store that contains the set of components the group
// should be running.
func (m *manager) StartComponent(ctx context.Context, req *protos.ComponentToStart) error {
	if err := m.assigner.RegisterComponentToStart(ctx, req); err != nil {
		return fmt.Errorf("cannot register component to start for deployment version %q of application %q: %w",
			req.DeploymentId, req.App, err)
	}
	return nil
}

// StartColocationGroup starts a new colocation group for a given application
// version.
func (m *manager) StartColocationGroup(_ context.Context, req *nanny.ColocationGroupStartRequest) error {
	if err := m.startColocationGroup(m.ctx, req.Config, req.Group); err != nil {
		return fmt.Errorf("cannot start colocation group %q in version %q: %w", req.Group.Name, req.Config.Deployment.Id, err)
	}
	return nil
}

func (m *manager) RegisterReplica(ctx context.Context, req *nanny.ReplicaToRegister) error {
	if err := m.assigner.RegisterReplica(ctx, req); err != nil {
		return fmt.Errorf("cannot register replica of group %q in version %v of application %q: %w",
			req.Replica.Group, req.Replica.DeploymentId, req.Replica.App, err)
	}
	return nil
}

func (m *manager) ReportLoad(ctx context.Context, req *protos.WeaveletLoadReport) error {
	if err := m.assigner.OnNewLoadReport(ctx, req); err != nil {
		return fmt.Errorf("cannot handle load report of group %q in version %v of application %q: %w", req.Group, req.DeploymentId, req.App, err)
	}
	return nil
}

func (m *manager) GetListenerAddress(ctx context.Context, req *nanny.GetListenerAddressRequest) (*protos.GetAddressReply, error) {
	port, err := m.getListenerPort(ctx, req.Config, req.Group, req.Listener)
	if err != nil {
		return nil, fmt.Errorf("cannot get address for group %s listener %s in version %v of application %q: %w", req.Group.Name, req.Listener, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return &protos.GetAddressReply{Address: fmt.Sprintf(":%d", port)}, nil
}

func (m *manager) ExportListener(ctx context.Context, req *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	lis := req.Listener
	if err := m.assigner.RegisterListener(ctx, req.Config.Deployment, req.Group, req.Listener); err != nil {
		return nil, fmt.Errorf("cannot register listener %q in version %v of application %q: %w", lis.Name, req.Config.Deployment.Id, req.Config.Deployment.App.Name, err)
	}
	return m.exportListener(ctx, req.Config, req.Group, req.Listener)
}

func (m *manager) GetRoutingInfo(_ context.Context, req *protos.GetRoutingInfo) (*protos.RoutingInfo, error) {
	reply, err := m.assigner.GetRoutingInfo(req)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve the address of group %q in version %v of application %q: %v",
			req.Group, req.DeploymentId, req.App, err)
	}
	return reply, nil
}

func (m *manager) GetComponentsToStart(_ context.Context, req *protos.GetComponentsToStart) (*protos.ComponentsToStart, error) {
	reply, err := m.assigner.GetComponentsToStart(req)
	if err != nil {
		return nil, fmt.Errorf("cannot get components to start for deployment version %q of application %q: %w",
			req.DeploymentId, req.App, err)
	}
	return reply, nil
}
