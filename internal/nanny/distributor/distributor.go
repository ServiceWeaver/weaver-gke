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

// Package distributor handles the distribution of new application versions in a
// particular deployment location (e.g., a cloud region).
//
// In particular, this package handles the starting of application versions, as
// well as gradual traffic shifting across multiple versions of the same
// application in that location.
package distributor

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/profiling"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"golang.org/x/exp/slog"
)

// A distributor oversees all of the deployments in a single location (e.g., a
// single cluster in GKE). It communicates with the manager to start and stop
// application versions, and it computes how external traffic should be routed
// within its location.
//
// The distributor persists in the store the *desired* state of all
// applications running in its location. The /distributor_state key stores a
// list of all applications, and for each application a, the key
// /distributor/application/a stores all of the versions of the application.
//
//     /distributor_state            -> DistributorState{"todo", "chat"}
//     /distributor/application/todo -> AppState{...}
//     /distributor/application/chat -> AppState{...}
//
// Note that the store holds the *desired* state, but not necessarily the
// actual state. For example, an application version may be marked to start but
// not yet started. The distributor runs an annealing loop that tries to
// converge the actual state towards the desired state. It will, for example,
// repeatedly try to start any application versions that should be started but
// aren't.
//
// For performance reasons, it's ideal to have a single distributor running at
// a time, but this is not needed for correctness. Multiple distributors can
// safely run concurrently.

// TODO(mwhittaker): Triple check that there aren't any fault tolerance issues.
// Also write some tests to catch any subtle bugs.

// TODO(mwhittaker): Right now, applications are never *fully* deleted. Once
// an AppState is created for an app, we will never delete it. This is to
// prevent races between deleting and starting a version. We need to implement
// some sort of garbage collection to remove apps for good.

// TODO(mwhittaker): There are some spots in the code where we load an
// AppState, modify it, store it, then load it again, modify it, and store it.
// We can reuse the state rather than reading it twice.

const (
	// Internal domain name for applications' private listeners.
	InternalDNSDomain = "serviceweaver.internal"

	// URL suffixes for various HTTP endpoints exported by the distributor.
	distributeURL                  = "/distributor/distribute"
	cleanupURL                     = "/distributor/cleanup"
	getApplicationStateURL         = "/distributor/get_application_state"
	getPublicTrafficAssignmentURL  = "/distributor/get_public_traffic_assignment"
	getPrivateTrafficAssignmentURL = "/distributor/get_private_traffic_assignment"
	runProfilingURL                = "/distributor/run_profiling"
)

// MetricCount stores the count for a Counter metric, along with its
// label values.
type MetricCount struct {
	LabelVals []string
	Count     float64
}

// Distributor manages the distribution of external traffic across application
// versions inside a single deployment location (e.g., a cluster inside GKE).
type Distributor struct {
	// Dealing with contexts in the handlers is finicky. We have to make sure
	// timeouts are:
	//
	//   1. propagated back (i.e., by the app code) to the handlers as errors;
	//      and
	//   2. handled correctly at the clients that call the handlers (e.g.,
	//      retry + log an error).
	//
	// We also may want to look into increasing the context timeouts if they
	// are too short; otherwise, we'd be retrying over and over with no
	// success.
	//
	// See tg/1461148 for more discussion.
	ctx                   context.Context
	store                 store.Store
	logger                *slog.Logger
	manager               clients.ManagerClient
	region                string
	babysitterConstructor func(addr string) clients.BabysitterClient

	// The following intervals determine how often the distributor performs
	// various actions as part of its annealing loop. If an interval is 0, the
	// corresponding annealing behavior is disabled.
	manageAppsInterval           time.Duration // manage application states
	computeTrafficInterval       time.Duration // compute traffic assignment
	applyTrafficInterval         time.Duration // apply latest traffic assignment
	detectAppliedTrafficInterval time.Duration // detect applied traffic

	// Applies a traffic assignment locally.
	applyTraffic func(context.Context, *nanny.TrafficAssignment) error

	// Retrieves application version's network listeners.
	getListeners func(context.Context, *config.GKEConfig) ([]*nanny.Listener, error)

	// Aggregates the counts for the given counter metric, grouping them by the
	// given set of labels.
	getMetricCounts func(context.Context, string, ...string) ([]MetricCount, error)
}

var _ clients.DistributorClient = &Distributor{}

// Start starts the distributor that manages the distribution of external traffic
// across applications' versions inside a single deployment location.
func Start(ctx context.Context,
	mux *http.ServeMux,
	store store.Store,
	logger *slog.Logger,
	manager clients.ManagerClient,
	region string,
	babysitterConstructor func(addr string) clients.BabysitterClient,
	manageAppsInterval time.Duration, // disabled if 0
	computeTrafficInterval time.Duration, // disabled if 0
	applyTrafficInterval time.Duration, // disabled if 0
	detectAppliedTrafficInterval time.Duration, // disabled if 0
	applyTraffic func(context.Context, *nanny.TrafficAssignment) error,
	getListeners func(context.Context, *config.GKEConfig) ([]*nanny.Listener, error),
	getMetricCounts func(context.Context, string, ...string) ([]MetricCount, error),
) (*Distributor, error) {
	// Create and return the distributor.
	d := &Distributor{
		ctx:                          ctx,
		store:                        store,
		logger:                       logger,
		manager:                      manager,
		region:                       region,
		babysitterConstructor:        babysitterConstructor,
		manageAppsInterval:           manageAppsInterval,
		computeTrafficInterval:       computeTrafficInterval,
		applyTrafficInterval:         applyTrafficInterval,
		detectAppliedTrafficInterval: detectAppliedTrafficInterval,
		applyTraffic:                 applyTraffic,
		getListeners:                 getListeners,
		getMetricCounts:              getMetricCounts,
	}
	d.addHandlers(mux)
	go d.anneal(ctx)
	return d, nil
}

// addHandlers adds handlers for the HTTP endpoints exposed by the distributor.
func (d *Distributor) addHandlers(mux *http.ServeMux) {
	mux.HandleFunc(distributeURL, protomsg.HandlerDo(d.logger, d.handleDistribute))
	mux.HandleFunc(cleanupURL, protomsg.HandlerDo(d.logger, d.handleCleanup))
	mux.HandleFunc(getApplicationStateURL, protomsg.HandlerFunc(d.logger, d.GetApplicationState))
	mux.HandleFunc(getPublicTrafficAssignmentURL, protomsg.HandlerThunk(d.logger, d.GetPublicTrafficAssignment))
	mux.HandleFunc(getPrivateTrafficAssignmentURL, protomsg.HandlerThunk(d.logger, d.GetPrivateTrafficAssignment))
	mux.HandleFunc(runProfilingURL, protomsg.HandlerFunc(d.logger, d.handleRunProfiling))
}

// handleDistribute is the handler for the DistributeURL endpoint.
func (d *Distributor) handleDistribute(ctx context.Context, req *nanny.ApplicationDistributionRequest) error {
	versions := make([]string, len(req.Requests))
	for i, v := range req.Requests {
		versions[i] = v.Config.Deployment.Id
	}

	d.logger.Info("Registering for distribution", "versions", versions, "application", req.AppName)
	if err := d.Distribute(ctx, req); err != nil {
		d.logger.Error("Cannot registering for distribution", "err", err, "versions", versions, "application", req.AppName)
		return err
	}
	d.logger.Info("Successfully registered for distribution", "versions", versions, "application", req.AppName)
	return nil
}

// handleCleanup is the handler for the CleanupURL endpoint.
func (d *Distributor) handleCleanup(ctx context.Context, req *nanny.ApplicationCleanupRequest) error {
	d.logger.Info("Registering for cleanup", "versions", req.Versions, "application", req.AppName)
	if err := d.Cleanup(ctx, req); err != nil {
		d.logger.Error("Cannot register for cleanup", "err", err, "versions", req.Versions, "application", req.AppName)
		return err
	}
	d.logger.Info("Successfully registered for cleanup", "versions", req.Versions, "application", req.AppName)
	return nil
}

// handleProfile is the handler for the ProfileURL endpoint.
func (d *Distributor) handleRunProfiling(ctx context.Context, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	d.logger.Info("Profiling", "version", req.VersionId, "application", req.AppName)
	prof, err := d.RunProfiling(ctx, req)
	if prof == nil {
		d.logger.Error("Cannot profile", "err", err, "version", req.VersionId, "application", req.AppName)
		return nil, err
	}
	if len(prof.Data) == 0 {
		d.logger.Info("Empty profile", "version", req.VersionId, "application", req.AppName)
	} else if err != nil {
		d.logger.Error("Partial profile", "err", err, "version", req.VersionId, "application", req.AppName)
	} else {
		d.logger.Info("Successfully profiled", "version", req.VersionId, "application", req.AppName)
	}
	return prof, err
}

// Distribute registers the provided versions for distribution and also tries
// to Distribute them.
func (d *Distributor) Distribute(ctx context.Context, req *nanny.ApplicationDistributionRequest) error {
	if req.AppName == "" {
		return fmt.Errorf("empty app name")
	}

	now := time.Now()
	if err := d.registerVersions(ctx, req, now); err != nil {
		return err
	}

	// Try to launch the new version and update the traffic assignment
	// immediately. If anything fails, ignore the error. The annealing loop
	// will retry.
	if err := d.mayDeployApp(ctx, req.AppName); err != nil {
		d.logger.Error("mayDeployApp", "err", err, "app", req.AppName)
	}
	if err := d.getListenerState(ctx, req.AppName); err != nil {
		d.logger.Error("getListenerState", "err", err, "app", req.AppName)
	}
	if _, err := d.ComputeTrafficAssignments(ctx, time.Now()); err != nil {
		d.logger.Error("ComputeTrafficAssignmentsForApp", "err", err, "app", req.AppName)
	}
	return nil
}

// registerVersions registers the provided application versions.
func (d *Distributor) registerVersions(ctx context.Context, req *nanny.ApplicationDistributionRequest, now time.Time) error {
	// Register the app.
	if err := d.registerApp(ctx, req.AppName); err != nil {
		return err
	}

	// Register the new versions.
	state, version, err := d.loadAppState(ctx, req.AppName)
	if err != nil {
		return err
	}
	changed := false
	for _, v := range req.Requests {
		if isDeleted(state, v.Config.Deployment.Id) {
			// The version has already been deleted.
			continue
		}

		if versionIndex(state, v.Config.Deployment.Id) >= 0 {
			// The version has already been registered.
			continue
		}

		state.Versions = append(state.Versions, &AppVersionState{
			Config:   v.Config,
			Schedule: nanny.NewSchedule(v.TargetFn),
			Order:    v.SubmissionId,
			Status:   AppVersionState_STARTING,
		})
		changed = true
	}
	if !changed {
		return nil
	}

	// Sort the versions by submission order.
	sort.Slice(state.Versions, func(i, j int) bool {
		return state.Versions[i].Order < state.Versions[j].Order
	})

	// Save the application's state.
	_, err = d.saveAppState(ctx, req.AppName, state, version)
	return err
}

// registerApp registers the provided application.
func (d *Distributor) registerApp(ctx context.Context, app string) error {
	state, version, err := d.loadState(ctx)
	if err != nil {
		return err
	}
	for _, a := range state.Applications {
		if app == a {
			// The application is already registered.
			return nil
		}
	}
	state.Applications = append(state.Applications, app)
	return d.saveState(ctx, state, version)
}

// versionIndex returns the index of the version with the provided deployment
// id, or -1 if no such version exists.
func versionIndex(state *AppState, id string) int {
	for i, version := range state.Versions {
		if version.Config.Deployment.Id == id {
			return i
		}
	}
	return -1
}

// isDeleted returns whether the provided version has been deleted.
func isDeleted(state *AppState, id string) bool {
	for _, deleted := range state.DeletedVersions {
		if id == deleted {
			return true
		}
	}
	return false
}

// Cleanup registers the provided versions for Cleanup and also tries
// to clean them up.
func (d *Distributor) Cleanup(ctx context.Context, req *nanny.ApplicationCleanupRequest) error {
	if err := d.markVersionsToStop(ctx, req.AppName, req.Versions); err != nil {
		return err
	}

	// Try to clean up the version and update the traffic assignment
	// immediately. If anything fails, ignore the error.  The annealing loop
	// will retry.
	if err := d.mayCleanupApp(ctx, req.AppName); err != nil {
		d.logger.Error("mayCleanupApp", "err", err, "app", req.AppName)
	}
	if _, err := d.ComputeTrafficAssignments(ctx, time.Now()); err != nil {
		d.logger.Error("ComputeTrafficAssignmentsForApp", "err", err, "app", req.AppName)
	}
	return nil
}

// markVersionsToStop records that the provided versions should be stopped.
func (d *Distributor) markVersionsToStop(ctx context.Context, app string, versionsToStop []string) error {
	state, version, err := d.loadAppState(ctx, app)
	if err != nil {
		return err
	}
	if *version == store.Missing {
		// If a controller receives a request to start an application, fails to
		// deploy it, and then receives a request to delete the application,
		// the distributor may receive a request to delete a version for an app
		// it doesn't know about. We cannot fail in this situation, as the
		// controller will keep trying to issue the delete.
		//
		// However, we also cannot return successfully and do nothing, as this
		// allows for the possibility of a race between the starting and
		// stopping of a version. We have to record the version as deleted,
		// even if we haven't heard about it before.
		if err := d.registerApp(ctx, app); err != nil {
			return err
		}
	}

	changed := false
	for _, id := range versionsToStop {
		if isDeleted(state, id) {
			continue
		}

		if i := versionIndex(state, id); i >= 0 {
			if state.Versions[i].Status != AppVersionState_STOPPING &&
				state.Versions[i].Status != AppVersionState_STOPPED {
				state.Versions[i].Status = AppVersionState_STOPPING
				changed = true
			}
			continue
		}

		// TODO(mwhittaker): Think harder about whether this is correct. Do we
		// need to add an entry in the STOPPING or STOPPED state? I don't think
		// so, but I should double check.
		state.DeletedVersions = append(state.DeletedVersions, id)
		changed = true
	}
	if !changed {
		return nil
	}
	_, err = d.saveAppState(ctx, app, state, version)
	return err
}

// GetApplicationState returns the latest application state.
func (d *Distributor) GetApplicationState(ctx context.Context, req *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error) {
	// Try to freshen our state. If this fails, we'll return a slightly stale
	// state.
	if err := d.ManageAppStates(ctx); err != nil {
		d.logger.Error("ManageAppStates", "err", err)
	}

	state, version, err := d.loadAppState(ctx, req.AppName)
	if err != nil {
		return nil, err
	}
	if *version == store.Missing {
		return &nanny.ApplicationStateAtDistributor{}, nil
	}

	reply := &nanny.ApplicationStateAtDistributor{}
	for _, v := range state.Versions {
		reply.VersionState = append(reply.VersionState,
			&nanny.VersionStateAtDistributor{
				VersionId:                  v.Config.Deployment.Id,
				RolloutCompleted:           nanny.Done(v.Schedule),
				LastTrafficFractionApplied: nanny.Fraction(v.Schedule),
				IsDeployed:                 v.Status != AppVersionState_STARTING,
				ReplicaSets:                v.ReplicaSets,
			})
	}

	// Note that from the controller's perspective, a version is "deleted" as
	// soon as it is stopped.
	//
	// TODO(rgrandl): Right now the distributor keeps state regarding all the
	// deleted application's versions ever. This is because it doesn't really
	// know whether the controller received successfully this information.
	// However, in a future CL we will associate a TTL with each deleted
	// version, s.t., if the distributor doesn't receive a delete request for
	// an already deleted version for some time, it is safe to also delete its
	// corresponding state.
	reply.DeletedVersions = append(reply.DeletedVersions, state.DeletedVersions...)
	for _, v := range state.Versions {
		if v.Status == AppVersionState_STOPPED {
			reply.DeletedVersions = append(reply.DeletedVersions, v.Config.Deployment.Id)
		}
	}
	return reply, nil
}

// GetPublicTrafficAssignment collects per-application traffic assignments
// for public listeners and aggregates them into a single per-distributor
// traffic assignment for public listeners.
func (d *Distributor) GetPublicTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	return d.getTrafficAssignment(ctx, true /*public*/)
}

// GetPrivateTrafficAssignment collects per-application traffic assignments
// for private listeners and aggregates them into a single per-distributor
// traffic assignment for private listeners.
func (d *Distributor) GetPrivateTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	return d.getTrafficAssignment(ctx, false /*public*/)
}

func (d *Distributor) getTrafficAssignment(ctx context.Context, public bool) (*nanny.TrafficAssignment, error) {
	// Try to freshen our traffic assignment. If this fails, we'll return a
	// slightly stale traffic assignment.
	if _, err := d.ComputeTrafficAssignments(ctx, time.Now()); err != nil {
		d.logger.Error("ComputeTrafficAssignments", "err", err)
	}

	state, _, err := d.loadState(ctx)
	if err != nil {
		return nil, err
	}
	if public {
		return state.PublicTrafficAssignment, nil
	}
	return state.PrivateTrafficAssignment, nil
}

// RunProfiling profiles a sample of the application version's replicas and
// computes a representative profile for the application version.
func (d *Distributor) RunProfiling(ctx context.Context, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	// Get the ReplicaSet information for the given application version.
	states, err := d.manager.GetReplicaSetState(ctx, &nanny.GetReplicaSetStateRequest{
		AppName:   req.AppName,
		VersionId: req.VersionId,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot get ReplicaSet states for version %q of application %q: %w", req.AppName, req.VersionId, err)
	}

	groups := make([][]func() ([]byte, error), 0, len(states.ReplicaSets))
	for _, g := range states.ReplicaSets {
		// Compute a randomly ordered list of healthy babysitters.
		group := make([]func() ([]byte, error), 0, len(g.Pods))
		for _, idx := range rand.Perm(len(g.Pods)) {
			if g.Pods[idx].HealthStatus != protos.HealthStatus_HEALTHY {
				continue
			}
			addr := g.Pods[idx].BabysitterAddr
			group = append(group, func() ([]byte, error) {
				preq := &protos.GetProfileRequest{
					ProfileType:   req.ProfileType,
					CpuDurationNs: req.CpuDurationNs,
				}
				babysitter := d.babysitterConstructor(addr)
				preply, err := babysitter.RunProfiling(ctx, preq)
				if err != nil {
					return nil, err
				}
				return preply.Data, nil
			})
		}
		groups = append(groups, group)
	}
	data, err := profiling.ProfileGroups(groups)
	return &protos.GetProfileReply{Data: data}, err
}
