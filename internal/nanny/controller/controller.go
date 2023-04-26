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

// Package controller handles the deployment of new application versions.
//
// It receives application versions from the command-line tool, generates a
// rollout strategy for each, and manages their rollout.
package controller

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/profiling"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A controller manages the rollout of an application version across a set of
// locations. When a controller receives a new application version, it computes
// a schedule for rolling out the version (e.g., 1 minute in region "us-west1",
// then 2 minutes in "us-east1"). The controller then communicates with the
// distributors in these locations to perform the rollout. A controller also
// manages the routing of traffic across regions.
//
// A controller persists in the store the *desired* state of all applications.
// The /controller_state key stores a list of all applications, and for each
// application a, the key /controller/application/a stores all of the versions
// of the application.
//
//     /controller_state            -> ControllerState{"todo", "chat"}
//     /controller/application/todo -> AppState{...}
//     /controller/application/chat -> AppState{...}
//
// Note that the store holds the *desired* state, but not necessarily the
// actual state. The controller runs an annealing loop that tries to converge
// the actual state towards the desired state. It will, for example, repeatedly
// try to start any application versions that should be started but aren't.
//
// For performance reasons, it's ideal to have a single controller running at a
// time, but this is not needed for correctness. Multiple controllers can
// safely run concurrently.

const (
	// URL suffixes for various controller handlers.
	RolloutURL      = "/controller/rollout"
	KillURL         = "/controller/kill"
	StatusURL       = "/controller/status"
	RunProfilingURL = "/controller/run_profiling"
	FetchMetricsURL = "/controller/fetch_metrics"
)

type controller struct {
	// TODO(mwhittaker): See the comment on distributor.ctx.
	ctx              context.Context
	store            store.Store
	logger           *slog.Logger
	rolloutProcessor rolloutProcessor
	actuationDelay   time.Duration
	distributor      func(addr string) clients.DistributorClient
	applyTraffic     func(context.Context, *nanny.TrafficAssignment) error
}

// Start starts the controller service.
func Start(ctx context.Context,
	mux *http.ServeMux,
	store store.Store,
	logger *slog.Logger,
	actuationDelay time.Duration,
	distributor func(addr string) clients.DistributorClient,
	fetchAssignmentsInterval time.Duration,
	applyAssignmentInterval time.Duration,
	manageAppInterval time.Duration,
	applyTraffic func(context.Context, *nanny.TrafficAssignment) error) (*controller, error) {
	c := &controller{
		ctx:              ctx,
		store:            store,
		logger:           logger,
		rolloutProcessor: &basicRolloutProcessor{},
		actuationDelay:   actuationDelay,
		distributor:      distributor,
		applyTraffic:     applyTraffic,
	}
	c.addHandlers(mux)
	go c.anneal(ctx, fetchAssignmentsInterval, applyAssignmentInterval, manageAppInterval)
	return c, nil
}

// addHandlers adds handlers for the HTTP endpoints exposed by the controller.
func (c *controller) addHandlers(mux *http.ServeMux) {
	mux.HandleFunc(RolloutURL, protomsg.HandlerDo(c.logger, c.handleRollout))
	mux.HandleFunc(KillURL, protomsg.HandlerDo(c.logger, c.handleKill))
	mux.HandleFunc(StatusURL, protomsg.HandlerFunc(c.logger, c.status))
	mux.HandleFunc(RunProfilingURL, protomsg.HandlerFunc(c.logger, c.handleRunProfiling))
}

// handleRollout is the handler for the RolloutURL endpoint.
func (c *controller) handleRollout(ctx context.Context, req *RolloutRequest) error {
	id := req.Config.Deployment.Id
	name := req.Config.Deployment.App.Name
	c.logger.Info("Registering for rollout", "version", id, "application", name)
	if err := c.rollout(ctx, req); err != nil {
		c.logger.Error("Cannot register for rollout", "err", err, "version", id, "application", name)
		return err
	}
	c.logger.Info("Successfully registering for rollout", "version", id, "application", name)
	return nil
}

// handleKill is the handler for the KillURL endpoint.
func (c *controller) handleKill(ctx context.Context, req *KillRequest) error {
	c.logger.Info("Killing", "application", req.App)
	if err := c.kill(ctx, req); err != nil {
		c.logger.Error("Cannot kill", "err", err, "application", req.App)
		return err
	}
	c.logger.Info("Successfully killed", "application", req.App)
	return nil
}

// handleRunProfiling is the handler for the RunProfilingURL endpoint.
func (c *controller) handleRunProfiling(ctx context.Context, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	// Load the application state.
	state, version, err := c.loadAppState(ctx, req.AppName)
	if err != nil {
		return nil, err
	}
	if *version == store.Missing {
		return nil, fmt.Errorf("application %q not found", req.AppName)
	}
	var versionState *AppVersionState
	if req.VersionId == "" {
		// Get the latest application version.
		if len(state.Versions) == 0 {
			return nil, fmt.Errorf("no versions of application %q are running", req.AppName)
		}
		vs := maps.Values(state.Versions)
		sort.Slice(vs, func(i, j int) bool {
			return vs[i].SubmissionId < vs[j].SubmissionId
		})
		versionState = vs[len(vs)-1]
		req.VersionId = versionState.Config.Deployment.Id
	} else {
		var ok bool
		if versionState, ok = state.Versions[req.VersionId]; !ok {
			return nil, fmt.Errorf("version %q of application %q not found", req.VersionId, req.AppName)
		}
	}

	c.logger.Info("Profiling", "version", req.VersionId, "application", req.AppName)
	prof, err := c.runProfiling(ctx, versionState, req)
	if prof == nil {
		c.logger.Error("Cannot profile", "err", err, "version", req.VersionId, "application", req.AppName)
		return nil, err
	}
	if len(prof.Data) == 0 {
		c.logger.Info("Empty profile", "version", req.VersionId, "application", req.AppName)
	} else if err != nil {
		c.logger.Error("Partial profile", "err", err, "version", req.VersionId, "application", req.AppName)
	} else {
		c.logger.Info("Successfully profiled", "version", req.VersionId, "application", req.AppName)
	}
	return prof, nil
}

func (c *controller) rollout(ctx context.Context, req *RolloutRequest) error {
	// Update the controller state.
	state, version, err := c.loadState(ctx)
	if err != nil {
		return err
	}
	changed := false
	for _, loc := range req.Locations {
		if _, found := state.Distributors[loc.Name]; !found {
			changed = true
			state.Distributors[loc.Name] = &DistributorState{Location: loc}
		}
	}
	app := req.Config.Deployment.App.Name
	if _, found := state.Applications[app]; !found {
		changed = true
		state.Applications[app] = true
	}
	if changed {
		if err := c.saveState(ctx, state, version); err != nil {
			return err
		}
	}

	// Load the app state.
	appState, version, err := c.loadAppState(ctx, app)
	if err != nil {
		return err
	}
	if _, found := appState.Versions[req.Config.Deployment.Id]; found {
		return nil
	}

	// Compute the rollout strategy.
	durationHint := time.Duration(req.Config.Deployment.App.RolloutNanos)
	rolloutLocs := make([]string, len(req.Locations))
	for i := 0; i < len(req.Locations); i++ {
		rolloutLocs[i] = req.Locations[i].Name
	}
	rolloutStrategy, err := c.rolloutProcessor.computeRolloutStrategy(
		rolloutProperties{
			durationHint:   durationHint,
			waitTimeFrac:   0.2,
			locations:      rolloutLocs,
			actuationDelay: c.actuationDelay,
		})
	if err != nil {
		return err
	}

	// Get a submission id for the new rollout request.
	id, err := nextSubmissionID(ctx, c.store)
	if err != nil {
		return err
	}

	// Initialize the rollout status.
	distributors := map[string]*AppVersionDistributorState{}
	for idx, wave := range rolloutStrategy.Waves {
		for d := range wave.TargetFunctions {
			distributors[d] = &AppVersionDistributorState{
				WaveIdx: int64(idx),
				Status:  AppVersionDistributorStatus_STARTING,
			}
		}
	}
	for loc := range rolloutStrategy.Waves[0].TargetFunctions {
		distributors[loc].Status = AppVersionDistributorStatus_STARTING
	}

	// Save the app state.
	now := timestamppb.Now()
	appState.Versions[req.Config.Deployment.Id] = &AppVersionState{
		Config:          req.Config,
		RolloutStrategy: rolloutStrategy,
		WaveIdx:         0,
		TimeWaveStarted: now,
		Distributors:    distributors,
		SubmissionId:    int64(id),
		SubmissionTime:  now,
	}
	if err := c.saveAppState(ctx, app, appState, version); err != nil {
		return err
	}

	// Try to launch the new version immediately. If it fails, ignore the
	// error. The annealing loop will retry.
	if err := c.maySendDistributionRequests(ctx, app); err != nil {
		c.logger.Error("maySendDistributionRequests", "err", err, "app", app)
	}
	return nil
}

func (c *controller) kill(ctx context.Context, req *KillRequest) error {
	// Mark every location of every version of the app for deletion.
	state, version, err := c.loadAppState(ctx, req.App)
	if err != nil {
		return err
	}
	if *version == store.Missing {
		return fmt.Errorf("application %q not found", req.App)
	}
	for _, v := range state.Versions {
		v.WaveIdx = int64(len(v.RolloutStrategy.Waves))
		v.TimeWaveStarted = nil
		v.TimeWaveRolledOut = nil
		for _, d := range v.Distributors {
			if d.Status != AppVersionDistributorStatus_DELETED {
				d.Status = AppVersionDistributorStatus_DELETING
			}
		}
	}
	if err := c.saveAppState(ctx, req.App, state, version); err != nil {
		return err
	}

	// Try to launch the new version and update our traffic assignments
	// immediately. If anything fails, ignore the error. The annealing loop
	// will retry.
	if err := c.maySendCleanupRequests(ctx, req.App); err != nil {
		c.logger.Error("maySendCleanupRequests", "err", err, "app", req.App)
	}
	if err := c.fetchTrafficAssignments(ctx); err != nil {
		c.logger.Error("fetchTrafficAssignments", "err", err, "app", req.App)
	}
	// TODO(mwhittaker): Call applyTrafficAssignment?
	return nil
}

func (c *controller) status(ctx context.Context, req *StatusRequest) (*Status, error) {
	// Try to freshen our state and traffic assignment. If these commands fail,
	// we'll return a slightly stale status.
	if err := c.manageState(ctx); err != nil {
		c.logger.Error("manageState", "err", err)
	}
	if err := c.fetchTrafficAssignments(ctx); err != nil {
		c.logger.Error("fetchTrafficAssignments", "err", err)
	}

	state, _, err := c.loadState(ctx)
	if err != nil {
		return nil, err
	}

	// Compute the public traffic assignment.
	traffic, err := c.traffic(ctx)
	if err != nil {
		return nil, err
	}
	status := &Status{Traffic: traffic}

	// Fetch the private traffic assignment at every location.
	for _, d := range state.Distributors {
		distributor := c.distributor(d.Location.DistributorAddr)
		traffic, err := distributor.GetPrivateTrafficAssignment(ctx)
		if err != nil {
			return nil, fmt.Errorf("get distributor %q private traffic: %w", d.Location.Name, err)
		}
		for _, assignment := range traffic.HostAssignment {
			for _, alloc := range assignment.Allocs {
				alloc.Location = d.Location.Name
			}
		}
		status.PrivateTraffic = append(status.PrivateTraffic, traffic)
	}

	// Compute the status of every app.
	apps := []string{req.App}
	if req.App == "" {
		apps = maps.Keys(state.Applications)
	}
	for _, app := range apps {
		appState, _, err := c.loadAppState(ctx, app)
		if err != nil {
			return nil, err
		}
		if len(appState.Versions) == 0 {
			// Ignore apps without any versions.
			continue
		}
		appStatus, err := c.appStatus(ctx, req, state, app, appState)
		if err != nil {
			return nil, fmt.Errorf("app %q status: %w", app, err)
		}
		status.Apps = append(status.Apps, appStatus)
	}
	return status, nil
}

func (c *controller) appStatus(ctx context.Context, req *StatusRequest, state *ControllerState, app string, appState *AppState) (*AppStatus, error) {
	versions := []string{req.Version}
	if req.Version == "" {
		versions = maps.Keys(appState.Versions)
	}

	appStatus := &AppStatus{App: app}
	for _, v := range versions {
		versionState, ok := appState.Versions[v]
		if !ok {
			return nil, fmt.Errorf("version %q not found", v)
		}
		appStatus.Versions = append(appStatus.Versions,
			appVersionStateToStatus(app, state, versionState))
	}

	// Compute the set of change times, the times at which we predict the
	// traffic assignment will change.
	now := time.Now()
	times := []time.Time{now}
	for _, version := range appState.Versions {
		t := version.TimeWaveStarted
		if t == nil {
			// The rollout is done. There are no change times.
			continue
		}
		ts, err := changeTimes(version.RolloutStrategy, int(version.WaveIdx), t.AsTime())
		if err != nil {
			return nil, fmt.Errorf("error getting change times for app %q: %v", app, err)
		}
		times = append(times, ts...)
	}
	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	// Compute the traffic assignment at every change time.
	p := &ProjectedTraffic{App: app}
	appStatus.ProjectedTraffic = p
	for _, t := range times {
		if t.Before(now) {
			continue
		}
		traffic, err := projectedTraffic(app, appState.Versions, t)
		if err != nil {
			return nil, fmt.Errorf("error projecting traffic for app %q: %v", app, err)
		}
		p.Projections = append(p.Projections, &Projection{Time: timestamppb.New(t), Traffic: traffic})
	}

	return appStatus, nil
}

func appVersionStateToStatus(app string, state *ControllerState, versionState *AppVersionState) *AppVersionStatus {
	status := &AppVersionStatus{
		App:            app,
		Id:             versionState.Config.Deployment.Id,
		SubmissionId:   versionState.SubmissionId,
		SubmissionTime: versionState.SubmissionTime,
		GkeConfig:      versionState.Config,
	}

	statuses := map[AppVersionDistributorStatus]int{}
	for _, d := range versionState.Distributors {
		statuses[d.Status]++
	}
	switch {
	case statuses[AppVersionDistributorStatus_UNKNOWN] > 0:
		status.Status = AppVersionStatus_UNKNOWN
	case statuses[AppVersionDistributorStatus_DELETING] > 0,
		statuses[AppVersionDistributorStatus_DELETED] > 0:
		status.Status = AppVersionStatus_DELETING
	case statuses[AppVersionDistributorStatus_STARTING] > 0,
		statuses[AppVersionDistributorStatus_ROLLING_OUT] > 0:
		status.Status = AppVersionStatus_ROLLING_OUT
	default:
		// Every status is ROLLED_OUT.
		status.Status = AppVersionStatus_ACTIVE
	}

	getPublicHostname := func(lis string) string {
		cfg := versionState.Config
		for _, public := range cfg.PublicListener {
			if lis == public.Name {
				return public.Hostname
			}
		}
		return ""
	}
	var groups []*ReplicaSetStatus
	listeners := map[string]*ListenerStatus{}
	for loc, d := range versionState.Distributors {
		if d.ReplicaSets == nil {
			continue
		}
		for _, group := range d.ReplicaSets.ReplicaSets {
			var numHealthyReplicas int
			for _, pod := range group.Pods {
				if pod.HealthStatus == protos.HealthStatus_HEALTHY {
					numHealthyReplicas++
				}
			}
			for _, l := range group.Listeners {
				var hostname string
				var public bool
				if hostname = getPublicHostname(l); hostname != "" {
					public = true
				} else {
					public = false
					hostname = fmt.Sprintf("%s.%s.%s", l, loc, distributor.InternalDNSDomain)
				}
				ls := listeners[l]
				if ls == nil {
					ls = &ListenerStatus{
						Name:     l,
						Public:   public,
						Hostname: []string{hostname},
					}
					listeners[l] = ls
					continue
				}
				if !public {
					ls.Hostname = append(ls.Hostname, hostname)
				}

			}
			groups = append(groups, &ReplicaSetStatus{
				Name:            group.Name,
				Location:        loc,
				HealthyReplicas: int64(numHealthyReplicas),
				TotalReplicas:   int64(len(group.Pods)),
				Components:      group.Components,
			})
		}
	}
	status.ReplicaSets = groups
	status.Listeners = maps.Values(listeners)
	return status
}

func (c *controller) runProfiling(ctx context.Context, v *AppVersionState, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	// NOTE: For now, profile in all the location the application is being
	// rolled out to. In the future, we may want to pick only one, but we also
	// have to make sure this location is receiving traffic.
	var addrs []string
	for loc, d := range v.Distributors {
		if d.Status != AppVersionDistributorStatus_ROLLED_OUT &&
			d.Status != AppVersionDistributorStatus_ROLLING_OUT {
			continue
		}
		addr, err := c.getDistributorAddr(ctx, loc)
		if err != nil {
			c.logger.Error("cannot profile", "err", err, "location", loc)
			continue
		}
		addrs = append(addrs, addr)
	}

	groups := make([][]func() ([]byte, error), 0, len(addrs))
	for _, addr := range addrs {
		d := c.distributor(addr)
		group := []func() ([]byte, error){func() ([]byte, error) {
			p, err := d.RunProfiling(ctx, req)
			if p != nil {
				return p.Data, err
			}
			return nil, err
		}}
		groups = append(groups, group)
	}
	data, err := profiling.ProfileGroups(groups)
	return &protos.GetProfileReply{Data: data}, err
}

// traffic returns the current global traffic assignment.
func (c *controller) traffic(ctx context.Context) (*nanny.TrafficAssignment, error) {
	state, _, err := c.loadState(ctx)
	if err != nil {
		return nil, err
	}

	var assignments []*nanny.TrafficAssignment
	for _, ta := range state.Distributors {
		if ta.TrafficAssignment != nil {
			assignments = append(assignments, ta.TrafficAssignment)
		}
	}

	// Normalize the weights of each traffic allocation and compute the
	// final assignment.
	return nanny.MergeTrafficAssignments(assignments), nil
}

// projectedTraffic returns the projected traffic assignment at the provided
// time, assuming there are no delays in the rollout.
//
// In reality, we assign traffic to hostnames, not to apps. Thus, traffic
// assignment becomes very complicated when different apps use the same
// hostname, different deployments of an app use different hostnames, etc. Even
// worse, we don't know ahead of time which hostnames a deployment will use, so
// we cannot predict future traffic assignments.
//
// We sidestep these issues by pretending that every version of app uses a
// single hostname, and the hostname is the same as the app name. For example,
// we pretend that the "todo" app uses the "todo" hostname. Thus, this method
// returns a projected traffic assignment at the granularity of an app, rather
// than at the granularity of a hostname.
func projectedTraffic(app string, versions map[string]*AppVersionState, at time.Time) (*nanny.TrafficAssignment, error) {
	// See the comment above the application.traffic method.
	listeners := map[string][]*nanny.Listener{app: {{Name: app}}}

	// Get a list of versions in submission id order.
	var vs []*AppVersionState
	for _, v := range versions {
		vs = append(vs, v)
	}
	sort.Slice(vs, func(i, j int) bool {
		return vs[i].SubmissionId < vs[j].SubmissionId
	})

	// Construct the cascade targets for every location. We have a list of
	// versions, and every version has a RolloutStrategy with a list of
	// locations. We invert this to have a list of locations, with every
	// location having a list of cascade targets.
	locTargets := map[string][]*nanny.CascadeTarget{}
	for _, v := range vs {
		for _, wave := range v.RolloutStrategy.Waves {
			for location := range wave.TargetFunctions {
				start := v.TimeWaveStarted.AsTime()
				f, err := fraction(v.RolloutStrategy, location, int(v.WaveIdx), start, at.Sub(start))
				if err != nil {
					return nil, err
				}
				if f == 0.0 {
					// Don't include targets for apps without any traffic.
					continue
				}
				locTargets[location] = append(locTargets[location], &nanny.CascadeTarget{
					Location:        location,
					AppName:         app,
					VersionId:       v.Config.Deployment.Id,
					Listeners:       listeners,
					TrafficFraction: f,
				})
			}
		}
	}

	// Compute the traffic assignment for every location and then merge them
	// together.
	var traffics []*nanny.TrafficAssignment
	for loc, targets := range locTargets {
		traffic, err := nanny.CascadeTraffic(targets)
		if err != nil {
			return nil, fmt.Errorf("cannot compute traffic for %s: %w", loc, err)
		}
		traffics = append(traffics, traffic)
	}
	return nanny.MergeTrafficAssignments(traffics), nil
}
