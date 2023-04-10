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

package assigner

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/proto"
)

// An assigner is responsible for two things, one minor and one major. First,
// and more minor, the assigner persists all of the components that should be
// started. Babysitters and weavelets watch this information to know which
// components to start. We don't elaborate on this, as it involves
// straightforward reads and writes to the store.
//
// Second, and more major, the assigner is responsible for computing routing
// assignments for all components. To create routing assignments, the assigner
// needs to know two critical pieces of information: the load on every weavelet
// and the health of every weavelet. If the load on a component changes or if
// a weavelet crashes, the assigner needs to react and generate a new assignment.
//
// === Persisted State ===
//
// The assigner persists *most* of its state in the store. The /assigner_state
// key stores a list of all applications. For each application A, the key
// /assigner/application/A stores the ids of all Kubernetes ReplicaSets across
// all version of the app. For every ReplicaSet, the key
// /app/$APP/deployment/$ID/replica_set/$REPLICA_SET/replica_set_info
// stores information about the ReplicaSet. Specifically, it stores the routing
// assignments for all the components in the ReplicaSet, the load on every
// replica (i.e., Pod) under the current assignment, and a list of all the
// replicas (i.e., Pods).
//
//     /assigner_state -> ["todo", "chat"]
//     /assigner/application/todo -> [v1.Todo, v1.Store]
//     /assigner/application/chat -> [v2.Chat, v2.Cache]
//     /app/todo/deployment/v1/replica_set/Todo/replica_set_info  -> ...
//     /app/todo/deployment/v1/replica_set/Store/replica_set_info -> ...
//     /app/chat/deployment/v2/replica_set/Chat/replica_set_info  -> ...
//     /app/chat/deployment/v2/replica_set/Cache/replica_set_info -> ...
//
// The assigner stores most of its state in the store, but not everything. The
// assigner monitors the health of every weavelet by probing its /healthz server
// periodically. Storing this information in the store would be prohibitively
// expensive. Instead, every assigner (remember that the assigner can be
// replicated) independently computes the health of every replica, keeping the
// health information in memory.
//
// TODO(mwhittaker): Implement a smarter way to consolidate different
// assigner's view of health and explain it briefly here.

const (
	// RoutingInfoKey is the key where we store routing information for a given
	// Kubernetes ReplicaSet.
	routingInfoKey = "routing_entries"

	// appVersionStateKey is the key where we store the assigner's state for a
	// given application version.
	appVersionStateKey = "assigner_app_version_state"

	// Interval at which the assigner checks weavelets for their health status.
	healthCheckInterval = 2 * time.Second

	// Duration after which if no healthy reports were received, we should start
	// checking whether the replica still exists.
	timeToCheckIfReplicaExists = 30 * healthCheckInterval
)

// Assigner generates routing information for a set of managed Kubernetes
// ReplicaSets.
//
// In the current implementation, the assigner generates new routing information
// when (1) new sharded components become managed by the ReplicaSet and/or when
// (2) the ReplicaSet has new Pods.
type Assigner struct {
	// TODO(mwhittaker): Pass in a logger.
	ctx                   context.Context
	store                 store.Store
	logger                *slog.Logger
	algo                  Algorithm
	babysitterConstructor func(addr string) clients.BabysitterClient
	replicaExists         func(ctx context.Context, podName string) (bool, error)

	// replicas stores the set of all weavelets, grouped by replica set id and
	// then indexed by weavelet address.
	mu       sync.RWMutex
	replicas map[replicaSetId]map[string]*replica
}

// replicaSetId uniquely identifies a Kubernetes ReplicaSet.
//
// NOTE that replicaSetId is a clone of ReplicaSetId, but we duplicate the
// definition here in a plain struct so that we can use replicaSetIds as keys
// in maps.
type replicaSetId struct {
	app  string
	id   string
	name string
}

// replica includes information about a single replica (i.e. weavelet).
type replica struct {
	addr   string             // internal listener address for the replica
	health *healthTracker     // health tracker
	cancel context.CancelFunc // cancels health checker
}

func NewAssigner(
	ctx context.Context,
	store store.Store,
	logger *slog.Logger,
	algo Algorithm,
	babysitterConstructor func(addr string) clients.BabysitterClient,
	replicaExists func(context.Context, string) (bool, error)) *Assigner {
	assigner := &Assigner{
		ctx:                   ctx,
		store:                 store,
		logger:                logger,
		algo:                  algo,
		babysitterConstructor: babysitterConstructor,
		replicaExists:         replicaExists,
		replicas:              map[replicaSetId]map[string]*replica{},
	}
	go func() {
		if err := assigner.anneal(); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "assigner anneal: %v\n", err)
			os.Exit(1)
		}
	}()
	return assigner
}

// registerApp registers the provided app in the assigner's state.
func (a *Assigner) registerApp(ctx context.Context, app string) error {
	_, _, err := a.applyState(ctx, func(state *AssignerState) bool {
		for _, existing := range state.Applications {
			if app == existing {
				return false
			}
		}
		state.Applications = append(state.Applications, app)
		return true
	})
	if err != nil {
		return fmt.Errorf("register app %q: %v", app, err)
	}
	return nil
}

// registerReplicaSet registers the provided Kubernetes ReplicaSet in the
// appropriate app's state.
func (a *Assigner) registerReplicaSet(ctx context.Context, rid *ReplicaSetId) error {
	_, _, err := a.applyAppState(ctx, rid.App, func(state *AppState) bool {
		for _, existing := range state.ReplicaSetIds {
			if proto.Equal(rid, existing) {
				return false
			}
		}
		state.ReplicaSetIds = append(state.ReplicaSetIds, rid)
		return true
	})
	if err != nil {
		return fmt.Errorf("register replica set %v: %v", rid, err)
	}
	return nil
}

// RegisterActiveComponent registers the given component has been activated. It
// returns the name of the Kubernetes ReplicaSet that should host the component.
func (a *Assigner) RegisterActiveComponent(ctx context.Context, req *nanny.ActivateComponentRequest) error {
	// TODO(spetrovic): Avoid parsing Deployment.Id, so that it can be an
	// abitrary string.
	id, err := uuid.Parse(req.Config.Deployment.Id)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", req.Config.Deployment.Id, err)
	}
	rsName := nanny.ReplicaSetForComponent(req.Component, req.Config)

	var state AppVersionState
	edit := func(version *store.Version) error {
		// Find the Kubernetes ReplicaSet that should host the component.
		rs := findOrCreateReplicaSet(&state, rsName)

		// Register the component as activated, if not already registered.
		if rs.Components == nil {
			rs.Components = map[string]bool{}
		}
		if _, found := rs.Components[req.Component]; found {
			// No (meaningful) modifications: skip any writes.
			return store.ErrUnchanged
		}
		rs.Components[req.Component] = req.Routed
		return nil
	}

	// Track the key in the store under histKey.
	appName := req.Config.Deployment.App.Name
	key := store.DeploymentKey(appName, id, appVersionStateKey)
	histKey := store.DeploymentKey(appName, id, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(a.ctx, a.store, key, &state, edit)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Register the app and the ReplicaSet.
	if err := a.registerApp(ctx, appName); err != nil {
		return err
	}
	rid := &ReplicaSetId{App: appName, Id: id.String(), Name: rsName}
	if err := a.registerReplicaSet(ctx, rid); err != nil {
		return err
	}

	if !req.Routed {
		return nil
	}

	// Create an initial assignment for the component.
	if _, _, err := a.applyReplicaSetInfo(ctx, rid, func(rs *ReplicaSetInfo) bool {
		if rs.Components == nil {
			rs.Components = map[string]*Assignment{}
		}
		if _, found := rs.Components[req.Component]; found {
			return false
		}
		rs.Components[req.Component] = &Assignment{
			Constraints: &AlgoConstraints{},
			Stats:       &Statistics{},
		}
		rs.Version++
		return true
	}); err != nil {
		return err
	}
	return a.mayGenerateNewRoutingInfo(ctx, rid)
}

func (a *Assigner) RegisterReplica(ctx context.Context, req *nanny.RegisterReplicaRequest) error {
	id, err := uuid.Parse(req.Config.Deployment.Id)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", req.Config.Deployment.Id, err)
	}

	var state AppVersionState
	edit := func(version *store.Version) error {
		rs := findOrCreateReplicaSet(&state, req.ReplicaSet)
		for _, addr := range rs.WeaveletAddrs {
			if req.WeaveletAddress == addr {
				// If the address is already registered in the store, then we
				// skip any writes.
				return store.ErrUnchanged
			}
		}
		rs.WeaveletAddrs = append(rs.WeaveletAddrs, req.WeaveletAddress)
		return nil
	}

	// Track the key in the store under histKey.
	appName := req.Config.Deployment.App.Name
	key := store.DeploymentKey(appName, id, appVersionStateKey)
	histKey := store.DeploymentKey(appName, id, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(a.ctx, a.store, key, &state, edit)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Register the app and replica set.
	if err := a.registerApp(ctx, appName); err != nil {
		return err
	}
	rid := &ReplicaSetId{
		App:  appName,
		Id:   id.String(),
		Name: req.ReplicaSet,
	}
	if err := a.registerReplicaSet(ctx, rid); err != nil {
		return err
	}

	// Register the replica.
	if _, _, err := a.applyReplicaSetInfo(ctx, rid, func(rs *ReplicaSetInfo) bool {
		if rs.Pods == nil {
			rs.Pods = map[string]*Pod{}
		}
		if _, found := rs.Pods[req.WeaveletAddress]; found {
			return false
		}
		rs.Pods[req.WeaveletAddress] = &Pod{
			Name:              req.PodName,
			BabysitterAddress: req.BabysitterAddress,
			HealthStatus:      protos.HealthStatus_HEALTHY,
		}
		rs.Version++
		return true
	}); err != nil {
		return err
	}

	a.addChecker(rid, req.WeaveletAddress, req.PodName, req.BabysitterAddress, protos.HealthStatus_HEALTHY)
	return a.mayGenerateNewRoutingInfo(ctx, rid)
}

func (a *Assigner) RegisterListener(ctx context.Context, req *nanny.ExportListenerRequest) error {
	id, err := uuid.Parse(req.Config.Deployment.Id)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", req.Config.Deployment.Id, err)
	}
	app := req.Config.Deployment.App.Name

	var state AppVersionState
	edit := func(version *store.Version) error {
		rs := findOrCreateReplicaSet(&state, req.ReplicaSet)
		for _, l := range rs.Listeners {
			if req.Listener.Name == l { // Already registered.
				return store.ErrUnchanged
			}
		}
		rs.Listeners = append(rs.Listeners, req.Listener.Name)
		return nil
	}

	// Track the key in the store under histKey.
	key := store.DeploymentKey(app, id, appVersionStateKey)
	histKey := store.DeploymentKey(app, id, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(a.ctx, a.store, key, &state, edit)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Register the app and ReplicaSet, if needed.
	if err := a.registerApp(ctx, app); err != nil {
		return err
	}
	rid := &ReplicaSetId{
		App:  app,
		Id:   id.String(),
		Name: req.ReplicaSet,
	}
	if err := a.registerReplicaSet(ctx, rid); err != nil {
		return err
	}

	// Register the listener with the replica set.
	_, _, err = a.applyReplicaSetInfo(ctx, rid, func(rs *ReplicaSetInfo) bool {
		for _, l := range rs.Listeners {
			if req.Listener.Name == l { // Already registered.
				return false
			}
		}
		rs.Listeners = append(rs.Listeners, req.Listener.Name)
		rs.Version++
		return true
	})
	return err
}

// UnregisterReplicaSets unregisters all Kubernetes ReplicaSets for the given
// application versions.
func (a *Assigner) UnregisterReplicaSets(ctx context.Context, app string, versions []string) error {
	// TODO(mwhittaker): Make sure this doesn't have races.
	versionSet := map[string]bool{}
	for _, version := range versions {
		versionSet[version] = true
	}

	// Compute the ReplicaSets we should delete and the ReplicaSets we should
	// keep.
	appState, version, err := a.loadAppState(ctx, app)
	if err != nil {
		return fmt.Errorf("load app %q state: %w", app, err)
	}
	if *version == store.Missing {
		return fmt.Errorf("app %q state missing", app)
	}
	var toDelete []*ReplicaSetId
	var toKeep []*ReplicaSetId
	for _, rid := range appState.ReplicaSetIds {
		if versionSet[rid.Id] {
			toDelete = append(toDelete, rid)
		} else {
			toKeep = append(toKeep, rid)
		}
	}

	// Delete the corresponding ReplicaSetInfos and RoutingInfos.
	for _, rid := range toDelete {
		id, err := uuid.Parse(rid.Id)
		if err != nil {
			return fmt.Errorf("ReplicaSet %v invalid id: %w", rid, err)
		}
		key := store.ReplicaSetKey(rid.App, id, rid.Name, routingInfoKey)
		if err := a.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete routing info %v: %w", rid, err)
		}
		if err := a.store.Delete(ctx, replicaSetInfoKey(rid)); err != nil {
			return fmt.Errorf("delete ReplicaSet %v state: %w", rid, err)
		}
	}

	// Update the AppState.
	appState.ReplicaSetIds = toKeep
	if _, err := a.saveAppState(ctx, app, appState, version); err != nil {
		return fmt.Errorf("save app %q state: %w", app, err)
	}

	// Stop any stale checkers.
	a.deleteCheckers(app, appState)
	return nil
}

func (a *Assigner) GetRoutingInfo(req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	// Keep retrying as long as we keep getting spurious store.Unchanged
	// errors.
	for r := retry.Begin(); r.Continue(a.ctx); {
		info, err := a.getRoutingInfo(req)
		if err == nil {
			return info, nil
		}
		if errors.Is(err, store.Unchanged) {
			continue
		}
		return nil, err
	}
	return nil, a.ctx.Err()
}

func (a *Assigner) getRoutingInfo(req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	id, err := uuid.Parse(req.Config.Deployment.Id)
	if err != nil {
		return nil, fmt.Errorf("bad version %v: %w", req.Config.Deployment.Id, err)
	}
	rsName := nanny.ReplicaSetForComponent(req.Component, req.Config)

	// Fetch routing infos for the ReplicaSet from the store.
	info := &VersionedRoutingInfo{}
	key := store.ReplicaSetKey(req.Config.Deployment.App.Name, id, rsName, routingInfoKey)
	var v *store.Version
	if req.Version != "" {
		v = &store.Version{Opaque: req.Version}
	}
	newVersion, err := store.GetProto(a.ctx, a.store, key, info, v)
	if err != nil {
		return nil, err
	}

	// Reply.
	return &nanny.GetRoutingReply{
		Version: newVersion.Opaque,
		Routing: &protos.RoutingInfo{
			Component:  req.Component,
			Replicas:   info.WeaveletAddrs,
			Assignment: info.Assignments[req.Component],
		},
	}, nil
}

func (a *Assigner) GetComponentsToStart(req *nanny.GetComponentsRequest) (
	*nanny.GetComponentsReply, error) {
	// Keep retrying as long as we keep getting spurious store.Unchanged
	// errors.
	for r := retry.Begin(); r.Continue(a.ctx); {
		info, err := a.getComponentsToStart(req)
		if err == nil {
			return info, nil
		}
		if errors.Is(err, store.Unchanged) {
			continue
		}
		return nil, err
	}
	return nil, a.ctx.Err()
}

func (a *Assigner) getComponentsToStart(req *nanny.GetComponentsRequest) (
	*nanny.GetComponentsReply, error) {
	id, err := uuid.Parse(req.Config.Deployment.Id)
	if err != nil {
		return nil, fmt.Errorf("bad version %v: %w", req.Config.Deployment.Id, err)
	}

	// Fetch components to start from the store.
	var v *store.Version
	if req.Version != "" {
		v = &store.Version{Opaque: req.Version}
	}

	var state AppVersionState
	key := store.DeploymentKey(req.Config.Deployment.App.Name, id, appVersionStateKey)
	newVersion, err := store.GetProto(a.ctx, a.store, key, &state, v)
	if err != nil {
		return nil, err
	}

	var reply nanny.GetComponentsReply
	reply.Version = newVersion.Opaque
	if rs, found := state.ReplicaSets[req.ReplicaSet]; found {
		for c := range rs.Components {
			reply.Components = append(reply.Components, c)
		}
	}
	return &reply, nil
}

// GetReplicaSetState returns the state of all Kubernetes ReplicaSets for an
// application version or a collection of applications and their versions.
func (a *Assigner) GetReplicaSetState(ctx context.Context, req *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error) {
	if req.AppName != "" {
		return a.getReplicaSets(ctx, req.AppName, req.VersionId)
	}
	if req.VersionId != "" {
		return nil, fmt.Errorf("invalid request")
	}
	state, _, err := a.loadState(ctx)
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	var sets []*nanny.ReplicaSetState_ReplicaSet
	var errors []string
	for _, app := range state.Applications {
		ps, err := a.getReplicaSets(ctx, app, "" /*appVersion*/)
		if err != nil {
			errors = append(errors, err.Error())
			continue
		}
		sets = append(sets, ps.ReplicaSets...)
		errors = append(errors, ps.Errors...)
	}
	if sets == nil && errors != nil {
		// No results but have errors: that's an error.
		return nil, fmt.Errorf("cannot get ReplicaSet state: %v", errors)
	}
	return &nanny.ReplicaSetState{
		ReplicaSets: sets,
		Errors:      errors,
	}, nil
}

// getReplicaSets returns the information about the application version's
// Kubernetes ReplicaSets. If the application version is empty, it returns the
// ReplicaSets for all versions of the application.
// REQUIRES: appName != ""
func (a *Assigner) getReplicaSets(ctx context.Context, appName, appVersion string) (*nanny.ReplicaSetState, error) {
	// Load the app's state.
	state, version, err := a.loadAppState(ctx, appName)
	if err != nil {
		return nil, fmt.Errorf("load app %q state: %w", appName, err)
	}
	if *version == store.Missing {
		return nil, fmt.Errorf("app %q state missing", appName)
	}

	// Iterate over all application ReplicaSets and select those that match
	// the given app version. If the app version is empty, all application
	// ReplicaSets will be matched.
	ridsByVersion := map[string][]*ReplicaSetId{}
	for _, rid := range state.ReplicaSetIds {
		if appVersion != "" && rid.Id != appVersion { // mismatched app version
			continue
		}
		ridsByVersion[rid.Id] = append(ridsByVersion[rid.Id], rid)
	}

	var sets []*nanny.ReplicaSetState_ReplicaSet
	var errors []string
	for vid, rids := range ridsByVersion {
		componentsByReplicaSet, err := a.components(ctx, appName, vid)
		if err != nil {
			return nil, err
		}
		for _, rid := range rids {
			rs, version, err := a.loadReplicaSetInfo(ctx, rid)
			if err != nil {
				errors = append(errors, err.Error())
				continue
			}
			if *version == store.Missing {
				errors = append(errors, fmt.Sprintf("ReplicaSet %q info missing", rid))
				continue
			}
			var pods []*nanny.ReplicaSetState_ReplicaSet_Pod
			for addr, replica := range rs.Pods {
				pods = append(pods, &nanny.ReplicaSetState_ReplicaSet_Pod{
					WeaveletAddr:   addr,
					BabysitterAddr: replica.BabysitterAddress,
					HealthStatus:   replica.HealthStatus,
				})
			}
			components, ok := componentsByReplicaSet[rid.Name]
			if !ok {
				errors = append(errors, fmt.Sprintf("no components found for ReplicaSet %v", rid))
				continue
			}
			sets = append(sets, &nanny.ReplicaSetState_ReplicaSet{
				Name:       rid.Name,
				Pods:       pods,
				Components: components,
				Listeners:  rs.Listeners,
			})
		}
	}

	if sets == nil && errors != nil {
		// No results but have errors: that's an error.
		return nil, fmt.Errorf("cannot get ReplicaSet states for app %s: %v", appName, errors)
	}
	return &nanny.ReplicaSetState{
		ReplicaSets: sets,
		Errors:      errors,
	}, nil
}

// components returns the names of the active components in the given deployment,
// grouped by the ReplicaSets that host them.
func (a *Assigner) components(ctx context.Context, app, vid string) (map[string][]string, error) {
	id, err := uuid.Parse(vid)
	if err != nil {
		return nil, fmt.Errorf("invalid id %q: %w", vid, err)
	}

	// Load the AppVersionState.
	key := store.DeploymentKey(app, id, appVersionStateKey)
	var state AppVersionState
	if _, err := store.GetProto(ctx, a.store, key, &state, nil); err != nil {
		return nil, fmt.Errorf("load app %q version %q state: %w", app, vid, err)
	}

	// Massage the AppVersionState.
	components := map[string][]string{}
	for _, rs := range state.ReplicaSets {
		components[rs.Name] = maps.Keys(rs.Components)
	}
	return components, nil
}

// OnNewLoadReport handles a new load report received from a replica.
func (a *Assigner) OnNewLoadReport(ctx context.Context, req *nanny.LoadReport) error {
	rid := &ReplicaSetId{App: req.Config.Deployment.App.Name, Id: req.Config.Deployment.Id, Name: req.ReplicaSet}
	var rs ReplicaSetInfo
	_, err := store.UpdateProto(ctx, a.store, replicaSetInfoKey(rid), &rs, func(version *store.Version) error {
		if *version == store.Missing {
			return fmt.Errorf("ReplicaSet %v missing", rid)
		}

		// Update the ReplicaSet's load information.
		var errs []error
		for c, cLoad := range req.Load.Loads {
			// Ignore load reports for unknown components.
			assignment, found := rs.Components[c]
			if !found {
				continue
			}

			// Ignore load reports if there is no assignment for the component.
			// TODO(rgrandl): maybe we should panic instead, this sounds like a
			// bug.
			//
			// Ignore load reports if the versions mismatch.
			if assignment == nil || assignment.Version != cLoad.Version {
				continue
			}

			// TODO(mwhittaker): Ignore stale load updates.
			if err := updateLoad(assignment, req.WeaveletAddr, cLoad); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
	return err
}

// mayGenerateNewRoutingInfo may generate new routing information for a given
// Kubernetes ReplicaSet.
func (a *Assigner) mayGenerateNewRoutingInfo(ctx context.Context, rid *ReplicaSetId) error {
	// Update the assignments.
	rs := &ReplicaSetInfo{}
	if _, err := store.UpdateProto(ctx, a.store, replicaSetInfoKey(rid), rs, func(version *store.Version) error {
		if *version == store.Missing {
			return fmt.Errorf("ReplicaSet %v not found", rid)
		}

		healthyReplicas, changed := a.healthyReplicas(rid, rs)
		for component, currAssignment := range rs.Components {
			updateCandidateResources(currAssignment, healthyReplicas)
			if !shouldChange(currAssignment) {
				continue
			}
			newAssignment, err := a.algo(currAssignment)
			if err != nil {
				return err
			}
			if newAssignment == nil {
				panic("algo can't generate a nil assignment")
			}
			rs.Components[component] = newAssignment
			changed = true
		}
		if !changed {
			return store.ErrUnchanged
		}
		rs.Version++
		return nil
	}); err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Update the routing information.
	//
	// Note that even if the assignments don't change, we still try to update
	// the routing info. This is because we may have previously failed between
	// updating the assignments and updating the routing info.
	var healthyReplicas []string
	for addr, replica := range rs.Pods {
		if replica.HealthStatus == protos.HealthStatus_HEALTHY {
			healthyReplicas = append(healthyReplicas, addr)
		}
	}
	sort.Strings(healthyReplicas)

	// Convert assigner.Assignment to protos.Assignment for each component.
	assignments := map[string]*protos.Assignment{}
	for component, assignment := range rs.Components {
		assignments[component] = toProto(assignment)
	}

	// Save the routing information.
	return a.writeRoutingInfoInStore(rid, &VersionedRoutingInfo{
		Version:       rs.Version,
		WeaveletAddrs: healthyReplicas,
		Assignments:   assignments,
	})
}

// healthyReplicas updates and returns the set of healthy replicas, along with
// a bool indicating whether the set of healthy replicas changed.
//
// Recall that every assigner independently health checks every weavelet, so
// there is no single source of truth for the set of healthy replicas.
// healthyReplicas returns the set of replicas that this assigner thinks are
// healthy. For every replica, if our local health checker says a replica is
// healthy or unhealthy, we consider replica healthy or unhealthy respectively.
// If our health checker is unsure of the health status, or we don't know about
// the replica at all, we leave its health status unchanged.
func (a *Assigner) healthyReplicas(rid *ReplicaSetId, rs *ReplicaSetInfo) ([]string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Update the health status of every replica.
	changed := false
	for addr, replicaProto := range rs.Pods {
		replicas, ok := a.replicas[unprotoRid(rid)]
		if !ok {
			continue
		}
		replica, ok := replicas[addr]
		if !ok {
			continue
		}
		if status := replica.health.status(); status == protos.HealthStatus_HEALTHY {
			changed = changed || (replicaProto.HealthStatus != protos.HealthStatus_HEALTHY)
			replicaProto.HealthStatus = status
		} else if status == protos.HealthStatus_UNHEALTHY {
			changed = changed || (replicaProto.HealthStatus != protos.HealthStatus_UNHEALTHY)
			replicaProto.HealthStatus = status
		} else if status == protos.HealthStatus_TERMINATED {
			changed = changed || (replicaProto.HealthStatus != protos.HealthStatus_TERMINATED)
			replicaProto.HealthStatus = status
		}
	}

	// Gather the set of healthy replicas.
	var healthyReplicas []string
	for addr, replica := range rs.Pods {
		if replica.HealthStatus == protos.HealthStatus_HEALTHY {
			healthyReplicas = append(healthyReplicas, addr)
		}
	}
	sort.Strings(healthyReplicas)
	return healthyReplicas, changed
}

func (a *Assigner) writeRoutingInfoInStore(rid *ReplicaSetId, info *VersionedRoutingInfo) error {
	// Record the routing info key for later garbage collection.
	uuid, err := uuid.Parse(rid.Id)
	if err != nil {
		return fmt.Errorf("invalid id %q: %w", rid.Id, err)
	}
	key := store.ReplicaSetKey(rid.App, uuid, rid.Name, routingInfoKey)
	histKey := store.DeploymentKey(rid.App, uuid, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}

	// Update the routing info.
	existing := &VersionedRoutingInfo{}
	_, err = store.UpdateProto(a.ctx, a.store, key, existing, func(*store.Version) error {
		if existing.Version >= info.Version {
			// Our routing info is stale.
			return store.ErrUnchanged
		}
		existing.Reset()
		proto.Merge(existing, info)
		return nil
	})
	if errors.Is(err, store.ErrUnchanged) {
		err = nil
	}
	return err
}

// anneal runs an infinite loop that (1) repeatedly attempts to advance the
// current set of assignments and (2) reconciles differences between the
// persisted state and an assigner's local state.
func (a *Assigner) anneal() error {
	// pick samples a time uniformly from [1.2i, 1.4i] where i is
	// LoadReportInterval. We introduce jitter to avoid concurrently running
	// assigners from trying to write to the store at the same time.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pick := func() time.Duration {
		const i = float64(clients.LoadReportInterval)
		const low = int64(i * 1.2)
		const high = int64(i * 1.4)
		return time.Duration(r.Int63n(high-low+1) + low)
	}

	assignmentsTicker := time.NewTicker(pick())
	checkersTicker := time.NewTicker(3 * healthCheckInterval)
	defer assignmentsTicker.Stop()
	defer checkersTicker.Stop()

	for {
		select {
		case <-assignmentsTicker.C:
			assignmentsTicker.Reset(pick())
			if err := a.advanceAssignments(a.ctx); err != nil {
				a.logger.Error("anneal assignments", err)
			}
		case <-checkersTicker.C:
			if err := a.annealCheckers(a.ctx); err != nil {
				a.logger.Error("anneal checkers", err)
			}
		case <-a.ctx.Done():
			return a.ctx.Err()
		}
	}
}

// advanceAssignments attempts to advance the current set of assignments.
func (a *Assigner) advanceAssignments(ctx context.Context) error {
	state, version, err := a.loadState(ctx)
	if err != nil {
		return fmt.Errorf("load assigner state: %w", err)
	}
	if *version == store.Missing {
		return fmt.Errorf("assigner state missing")
	}

	var errs []error
	for _, app := range state.Applications {
		appState, version, err := a.loadAppState(ctx, app)
		if err != nil {
			errs = append(errs, fmt.Errorf("load app %q state: %w", app, err))
			continue
		}
		if *version == store.Missing {
			errs = append(errs, fmt.Errorf("app %q state missing", app))
			continue
		}

		for _, rid := range appState.ReplicaSetIds {
			if err := a.mayGenerateNewRoutingInfo(ctx, rid); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// annealCheckers attempts to move the actual set of health checkers towards
// the expected set of checkers.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) annealCheckers(ctx context.Context) error {
	// Load the assigner's state.
	state, version, err := a.loadState(ctx)
	if err != nil {
		return fmt.Errorf("load assigner state: %w", err)
	}
	if *version == store.Missing {
		return fmt.Errorf("assigner state missing")
	}

	// Anneal every app.
	var errs []error
	for _, app := range state.Applications {
		if err := a.annealCheckersForApp(ctx, app); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// annealCheckersForApp attempts to move the actual set of checkers towards the
// expected set of checkers for the provided app.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) annealCheckersForApp(ctx context.Context, app string) error {
	// Load the app's state.
	state, version, err := a.loadAppState(ctx, app)
	if err != nil {
		return fmt.Errorf("load app %q state: %w", app, err)
	}
	if *version == store.Missing {
		return fmt.Errorf("app %q state missing", app)
	}

	// Delete any stale checkers.
	a.deleteCheckers(app, state)

	// Add any new checkers.
	var errs []error
	for _, rid := range state.ReplicaSetIds {
		replicaSet, version, err := a.loadReplicaSetInfo(ctx, rid)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if *version == store.Missing {
			// If we register a ReplicaSet but no components or replicas, the
			// ReplicaSet doesn't get a corresponding ReplicaSet info.
			// That is okay.
			a.logger.Debug("info missing", "rid", rid)
			continue
		}
		a.updateCheckers(rid, replicaSet, protos.HealthStatus_UNKNOWN)
	}
	return errors.Join(errs...)
}

// deleteCheckers stops and deletes any health checkers that are no longer
// present in the provided app state. When an application version is deleted,
// all corresponding ReplicaSets are removed from the app state. The checkers
// for these ReplicaSets are stopped and deleted.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) deleteCheckers(app string, state *AppState) {
	rids := map[replicaSetId]bool{}
	for _, rid := range state.ReplicaSetIds {
		rids[unprotoRid(rid)] = true
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for rid, replicas := range a.replicas {
		if rid.app != app {
			// Don't delete checkers for other apps.
			continue
		}
		if _, found := rids[rid]; found {
			continue
		}
		for _, replica := range replicas {
			replica.cancel()
		}
		delete(a.replicas, rid)
	}
}

// updateCheckers updates the health checkers for all of the replicas in the
// provided ReplicaSet. It creates new health checkers, skips the replicas for
// which we already have a checker, and deletes checkers for terminated replicas.
//
// The new health checkers begin with the provided health status.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) updateCheckers(rid *ReplicaSetId, rs *ReplicaSetInfo, health protos.HealthStatus) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for addr, replica := range rs.Pods {
		if replica.HealthStatus == protos.HealthStatus_TERMINATED {
			// Delete any active health checker for the terminated replica.
			replicas := a.replicas[unprotoRid(rid)]
			if replicas == nil {
				continue
			}
			r := replicas[addr]
			if r != nil {
				delete(replicas, addr)
				r.cancel()
			}
		} else {
			a.addCheckerLocked(rid, addr, replica.Name, replica.BabysitterAddress, health)
		}
	}
	return nil
}

// addChecker creates a new health checker for the provided replica, if one
// does not already exist. The new health checker begins with the provided
// health status.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) addChecker(rid *ReplicaSetId, addr, podName, babysitterAddr string, health protos.HealthStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.addCheckerLocked(rid, addr, podName, babysitterAddr, health)
}

// addCheckerLocked creates a new health checker for the provided replica, if
// one does not already exist. The new health checker begins with the provided
// health status.
//
// REQUIRES: a.mu is held.
func (a *Assigner) addCheckerLocked(rid *ReplicaSetId, addr, podName, babysitterAddr string, health protos.HealthStatus) {
	replicas, ok := a.replicas[unprotoRid(rid)]
	if !ok {
		replicas = map[string]*replica{}
		a.replicas[unprotoRid(rid)] = replicas
	}

	if _, found := replicas[addr]; found {
		// The checker already exists.
		return
	}

	ctx, cancel := context.WithCancel(a.ctx)
	replica := &replica{
		addr:   addr,
		health: newHealthTracker(health),
		cancel: cancel,
	}
	// TODO(mwhittaker): Handle errors returned by healthCheck.
	go a.healthCheck(ctx, rid, replica, podName, babysitterAddr)
	replicas[addr] = replica
}

// healthCheck checks the health of a weavelet, updating the health of the
// provided replica accordingly.
func (a *Assigner) healthCheck(ctx context.Context, rid *ReplicaSetId, replica *replica, podName, babysitterAddr string) error {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	req := &protos.GetHealthRequest{}
	babysitter := a.babysitterConstructor(babysitterAddr)
	if babysitter == nil {
		return fmt.Errorf("nil babysitter: %s", babysitterAddr)
	}

	for {
		select {
		case <-ticker.C:
			// Check the babysitter.
			healthCtx, cancel := context.WithTimeout(ctx, healthCheckInterval)
			var status protos.HealthStatus
			reply, err := babysitter.CheckHealth(healthCtx, req)
			if err != nil {
				// Babysitter is unreachable, set status as unhealthy.
				status = protos.HealthStatus_UNHEALTHY
			} else {
				status = reply.Status
			}
			cancel()

			// If the received status is unhealthy and it's been a while since
			// we have received a healthy report, we should check whether the
			// replica still exists. If not, we should mark it as terminated.
			shouldCheckIfExists := time.Since(replica.health.lastTimeRecvdHealthy) >= timeToCheckIfReplicaExists
			if status == protos.HealthStatus_UNHEALTHY && shouldCheckIfExists {
				exists, err := a.replicaExists(ctx, podName)
				if err != nil {
					a.logger.Error("cannot check if replica Pod exists", err, "pod", podName)
				} else if !exists {
					status = protos.HealthStatus_TERMINATED
				}
			}

			// Update the health status, generating a new assignment if needed.
			//
			// TODO(mwhittaker): When an assigner starts up, there will be
			// replicas for which it doesn't know the health. Add logic to
			// handle this better.
			if replica.health.report(status) {
				a.mayGenerateNewRoutingInfo(ctx, rid)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// unprotoRid converts a ReplicaSetId to a replicaSetId.
func unprotoRid(rid *ReplicaSetId) replicaSetId {
	return replicaSetId{app: rid.App, id: rid.Id, name: rid.Name}
}

func findOrCreateReplicaSet(state *AppVersionState, replicaSet string) *ReplicaSetState {
	if state.ReplicaSets == nil {
		state.ReplicaSets = map[string]*ReplicaSetState{}
	}
	rs, ok := state.ReplicaSets[replicaSet]
	if !ok {
		rs = &ReplicaSetState{Name: replicaSet}
		state.ReplicaSets[replicaSet] = rs
	}
	return rs
}
