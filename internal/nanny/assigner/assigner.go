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
	"github.com/ServiceWeaver/weaver-gke/internal/errlist"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// An assigner is responsible for two things, one minor and one major. First,
// and more minor, the assigner persists all of the groups and components that
// should be started. Babysitters and weavelets watch this information to know
// which groups and components to start. We don't elaborate on this, as it
// involves straightforward reads and writes to the store.
//
// Second, and more major, the assigner is responsible for computing routing
// assignments for all components. To create routing assignments, the assigner
// needs to know two critical pieces of information: the load on every replica
// of a component and the health of every group replica. If the load on
// a component changes or if a replica crashes, the assigner needs to react and
// generate a new assignment.
//
// === Persisted State ===
//
// The assigner persists *most* of its state in the store. The /assigner_state
// key stores a list of all applications. For each application A, the key
// /assigner/application/A stores the ids of every colocation group in every
// version of the app. For every colocation group, the key
// /app/$APP/deployment/$ID/group/$PROC/group_info stores information about
// the group. Specifically, it stores the routing assignments for all the
// components in the group, the load on every replica under the current
// assignment, and a list of all the group replicas.
//
//     /assigner_state -> ["todo", "chat"]
//     /assigner/application/todo -> [v1.Todo, v1.Store]
//     /assigner/application/chat -> [v2.Chat, v2.Cache]
//     /app/todo/deployment/v1/group/Todo/group_info  -> ...
//     /app/todo/deployment/v1/group/Store/group_info -> ...
//     /app/chat/deployment/v2/group/Chat/group_info  -> ...
//     /app/chat/deployment/v2/group/Cache/group_info -> ...
//
// The assigner stores most of its state in the store, but not everything. The
// assigner monitors the health of every replica by probing its /healthz server
// periodically. Storing this information in the store would be prohibitively
// expensive. Instead, every assigner (remember that the assigner can be
// replicated) independently computes the health of every replica, keeping the
// health information in memory.
//
// TODO(mwhittaker): Implement a smarter way to consolidate different
// assigner's view of health and explain it briefly here.

const (
	// RoutingInfoKey is the key where we store routing information for a given
	// colocation group.
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

// Assigner generates routing information for a set of managed colocation groups.
//
// In the current implementation, the assigner generates new routing information
// when (1) new sharded components are managed by the group and/or when (2) the
// group has new replicas.
//
// TODO(rgrandl): ensure correctness when multiple Assigner instances may try
// to generate routing information at the same time.
type Assigner struct {
	// TODO(mwhittaker): Pass in a logger.
	ctx                   context.Context
	store                 store.Store
	logger                *logging.FuncLogger
	algo                  Algorithm
	babysitterConstructor func(addr string) clients.BabysitterClient
	replicaExists         func(ctx context.Context, podName string) (bool, error)

	// replicas stores the set of all colocation group replicas, grouped by
	// groups id and then indexed by weavelet address.
	mu       sync.RWMutex
	replicas map[groupId]map[string]*replica
}

// groupId uniquely identifies a colocation group.
//
// NOTE that groupId is a clone of GroupId, but we duplicate the definition
// here in a plain struct so that we can use groupIds as keys in maps.
type groupId struct {
	app   string
	id    string
	group string
}

// replica includes information about a single replica (i.e. weavelet).
//
// NOTE that replica stores assigner local replica state, while Replica stores
// state shared across all assigners.
type replica struct {
	addr   string             // internal listener address for the replica
	health *healthTracker     // health tracker
	cancel context.CancelFunc // cancels health checker
}

func NewAssigner(
	ctx context.Context,
	store store.Store,
	logger *logging.FuncLogger,
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
		replicas:              map[groupId]map[string]*replica{},
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

// registerGroup registers the provided group in the appropriate app's
// state.
func (a *Assigner) registerGroup(ctx context.Context, gid *GroupId) error {
	_, _, err := a.applyAppState(ctx, gid.App, func(state *AppState) bool {
		for _, existing := range state.GroupIds {
			if proto.Equal(gid, existing) {
				return false
			}
		}
		state.GroupIds = append(state.GroupIds, gid)
		return true
	})
	if err != nil {
		return fmt.Errorf("register group %v: %v", gid, err)
	}
	return nil
}

func (a *Assigner) RegisterComponentToStart(ctx context.Context, req *protos.ComponentToStart) error {
	id, err := uuid.Parse(req.DeploymentId)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", req.DeploymentId, err)
	}

	var state AppVersionState
	edit := func(*store.Version) error {
		// Register the group if not already registered.
		if state.Groups == nil {
			state.Groups = map[string]*ColocationGroupState{}
		}
		if _, found := state.Groups[req.ColocationGroup]; !found {
			state.Groups[req.ColocationGroup] = &ColocationGroupState{
				Name: req.ColocationGroup,
			}
		}
		group := state.Groups[req.ColocationGroup]
		if len(req.Component) == 0 { // No component to register.
			return nil
		}

		// Register the component if not already registered.
		if group.Components == nil {
			group.Components = map[string]bool{}
		}
		if _, found := group.Components[req.Component]; found {
			// If the component is already registered in the store, then we skip
			// any writes.
			return store.ErrUnchanged
		}
		group.Components[req.Component] = req.IsRouted
		return nil
	}

	// Track the key in the store under histKey.
	key := store.DeploymentKey(req.App, id, appVersionStateKey)
	histKey := store.DeploymentKey(req.App, id, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(a.ctx, a.store, key, &state, edit)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Register the app and group.
	if err := a.registerApp(ctx, req.App); err != nil {
		return err
	}
	gid := &GroupId{App: req.App, Id: req.DeploymentId, Group: req.ColocationGroup}
	if err := a.registerGroup(ctx, gid); err != nil {
		return err
	}

	if !req.IsRouted {
		return nil
	}

	// Create an initial assignment for the component.
	if _, _, err := a.applyGroupInfo(ctx, gid, func(group *GroupInfo) bool {
		if group.Components == nil {
			group.Components = map[string]*Assignment{}
		}
		if _, found := group.Components[req.Component]; found {
			return false
		}
		group.Components[req.Component] = &Assignment{
			App:          gid.App,
			DeploymentId: gid.Id,
			Component:    req.Component,
			Constraints:  &AlgoConstraints{},
			Stats:        &Statistics{},
		}
		group.Version++
		return true
	}); err != nil {
		return err
	}
	return a.mayGenerateNewRoutingInfo(ctx, gid)
}

func (a *Assigner) RegisterReplica(ctx context.Context, req *nanny.ReplicaToRegister) error {
	id, err := uuid.Parse(req.Replica.DeploymentId)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", req.Replica.DeploymentId, err)
	}

	var existing AppVersionState
	edit := func(version *store.Version) error {
		group := findOrCreateGroup(&existing, req.Replica.Group)
		for _, replica := range group.Replicas {
			if req.Replica.Address == replica {
				// If the replica is already registered in the store, then we skip any writes.
				return store.ErrUnchanged
			}
		}
		group.Replicas = append(group.Replicas, req.Replica.Address)
		return nil
	}

	// Track the key in the store under histKey.
	key := store.DeploymentKey(req.Replica.App, id, appVersionStateKey)
	histKey := store.DeploymentKey(req.Replica.App, id, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(a.ctx, a.store, key, &existing, edit)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return err
	}

	// Register the app and group.
	if err := a.registerApp(ctx, req.Replica.App); err != nil {
		return err
	}
	gid := &GroupId{
		App:   req.Replica.App,
		Id:    req.Replica.DeploymentId,
		Group: req.Replica.Group,
	}
	if err := a.registerGroup(ctx, gid); err != nil {
		return err
	}

	// Register the replica.
	if _, _, err := a.applyGroupInfo(ctx, gid, func(group *GroupInfo) bool {
		if group.Replicas == nil {
			group.Replicas = map[string]*Replica{}
		}
		if _, found := group.Replicas[req.Replica.Address]; found {
			return false
		}
		group.Replicas[req.Replica.Address] = &Replica{
			PodName:           req.PodName,
			BabysitterAddress: req.BabysitterAddress,
			HealthStatus:      protos.HealthStatus_HEALTHY,
		}
		group.Version++
		return true
	}); err != nil {
		return err
	}

	a.addChecker(gid, req.Replica.Address, req.PodName, req.BabysitterAddress, protos.HealthStatus_HEALTHY)
	return a.mayGenerateNewRoutingInfo(ctx, gid)
}

func (a *Assigner) RegisterListener(ctx context.Context, dep *protos.Deployment, group *protos.ColocationGroup, lis *protos.Listener) error {
	id, err := uuid.Parse(dep.Id)
	if err != nil {
		return fmt.Errorf("bad version %v: %w", dep.Id, err)
	}
	app := dep.App.Name

	var state AppVersionState
	edit := func(version *store.Version) error {
		group := findOrCreateGroup(&state, group.Name)
		for _, l := range group.Listeners {
			if lis.Name == l.Name { // Already registered.
				return store.ErrUnchanged
			}
		}
		group.Listeners = append(group.Listeners, lis)
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

	// Register the app and group, if needed.
	if err := a.registerApp(ctx, app); err != nil {
		return err
	}
	gid := &GroupId{
		App:   app,
		Id:    dep.Id,
		Group: group.Name,
	}
	if err := a.registerGroup(ctx, gid); err != nil {
		return err
	}

	// Register the listener with the group.
	_, _, err = a.applyGroupInfo(ctx, gid, func(group *GroupInfo) bool {
		for _, l := range group.Listeners {
			if lis.Name == l.Name { // Already registered.
				return false
			}
		}
		group.Listeners = append(group.Listeners, lis)
		group.Version++
		return true
	})
	return err
}

// UnregisterGroups unregister groups that shouldn't be managed by the
// assigner.
func (a *Assigner) UnregisterGroups(ctx context.Context, app string, versions []string) error {
	// TODO(mwhittaker): Make sure this doesn't have races.
	versionSet := map[string]bool{}
	for _, version := range versions {
		versionSet[version] = true
	}

	// Compute the gids we should delete and the gids we should keep.
	appState, version, err := a.loadAppState(ctx, app)
	if err != nil {
		return fmt.Errorf("load app %q state: %w", app, err)
	}
	if *version == store.Missing {
		return fmt.Errorf("app %q state missing", app)
	}
	var toDelete []*GroupId
	var toKeep []*GroupId
	for _, gid := range appState.GroupIds {
		if versionSet[gid.Id] {
			toDelete = append(toDelete, gid)
		} else {
			toKeep = append(toKeep, gid)
		}
	}

	// Delete the corresponding GroupInfos and RoutingInfos.
	for _, gid := range toDelete {
		id, err := uuid.Parse(gid.Id)
		if err != nil {
			return fmt.Errorf("group %v invalid id: %w", gid, err)
		}
		key := store.ColocationGroupKey(gid.App, id, gid.Group, routingInfoKey)
		if err := a.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete routing info %v: %w", gid, err)
		}
		if err := a.store.Delete(ctx, gidKey(gid)); err != nil {
			return fmt.Errorf("delete group %v state: %w", gid, err)
		}
	}

	// Update the AppState.
	appState.GroupIds = toKeep
	if _, err := a.saveAppState(ctx, app, appState, version); err != nil {
		return fmt.Errorf("save app %q state: %w", app, err)
	}

	// Stop any stale checkers.
	a.deleteCheckers(app, appState)
	return nil
}

func (a *Assigner) GetRoutingInfo(req *protos.GetRoutingInfo) (*protos.RoutingInfo, error) {
	id, err := uuid.Parse(req.DeploymentId)
	if err != nil {
		return nil, fmt.Errorf("bad version %v: %w", req.DeploymentId, err)
	}

	// Fetch routing info request from the store.
	var reply VersionedRoutingInfo
	key := store.ColocationGroupKey(req.App, id, req.Group, routingInfoKey)

	var v *store.Version
	if req.Version != "" {
		v = &store.Version{Opaque: req.Version}
	}

	newVersion, err := store.GetProto(a.ctx, a.store, key, &reply, v)
	if err != nil {
		if errors.Is(err, store.Unchanged) {
			return &protos.RoutingInfo{Unchanged: true}, nil
		}
		return nil, err
	}
	if reply.Info == nil {
		reply.Info = &protos.RoutingInfo{}
	}
	reply.Info.Version = newVersion.Opaque
	return reply.Info, nil
}

func (a *Assigner) GetComponentsToStart(req *protos.GetComponentsToStart) (
	*protos.ComponentsToStart, error) {
	id, err := uuid.Parse(req.DeploymentId)
	if err != nil {
		return nil, fmt.Errorf("bad version %v: %w", req.DeploymentId, err)
	}

	// Fetch components to start from the store.
	var v *store.Version
	if req.Version != "" {
		v = &store.Version{Opaque: req.Version}
	}

	var state AppVersionState
	key := store.DeploymentKey(req.App, id, appVersionStateKey)
	newVersion, err := store.GetProto(a.ctx, a.store, key, &state, v)
	if err != nil {
		if errors.Is(err, store.Unchanged) {
			return &protos.ComponentsToStart{Unchanged: true}, nil
		}
		return nil, err
	}

	var reply protos.ComponentsToStart
	reply.Version = newVersion.Opaque
	if group, found := state.Groups[req.Group]; found {
		for c := range group.Components {
			reply.Components = append(reply.Components, c)
		}
	}
	return &reply, nil
}

// GetGroupState returns the state of all groups for an application
// version or a collection of applications and their versions.
func (a *Assigner) GetGroupState(ctx context.Context, req *nanny.GroupStateRequest) (*nanny.GroupState, error) {
	if req.AppName != "" {
		return a.getAppGroups(ctx, req.AppName, req.VersionId)
	}
	if req.VersionId != "" {
		return nil, fmt.Errorf("invalid request")
	}
	state, _, err := a.loadState(ctx)
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	var groups []*nanny.GroupState_Group
	var errors []string
	for _, app := range state.Applications {
		ps, err := a.getAppGroups(ctx, app, "" /*appVersion*/)
		if err != nil {
			errors = append(errors, err.Error())
			continue
		}
		groups = append(groups, ps.Groups...)
		errors = append(errors, ps.Errors...)
	}
	if groups == nil && errors != nil {
		// No results but have errors: that's an error.
		return nil, fmt.Errorf("cannot get group state: %v", errors)
	}
	return &nanny.GroupState{
		Groups: groups,
		Errors: errors,
	}, nil
}

// getAppGroups returns the information about the application version's
// colocation groups. If the application version is empty, it returns the
// colocation groups for all versions of the application.
// REQUIRES: appName != ""
func (a *Assigner) getAppGroups(ctx context.Context, appName, appVersion string) (*nanny.GroupState, error) {
	// Load the app's state.
	state, version, err := a.loadAppState(ctx, appName)
	if err != nil {
		return nil, fmt.Errorf("load app %q state: %w", appName, err)
	}
	if *version == store.Missing {
		return nil, fmt.Errorf("app %q state missing", appName)
	}

	// Iterate over all application groups and select those that match
	// the given app version. If the app version is empty, all application
	// groups will be matched.
	gidsByVersion := map[string][]*GroupId{}
	for _, gid := range state.GroupIds {
		if appVersion != "" && gid.Id != appVersion { // mismatched app version
			continue
		}
		gidsByVersion[gid.Id] = append(gidsByVersion[gid.Id], gid)
	}

	var groups []*nanny.GroupState_Group
	var errors []string
	for vid, gids := range gidsByVersion {
		componentsByGroup, err := a.components(ctx, appName, vid)
		if err != nil {
			return nil, err
		}
		for _, gid := range gids {
			group, version, err := a.loadGroupInfo(ctx, gid)
			if err != nil {
				errors = append(errors, err.Error())
				continue
			}
			if *version == store.Missing {
				errors = append(errors, fmt.Sprintf("group %q info missing", gid))
				continue
			}
			var replicas []*nanny.GroupState_Group_Replica
			for addr, replica := range group.Replicas {
				replicas = append(replicas, &nanny.GroupState_Group_Replica{
					WeaveletAddr:   addr,
					BabysitterAddr: replica.BabysitterAddress,
					HealthStatus:   replica.HealthStatus,
				})
			}
			components, ok := componentsByGroup[gid.Group]
			if !ok {
				errors = append(errors, fmt.Sprintf("no components found for group %v", gid))
				continue
			}
			groups = append(groups, &nanny.GroupState_Group{
				Name:       gid.Group,
				Replicas:   replicas,
				Components: components,
				Listeners:  group.Listeners,
			})
		}
	}

	if groups == nil && errors != nil {
		// No results but have errors: that's an error.
		return nil, fmt.Errorf("cannot get group states for app %s: %v", appName, errors)
	}
	return &nanny.GroupState{
		Groups: groups,
		Errors: errors,
	}, nil
}

// components returns the names of the components in the given deployment,
// grouped by the group name.
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
	for group, groupState := range state.Groups {
		components[group] = maps.Keys(groupState.Components)
	}
	return components, nil
}

// OnNewLoadReport handles a new load report received from a replica.
func (a *Assigner) OnNewLoadReport(ctx context.Context, req *protos.WeaveletLoadReport) error {
	gid := &GroupId{App: req.App, Id: req.DeploymentId, Group: req.Group}
	var group GroupInfo
	_, err := store.UpdateProto(ctx, a.store, gidKey(gid), &group, func(version *store.Version) error {
		if *version == store.Missing {
			return fmt.Errorf("group %v missing", gid)
		}

		// Update the group's load information.
		var errs errlist.ErrList
		for c, cLoad := range req.Loads {
			// Ignore load reports for unknown components.
			assignment, found := group.Components[c]
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
			if err := updateLoad(assignment, req.Replica, cLoad); err != nil {
				errs = append(errs, err)
			}
		}
		return errs.ErrorOrNil()
	})
	return err
}

// mayGenerateNewRoutingInfo may generate new routing information for a given group.
func (a *Assigner) mayGenerateNewRoutingInfo(ctx context.Context, gid *GroupId) error {
	// Update the assignments.
	group := &GroupInfo{}
	if _, err := store.UpdateProto(ctx, a.store, gidKey(gid), group, func(version *store.Version) error {
		if *version == store.Missing {
			return fmt.Errorf("group %v not found", gid)
		}

		healthyReplicas, changed := a.healthyReplicas(gid, group)
		for component, currAssignment := range group.Components {
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
			group.Components[component] = newAssignment
			changed = true
		}
		if !changed {
			return store.ErrUnchanged
		}
		group.Version++
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
	for addr, replica := range group.Replicas {
		if replica.HealthStatus == protos.HealthStatus_HEALTHY {
			healthyReplicas = append(healthyReplicas, addr)
		}
	}
	sort.Strings(healthyReplicas)

	routingInfo := protos.RoutingInfo{
		Replicas: healthyReplicas,
	}
	for _, assignment := range group.Components {
		routingInfo.Assignments = append(routingInfo.Assignments, toProto(assignment))
	}

	// Save the routing information.
	return a.writeRoutingInfoInStore(gid, group, &routingInfo)
}

// healthyReplicas updates and returns the set of healthy replicas, along with
// a bool indicating whether the set of healthy replicas changed.
//
// Recall that every assigner independently health checks every weavelet, so
// there is no single source of truth for the set of healthy replicas.
// healthyReplicas returns the set of replicas that this assigner thinks are
// healthy. For every replica of the provided group, if our local health
// checker says a replica is healthy or unhealthy, we consider replica healthy
// or unhealthy respectively. If our health checker is unsure of the health
// status, or we don't know about the replica at all, we leave its health
// status unchanged.
func (a *Assigner) healthyReplicas(gid *GroupId, group *GroupInfo) ([]string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Update the health status of every replica.
	changed := false
	for addr, replicaProto := range group.Replicas {
		replicas, ok := a.replicas[unprotoGid(gid)]
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
	for addr, replica := range group.Replicas {
		if replica.HealthStatus == protos.HealthStatus_HEALTHY {
			healthyReplicas = append(healthyReplicas, addr)
		}
	}
	sort.Strings(healthyReplicas)
	return healthyReplicas, changed
}

func (a *Assigner) writeRoutingInfoInStore(gid *GroupId, group *GroupInfo, routingInfo *protos.RoutingInfo) error {
	// Record the routing info key for later garbage collection.
	uuid, err := uuid.Parse(gid.Id)
	if err != nil {
		return fmt.Errorf("invalid id %q: %w", gid.Id, err)
	}
	key := store.ColocationGroupKey(gid.App, uuid, gid.Group, routingInfoKey)
	histKey := store.DeploymentKey(gid.App, uuid, store.HistoryKey)
	err = store.AddToSet(a.ctx, a.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}

	// Update the routing info.
	existing := &VersionedRoutingInfo{}
	_, err = store.UpdateProto(a.ctx, a.store, key, existing, func(*store.Version) error {
		if existing.Version >= group.Version {
			// Our routing info is stale.
			return store.ErrUnchanged
		}
		existing.Version = group.Version
		existing.Info = routingInfo
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
		const i = float64(runtime.LoadReportInterval)
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

	var errs errlist.ErrList
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

		for _, gid := range appState.GroupIds {
			if err := a.mayGenerateNewRoutingInfo(ctx, gid); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs.ErrorOrNil()
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
	var errs errlist.ErrList
	for _, app := range state.Applications {
		if err := a.annealCheckersForApp(ctx, app); err != nil {
			errs = append(errs, err)
		}
	}
	return errs.ErrorOrNil()
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
	var errs errlist.ErrList
	for _, gid := range state.GroupIds {
		group, version, err := a.loadGroupInfo(ctx, gid)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if *version == store.Missing {
			// If we register a group but no components or replicas, the group
			// doesn't get a corresponding group info. That is okay.
			a.logger.Debug("info missing", "gid", gid)
			continue
		}
		a.updateCheckers(gid, group, protos.HealthStatus_UNKNOWN)
	}
	return errs.ErrorOrNil()
}

// deleteCheckers stops and deletes any health checkers that are no longer
// present in the provided app state. When an application version is deleted,
// all corresponding proccesses are removed from the app state. The checkers
// for these groups are stopped and deleted.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) deleteCheckers(app string, state *AppState) {
	gids := map[groupId]bool{}
	for _, gid := range state.GroupIds {
		gids[unprotoGid(gid)] = true
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for gid, replicas := range a.replicas {
		if gid.app != app {
			// Don't delete checkers for other apps.
			continue
		}
		if _, found := gids[gid]; found {
			continue
		}
		for _, replica := range replicas {
			replica.cancel()
		}
		delete(a.replicas, gid)
	}
}

// updateCheckers updates the health checkers for all of the replicas in the
// provided group; creates new health checkers, skip the replicas for which we
// already have a checker, and deletes checkers for terminated replicas.
//
// The new health checkers begin with the provided health status.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) updateCheckers(gid *GroupId, group *GroupInfo, health protos.HealthStatus) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for addr, replica := range group.Replicas {
		if replica.HealthStatus == protos.HealthStatus_TERMINATED {
			// Delete any active health checker for the terminated replica.
			replicas := a.replicas[unprotoGid(gid)]
			if replicas == nil {
				continue
			}
			r := replicas[addr]
			if r != nil {
				delete(replicas, addr)
				r.cancel()
			}
		} else {
			a.addCheckerLocked(gid, addr, replica.PodName, replica.BabysitterAddress, health)
		}
	}
	return nil
}

// addChecker creates a new health checker for the provided replica, if one
// does not already exist. The new health checker begins with the provided
// health status.
//
// REQUIRES: a.mu is NOT held.
func (a *Assigner) addChecker(gid *GroupId, addr, podName, babysitterAddr string, health protos.HealthStatus) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.addCheckerLocked(gid, addr, podName, babysitterAddr, health)
}

// addCheckerLocked creates a new health checker for the provided replica, if
// one does not already exist. The new health checker begins with the provided
// health status.
//
// REQUIRES: a.mu is held.
func (a *Assigner) addCheckerLocked(gid *GroupId, addr, podName, babysitterAddr string, health protos.HealthStatus) {
	replicas, ok := a.replicas[unprotoGid(gid)]
	if !ok {
		replicas = map[string]*replica{}
		a.replicas[unprotoGid(gid)] = replicas
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
	go a.healthCheck(ctx, gid, replica, podName, babysitterAddr)
	replicas[addr] = replica
}

// healthCheck checks the health of a group replica, updating the health of the
// provided replica accordingly.
func (a *Assigner) healthCheck(ctx context.Context, gid *GroupId, replica *replica, podName, babysitterAddr string) error {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	req := &clients.HealthCheck{
		Addr:    replica.addr,
		Timeout: durationpb.New(healthCheckInterval),
	}
	babysitter := a.babysitterConstructor(babysitterAddr)
	if babysitter == nil {
		return fmt.Errorf("nil babysitter: %s", babysitterAddr)
	}

	for {
		select {
		case <-ticker.C:
			// Check the babysitter.
			var status protos.HealthStatus
			reply, err := babysitter.CheckHealth(ctx, req)
			if err != nil {
				// Babysitter is unreachable, set status as unhealthy.
				status = protos.HealthStatus_UNHEALTHY
			} else {
				status = reply.Status
			}

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
				a.mayGenerateNewRoutingInfo(ctx, gid)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// unprotoGid converts a GroupId to a groupId.
func unprotoGid(gid *GroupId) groupId {
	return groupId{app: gid.App, id: gid.Id, group: gid.Group}
}

func findOrCreateGroup(ap *AppVersionState, group string) *ColocationGroupState {
	if ap.Groups == nil {
		ap.Groups = map[string]*ColocationGroupState{}
	}
	if _, found := ap.Groups[group]; !found {
		ap.Groups[group] = &ColocationGroupState{Name: group}
	}
	return ap.Groups[group]
}
