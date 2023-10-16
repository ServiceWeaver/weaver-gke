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
	"log/slog"
	"math/rand"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"golang.org/x/exp/slices"
)

// An assigner is responsible for two things, one minor and one major. First,
// and more minor, the assigner persists information about active components
// and replica sets for a given application version. Babysitters watch this
// information to know which components to start. We don't elaborate on this,
// as it involves straightforward reads and writes to the store.
//
// Second, and more major, the assigner is responsible for computing routing
// assignments for all components. To create routing assignments, the assigner
// needs to know two critical pieces of information: the load on every weavelet
// and the health of every weavelet. If the load on a component changes or if
// a weavelet crashes, the assigner needs to react and generate a new assignment.
//
// === Persisted State ===
//
// The assigner persists most of its state in the store. In particular, the
// components and routing information for a given replica set are stored
// under the key:
//   /assigner/app/$APP/version/$APP_VERSION/replica_set/$REPLICA_SET/state
//
// The assigner stores most of its state in the store, but not everything. The
// assigner monitors the health of every weavelet by probing its /healthz server
// periodically. Storing this information in the store would be prohibitively
// expensive. Instead, every assigner (remember that the assigner can be
// replicated) independently computes the health of every replica, keeping the
// health information in memory.

// Assigner generates routing information for a set of managed Kubernetes
// ReplicaSets.
//
// In the current implementation, the assigner generates new routing information
// when (1) new components become managed by the ReplicaSet, or (2) the load
// changes for a given Pod, or (3) the ReplicaSet acquires new Pods.
type Assigner struct {
	ctx    context.Context
	store  store.Store
	logger *slog.Logger
	algo   Algorithm

	// How often to update routing information for each component? If zero,
	// routing information isn't updated.
	updateRoutingInterval time.Duration

	// getHealthyPods returns information about the current set of healthy
	// Pods for a given replica set.
	getHealthyPods func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error)
}

func NewAssigner(
	ctx context.Context,
	store store.Store,
	logger *slog.Logger,
	algo Algorithm,
	updateRoutingInterval time.Duration,
	getHealthyPods func(ctx context.Context, cfg *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error)) *Assigner {
	a := &Assigner{
		ctx:                   ctx,
		store:                 store,
		logger:                logger,
		algo:                  algo,
		updateRoutingInterval: updateRoutingInterval,
		getHealthyPods:        getHealthyPods,
	}
	go func() {
		if err := a.anneal(); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "assigner anneal: %v\n", err)
			os.Exit(1)
		}
	}()
	return a
}

// RegisterComponent registers a given activated component.
func (a *Assigner) RegisterComponent(ctx context.Context, req *nanny.ActivateComponentRequest) error {
	// Add the component to the replica set state, if not already added.
	rsKey := replicaSetStateKey(req.Config, req.ReplicaSet)
	_, _, err := applyProto(ctx, a.store, rsKey, func(state *ReplicaSetState, v *store.Version) bool {
		var modified bool
		if *v == store.Missing { // first time seeing this ReplicaSet
			state.ReplicaSet = &nanny.ReplicaSet{
				Name:   req.ReplicaSet,
				Config: req.Config,
			}
			modified = true
		}
		if !slices.Contains(state.ReplicaSet.Components, req.Component) {
			state.ReplicaSet.Components = append(state.ReplicaSet.Components, req.Component)
			modified = true
		}
		if _, ok := state.RoutingAssignments[req.Component]; !ok && req.Routed {
			if state.RoutingAssignments == nil {
				state.RoutingAssignments = map[string]*Assignment{}
			}
			state.RoutingAssignments[req.Component] = &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			}
			modified = true
		}
		return modified
	})
	return err
}

// RegisterListener registers a given instantiated listener.
func (a *Assigner) RegisterListener(ctx context.Context, req *nanny.ExportListenerRequest) error {
	rsKey := replicaSetStateKey(req.Config, req.ReplicaSet)
	_, _, err := applyProto(ctx, a.store, rsKey, func(state *ReplicaSetState, v *store.Version) bool {
		var modified bool
		if *v == store.Missing { // first time seeing this ReplicaSet
			state.ReplicaSet = &nanny.ReplicaSet{
				Name:   req.ReplicaSet,
				Config: req.Config,
			}
			modified = true
		}
		if !slices.Contains(state.ReplicaSet.Listeners, req.Listener.Name) {
			state.ReplicaSet.Listeners = append(state.ReplicaSet.Listeners, req.Listener.Name)
			modified = true
		}
		return modified
	})
	return err
}

// StopAppVersion stops all work for the given app version and cleans up its
// state.
//
// If the method returns an error, it should be invoked again as the operation
// likely hasn't been fully applied.
//
// TODO(mwhittaker): The store deletions below race any operations that
// write to the store. We should make sure that every operation that could
// potentially write to the store first checks to see if the version has
// been stopped. This is complicated by the fact that the store writes are
// spread between the assigner, manager, the gke deployer, and the
// gke-local deployer.
func (a *Assigner) StopAppVersion(ctx context.Context, app, appVersion string) error {
	// Get the current set of (replica set) keys for the application version.
	opts := store.ListOptions{
		Prefix: path.Join("/assigner", "app", app, "version", appVersion),
	}
	keys, err := a.store.List(a.ctx, opts)
	if err != nil {
		return fmt.Errorf("cannot get keys for app %s version %s, will retry: %w", app, appVersion, err)
	}
	var errs []error
	for _, key := range keys {
		if err := a.store.Delete(a.ctx, key); err != nil {
			errs = append(errs, fmt.Errorf("cannot delete key %q, will retry: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// GetRoutingInfo returns the routing information for a given component.
// This call will block if there have been no routing changes since the
// last call to GetRoutingInfo().
func (a *Assigner) GetRoutingInfo(req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	// Keep retrying as long as we keep getting spurious store.Unchanged errors.
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
	rsKey := replicaSetStateKey(req.Config, req.ReplicaSet)
	var prevVersion *store.Version
	if req.Version != "" {
		prevVersion = &store.Version{Opaque: req.Version}
	}

	state := &ReplicaSetState{}
	newVersion, err := store.GetProto(a.ctx, a.store, rsKey, state, prevVersion)
	if err != nil {
		return nil, err
	}
	if *newVersion == store.Missing { // Replica set doesn't (yet) exist.
		return &nanny.GetRoutingReply{
			Version: newVersion.Opaque,
			Routing: &protos.RoutingInfo{},
		}, nil
	}
	var weaveletAddrs []string
	if len(state.ReplicaSet.Pods) > 0 {
		weaveletAddrs = make([]string, len(state.ReplicaSet.Pods))
		for i, pod := range state.ReplicaSet.Pods {
			weaveletAddrs[i] = pod.WeaveletAddr
		}
	}
	return &nanny.GetRoutingReply{
		Version: newVersion.Opaque,
		Routing: &protos.RoutingInfo{
			Component:  req.Component,
			Replicas:   weaveletAddrs,
			Assignment: toProto(state.RoutingAssignments[req.Component]),
		},
	}, nil
}

// GetComponentsToStart returns the set of components the given replica
// set should start.
// This call will block if there have been no component changes since the
// last call to GetComponentsToStart().
func (a *Assigner) GetComponentsToStart(req *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	// Keep retrying as long as we keep getting spurious store.Unchanged errors.
	for r := retry.Begin(); r.Continue(a.ctx); {
		reply, err := a.getComponentsToStart(req)
		if err == nil {
			return reply, nil
		}
		if errors.Is(err, store.Unchanged) {
			continue
		}
		return nil, err
	}
	return nil, a.ctx.Err()
}

func (a *Assigner) getComponentsToStart(req *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	rsKey := replicaSetStateKey(req.Config, req.ReplicaSet)
	var prevVersion *store.Version
	if req.Version != "" {
		prevVersion = &store.Version{Opaque: req.Version}
	}
	state := &ReplicaSetState{}
	newVersion, err := store.GetProto(a.ctx, a.store, rsKey, state, prevVersion)
	if err != nil {
		return nil, err
	}
	if *newVersion == store.Missing {
		return &nanny.GetComponentsReply{Version: newVersion.Opaque}, nil
	}
	return &nanny.GetComponentsReply{
		Version:    newVersion.Opaque,
		Components: state.ReplicaSet.Components,
	}, nil
}

// GetReplicaSets returns the state of all Kubernetes ReplicaSets for an
// application version or a collection of applications and their versions.
func (a *Assigner) GetReplicaSets(ctx context.Context, req *nanny.GetReplicaSetsRequest) (*nanny.GetReplicaSetsReply, error) {
	if req.AppName == "" && req.VersionId != "" {
		return nil, fmt.Errorf("invalid request")
	}
	prefix := "/assigner"
	if req.AppName != "" {
		prefix = path.Join(prefix, "app", req.AppName)
	}
	if req.VersionId != "" {
		prefix = path.Join(prefix, "version", req.VersionId)
	}
	keys, err := a.store.List(ctx, store.ListOptions{Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("cannot list replica set keys with prefix %s: %w", prefix, err)
	}
	var reply nanny.GetReplicaSetsReply
	var errs []error
	for _, key := range keys {
		state := &ReplicaSetState{}
		version, err := store.GetProto(a.ctx, a.store, key, state, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("cannot read replica set with key %s: %w", key, err))
		}
		if *version == store.Missing {
			// Key deleted since the call to List: ignore it.
			continue
		}
		reply.ReplicaSets = append(reply.ReplicaSets, state.ReplicaSet)
	}
	if errs != nil {
		return nil, errors.Join(errs...)
	}
	slices.SortFunc(reply.ReplicaSets, func(a, b *nanny.ReplicaSet) bool {
		return a.Name < b.Name
	})
	return &reply, nil
}

// anneal runs an infinite loop that repeatedly attempts to advance the
// current set of routing assignments.
func (a *Assigner) anneal() error {
	if a.updateRoutingInterval == 0 { // annealing disabled
		return nil
	}

	// pick samples a time uniformly from [1.2i, 1.4i] where i is
	// LoadReportInterval. We introduce jitter to avoid concurrently running
	// assigners from trying to write to the store at the same time.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pick := func() time.Duration {
		i := float64(a.updateRoutingInterval)
		low := int64(i * 1.2)
		high := int64(i * 1.4)
		return time.Duration(r.Int63n(high-low+1) + low)
	}
	ticker := time.NewTicker(pick())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ticker.Reset(pick())
			if err := a.annealRouting(a.ctx); err != nil {
				a.logger.Error("anneal routing infos", "err", err)
			}
		case <-a.ctx.Done():
			return a.ctx.Err()
		}
	}
}

func (a *Assigner) annealRouting(ctx context.Context) error {
	rsKeys, err := a.store.List(ctx, store.ListOptions{Prefix: "/assigner"})
	if err != nil {
		return fmt.Errorf("cannot list replica set keys, will retry: %w", err)
	}

	const maxParallelism = 32
	ch := make(chan struct{}, maxParallelism)
	var wait sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, rsKey := range rsKeys {
		rsKey := rsKey
		ch <- struct{}{}
		wait.Add(1)
		go func() {
			defer func() {
				<-ch
				wait.Done()
			}()
			if err := a.annealRoutingForReplicaSet(ctx, rsKey); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
		}()
	}
	wait.Wait()
	return errors.Join(errs...)
}

func (a *Assigner) annealRoutingForReplicaSet(ctx context.Context, key string) error {
	a.logger.Debug("Anneal routing info", "key", key)

	// Fetch an updated set of healthy newPods and their load information.
	// We do this outside of the update loop below, since it is an
	// expensive operation that involves network calls to the weavelets.
	var newPods []*nanny.Pod
	{
		state := &ReplicaSetState{}
		v, err := store.GetProto(ctx, a.store, key, state, nil)
		if err != nil {
			return fmt.Errorf("cannot read replica set with key %s: %w", key, err)
		}
		if *v == store.Missing { // key deleted
			return nil
		}
		if newPods, err = a.getHealthyPods(ctx, state.ReplicaSet.Config, state.ReplicaSet.Name); err != nil {
			return err
		}
	}
	slices.SortFunc(newPods, func(a, b *nanny.Pod) bool {
		if a.WeaveletAddr == b.WeaveletAddr {
			return a.BabysitterAddr < b.BabysitterAddr
		}
		return a.WeaveletAddr < b.WeaveletAddr
	})
	newWeaveletAddrs := make([]string, len(newPods))
	for i, pod := range newPods {
		newWeaveletAddrs[i] = pod.WeaveletAddr
	}
	sort.Strings(newWeaveletAddrs)

	_, _, err := applyProto(ctx, a.store, key, func(state *ReplicaSetState, v *store.Version) bool {
		if *v == store.Missing { // key deleted
			return false
		}

		var modified bool

		// Update the set of pods, if necessary.
		if !slices.EqualFunc(state.ReplicaSet.Pods, newPods, func(a, b *nanny.Pod) bool {
			// Ignore the load information, since it changes all the time
			// and doesn't always affect routing assignments.
			return a.WeaveletAddr == b.WeaveletAddr && a.BabysitterAddr == b.BabysitterAddr
		}) {
			modified = true
			state.ReplicaSet.Pods = newPods
		}

		// Update load information in the routing assignments.
		for _, pod := range newPods {
			if pod.Load == nil {
				continue
			}
			for c, load := range pod.Load.Loads {
				// Ignore load reports for unknown components.
				assignment, ok := state.RoutingAssignments[c]
				if !ok {
					a.logger.Debug(fmt.Sprintf("anneal replica set %s: load update for not-yet-activated component %s; dropping the update", state.ReplicaSet.Name, c))
					continue
				}
				if assignment == nil {
					panic(fmt.Sprintf("anneal replica set %s: nil assignment for component %s", state.ReplicaSet.Name, c))
				}

				// Ignore load reports if the versions don't match.
				if assignment.Version != load.Version {
					a.logger.Debug(fmt.Sprintf("anneal replica set %s: load update version %v doesn't match the assignment version %v for component %s; dropping the update", state.ReplicaSet.Name, load.Version, assignment.Version, c))
					continue
				}

				// TODO(mwhittaker): Ignore stale load updates.
				if err := updateLoad(assignment, pod.WeaveletAddr, load); err != nil {
					a.logger.Error(fmt.Sprintf("anneal replica set %s: cannot apply load update for component %s; dropping the update", state.ReplicaSet.Name, c), "error", err)
					continue
				}
			}
		}

		// Re-compute routing assignments.
		for c, currAssignment := range state.RoutingAssignments {
			updateCandidateResources(currAssignment, newWeaveletAddrs)
			if !shouldChange(currAssignment) {
				continue
			}
			modified = true
			newAssignment, err := a.algo(currAssignment)
			if err != nil {
				a.logger.Error(fmt.Sprintf("anneal replica set %s: cannot compute new assignment for component %s; preserving the old assignment", state.ReplicaSet.Name, c), "error", err)
			}
			if newAssignment == nil {
				panic(fmt.Sprintf("anneal replica set %s: computed nil assignment for component %s", state.ReplicaSet.Name, c))
			}
			state.RoutingAssignments[c] = newAssignment
		}

		return modified
	})
	return err
}
