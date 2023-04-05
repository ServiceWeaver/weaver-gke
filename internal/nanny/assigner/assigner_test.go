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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
)

// op is an operation performed against an assigner.
type op interface {
	do(context.Context, *Assigner) error
}

type registerReplica struct {
	app, id, replicaSet string
	replica             string
}

type registerComponent struct {
	app, id    string
	component  string
	routed     bool
	replicaSet string // ReplicaSet name, if different from component.
}

type registerListener struct {
	app, id, replicaSet string
	listener            string
}

type reportHealth struct {
	app, id, replicaSet string
	replica             string
	health              []protos.HealthStatus
}

type checkRoutingInfo struct {
	app, id, component string
	replicaSet         string // ReplicaSet name, if different from component.
	wantAssignment     bool
	replicas           []string
}

type checkReplicaSetState struct {
	app, id     string
	replicaSets map[string]replicaSetState
}

type replicaSetState struct {
	Replicas   []string
	Components []string
	Listeners  []string
}

func (r registerReplica) do(ctx context.Context, assigner *Assigner) error {
	return assigner.RegisterReplica(ctx, &nanny.RegisterReplicaRequest{
		ReplicaSet:      r.replicaSet,
		WeaveletAddress: r.replica,
		Config:          makeConfig(r.app, r.id),
	})
}

func (r registerComponent) do(ctx context.Context, assigner *Assigner) error {
	return assigner.RegisterActiveComponent(ctx, &nanny.ActivateComponentRequest{
		Component: r.component,
		Routed:    r.routed,
		Config:    makeConfig(r.app, r.id, r.replicaSet, r.component),
	})
}

func (r registerListener) do(ctx context.Context, assigner *Assigner) error {
	return assigner.RegisterListener(ctx, &nanny.ExportListenerRequest{
		ReplicaSet: r.replicaSet,
		Listener:   &nanny.Listener{Name: r.listener},
		Config:     makeConfig(r.app, r.id),
	})
}

func (r reportHealth) do(ctx context.Context, assigner *Assigner) error {
	// Give the assigner a chance to learn about weavelets and create probers
	// for them. It's pointless to report the health for a ReplicaSet that a
	// weavelet doesn't know about.
	if err := assigner.annealCheckers(ctx); err != nil {
		return err
	}

	for _, h := range r.health {
		rid := &ReplicaSetId{App: r.app, Id: r.id, Name: r.replicaSet}
		if err := reportHealthStatus(ctx, assigner, rid, r.replica, h); err != nil {
			return err
		}
	}
	return nil
}

func (c checkRoutingInfo) do(_ context.Context, assigner *Assigner) error {
	info, err := assigner.GetRoutingInfo(&nanny.GetRoutingRequest{
		Component: c.component,
		Version:   "",
		Config:    makeConfig(c.app, c.id, c.replicaSet, c.component),
	})
	if err != nil {
		return err
	}
	if info == nil || info.Routing == nil {
		return fmt.Errorf("nil routing info")
	}

	// Check the replicas.
	less := func(x, y string) bool { return x < y }
	if diff := cmp.Diff(c.replicas, info.Routing.Replicas, cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad replicas (-want +got)\n%s", diff)
	}

	// Check the assignment.
	actualAssignment := info.Routing.Assignment != nil
	if c.wantAssignment != actualAssignment {
		return fmt.Errorf("has assignment diff, want %v, got %v", c.wantAssignment, actualAssignment)
	}
	if !c.wantAssignment {
		return nil
	}
	assignment, err := FromProto(info.Routing.Assignment)
	if err != nil {
		return err
	}
	if diff := cmp.Diff(c.replicas, AssignedResources(assignment), cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad replicas (-want +got)\n%s", diff)
	}
	return nil
}

func (c checkReplicaSetState) do(ctx context.Context, assigner *Assigner) error {
	state, err := assigner.GetReplicaSetState(ctx, &nanny.GetReplicaSetStateRequest{
		AppName:   c.app,
		VersionId: c.id,
	})
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("nil ReplicaSet state")
	}
	got := map[string]replicaSetState{}
	for _, rs := range state.ReplicaSets {
		var replicas []string
		for _, replica := range rs.Pods {
			health := "healthy"
			if replica.HealthStatus != protos.HealthStatus_HEALTHY {
				health = "un" + health
			}
			replicas = append(replicas, fmt.Sprintf("%s:%s", replica.WeaveletAddr, health))
		}
		got[rs.Name] = replicaSetState{
			Replicas:   replicas,
			Components: rs.Components,
			Listeners:  rs.Listeners,
		}
	}
	less := func(x, y string) bool { return x < y }
	if diff := cmp.Diff(c.replicaSets, got, cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad ReplicaSets (-want +got)\n%s", diff)
	}
	return nil
}

type mockBabysitterClient struct{}

var _ clients.BabysitterClient = &mockBabysitterClient{}

func (b mockBabysitterClient) CheckHealth(context.Context, *protos.GetHealthRequest) (*protos.GetHealthReply, error) {
	return &protos.GetHealthReply{Status: protos.HealthStatus_HEALTHY}, nil
}

func (b mockBabysitterClient) RunProfiling(context.Context, *protos.GetProfileRequest) (*protos.GetProfileReply, error) {
	panic("implement me")
}

func makeConfig(app, id string, colocate ...string) *config.GKEConfig {
	var coloc []string
	for _, c := range colocate {
		if c == "" {
			continue
		}
		coloc = append(coloc, c)
	}
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{
				Name:     app,
				Colocate: []*protos.ComponentGroup{{Components: coloc}},
			},
			Id: id,
		},
	}
}

func TestAssigner(t *testing.T) {
	depId := uuid.New().String()

	for _, c := range []struct {
		name       string
		operations []op
	}{
		{
			// Test plan: Register a process replica to start. Verify that
			// routing info is generated, but no assignments.
			name: "register_replica",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					replicas:  []string{"replica1"},
				},
			},
		},
		{
			// Test plan: Register a component to start. Verify that no routing
			// info is generated because no replica was started to manage the
			// component.
			name: "register_component",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
			},
		},
		{
			// Test plan: Register a replica for a ReplicaSet 1 and a component
			// to start but for a different ReplicaSet 2. Verify that routing
			// info is generated for ReplicaSet 1 but not for ReplicaSet 2.
			name: "register_component_for_different_replica_sets",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component2",
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					replicas:  []string{"replica1"},
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component2",
				},
			},
		},
		{
			// Test plan: Register a ReplicaSet to start and a corresponding
			// unrouted component. Verify that no routing information is
			// generated, because the ReplicaSet has no replica.
			name: "register_replica_set_to_start_register_unrouted_component",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
					routed:    false,
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
			},
		},
		{
			// Test plan: Register a replica for a given ReplicaSet, along with an
			// unrouted and a routed component. Verify that routing information is
			// generated that contains one assignment for the routed component.
			// Next, register a new replica and verify that new routing info is
			// generated to reflect the new replica.
			name: "register_replica_to_start_register_routed_unrouted_components",
			operations: []op{
				registerComponent{ // unrouted component
					app:       "app1",
					id:        depId,
					component: "component1",
					routed:    false,
				},
				registerComponent{ // routed component
					app:        "app1",
					id:         depId,
					component:  "component2",
					replicaSet: "component1",
					routed:     true,
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: false,
					replicas:       []string{"replica1"},
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component2",
					replicaSet:     "component1",
					wantAssignment: true,
					replicas:       []string{"replica1"},
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica2",
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: false,
					replicas:       []string{"replica1", "replica2"},
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component2",
					replicaSet:     "component1",
					wantAssignment: true,
					replicas:       []string{"replica1", "replica2"},
				},
			},
		},
		{
			// Test plan: Register two replicas for a given ReplicaSet, along with
			// a routed component. Verify that routing information considers both
			// replicas. Next, make one of the replicas unhealthy. Verify that
			// a new assignment is generated only for the healthy replica.
			// Next, the unhealthy replica becomes healthy again. Verify that a
			// new assignment is generated and contains both replicas.
			name: "register_replica_to_start_resurrected_replica",
			operations: []op{
				registerReplica{ // replica 1
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				registerReplica{ // replica 2
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica2",
				},
				registerComponent{ // routed component
					app:       "app1",
					id:        depId,
					component: "component1",
					routed:    true,
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					replicas:       []string{"replica1", "replica2"},
				},
				reportHealth{ // replica 1 becomes unhealthy
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY},
				},
				checkRoutingInfo{ // an assignment is generated that contains only the healthy replica 2
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					replicas:       []string{"replica2"},
				},
				reportHealth{ // replica 1 becomes healthy again
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY},
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					replicas:       []string{"replica1", "replica2"},
				},
			},
		},
		{
			// Test plan: Register a ReplicaSet replica to start for two
			// applications. Verify that routing info is generated for both.
			name: "register_two_apps",
			operations: []op{
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				registerReplica{
					app:        "app2",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				reportHealth{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY},
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					replicas:  nil,
				},
				checkRoutingInfo{
					app:       "app2",
					id:        depId,
					component: "component1",
					replicas:  []string{"replica1"},
				},
			},
		},
		{
			// Test plan: Register a number of ReplicaSet replicas, and verify
			// that the ReplicaSet state is as expected.
			name: "replica_set_state",
			operations: []op{
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica2",
				},
				registerReplica{
					app:        "app1",
					id:         depId,
					replicaSet: "component3",
					replica:    "replica1",
				},
				registerReplica{
					app:        "app1",
					id:         uuid.New().String(),
					replicaSet: "empty1",
					replica:    "replica1",
				},
				registerReplica{
					app:        "app2",
					id:         depId,
					replicaSet: "component4",
					replica:    "replica1",
				},
				registerReplica{
					app:        "app2",
					id:         uuid.New().String(),
					replicaSet: "empty2",
					replica:    "replica2",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				registerComponent{
					app:        "app1",
					id:         depId,
					component:  "component2",
					replicaSet: "component1",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component3",
				},
				registerComponent{
					app:       "app2",
					id:        depId,
					component: "component4",
				},
				registerListener{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					listener:   "listener1",
				},
				registerListener{
					app:        "app1",
					id:         depId,
					replicaSet: "component3",
					listener:   "listener2",
				},
				registerListener{
					app:        "app1",
					id:         depId,
					replicaSet: "component3",
					listener:   "listener3",
				},
				reportHealth{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY},
				},
				checkReplicaSetState{
					app: "app1",
					id:  depId,
					replicaSets: map[string]replicaSetState{
						"component1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"component3": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
					},
				},
				checkReplicaSetState{
					app: "app2",
					id:  depId,
					replicaSets: map[string]replicaSetState{
						"component4": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component4"},
						},
					},
				},
				checkReplicaSetState{
					app: "app1",
					// id intentionally empty
					replicaSets: map[string]replicaSetState{
						"component1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"component3": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
						"empty1": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{},
						},
					},
				},
				checkReplicaSetState{
					// app and id intentionally empty
					replicaSets: map[string]replicaSetState{
						"component1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"component3": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
						"empty1": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{},
						},
						"component4": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component4"},
						},
						"empty2": {
							Replicas:   []string{"replica2:healthy"},
							Components: []string{},
						},
					},
				},
			},
		},
		{
			// Test plan: Register two replicas for a given ReplicaSet, along with
			// a routed component. Verify that routing information considers both
			// replicas. Next, make one of the replicas terminated. Verify that
			// a new assignment is generated only for the healthy replica.
			name: "register_replica_to_start_terminated_replica",
			operations: []op{
				registerReplica{ // replica 1
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
				},
				registerReplica{ // replica 2
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica2",
				},
				registerComponent{ // routed component
					app:       "app1",
					id:        depId,
					component: "component1",
					routed:    true,
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					replicas:       []string{"replica1", "replica2"},
				},
				reportHealth{ // replica 1 becomes terminated
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					replica:    "replica1",
					health:     []protos.HealthStatus{protos.HealthStatus_TERMINATED},
				},
				checkRoutingInfo{ // an assignment is generated that contains only the healthy replica 2
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					replicas:       []string{"replica2"},
				},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			assigner := NewAssigner(ctx, store.NewFakeStore(), logging.NewTestLogger(t),
				EqualDistributionAlgorithm, func(addr string) clients.BabysitterClient { return &mockBabysitterClient{} },
				func(context.Context, string) (bool, error) { return true, nil })
			for i, operation := range c.operations {
				t.Logf("> Operation %d: %T%+v", i, operation, operation)
				if err := operation.do(ctx, assigner); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestConcurrentAssigners(t *testing.T) {
	// Test plan: Create multiple assigners. Given a list of operation batches,
	// of type [][]op, we serially execute each batch. Within a batch, we
	// execute operations in random order against randomly chosen assigners.
	// Finally, check that the routing info is what we expect.
	depId := uuid.New().String()
	for _, c := range []struct {
		name  string
		ops   [][]op
		check checkRoutingInfo
	}{
		// Test plan: register a single replica.
		{
			name: "register_replica",
			ops: [][]op{
				{
					registerReplica{
						app:        "app1",
						id:         depId,
						replicaSet: "component1",
						replica:    "replica1",
					},
				},
			},
			check: checkRoutingInfo{
				app:       "app1",
				id:        depId,
				component: "component1",
				replicas:  []string{"replica1"},
			},
		},
		// Test plan: register a single component.
		{
			name: "register_component",
			ops: [][]op{
				{
					registerComponent{
						app:       "app1",
						id:        depId,
						component: "component1",
						routed:    false,
					},
				},
			},
			check: checkRoutingInfo{
				app:       "app1",
				id:        depId,
				component: "component1",
				replicas:  nil,
			},
		},
		// Test plan: register multiple replicas.
		{
			name: "multiple_replicas",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r2"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r3"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r4"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:       "a1",
				id:        depId,
				component: "c1",
				replicas:  []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register one component and multiple replicas.
		{
			name: "one_component_multiple_replicas",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, component: "c1", routed: true},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r2"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r3"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r4"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:            "a1",
				id:             depId,
				component:      "c1",
				wantAssignment: true,
				replicas:       []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register multiple components and multiple replicas.
		{
			name: "multiple_components_multiple_replicas",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c2", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c3", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c4", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c5", routed: true},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r2"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r3"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r4"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:            "a1",
				id:             depId,
				component:      "c1",
				wantAssignment: true,
				replicas:       []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register one replica and have it become unhealthy.
		{
			name: "one_replica_die",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
				},
			},
			check: checkRoutingInfo{
				app:       "a1",
				id:        depId,
				component: "c1",
				replicas:  nil,
			},
		},
		// Test plan: register one replica and have it become unhealthy and
		// then healthy again.
		{
			name: "one_replica_resurrect",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						},
					},
				},
			},
			check: checkRoutingInfo{
				app:       "a1",
				id:        depId,
				component: "c1",
				replicas:  []string{"r1"},
			},
		},
		// Test plan: register multiple replicas and have multiple become
		// unhealthy.
		{
			name: "multiple_replicas_die",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r2"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r3"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r4"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r5"},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY,
						},
					},
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r2",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
				},
			},
			check: checkRoutingInfo{
				app:       "a1",
				id:        depId,
				component: "c1",
				replicas:  []string{"r3", "r4", "r5"},
			},
		},
		// Test plan: register multiple replicas and have some become unhealthy
		// and some become healthy again.
		{
			name: "multiple_replicas_health_changes",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r1"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r2"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r3"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r4"},
					registerReplica{app: "a1", id: depId, replicaSet: "c1", replica: "r5"},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r2",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
				},
				{
					reportHealth{
						app:        "a1",
						id:         depId,
						replicaSet: "c1",
						replica:    "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
							protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						},
					},
				},
			},
			check: checkRoutingInfo{
				app:       "a1",
				id:        depId,
				component: "c1",
				replicas:  []string{"r1", "r3", "r4", "r5"},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Construct n assigners.
			const n = 3
			ctx := context.Background()
			store := store.NewFakeStore()
			var assigners [n]*Assigner
			for i := 0; i < n; i++ {
				assigners[i] = NewAssigner(ctx, store, logging.NewTestLogger(t), EqualDistributionAlgorithm,
					func(addr string) clients.BabysitterClient { return &mockBabysitterClient{} },
					func(context.Context, string) (bool, error) { return true, nil })
			}

			// Execute the operations, batch by batch.
			i := 0
			for _, batch := range c.ops {
				// Shuffle the batch. Regardless of the order of operations in
				// a batch, the final routing information should be the same.
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				r.Shuffle(len(batch), func(i, j int) {
					batch[i], batch[j] = batch[j], batch[i]
				})

				for _, op := range batch {
					if _, ok := op.(reportHealth); ok {
						// If different assigners disagree on the health of a
						// replica, it can lead to oscillations in the routing
						// assignment. We deliver health reports to all
						// assigners so that they agree.
						for j, assigner := range assigners {
							t.Logf("> Operation %d on Assigner %d: %T%+v", i, j, op, op)
							if err := op.do(ctx, assigner); err != nil {
								t.Fatal(err)
							}
						}
					} else {
						j := r.Intn(n)
						t.Logf("> Operation %d on Assigner %d: %T%+v", i, j, op, op)
						if err := op.do(ctx, assigners[j]); err != nil {
							t.Fatal(err)
						}
					}
					i++
				}
			}

			// Check the final routing info.
			for i, assigner := range assigners {
				t.Logf("> Check on Assigner %d: %T%+v", i, c.check, c.check)
				if err := c.check.do(ctx, assigner); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestUnregisterReplicaSets(t *testing.T) {
	ctx := context.Background()
	assigner := NewAssigner(ctx, store.NewFakeStore(), logging.NewTestLogger(t),
		EqualDistributionAlgorithm, func(addr string) clients.BabysitterClient { return &mockBabysitterClient{} },
		func(context.Context, string) (bool, error) { return true, nil })

	// Register a component with two replicas for two versions.
	v1 := uuid.New().String()
	v2 := uuid.New().String()
	for i, op := range []op{
		registerComponent{app: "a", id: v1, component: "c1", routed: true},
		registerReplica{app: "a", id: v1, replicaSet: "c1", replica: "r1"},
		registerReplica{app: "a", id: v1, replicaSet: "c1", replica: "r2"},
		registerComponent{app: "a", id: v2, component: "c1", routed: true},
		registerReplica{app: "a", id: v2, replicaSet: "c1", replica: "r1"},
		registerReplica{app: "a", id: v2, replicaSet: "c1", replica: "r2"},
	} {
		t.Logf("> Operation %d: %T%+v", i, op, op)
		if err := op.do(ctx, assigner); err != nil {
			t.Fatal(err)
		}
	}

	// Unregister one of the versions.
	if err := assigner.UnregisterReplicaSets(ctx, "a", []string{v1}); err != nil {
		t.Fatal(err)
	}

	// Check that v1's routing info was deleted, but v2's wasn't.
	checks := []checkRoutingInfo{
		{
			app:       "a",
			id:        v1,
			component: "c1",
			replicas:  nil,
		},
		{
			app:            "a",
			id:             v2,
			component:      "c1",
			wantAssignment: true,
			replicas:       []string{"r1", "r2"},
		},
	}
	for i, check := range checks {
		t.Logf("> Check %d: %T%+v", i, check, check)
		if err := check.do(ctx, assigner); err != nil {
			t.Fatal(err)
		}
	}

	// Check that v1's ReplicaSet info was deleted.
	rid := &ReplicaSetId{App: "a", Id: v1, Name: "c1"}
	_, version, err := assigner.loadReplicaSetInfo(ctx, rid)
	if err != nil {
		t.Fatal(err)
	}
	if *version != store.Missing {
		t.Fatalf("bad ReplicaSet info version: got %v, want Missing", *version)
	}

	// Check that the assigner's health checkers were deleted.
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	if _, found := assigner.replicas[unprotoRid(rid)]; found {
		t.Fatalf("ReplicaSet %v health checkers not deleted", rid)
	}
}

// reportHealthStatus reports the provided health status for the provided
// replica. Normally, an assigner checks an envelope to determine its
// health status. reportHealthStatus allows us to fake these checks.
func reportHealthStatus(ctx context.Context, a *Assigner, rid *ReplicaSetId, addr string, status protos.HealthStatus) error {
	updateHealth := func() (bool, error) {
		// Pull this code into a function so that we can defer Unlock.
		a.mu.Lock()
		defer a.mu.Unlock()
		replicas, ok := a.replicas[unprotoRid(rid)]
		if !ok {
			return false, fmt.Errorf("ReplicaSet %v not found", rid)
		}
		replica, ok := replicas[addr]
		if !ok {
			return false, fmt.Errorf("replica %q not found", addr)
		}
		return replica.health.report(status), nil
	}
	changed, err := updateHealth()
	if err != nil {
		return err
	}
	if changed {
		return a.mayGenerateNewRoutingInfo(ctx, rid)
	}
	return nil
}
