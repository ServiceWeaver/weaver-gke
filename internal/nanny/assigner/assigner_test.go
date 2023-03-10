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
	app, id, group string
	replica        string
}

type registerComponent struct {
	app, id, group string
	component      string
	routed         bool
}

type registerListener struct {
	app, id, group string
	listener       string
}

type reportHealth struct {
	app, id, group string
	replica        string
	health         []protos.HealthStatus
}

type checkRoutingInfo struct {
	app, id, group string
	numAssignments int
	replicas       []string
}

type checkGroupState struct {
	app, id string
	groups  map[string]groupState
}

type groupState struct {
	Replicas   []string
	Components []string
	Listeners  []string
}

func (r registerReplica) do(ctx context.Context, assigner *Assigner) error {
	return assigner.RegisterReplica(ctx, &nanny.ReplicaToRegister{
		Replica: &protos.ReplicaToRegister{
			App:          r.app,
			DeploymentId: r.id,
			Group:        r.group,
			Address:      r.replica,
		},
	})
}

func (r registerComponent) do(ctx context.Context, assigner *Assigner) error {
	return assigner.RegisterComponentToStart(ctx, &protos.ComponentToStart{
		App:             r.app,
		DeploymentId:    r.id,
		ColocationGroup: r.group,
		Component:       r.component,
		IsRouted:        r.routed,
	})
}

func (r registerListener) do(ctx context.Context, assigner *Assigner) error {
	dep := &protos.Deployment{
		App: &protos.AppConfig{Name: r.app},
		Id:  r.id,
	}
	return assigner.RegisterListener(ctx, dep,
		&protos.ColocationGroup{Name: r.group}, &protos.Listener{Name: r.listener})
}

func (r reportHealth) do(ctx context.Context, assigner *Assigner) error {
	// Give the assigner a chance to learn about weavelets and create probers
	// for them. It's pointless to report the health for a group that a
	// weavelet doesn't know about.
	if err := assigner.annealCheckers(ctx); err != nil {
		return err
	}

	for _, h := range r.health {
		pid := &GroupId{App: r.app, Id: r.id, Group: r.group}
		if err := reportHealthStatus(ctx, assigner, pid, r.replica, h); err != nil {
			return err
		}
	}
	return nil
}

func (c checkRoutingInfo) do(_ context.Context, assigner *Assigner) error {
	info, err := assigner.GetRoutingInfo(&protos.GetRoutingInfo{
		App:          c.app,
		DeploymentId: c.id,
		Group:        c.group,
		Version:      "",
	})
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("nil routing info")
	}

	// Check the replicas.
	less := func(x, y string) bool { return x < y }
	if diff := cmp.Diff(c.replicas, info.Replicas, cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad replicas (-want +got)\n%s", diff)
	}

	// Check the assignments.
	if got, want := len(info.Assignments), c.numAssignments; got != want {
		return fmt.Errorf("bad number of assignments: got %d, want %d", got, want)
	}
	for _, proto := range info.Assignments {
		assignment, err := FromProto(proto)
		if err != nil {
			return err
		}
		if diff := cmp.Diff(c.replicas, AssignedResources(assignment), cmpopts.SortSlices(less)); diff != "" {
			return fmt.Errorf("bad replicas (-want +got)\n%s", diff)
		}
	}
	return nil
}

func (c checkGroupState) do(ctx context.Context, assigner *Assigner) error {
	state, err := assigner.GetGroupState(ctx, &nanny.GroupStateRequest{
		AppName:   c.app,
		VersionId: c.id,
	})
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("nil version group state")
	}
	got := map[string]groupState{}
	for _, group := range state.Groups {
		var replicas []string
		for _, replica := range group.Replicas {
			health := "healthy"
			if replica.HealthStatus != protos.HealthStatus_HEALTHY {
				health = "un" + health
			}
			replicas = append(replicas, fmt.Sprintf("%s:%s", replica.WeaveletAddr, health))
		}
		var listeners []string
		for _, l := range group.Listeners {
			listeners = append(listeners, l.Name)
		}
		got[group.Name] = groupState{
			Replicas:   replicas,
			Components: group.Components,
			Listeners:  listeners,
		}
	}
	less := func(x, y string) bool { return x < y }
	if diff := cmp.Diff(c.groups, got, cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad groups (-want +got)\n%s", diff)
	}
	return nil
}

type mockBabysitterClient struct{}

var _ clients.BabysitterClient = &mockBabysitterClient{}

func (b mockBabysitterClient) CheckHealth(context.Context, *clients.HealthCheck) (*protos.HealthReport, error) {
	return &protos.HealthReport{Status: protos.HealthStatus_HEALTHY}, nil
}

func (b mockBabysitterClient) RunProfiling(context.Context, *protos.RunProfiling) (*protos.Profile, error) {
	panic("implement me")
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
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 0,
					replicas:       []string{"replica1"},
				},
			},
		},
		{
			// Test plan: Register a component to start. Verify that no routing
			// info is generated because there is no group and replica
			// started that manage the component.
			name: "register_component",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group1",
					routed:    false,
				},
				checkRoutingInfo{
					app:   "app1",
					id:    depId,
					group: "group1",
				},
			},
		},
		{
			// Test plan: Register a replica for a group 1 and a component to
			// start but for a different group 2. Verify that routing info is
			// generated for group 1 but not for group 2.
			name: "register_component_for_different_groups",
			operations: []op{
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group2",
					routed:    false,
				},
				checkRoutingInfo{
					app:      "app1",
					id:       depId,
					group:    "group1",
					replicas: []string{"replica1"},
				},
				checkRoutingInfo{
					app:   "app1",
					id:    depId,
					group: "group2",
				},
			},
		},
		{
			// Test plan: Register a group to start and a corresponding
			// unrouted component. Verify that no routing information is
			// generated, because the group has no replica.
			name: "register_group_to_start_register_unrouted_component",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group1",
					routed:    false,
				},
				checkRoutingInfo{
					app:   "app1",
					id:    depId,
					group: "group1",
				},
			},
		},
		{
			// Test plan: Register a replica for a given group, along with an
			// unrouted and a routed component. Verify that routing information is
			// generated that contains one assignment for the routed component.
			// Next, register a new replica and verify that new routing info is
			// generated to reflect the new replica.
			name: "register_replica_to_start_register_routed_unrouted_components",
			operations: []op{
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerComponent{ // unrouted component
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group1",
					routed:    false,
				},
				registerComponent{ // routed component
					app:       "app1",
					id:        depId,
					component: "component2",
					group:     "group1",
					routed:    true,
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica1"},
				},
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica2",
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica1", "replica2"},
				},
			},
		},
		{
			// Test plan: Register two replicas for a given group, along with
			// a routed component. Verify that routing information considers both
			// replicas. Next, make one of the replicas unhealthy. Verify that
			// a new assignment is generated only for the healthy replica.
			// Next, the unhealthy replica becomes healthy again. Verify that a
			// new assignment is generated and contains both replicas.
			name: "register_replica_to_start_resurrected_replica",
			operations: []op{
				registerReplica{ // replica 1
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerReplica{ // replica 2
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica2",
				},
				registerComponent{ // routed component
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group1",
					routed:    true,
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica1", "replica2"},
				},
				reportHealth{ // replica 1 becomes unhealthy
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY},
				},
				checkRoutingInfo{ // an assignment is generated that contains only the healthy replica 2
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica2"},
				},
				reportHealth{ // replica 1 becomes healthy again
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
						protos.HealthStatus_HEALTHY},
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica1", "replica2"},
				},
			},
		},
		{
			// Test plan: Register a group replica to start for two
			// applications. Verify that routing info is generated for both.
			name: "register_two_apps",
			operations: []op{
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerReplica{
					app:     "app2",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				reportHealth{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY},
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 0,
					replicas:       nil,
				},
				checkRoutingInfo{
					app:            "app2",
					id:             depId,
					group:          "group1",
					numAssignments: 0,
					replicas:       []string{"replica1"},
				},
			},
		},
		{
			// Test plan: Register a number of group replicas, and verify
			// that the group state is as expected.
			name: "group_state",
			operations: []op{
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica2",
				},
				registerReplica{
					app:     "app1",
					id:      depId,
					group:   "group2",
					replica: "replica1",
				},
				registerReplica{
					app:     "app1",
					id:      uuid.New().String(),
					group:   "group3",
					replica: "replica1",
				},
				registerReplica{
					app:     "app2",
					id:      depId,
					group:   "group4",
					replica: "replica1",
				},
				registerReplica{
					app:     "app2",
					id:      uuid.New().String(),
					group:   "group5",
					replica: "replica2",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					group:     "group1",
					component: "component1",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					group:     "group1",
					component: "component2",
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					group:     "group2",
					component: "component3",
				},
				registerComponent{
					app:       "app2",
					id:        depId,
					group:     "group4",
					component: "component4",
				},
				registerListener{
					app:      "app1",
					id:       depId,
					group:    "group1",
					listener: "listener1",
				},
				registerListener{
					app:      "app1",
					id:       depId,
					group:    "group2",
					listener: "listener2",
				},
				registerListener{
					app:      "app1",
					id:       depId,
					group:    "group2",
					listener: "listener3",
				},
				reportHealth{
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
					health: []protos.HealthStatus{
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY},
				},
				checkGroupState{
					app: "app1",
					id:  depId,
					groups: map[string]groupState{
						"group1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"group2": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
					},
				},
				checkGroupState{
					app: "app2",
					id:  depId,
					groups: map[string]groupState{
						"group4": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component4"},
						},
					},
				},
				checkGroupState{
					app: "app1",
					// id intentionally empty
					groups: map[string]groupState{
						"group1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"group2": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
						"group3": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{},
						},
					},
				},
				checkGroupState{
					// app and id intentionally empty
					groups: map[string]groupState{
						"group1": {
							Replicas:   []string{"replica1:unhealthy", "replica2:healthy"},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						"group2": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component3"},
							Listeners:  []string{"listener2", "listener3"},
						},
						"group3": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{},
						},
						"group4": {
							Replicas:   []string{"replica1:healthy"},
							Components: []string{"component4"},
						},
						"group5": {
							Replicas:   []string{"replica2:healthy"},
							Components: []string{},
						},
					},
				},
			},
		},
		{
			// Test plan: Register two replicas for a given group, along with
			// a routed component. Verify that routing information considers both
			// replicas. Next, make one of the replicas terminated. Verify that
			// a new assignment is generated only for the healthy replica.
			name: "register_replica_to_start_terminated_replica",
			operations: []op{
				registerReplica{ // replica 1
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
				},
				registerReplica{ // replica 2
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica2",
				},
				registerComponent{ // routed component
					app:       "app1",
					id:        depId,
					component: "component1",
					group:     "group1",
					routed:    true,
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica1", "replica2"},
				},
				reportHealth{ // replica 1 becomes terminated
					app:     "app1",
					id:      depId,
					group:   "group1",
					replica: "replica1",
					health:  []protos.HealthStatus{protos.HealthStatus_TERMINATED},
				},
				checkRoutingInfo{ // an assignment is generated that contains only the healthy replica 2
					app:            "app1",
					id:             depId,
					group:          "group1",
					numAssignments: 1,
					replicas:       []string{"replica2"},
				},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			assigner := NewAssigner(ctx, store.NewFakeStore(), &logging.NewTestLogger(t).FuncLogger,
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
						app:     "app1",
						id:      depId,
						group:   "group1",
						replica: "replica1",
					},
				},
			},
			check: checkRoutingInfo{
				app:            "app1",
				id:             depId,
				group:          "group1",
				numAssignments: 0,
				replicas:       []string{"replica1"},
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
						group:     "group1",
						routed:    false,
					},
				},
			},
			check: checkRoutingInfo{
				app:            "app1",
				id:             depId,
				group:          "group1",
				numAssignments: 0,
				replicas:       nil,
			},
		},
		// Test plan: register multiple replicas.
		{
			name: "multiple_replicas",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r2"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r3"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r4"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 0,
				replicas:       []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register one component and multiple replicas.
		{
			name: "one_component_multiple_replicas",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, group: "g1", component: "o1", routed: true},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r2"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r3"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r4"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 1,
				replicas:       []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register multiple components and multiple replicas.
		{
			name: "multiple_components_multiple_replicas",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, group: "g1", component: "o1", routed: true},
					registerComponent{app: "a1", id: depId, group: "g1", component: "o2", routed: true},
					registerComponent{app: "a1", id: depId, group: "g1", component: "o3", routed: true},
					registerComponent{app: "a1", id: depId, group: "g1", component: "o4", routed: true},
					registerComponent{app: "a1", id: depId, group: "g1", component: "o5", routed: true},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r2"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r3"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r4"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r5"},
				},
			},
			check: checkRoutingInfo{
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 5,
				replicas:       []string{"r1", "r2", "r3", "r4", "r5"},
			},
		},
		// Test plan: register one replica and have it become unhealthy.
		{
			name: "one_replica_die",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
				},
				{
					reportHealth{
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
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
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 0,
				replicas:       nil,
			},
		},
		// Test plan: register one replica and have it become unhealthy and
		// then healthy again.
		{
			name: "one_replica_resurrect",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
				},
				{
					reportHealth{
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
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
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
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
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 0,
				replicas:       []string{"r1"},
			},
		},
		// Test plan: register multiple replicas and have multiple become
		// unhealthy.
		{
			name: "multiple_replicas_die",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r2"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r3"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r4"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r5"},
				},
				{
					reportHealth{
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
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
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r2",
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
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 0,
				replicas:       []string{"r3", "r4", "r5"},
			},
		},
		// Test plan: register multiple replicas and have some become unhealthy
		// and some become healthy again.
		{
			name: "multiple_replicas_health_changes",
			ops: [][]op{
				{
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r1"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r2"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r3"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r4"},
					registerReplica{app: "a1", id: depId, group: "g1", replica: "r5"},
				},
				{
					reportHealth{
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
						health: []protos.HealthStatus{
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
							protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
						},
					},
					reportHealth{
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r2",
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
						app:     "a1",
						id:      depId,
						group:   "g1",
						replica: "r1",
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
				app:            "a1",
				id:             depId,
				group:          "g1",
				numAssignments: 0,
				replicas:       []string{"r1", "r3", "r4", "r5"},
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
				assigners[i] = NewAssigner(ctx, store, &logging.NewTestLogger(t).FuncLogger, EqualDistributionAlgorithm,
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

func TestUnregisterGroups(t *testing.T) {
	ctx := context.Background()
	assigner := NewAssigner(ctx, store.NewFakeStore(), &logging.NewTestLogger(t).FuncLogger,
		EqualDistributionAlgorithm, func(addr string) clients.BabysitterClient { return &mockBabysitterClient{} },
		func(context.Context, string) (bool, error) { return true, nil })

	// Register a component with two replicas for two versions.
	v1 := uuid.New().String()
	v2 := uuid.New().String()
	for i, op := range []op{
		registerComponent{app: "a", id: v1, group: "g", component: "o", routed: true},
		registerReplica{app: "a", id: v1, group: "g", replica: "r1"},
		registerReplica{app: "a", id: v1, group: "g", replica: "r2"},
		registerComponent{app: "a", id: v2, group: "g", component: "o", routed: true},
		registerReplica{app: "a", id: v2, group: "g", replica: "r1"},
		registerReplica{app: "a", id: v2, group: "g", replica: "r2"},
	} {
		t.Logf("> Operation %d: %T%+v", i, op, op)
		if err := op.do(ctx, assigner); err != nil {
			t.Fatal(err)
		}
	}

	// Unregister one of the versions.
	if err := assigner.UnregisterGroups(ctx, "a", []string{v1}); err != nil {
		t.Fatal(err)
	}

	// Check that v1's routing info was deleted, but v2's wasn't.
	checks := []checkRoutingInfo{
		{
			app:            "a",
			id:             v1,
			group:          "g",
			numAssignments: 0,
			replicas:       nil,
		},
		{
			app:            "a",
			id:             v2,
			group:          "g",
			numAssignments: 1,
			replicas:       []string{"r1", "r2"},
		},
	}
	for i, check := range checks {
		t.Logf("> Check %d: %T%+v", i, check, check)
		if err := check.do(ctx, assigner); err != nil {
			t.Fatal(err)
		}
	}

	// Check that v1's group info was deleted.
	pid := &GroupId{App: "a", Id: v1, Group: "g"}
	_, version, err := assigner.loadGroupInfo(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if *version != store.Missing {
		t.Fatalf("bad group info version: got %v, want Missing", *version)
	}

	// Check that the assigner's health checkers were deleted.
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	if _, found := assigner.replicas[unprotoGid(pid)]; found {
		t.Fatalf("group %v health checkers not deleted", pid)
	}
}

// reportHealthStatus reports the provided health status for the provided
// replica. Normally, an assigner checks an envelope to determine its
// health status. reportHealthStatus allows us to fake these checks.
func reportHealthStatus(ctx context.Context, a *Assigner, pid *GroupId, addr string, status protos.HealthStatus) error {
	updateHealth := func() (bool, error) {
		// Pull this code into a function so that we can defer Unlock.
		a.mu.Lock()
		defer a.mu.Unlock()
		replicas, ok := a.replicas[unprotoGid(pid)]
		if !ok {
			return false, fmt.Errorf("group %v not found", pid)
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
		return a.mayGenerateNewRoutingInfo(ctx, pid)
	}
	return nil
}
