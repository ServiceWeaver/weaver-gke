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
	"sync"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/testing/protocmp"
)

func makeConfig(app, id string) *config.GKEConfig {
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{Name: app},
			Id:  id,
		},
	}
}

func pod(addr string) *nanny.Pod {
	return &nanny.Pod{WeaveletAddr: addr}
}

type replicaSetKey struct {
	app, id, replicaSet string
}

// testState stores the testState for a given test.
type testState struct {
	mu sync.Mutex
	// Set of healthy pods, keyed by replica set name.
	pods map[replicaSetKey][]*nanny.Pod
}

func (s *testState) registerPod(cfg *config.GKEConfig, replicaSet, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pods == nil {
		s.pods = map[replicaSetKey][]*nanny.Pod{}
	}
	key := replicaSetKey{cfg.Deployment.App.Name, cfg.Deployment.Id, replicaSet}
	s.pods[key] = append(s.pods[key], pod(addr))
}

func (s *testState) unregisterPod(cfg *config.GKEConfig, replicaSet, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pods == nil {
		return
	}
	key := replicaSetKey{cfg.Deployment.App.Name, cfg.Deployment.Id, replicaSet}
	s.pods[key] = slices.DeleteFunc(s.pods[key], func(pod *nanny.Pod) bool {
		return pod.WeaveletAddr == addr
	})
}

func (s *testState) getHealthyPods(_ context.Context, cfg *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pods[replicaSetKey{cfg.Deployment.App.Name, cfg.Deployment.Id, replicaSet}], nil
}

// op is an operation performed against an assigner.
type op interface {
	do(context.Context, *Assigner, *testState) error
}

type registerComponent struct {
	app, id    string
	component  string
	routed     bool
	replicaSet string // if empty, assumed to be same as component name
}

type registerListener struct {
	app, id, replicaSet string
	listener            string
}

type startPod struct {
	app, id, replicaSet string
	pod                 string
}

type stopPod struct {
	app, id, replicaSet string
	pod                 string
}

type checkRoutingInfo struct {
	app, id, component string
	replicaSet         string // if empty, assumed to be same as component name
	wantAssignment     bool
	pods               []string
}

type checkReplicaSets struct {
	app, id     string
	replicaSets []*nanny.ReplicaSet
}

func (r registerComponent) do(ctx context.Context, assigner *Assigner, _ *testState) error {
	if r.replicaSet == "" {
		r.replicaSet = r.component
	}
	return assigner.RegisterComponent(ctx, &nanny.ActivateComponentRequest{
		Component:  r.component,
		Routed:     r.routed,
		ReplicaSet: r.replicaSet,
		Config:     makeConfig(r.app, r.id),
	})
}

func (r registerListener) do(ctx context.Context, assigner *Assigner, _ *testState) error {
	return assigner.RegisterListener(ctx, &nanny.ExportListenerRequest{
		ReplicaSet: r.replicaSet,
		Listener:   &nanny.Listener{Name: r.listener},
		Config:     makeConfig(r.app, r.id),
	})
}

func (r startPod) do(ctx context.Context, _ *Assigner, s *testState) error {
	s.registerPod(makeConfig(r.app, r.id), r.replicaSet, r.pod)
	return nil
}

func (r stopPod) do(ctx context.Context, assigner *Assigner, s *testState) error {
	s.unregisterPod(makeConfig(r.app, r.id), r.replicaSet, r.pod)
	return nil
}

func (c checkRoutingInfo) do(_ context.Context, assigner *Assigner, _ *testState) error {
	if c.replicaSet == "" {
		c.replicaSet = c.component
	}
	info, err := assigner.GetRoutingInfo(&nanny.GetRoutingRequest{
		Component:  c.component,
		Version:    "",
		ReplicaSet: c.replicaSet,
		Config:     makeConfig(c.app, c.id),
	})
	if err != nil {
		return err
	}
	var actualPods []string
	var actualAssignment *Assignment
	if info != nil && info.Routing != nil {
		actualPods = info.Routing.Replicas
		actualAssignment, err = FromProto(info.Routing.Assignment)
		if err != nil {
			return err
		}
	}

	// Check the pods.
	less := func(x, y string) bool { return x < y }
	if diff := cmp.Diff(c.pods, actualPods, cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad pods for component %s: (-want +got)\n%s", c.component, diff)
	}

	// Check the assignment.
	gotAssignment := actualAssignment != nil
	if c.wantAssignment != gotAssignment {
		return fmt.Errorf("component %s has assignment diff, want %v, got %v", c.component, c.wantAssignment, actualAssignment)
	}
	if !c.wantAssignment {
		return nil
	}
	if diff := cmp.Diff(c.pods, AssignedResources(actualAssignment), cmpopts.SortSlices(less)); diff != "" {
		return fmt.Errorf("bad pods (-want +got)\n%s", diff)
	}
	return nil
}

func (c checkReplicaSets) do(ctx context.Context, assigner *Assigner, _ *testState) error {
	reply, err := assigner.GetReplicaSets(ctx, &nanny.GetReplicaSetsRequest{
		AppName:   c.app,
		VersionId: c.id,
	})
	if err != nil {
		return err
	}
	if reply == nil {
		return fmt.Errorf("nil GetReplicaSetsReply")
	}
	if diff := cmp.Diff(c.replicaSets, reply.ReplicaSets, protocmp.Transform()); diff != "" {
		return fmt.Errorf("replica sets diff (-want +got)\n%s", diff)
	}
	return nil
}

func TestAssigner(t *testing.T) {
	depId := uuid.New().String()

	for _, c := range []struct {
		name       string
		operations []op
	}{
		{
			// Test plan: Register a component but don't start its pods.
			// Verify that no routing info is generated.
			name: "register_component_no_pods",
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
			// Test plan: Register a component and start its pod. Verify
			// the routing info.
			name: "register_component",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					pods:      []string{"pod1"},
				},
			},
		},
		{
			// Test plan: Register two components in different replica sets.
			// Start a pod of one replica set and verify routing info.
			name: "register_component_multiple_replica_sets",
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
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					pods:      []string{"pod1"},
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component2",
				},
			},
		},
		{
			// Test plan: Register two components in the same replica set.
			// One component is routed and the other is unrouted. Start a
			// single pod of that replica set, verify the routing info,
			// then start another pod and re-verify the routing info.
			name: "routing_info",
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
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: false,
					pods:           []string{"pod1"},
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component2",
					replicaSet:     "component1",
					wantAssignment: true,
					pods:           []string{"pod1"},
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod2",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component1",
					pods:      []string{"pod1", "pod2"},
				},
				checkRoutingInfo{
					app:            "app1",
					id:             depId,
					component:      "component2",
					replicaSet:     "component1",
					wantAssignment: true,
					pods:           []string{"pod1", "pod2"},
				},
			},
		},
		{
			// Test plan: Register a routed component, and two pods
			// of its containing replica sets. Verify the routing info.
			// Next, make one of the pods unhealthy and re-verify the
			// routing info.
			// Finally, make the unhealthy pod healthy again, and re-verify
			// the routing info.
			name: "routing_info_resurrected_pod",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
					routed:    true,
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod2",
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					pods:           []string{"pod1", "pod2"},
				},
				stopPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkRoutingInfo{ // an assignment is generated that contains only the healthy pod2
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					pods:           []string{"pod2"},
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkRoutingInfo{ // an assignment is generated that contains both replicas
					app:            "app1",
					id:             depId,
					component:      "component1",
					wantAssignment: true,
					pods:           []string{"pod1", "pod2"},
				},
			},
		},
		{
			// Test plan: Start a component and a pod of the containing
			// replica set in two different applications. Verify the routing
			// info.
			name: "register_two_apps",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component",
				},
				registerComponent{
					app:       "app2",
					id:        depId,
					component: "component",
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component",
					pod:        "pod1",
				},
				startPod{
					app:        "app2",
					id:         depId,
					replicaSet: "component",
					pod:        "pod2",
				},
				checkRoutingInfo{
					app:       "app1",
					id:        depId,
					component: "component",
					pods:      []string{"pod1"},
				},
				checkRoutingInfo{
					app:       "app2",
					id:        depId,
					component: "component",
					pods:      []string{"pod2"},
				},
			},
		},
		{
			// Test plan: Register a number of ReplicaSet pods, and verify
			// that the ReplicaSet state is as expected.
			name: "pods_set_state",
			operations: []op{
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component1",
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod1",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{{
						Name:       "component1",
						Config:     makeConfig("app1", depId),
						Pods:       []*nanny.Pod{pod("pod1")},
						Components: []string{"component1"},
					}},
				},
				registerComponent{
					app:        "app1",
					id:         depId,
					component:  "component2",
					replicaSet: "component1",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{{
						Name:       "component1",
						Config:     makeConfig("app1", depId),
						Pods:       []*nanny.Pod{pod("pod1")},
						Components: []string{"component1", "component2"},
					}},
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					pod:        "pod2",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{{
						Name:       "component1",
						Config:     makeConfig("app1", depId),
						Pods:       []*nanny.Pod{pod("pod1"), pod("pod2")},
						Components: []string{"component1", "component2"},
					}},
				},
				registerListener{
					app:        "app1",
					id:         depId,
					replicaSet: "component1",
					listener:   "listener1",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{{
						Name:       "component1",
						Config:     makeConfig("app1", depId),
						Pods:       []*nanny.Pod{pod("pod1"), pod("pod2")},
						Components: []string{"component1", "component2"},
						Listeners:  []string{"listener1"},
					}},
				},
				registerComponent{
					app:       "app1",
					id:        depId,
					component: "component3",
				},
				registerListener{
					app:        "app1",
					id:         depId,
					replicaSet: "component3",
					listener:   "listener2",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{
						{
							Name:       "component1",
							Config:     makeConfig("app1", depId),
							Pods:       []*nanny.Pod{pod("pod1"), pod("pod2")},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						{
							Name:       "component3",
							Config:     makeConfig("app1", depId),
							Components: []string{"component3"},
							Listeners:  []string{"listener2"},
						},
					},
				},
				startPod{
					app:        "app1",
					id:         depId,
					replicaSet: "component3",
					pod:        "pod4",
				},
				checkReplicaSets{
					app: "app1",
					id:  depId,
					replicaSets: []*nanny.ReplicaSet{
						{
							Name:       "component1",
							Config:     makeConfig("app1", depId),
							Pods:       []*nanny.Pod{pod("pod1"), pod("pod2")},
							Components: []string{"component1", "component2"},
							Listeners:  []string{"listener1"},
						},
						{
							Name:       "component3",
							Config:     makeConfig("app1", depId),
							Pods:       []*nanny.Pod{pod("pod4")},
							Components: []string{"component3"},
							Listeners:  []string{"listener2"},
						},
					},
				},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			s := &testState{}
			assigner := NewAssigner(ctx, store.NewFakeStore(),
				logging.NewTestSlogger(t, testing.Verbose()),
				EqualDistributionAlgorithm, 0, /*updateRoutingInterval*/
				s.getHealthyPods)
			for i, operation := range c.operations {
				t.Logf("> Operation %d: %T%+v", i, operation, operation)
				if err := operation.do(ctx, assigner, s); err != nil {
					t.Fatal(err)
				}
				if err := assigner.annealRouting(ctx); err != nil {
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
		name   string
		ops    [][]op
		checks []checkRoutingInfo
	}{
		// Test plan: register a single component pod.
		{
			name: "register_component",
			ops: [][]op{
				{
					registerComponent{
						app:       "app1",
						id:        depId,
						component: "component1",
					},
					startPod{
						app:        "app1",
						id:         depId,
						replicaSet: "component1",
						pod:        "pod1",
					},
				},
			},
			checks: []checkRoutingInfo{{
				app:       "app1",
				id:        depId,
				component: "component1",
				pods:      []string{"pod1"},
			}},
		},
		// Test plan: register a single component but no pods.
		{
			name: "register_component_no_pods",
			ops: [][]op{
				{
					registerComponent{
						app:       "app1",
						id:        depId,
						component: "component1",
					},
				},
			},
			checks: []checkRoutingInfo{{
				app:       "app1",
				id:        depId,
				component: "component1",
			}},
		},
		// Test plan: register multiple pods.
		{
			name: "multiple_pods",
			ops: [][]op{
				{
					registerComponent{
						app:       "a1",
						id:        depId,
						component: "c1",
					},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r3"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r4"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r5"},
				},
			},
			checks: []checkRoutingInfo{{
				app:       "a1",
				id:        depId,
				component: "c1",
				pods:      []string{"r1", "r2", "r3", "r4", "r5"},
			}},
		},
		// Test plan: register one component and multiple pods.
		{
			name: "one_component_multiple_pods",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, component: "c1", routed: true},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r3"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r4"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r5"},
				},
			},
			checks: []checkRoutingInfo{{
				app:            "a1",
				id:             depId,
				component:      "c1",
				wantAssignment: true,
				pods:           []string{"r1", "r2", "r3", "r4", "r5"},
			}},
		},
		// Test plan: register multiple components and multiple pods.
		{
			name: "multiple_components_multiple_pods",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c2", routed: false},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c3", routed: true},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c4", routed: false},
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c5", routed: true},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r3"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r4"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r5"},
				},
			},
			checks: []checkRoutingInfo{
				{
					app:            "a1",
					id:             depId,
					component:      "c1",
					wantAssignment: true,
					pods:           []string{"r1", "r2", "r3", "r4", "r5"},
				},
				{
					app:            "a1",
					id:             depId,
					component:      "c2",
					replicaSet:     "c1",
					wantAssignment: false,
					pods:           []string{"r1", "r2", "r3", "r4", "r5"},
				},
				{
					app:            "a1",
					id:             depId,
					component:      "c3",
					replicaSet:     "c1",
					wantAssignment: true,
					pods:           []string{"r1", "r2", "r3", "r4", "r5"},
				},
				{
					app:            "a1",
					id:             depId,
					component:      "c4",
					replicaSet:     "c1",
					wantAssignment: false,
					pods:           []string{"r1", "r2", "r3", "r4", "r5"},
				},
				{
					app:            "a1",
					id:             depId,
					component:      "c5",
					replicaSet:     "c1",
					wantAssignment: true,
					pods:           []string{"r1", "r2", "r3", "r4", "r5"},
				},
			},
		},
		// Test plan: register one pod and have it become unhealthy.
		{
			name: "one_pod_die",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: true},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
				{
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
			},
			checks: []checkRoutingInfo{{
				app:            "a1",
				id:             depId,
				component:      "c1",
				wantAssignment: true,
			}},
		},
		// Test plan: register one pod and have it become unhealthy and
		// then healthy again.
		{
			name: "one_pod_resurrect",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: false},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
				{
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
				{
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
			},
			checks: []checkRoutingInfo{{
				app:       "a1",
				id:        depId,
				component: "c1",
				pods:      []string{"r1"},
			}},
		},
		// Test plan: register multiple pods and have multiple become
		// unhealthy.
		{
			name: "multiple_pods_die",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: true},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r3"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r4"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r5"},
				},
				{
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
				},
			},
			checks: []checkRoutingInfo{{
				app:            "a1",
				id:             depId,
				component:      "c1",
				pods:           []string{"r3", "r4", "r5"},
				wantAssignment: true,
			}},
		},
		// Test plan: register multiple pods and have some become unhealthy
		// and some become healthy again.
		{
			name: "multiple_pods_resurrect",
			ops: [][]op{
				{
					registerComponent{app: "a1", id: depId, replicaSet: "c1", component: "c1", routed: false},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r3"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r4"},
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r5"},
				},
				{
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
					stopPod{app: "a1", id: depId, replicaSet: "c1", pod: "r2"},
				},
				{
					startPod{app: "a1", id: depId, replicaSet: "c1", pod: "r1"},
				},
			},
			checks: []checkRoutingInfo{{
				app:       "a1",
				id:        depId,
				component: "c1",
				pods:      []string{"r1", "r3", "r4", "r5"},
			}},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Construct n assigners.
			const n = 3
			ctx := context.Background()
			store := store.NewFakeStore()
			var assigners [n]*Assigner
			var state testState // share the load state across assigners
			for i := 0; i < n; i++ {
				assigners[i] = NewAssigner(ctx, store,
					logging.NewTestSlogger(t, testing.Verbose()),
					EqualDistributionAlgorithm, 0, /*updateRoutingInterval*/
					state.getHealthyPods)
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
					j := r.Intn(n)
					t.Logf("> Operation %d on Assigner %d: %T%+v", i, j, op, op)
					if err := op.do(ctx, assigners[j], &state); err != nil {
						t.Fatal(err)
					}
					i++

					// Anneal all assigners.
					for k, assigner := range assigners {
						t.Logf("> Anneal on Assigner %d", k)
						if err := assigner.annealRouting(ctx); err != nil {
							t.Fatal(err)
						}
					}
				}

			}

			// Check the final routing info.
			for i, assigner := range assigners {
				for _, check := range c.checks {
					t.Logf("> Check on Assigner %d: %+v", i, check)
					if err := check.do(ctx, assigner, &state); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestStopAppVersion(t *testing.T) {
	ctx := context.Background()
	s := &testState{}
	assigner := NewAssigner(ctx, store.NewFakeStore(),
		logging.NewTestSlogger(t, testing.Verbose()),
		EqualDistributionAlgorithm, 0, /*updateRoutingInterval*/
		s.getHealthyPods)

	// Register a component with two pods for two versions.
	v1 := uuid.New().String()
	v2 := uuid.New().String()
	for i, op := range []op{
		registerComponent{app: "a", id: v1, component: "c1", routed: true},
		startPod{app: "a", id: v1, replicaSet: "c1", pod: "r1"},
		startPod{app: "a", id: v1, replicaSet: "c1", pod: "r2"},
		registerComponent{app: "a", id: v2, component: "c1", routed: true},
		startPod{app: "a", id: v2, replicaSet: "c1", pod: "r1"},
		startPod{app: "a", id: v2, replicaSet: "c1", pod: "r2"},
	} {
		t.Logf("> Operation %d: %T%+v", i, op, op)
		if err := op.do(ctx, assigner, s); err != nil {
			t.Fatal(err)
		}
		if err := assigner.annealRouting(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Unregister one of the versions.
	if err := assigner.StopAppVersion(ctx, "a", v1); err != nil {
		t.Fatal(err)
	}

	// Check that v1's routing info was deleted, but v2's wasn't.
	for i, op := range []op{
		checkRoutingInfo{app: "a", id: v1, component: "c1", pods: nil},
		checkRoutingInfo{app: "a", id: v2, component: "c1", wantAssignment: true, pods: []string{"r1", "r2"}},
		checkReplicaSets{app: "a", id: v2, replicaSets: []*nanny.ReplicaSet{{
			Name:       "c1",
			Config:     makeConfig("a", v2),
			Pods:       []*nanny.Pod{pod("r1"), pod("r2")},
			Components: []string{"c1"},
		}}},
	} {
		t.Logf("> Operation %d: %T%+v", i, op, op)
		if err := op.do(ctx, assigner, s); err != nil {
			t.Fatal(err)
		}
	}
}
