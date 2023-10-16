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

package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/assigner"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
)

const (
	testListenerPort = 9999
)

// nextEvent reads the next event from manager, timing out if it takes too long.
func nextEvent(events chan string) string {
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second * 5):
		return "timeout"
	}
}

// testState stores the testState for a given test.
type testState struct {
	mu sync.Mutex
	// Set of healthy pods, keyed by replica set name.
	pods map[string][]*nanny.Pod
}

func (s *testState) registerPod(replicaSet, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pods == nil {
		s.pods = map[string][]*nanny.Pod{}
	}
	s.pods[replicaSet] = append(s.pods[replicaSet], &nanny.Pod{WeaveletAddr: addr})
}

func (s *testState) getHealthyPods(replicaSet string) ([]*nanny.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pods[replicaSet], nil
}

func newTestManager(t *testing.T) (endpoints.Manager, *testState, chan string) {
	t.Helper()
	ctx := context.Background()
	events := make(chan string, 100)
	s := &testState{}
	m := NewManager(ctx,
		store.NewFakeStore(),
		logging.NewTestSlogger(t, testing.Verbose()),
		"",               // dialAddr
		time.Millisecond, /*updateRoutingInterval*/
		func(_ context.Context, _ *config.GKEConfig, replicaSet string) ([]*nanny.Pod, error) {
			events <- "getHealthyPods " + replicaSet
			return s.getHealthyPods(replicaSet)
		},
		func(_ context.Context, _ *config.GKEConfig, replicaSet string, listener string) (int, error) {
			events <- "getListenerPort " + replicaSet + " " + listener
			return testListenerPort, nil
		},
		func(context.Context, *config.GKEConfig, string, *nanny.Listener) (*protos.ExportListenerReply, error) {
			return &protos.ExportListenerReply{ProxyAddress: "listener:8888"}, nil
		},
		func(_ context.Context, _ *config.GKEConfig, replicaSet string) error {
			events <- "startReplicaSet " + replicaSet
			return nil
		},
		func(context.Context, string, []*config.GKEConfig) error { return nil },
		func(context.Context, string, []*config.GKEConfig) error { return nil })
	return m, s, events
}

func makeConfig() *config.GKEConfig {
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{
				Name: "todo",
			},
			Id: "11111111-1111-1111-1111-111111111111",
		},
	}
}

func TestDeploy(t *testing.T) {
	m, _, events := newTestManager(t)

	cfg := makeConfig()
	if err := m.Deploy(context.Background(), &nanny.ApplicationDeploymentRequest{
		AppName:  "todo",
		Versions: []*config.GKEConfig{cfg},
	}); err != nil {
		t.Fatalf("couldn't deploy %v: %v", cfg, err)
	}

	if event := nextEvent(events); event != "startReplicaSet "+runtime.Main {
		t.Fatalf("main not started: got %q", event)
	}

	// Activate the component and check that it has started.
	if err := m.ActivateComponent(context.Background(), &nanny.ActivateComponentRequest{
		Component:  "bar",
		ReplicaSet: "bar",
		Config:     makeConfig(),
	}); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(events); event != "startReplicaSet bar" {
		t.Fatalf("bar not started: got %q", event)
	}
}

func TestGetListenerAddress(t *testing.T) {
	m, _, events := newTestManager(t)

	actual, err := m.GetListenerAddress(context.Background(), &nanny.GetListenerAddressRequest{
		ReplicaSet: "bar",
		Listener:   "foo",
		Config:     makeConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if event := nextEvent(events); event != "getListenerPort bar foo" {
		t.Fatalf("foo port not requested: got %q", event)
	}
	expect := &protos.GetListenerAddressReply{
		Address: fmt.Sprintf(":%d", testListenerPort),
	}
	if diff := cmp.Diff(actual, expect, protocmp.Transform()); diff != "" {
		t.Fatalf("bad address reply: (-want +got)\n%s", diff)
	}
}

func TestGetRoutingInfo(t *testing.T) {
	m, s, events := newTestManager(t)
	cfg := makeConfig()
	const replicaSet = "single"

	// waitNewEvent ignores all of the existing events and blocks until a
	// next event is generated.
	waitNewEvent := func() {
		for {
			select {
			case <-events: // drain
			default:
				<-events
				return
			}
		}
	}

	// registerPod register a new pod of the (single) replica set.
	registerPod := func(addr string) {
		s.registerPod(replicaSet, addr)
	}

	// activateComponent activates the given component inside the (single)
	// replica set.
	activateComponent := func(component string, isRouted bool) {
		if err := m.ActivateComponent(context.Background(), &nanny.ActivateComponentRequest{
			Component:  component,
			ReplicaSet: replicaSet,
			Routed:     isRouted,
			Config:     cfg,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// getRouting returns the routing information for a given component inside
	// the single replica set.
	getRouting := func(component string) *nanny.GetRoutingReply {
		reply, err := m.GetRoutingInfo(context.Background(), &nanny.GetRoutingRequest{
			Component:  component,
			ReplicaSet: replicaSet,
			Version:    "",
			Config:     cfg,
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(reply.Routing.Replicas)
		return reply
	}

	routingInfo := getRouting("nonexistent")
	if len(routingInfo.Routing.Replicas) != 0 {
		t.Fatalf("resolve(empty) = %v; want []", routingInfo.Routing.Replicas)
	}
	if routingInfo.Routing.Assignment != nil {
		t.Fatalf("no components registered; assignment should be nil")
	}

	testData := []string{
		"tcp://bar:1",
		"tcp://baz:2",
		"tcp://foo:3",
	}

	// Register two components to start.
	activateComponent("sharded", true)
	activateComponent("unsharded", false)

	s.registerPod(replicaSet, testData[0])
	s.registerPod(replicaSet, testData[1])

	// Wait for two new anneal events, to make sure the latest anneal takes
	// all of the components and pods into account.
	waitNewEvent()
	waitNewEvent()
	routingInfo = getRouting("unsharded")
	if diff := cmp.Diff(testData[:2], routingInfo.Routing.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if routingInfo.Version == "" {
		t.Fatal("no version returned by resolver")
	}
	if routingInfo.Routing.Assignment != nil {
		t.Fatalf("want empty assignment, got %v", prototext.Format(routingInfo.Routing.Assignment))
	}
	routingInfo = getRouting("sharded")
	if diff := cmp.Diff(testData[:2], routingInfo.Routing.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if routingInfo.Version == "" {
		t.Fatal("no version returned by resolver")
	}
	if routingInfo.Routing.Assignment == nil {
		t.Fatalf("nil assignment")
	}
	assignment, _ := assigner.FromProto(routingInfo.Routing.Assignment)
	got := assigner.AssignedResources(assignment)
	sort.Strings(got)
	if diff := cmp.Diff(got, []string{testData[0], testData[1]}); diff != "" {
		t.Fatalf("unexpected assignment result (-want,+got):\n%s", diff)
	}
	if assignment.GetVersion() != 1 {
		t.Fatalf("unexpected assignment version; want: 1; got: %d\n", assignment.GetVersion())
	}

	// Add a new pod and wait for two new anneal events to make sure the
	// latest anneal takes that pod into account.
	registerPod(testData[2])
	waitNewEvent()
	waitNewEvent()
	routingInfo = getRouting("sharded")
	if diff := cmp.Diff(testData[:3], routingInfo.Routing.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if routingInfo.Routing.Component != "sharded" {
		t.Fatal("assignments should contain an assignment for sharded")
	}
	if routingInfo.Routing.Assignment.Version != 2 {
		t.Fatalf("unexpected assignment version; want: 2; got: %d\n", routingInfo.Routing.Assignment.Version)
	}
}
