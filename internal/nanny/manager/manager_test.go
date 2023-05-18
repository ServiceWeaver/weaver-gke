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
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/assigner"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
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

func startServer(t *testing.T) (*httptest.Server, chan string) {
	t.Helper()
	ctx := context.Background()
	events := make(chan string, 100)
	mux := http.NewServeMux()

	// Start the manager server.
	if err := Start(ctx,
		mux,
		store.NewFakeStore(),
		logging.NewTestSlogger(t, testing.Verbose()),
		"", // dialAddr
		func(addr string) clients.BabysitterClient {
			return &babysitter.HttpClient{Addr: internal.ToHTTPAddress(addr)}
		},
		func(context.Context, string) (bool, error) { return true, nil },
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
		func(context.Context, string, []string) error { return nil },
		func(context.Context, string, []string) error { return nil }); err != nil {
		t.Fatal("error starting the manager server", err)
	}
	return httptest.NewServer(mux), events
}

func makeConfig(colocate ...string) *config.GKEConfig {
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{
				Name:     "todo",
				Colocate: []*protos.ComponentGroup{{Components: colocate}},
			},
			Id: "11111111-1111-1111-1111-111111111111",
		},
	}
}

func TestDeploy(t *testing.T) {
	ts, events := startServer(t)
	defer ts.Close()

	cfg := makeConfig()
	if err := protomsg.Call(context.Background(), protomsg.CallArgs{
		Client:  ts.Client(),
		Addr:    ts.URL,
		URLPath: deployURL,
		Request: &nanny.ApplicationDeploymentRequest{
			AppName:  "todo",
			Versions: []*config.GKEConfig{cfg},
		},
	}); err != nil {
		t.Fatalf("couldn't deploy %v: %v", cfg, err)
	}

	if event := nextEvent(events); event != "startReplicaSet "+runtime.Main {
		t.Fatalf("main not started: got %q", event)
	}

	// Verify that the process eventst)
	defer ts.Close()

	// Start the component and check that it has started.
	req := &nanny.ActivateComponentRequest{
		Component: "bar",
		Config:    makeConfig(),
	}

	if err := protomsg.Call(context.Background(), protomsg.CallArgs{
		Client:  ts.Client(),
		Addr:    ts.URL,
		URLPath: activateComponentURL,
		Request: req,
	}); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(events); event != "startReplicaSet bar" {
		t.Fatalf("bar not started: got %q", event)
	}
}

func TestGetListenerAddress(t *testing.T) {
	ts, events := startServer(t)
	defer ts.Close()

	// Start process and check that it has started.
	req := &nanny.GetListenerAddressRequest{
		ReplicaSet: "bar",
		Listener:   "foo",
		Config:     makeConfig(),
	}

	reply := &protos.GetListenerAddressReply{}
	if err := protomsg.Call(context.Background(), protomsg.CallArgs{
		Client:  ts.Client(),
		Addr:    ts.URL,
		URLPath: getListenerAddressURL,
		Request: req,
		Reply:   reply,
	}); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(events); event != "getListenerPort bar foo" {
		t.Fatalf("foo port not requested: got %q", event)
	}
	expect := &protos.GetListenerAddressReply{
		Address: fmt.Sprintf(":%d", testListenerPort),
	}
	if diff := cmp.Diff(reply, expect, protocmp.Transform()); diff != "" {
		t.Fatalf("bad address reply: (-want +got)\n%s", diff)
	}
}

func TestGetRoutingInfo(t *testing.T) {
	ts, _ := startServer(t)
	defer ts.Close()

	cfg := makeConfig("sharded", "unsharded")

	// add adds a new resolvable address.
	addReplica := func(addr string) {
		req := &nanny.RegisterReplicaRequest{
			ReplicaSet:        "sharded",
			PodName:           "unused",
			BabysitterAddress: "unused",
			WeaveletAddress:   addr,
			Config:            cfg,
		}
		if err := protomsg.Call(context.Background(), protomsg.CallArgs{
			Client:  ts.Client(),
			Addr:    ts.URL,
			URLPath: registerReplicaURL,
			Request: req,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// activateComponent activates the given component.
	startComponent := func(component string, isRouted bool) {
		req := &nanny.ActivateComponentRequest{
			Component: component,
			Routed:    isRouted,
			Config:    cfg,
		}
		if err := protomsg.Call(context.Background(), protomsg.CallArgs{
			Client:  ts.Client(),
			Addr:    ts.URL,
			URLPath: activateComponentURL,
			Request: req,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// resolve returns sorted list of resolvable addresses along with an assignment.
	resolve := func(version, component string) *nanny.GetRoutingReply {
		req := &nanny.GetRoutingRequest{
			Component: component,
			Version:   version,
			Config:    cfg,
		}
		var reply nanny.GetRoutingReply
		if err := protomsg.Call(context.Background(), protomsg.CallArgs{
			Client:  ts.Client(),
			Addr:    ts.URL,
			URLPath: getRoutingInfoURL,
			Request: req,
			Reply:   &reply,
		}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(reply.Routing.Replicas)
		return &reply
	}

	routingInfo := resolve("", "nonexistent")
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

	// All existing values returned.
	addReplica(testData[0])
	addReplica(testData[1])

	// Register two components to start.
	startComponent("sharded", true)
	startComponent("unsharded", false)

	routingInfo = resolve("", "unsharded")
	if diff := cmp.Diff(testData[:2], routingInfo.Routing.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if routingInfo.Version == "" {
		t.Fatal("no version returned by resolver")
	}
	if routingInfo.Routing.Assignment != nil {
		t.Fatalf("want empty assignment, got %v", prototext.Format(routingInfo.Routing.Assignment))
	}
	routingInfo = resolve("", "sharded")
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

	// Add a new replica. Check that resolver hangs until there is a change.
	const delay = time.Millisecond * 500
	start := time.Now()
	go func() {
		time.Sleep(delay)
		addReplica(testData[2]) // Delayed change that should make resolve call succeed.
	}()
	routingInfo = resolve(routingInfo.Version, "sharded")
	finish := time.Now()
	if diff := cmp.Diff(testData[:3], routingInfo.Routing.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if elapsed := finish.Sub(start); elapsed < delay {
		t.Fatalf("resolver returned in %v; expecting >= %v", elapsed, delay)
	}
	if routingInfo.Routing.Component != "sharded" {
		t.Fatal("assignments should contain an assignment for sharded")
	}
	if routingInfo.Routing.Assignment.Version != 2 {
		t.Fatalf("unexpected assignment version; want: 2; got: %d\n", routingInfo.Routing.Assignment.Version)
	}
}
