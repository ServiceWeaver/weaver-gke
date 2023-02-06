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
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ServiceWeaver/weaver-gke/internal"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/assigner"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
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
		&logging.NewTestLogger(t).FuncLogger,
		"", // dialAddr
		func(addr string) clients.BabysitterClient {
			return &babysitter.HttpClient{Addr: internal.ToHTTPAddress(addr)}
		},
		func(context.Context, string) (bool, error) { return true, nil },
		nil, // recordListener
		func(_ context.Context, _ *config.GKEConfig, group *protos.ColocationGroup) error {
			events <- "start " + group.Name
			return nil
		},
		func(context.Context, string, []string) error { return nil },
		func(context.Context, string, []string) error { return nil }); err != nil {
		t.Fatal("error starting the manager server", err)
	}
	return httptest.NewServer(mux), events
}

func makeConfig() *config.GKEConfig {
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{
				Name: "todo",
			},
			Id:                "11111111-1111-1111-1111-111111111111",
			UseLocalhost:      true,
			ProcessPicksPorts: true,
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

	// Verify that the process "main" is started.
	if event := nextEvent(events); event != "start main" {
		t.Fatalf("main process not started: got %q", event)
	}
}

func TestStartProcess(t *testing.T) {
	ts, events := startServer(t)
	defer ts.Close()

	// Start process and check that it has started.
	req := &nanny.ColocationGroupStartRequest{
		AppName: "todo",
		Config:  makeConfig(),
		Group:   &protos.ColocationGroup{Name: "bar"},
	}

	if err := protomsg.Call(context.Background(), protomsg.CallArgs{
		Client:  ts.Client(),
		Addr:    ts.URL,
		URLPath: startColocationGroupURL,
		Request: req,
	}); err != nil {
		t.Fatal(err)
	}
	if event := nextEvent(events); event != "start bar" {
		t.Fatalf("bar not started: got %q", event)
	}
}

func TestGetRoutingInfo(t *testing.T) {
	ts, _ := startServer(t)
	defer ts.Close()

	cfg := makeConfig()
	d := cfg.Deployment

	// add adds a new resolvable address.
	addReplica := func(addr string) {
		req := &nanny.ReplicaToRegister{
			Replica: &protos.ReplicaToRegister{
				App:          d.App.Name,
				DeploymentId: d.Id,
				Process:      "foo",
				Address:      addr,
			},
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

	// startComponents registers a set of components to be started.
	startComponent := func(component string, isRouted bool) {
		req := &protos.ComponentToStart{
			App:          d.App.Name,
			DeploymentId: d.Id,
			Process:      "foo",
			Component:    component,
			IsRouted:     isRouted,
		}
		if err := protomsg.Call(context.Background(), protomsg.CallArgs{
			Client:  ts.Client(),
			Addr:    ts.URL,
			URLPath: startComponentURL,
			Request: req,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// resolve returns sorted list of resolvable addresses along with an assignment.
	resolve := func(v string) *protos.RoutingInfo {
		req := &protos.GetRoutingInfo{
			App:          d.App.Name,
			DeploymentId: d.Id,
			Process:      "foo",
			Version:      v,
		}
		var reply protos.RoutingInfo
		if err := protomsg.Call(context.Background(), protomsg.CallArgs{
			Client:  ts.Client(),
			Addr:    ts.URL,
			URLPath: getRoutingInfoURL,
			Request: req,
			Reply:   &reply,
		}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(reply.Replicas)
		return &reply
	}

	routingInfo := resolve("")
	if len(routingInfo.Replicas) != 0 {
		t.Fatalf("resolve(empty) = %v; want []", routingInfo.Replicas)
	}
	if routingInfo.Assignments != nil {
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
	startComponent("unshardedComponent", false)
	startComponent("shardedComponent", true)

	routingInfo = resolve("")
	if diff := cmp.Diff(testData[:2], routingInfo.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if routingInfo.Version == "" {
		t.Fatal("no version returned by resolver")
	}
	assignment, _ := assigner.FromProto(routingInfo.Assignments[0])
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
	routingInfo = resolve(routingInfo.Version)
	finish := time.Now()
	if diff := cmp.Diff(testData[:3], routingInfo.Replicas); diff != "" {
		t.Fatalf("unexpected resolver result (-want,+got):\n%s", diff)
	}
	if elapsed := finish.Sub(start); elapsed < delay {
		t.Fatalf("resolver returned in %v; expecting >= %v", elapsed, delay)
	}
	if len(routingInfo.Assignments) != 1 {
		t.Fatal("assignments should contain only one entry")
	}
	if routingInfo.Assignments[0].Component != "shardedComponent" {
		t.Fatal("assignments should contain an assignment for shardedComponent")
	}
	if routingInfo.Assignments[0].Version != 2 {
		t.Fatalf("unexpected assignment version; want: 2; got: %d\n", routingInfo.Assignments[0].Version)
	}
}
