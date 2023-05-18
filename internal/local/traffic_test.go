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

package local

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/proxy"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
)

type appVersion struct {
	config    *config.GKEConfig
	listeners []*nanny.Listener
}

func newAppVersion(appName string, versionId uuid.UUID, lis []*nanny.Listener) *appVersion {
	return &appVersion{
		config: &config.GKEConfig{
			Deployment: &protos.Deployment{
				SingleProcess: true,
				App: &protos.AppConfig{
					Name:   appName,
					Binary: appName,
				},
				Id: versionId.String(),
			},
		},
		listeners: lis,
	}
}

func startTestProxy(t *testing.T) (string, chan *proxy.RouteRequest) {
	rs := make(chan *proxy.RouteRequest, 1000)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("cannot create proxy network listener: %v", err)
	}
	go func() {
		logger := logging.NewTestSlogger(t, testing.Verbose())
		mux := http.NewServeMux()
		handleRoute := func(ctx context.Context, req *proxy.RouteRequest) error {
			rs <- req
			return nil
		}
		mux.HandleFunc(proxy.RouteURL, protomsg.HandlerDo(logger, handleRoute))
		http.Serve(lis, mux)
	}()
	return fmt.Sprintf("http://%s", lis.Addr().String()), rs
}

func TestApplyTraffic(t *testing.T) {
	ctx := context.Background()
	proxyAddr, routes := startTestProxy(t)

	v1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	v2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	versions := []*appVersion{
		newAppVersion(
			"todo",
			v1,
			[]*nanny.Listener{
				{Name: "h1", Addr: "1.1.1.1:1"},
				{Name: "h1", Addr: "2.2.2.2:2"},
				{Name: "h1", Addr: "3.3.3.3:3"},
			}),
		newAppVersion(
			"todo",
			v2,
			[]*nanny.Listener{
				{Name: "h2", Addr: "4.4.4.4:4"},
			}),
	}

	alloc := func(version uuid.UUID, weight float32,
		lis *nanny.Listener) *nanny.TrafficAllocation {
		return &nanny.TrafficAllocation{
			VersionId:       version.String(),
			TrafficFraction: weight,
			Listener:        lis,
		}
	}

	// Update the traffic assignment.
	assignment := &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"h1": {
				Allocs: []*nanny.TrafficAllocation{
					alloc(v1, 0.1, versions[0].listeners[0]),
					alloc(v1, 0.1, versions[0].listeners[1]),
					alloc(v1, 0.8, versions[0].listeners[2]),
				},
			},
			"h2": {
				Allocs: []*nanny.TrafficAllocation{
					alloc(v2, 1.0, versions[1].listeners[0]),
				},
			},
		},
	}
	if err := applyTraffic(ctx, proxyAddr, assignment, "region"); err != nil {
		t.Fatalf("UpdateTrafficShare: %v", err)
	}

	pb := func(addr string, weight float32) *proxy.Backend {
		return &proxy.Backend{Address: addr, Weight: weight}
	}

	// Check that the proxy is updated with the correct information.
	got := <-routes
	want := &proxy.RouteRequest{
		Clump: "region",
		HostBackends: map[string]*proxy.HostBackends{
			"h1": {Backends: []*proxy.Backend{
				pb("1.1.1.1:1", 0.1),
				pb("2.2.2.2:2", 0.1),
				pb("3.3.3.3:3", 0.8),
			}},
			"h2": {Backends: []*proxy.Backend{pb("4.4.4.4:4", 1.0)}},
		},
	}

	// Check that the traffic map at the proxy is as expected.
	if diff := cmp.Diff(got, want, protocmp.Transform(),
		protocmp.SortRepeatedFields(&ListenState{}, "listeners")); diff != "" {
		t.Fatalf("bad routes: (-want +got)\n%s", diff)
	}
}
