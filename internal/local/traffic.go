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
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/local/proxy"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
)

// ApplyTraffic applies the traffic assignment on the proxy running at the given address.
func ApplyTraffic(ctx context.Context, proxyAddress string, assignment *nanny.TrafficAssignment, region string) error {
	// Construct the route request for the proxy.
	routes := map[string]*proxy.HostBackends{}
	for host, hostAssignment := range assignment.HostAssignment {
		var hostBackends proxy.HostBackends
		for _, alloc := range hostAssignment.Allocs {
			hostBackends.Backends = append(hostBackends.Backends, &proxy.Backend{
				Address: alloc.Listener.Addr,
				Weight:  alloc.TrafficFraction,
			})
		}
		routes[host] = &hostBackends
	}

	// Send the route request to the proxy.
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Host:    proxy.Host,
		Addr:    proxyAddress,
		URLPath: proxy.RouteURL,
		Request: &proxy.RouteRequest{Clump: region, HostBackends: routes},
	})
}
