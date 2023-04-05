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

package nanny

import (
	"fmt"

	"github.com/ServiceWeaver/weaver/runtime/protomsg"
)

// CascadeTarget encapsulates the traffic specific information about a version
// of a Service Weaver app. Typically, you create one CascadeTarget for every version of
// your app and then call CascadeTraffic to assign traffic between them.
type CascadeTarget struct {
	Location        string                 // e.g. "local", "us-central1"
	AppName         string                 // e.g. "collatz", "todo"
	VersionId       string                 // e.g. "6ba7b810-9dad-11d1"
	Listeners       map[string][]*Listener // listeners, keyed by host
	TrafficFraction float32                // the desired traffic fraction
}

// CascadeTraffic assigns traffic to a set of targets. For every host, a
// traffic fraction of 1.0 is cascaded across the versions that export the
// host.
//
// For example, imagine we version v1, v2, and v3 that export hosts [a, c],
// [a, b, c], and [b, c] respectively. v1, v2, and v3 have desired traffic
// fractions of 0.5, 0.4, and 0.4. We cascade traffic between the listeners as
// follows:
//
//	v1 (0.5) | v2 (0.4) | v3 (0.4)
//	---------+----------+---------
//	 a (0.6) |  a (0.4) |
//	         |  b (0.6) |  b (0.4)
//	 c (0.2) |  c (0.4) |  c (0.4)
func CascadeTraffic(targets []*CascadeTarget) (*TrafficAssignment, error) {
	// Collect the set of hosts.
	hosts := map[string]bool{}
	for _, target := range targets {
		for host := range target.Listeners {
			hosts[host] = true
		}
	}

	// Calculate a HostTrafficAssignment for every host.
	traffic := &TrafficAssignment{
		HostAssignment: map[string]*HostTrafficAssignment{},
	}
	for host := range hosts {
		hostTraffic, err := cascadeTrafficForHost(targets, host)
		if err != nil {
			return nil, err
		}
		traffic.HostAssignment[host] = hostTraffic
	}
	return traffic, nil
}

// cascadeTrafficForHost computes the traffic assignment for a single host.
func cascadeTrafficForHost(targets []*CascadeTarget, host string) (*HostTrafficAssignment, error) {
	// Select the targets that export the host. We call these candidates.
	var candidates []*CascadeTarget
	for _, target := range targets {
		if _, ok := target.Listeners[host]; ok {
			candidates = append(candidates, target)
		}
	}

	// Compute the traffic fraction for every candidate.
	fractions := make([]float32, len(candidates))
	for i, candidate := range candidates {
		fractions[i] = candidate.TrafficFraction
	}
	traffic, err := cascade(1.0, fractions)
	if err != nil {
		return nil, err
	}

	// Massage the candidates and traffic fractions into protos.
	var allocs []*TrafficAllocation
	for i, candidate := range candidates {
		numListeners := len(candidate.Listeners[host])
		if traffic[i] == 0 || numListeners == 0 {
			continue
		}

		var allocated float32
		trafficFraction := traffic[i] / float32(numListeners)
		for j, listener := range candidate.Listeners[host] {
			if j == numListeners-1 { // allocate the leftover traffic to the last listener
				trafficFraction = traffic[i] - allocated
			}
			allocs = append(allocs, &TrafficAllocation{
				Location:        candidate.Location,
				AppName:         candidate.AppName,
				VersionId:       candidate.VersionId,
				TrafficFraction: trafficFraction,
				Listener:        listener,
			})
			allocated += trafficFraction
		}
	}
	return &HostTrafficAssignment{Allocs: allocs}, nil
}

// cascade waterfalls a value across a set of targets, from the last to the
// first. For example, given targets [t1, t2, t3] = [0.5, 0.6, 0.7] and total
// 1.0, Cascade will first assign 0.7 to t3. It will then assign the remaining
// 0.3 of the total to t2. Finally, it assigns 0 to t1.
//
// If the total exceeds the sum of the targets, then the excess total is
// assigned to the first target. For example, given targets [t1, t2, t3] =
// [0.1, 0.2, 0.3] and total 1.0, Cascade assigns 0.3 to t3, 0.2 to t2, and 0.5
// to t1.
func cascade(total float32, targets []float32) ([]float32, error) {
	if total < 0 {
		return nil, fmt.Errorf("negative total: %f", total)
	}
	if len(targets) == 0 {
		return []float32{}, nil
	}

	n := len(targets)
	allocation := make([]float32, n)
	for i := n - 1; i >= 0; i-- {
		if targets[i] < 0 {
			return nil, fmt.Errorf("negative targets[%d]: %f", i, targets[i])
		}
		if targets[i] <= total {
			allocation[i] = targets[i]
			total -= targets[i]
		} else {
			allocation[i] = total
			total = 0
		}
	}
	allocation[0] += total
	return allocation, nil
}

// MergeTrafficAssignments merges the traffic assignments for each hostname
// across multiple components.
//
// A component can be an application or a location (e.g., a cloud region).
//
// At a high-level, for a given hostname H it does the following:
//
//   - all of H's traffic allocations in each component are appended together to
//     form the set of allocations for H in the merged set.
//
//   - if H appears in N components, the traffic allocations for H in each
//     component are normalized by a factor of 1/N.
//
// The above algorithm has the property that if the traffic allocations for H
// were adding up to 1.0 in the individual components, they will add up to 1.0
// in the merged set as well.
func MergeTrafficAssignments(components []*TrafficAssignment) *TrafficAssignment {
	merged := map[string]*HostTrafficAssignment{}

	// Tracks the number of components for each hostname.
	numComponents := map[string]int{}

	// Aggregates the traffic allocations for each hostname.
	for _, a := range components {
		for host, assignment := range a.HostAssignment {
			if _, found := merged[host]; !found {
				merged[host] = &HostTrafficAssignment{}
			}

			// Make a copy of the traffic allocations, as they will subsequently change.
			var allocs []*TrafficAllocation
			for _, alloc := range assignment.Allocs {
				allocs = append(allocs, protomsg.Clone(alloc))
			}
			merged[host].Allocs = append(merged[host].Allocs, allocs...)
			numComponents[host] += 1
		}
	}

	// Normalize traffic allocations for each hostname.
	//
	// This can happen when the same hostname has traffic allocations across
	// multiple components.
	//
	// E.g.:
	//
	// app        hostname        traffic_allocations
	// -------------------------------------------------
	// app1         host1          (v1, 0.4),  (v2, 0.6)
	// app2         host1          (v1, 1.0)
	//
	// host1 appears in two apps, and the overall traffic share across app1 and
	// app2 for host1 should be equal to 1. I.e., we will have to normalize the
	// traffic allocations by 2 (app1 and app2)
	//
	// The final allocation is as follows:
	// app        hostname        traffic_allocations
	// -------------------------------------------------
	// app1         host1          (v1, 0.2),  (v2, 0.3)
	// app2         host1          (v1, 0.5)
	for host, assignment := range merged {
		if v := numComponents[host]; v > 1 {
			for _, trafficAlloc := range assignment.Allocs {
				trafficAlloc.TrafficFraction = trafficAlloc.TrafficFraction / float32(v)
			}
		}
	}
	return &TrafficAssignment{
		HostAssignment: merged,
	}
}
