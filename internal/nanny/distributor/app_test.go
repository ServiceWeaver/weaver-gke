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

package distributor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	sync "sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// hostsState represents the traffic state for all hosts at a given time.
type hostsState struct {
	tick   int
	states []hostState
}

// hostState represents a state for the given hostname.
type hostState struct {
	Host   string
	Public bool
	Shares []trafficShare
}

// trafficShare represents a traffic share assigned to a given app version.
type trafficShare struct {
	Ver  string  // version name
	Frac float32 // traffic fraction assigned to the version
}

// versionAppTest represents an app version.
type versionAppTest struct {
	id                   uuid.UUID      // version id
	name                 string         // version name
	submissionId         int            // submission id
	targetFn             []fractionSpec // version target function
	listeners            []*protos.Listener
	publicListenerConfig []*config.GKEConfig_PublicListener
}

// fractionSpec represent a traffic fraction assignment and its duration.
type fractionSpec struct {
	frac float32 // assigned traffic fraction
	dur  int     // duration of frac assignment, in ticks
}

var (
	// Map from version name to a unique identifier for the version.
	versionMu    sync.Mutex
	versionIDMap map[string]uuid.UUID = map[string]uuid.UUID{}
)

func getVersionID(name string) uuid.UUID {
	versionMu.Lock()
	defer versionMu.Unlock()
	if id, ok := versionIDMap[name]; ok {
		return id
	}
	id := uuid.New()
	versionIDMap[name] = id
	return id
}

// newVersion creates a new app version.
func newAppVersion(name string, submissionId int, listeners []string, targetFn ...fractionSpec) versionAppTest {
	v := versionAppTest{
		id:           getVersionID(name),
		name:         name,
		submissionId: submissionId,
		targetFn:     targetFn,
	}
	for _, l := range listeners {
		if strings.Contains(l, ".") { // public listener
			name := strings.Split(l, ".")[0]
			v.publicListenerConfig = append(v.publicListenerConfig, &config.GKEConfig_PublicListener{
				Name:     name,
				Hostname: l,
			})
			l = name
		}
		v.listeners = append(v.listeners, &protos.Listener{Name: l})
	}
	return v
}

// f creates a new fraction specification.
func f(dur int, frac float32) fractionSpec {
	return fractionSpec{
		frac: frac,
		dur:  dur,
	}
}

// stateAt creates a new hosts state.
func stateAt(tick int, hosts ...hostState) hostsState {
	return hostsState{
		tick:   tick,
		states: hosts,
	}
}

// h creates a new host state.
func h(host string, public bool, shares ...trafficShare) hostState {
	return hostState{
		Host:   host,
		Public: public,
		Shares: shares,
	}
}

// share creates new traffic share.
func share(dep string, frac float32) trafficShare {
	return trafficShare{
		Ver:  dep,
		Frac: frac,
	}
}

// roundFrac rounds up the traffic fraction to two decimal places, for
// easier comparison using the cmp package.
func roundFrac(frac float32) float32 {
	return float32(math.Round(float64(frac)*100) / 100)
}

func toHostStates(versions map[string]versionAppTest, state *DistributorState) []hostState {
	var hosts []hostState
	for _, assignment := range []*nanny.TrafficAssignment{
		state.PublicTrafficAssignment,
		state.PrivateTrafficAssignment,
	} {
		if assignment == nil {
			continue
		}
		for host, hostAsn := range assignment.HostAssignment {
			var shares []trafficShare
			for _, alloc := range hostAsn.Allocs {
				// Round up to two decimal places for easier comparison
				// using the cmp package.
				frac := roundFrac(alloc.TrafficFraction)
				id := alloc.VersionId
				ver, ok := versions[id]
				if !ok {
					panic(fmt.Errorf("unknown version: %v", id))
				}
				shares = append(shares, share(ver.name, frac))
			}
			hosts = append(hosts, hostState{
				Host:   host,
				Public: assignment == state.PublicTrafficAssignment,
				Shares: shares,
			})
		}
	}
	return hosts
}

func TestUpdate(t *testing.T) {
	type testCase struct {
		name          string           // name of this test case
		versions      []versionAppTest // list of versions from oldest to newest
		expect        []hostsState     // expectation when traffic applied immediately
		expectDelayed []hostsState     // expectation when traffic applied with delay
	}

	const delayTicks = 4

	// Test assumptions:
	//   * At tick -1, all versions that don't have a target function specified
	//     are deployed.
	//   * At tick 0, all other versions are deployed.
	//   * The moment a version id deployed, all of its listeners are instantly
	//     registered.
	for _, c := range []testCase{
		{
			// Single version.
			//
			// Expected behavior:
			// Its listeners immediately get assigned 100% traffic share.
			name: "one_version",
			versions: []versionAppTest{
				newAppVersion("v", 1,
					[]string{"l1.host.com" /*public*/, "l2" /*private*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1), // initial state
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1), // initial state
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v", 1.0))),
			},
		},
		{
			// Two versions:
			// One installed and one new; both are exporting the same listener.
			//
			// Expected behavior:
			// The listener traffic should gradually shift to the new version.
			name: "two_versions_same_listener",
			versions: []versionAppTest{
				newAppVersion("v1", 1, []string{"l1" /*private*/}),
				newAppVersion("v2", 2, []string{"l1" /*private*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.9), share("v2", 0.1))),
				stateAt(10,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.3))),
				stateAt(20,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.2), share("v2", 0.8))),
				stateAt(30, // v2 fully rolled out
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.9), share("v2", 0.1))),
				stateAt(14,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.3))),
				stateAt(28,
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.2), share("v2", 0.8))),
				stateAt(42, // v2 fully rolled out
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0))),
			},
		},
		{
			// Two versions:
			// One installed and one new; both are exporting the same listener
			// name but using different hostnames.
			//
			// Expected behavior:
			// Listeners from both versions should receive 100% traffic because they
			// have different domain names.
			name: "two_versions_same_listener_different_hostnames",
			versions: []versionAppTest{
				newAppVersion("v1", 1, []string{"l1.host1.com" /*public*/}),
				newAppVersion("v2", 2, []string{"l1.host2.com" /*public*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.host1.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host1.com", true /*public*/, share("v1", 1.0)),
					h("l1.host2.com", true /*public*/, share("v2", 1.0))),
				stateAt(30, // v2 fully rolled out
					h("l1.host1.com", true /*public*/, share("v1", 1.0)),
					h("l1.host2.com", true /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.host1.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host1.com", true /*public*/, share("v1", 1.0)),
					h("l1.host2.com", true /*public*/, share("v2", 1.0))),
				stateAt(30, // v2 fully rolled out
					h("l1.host1.com", true /*public*/, share("v1", 1.0)),
					h("l1.host2.com", true /*public*/, share("v2", 1.0))),
			},
		},
		{
			// Two versions:
			// One installed and one new; both are exporting the same listener
			// but the listener is declared public in one version and private
			// in the other.
			//
			// Expected behavior:
			// Listeners from both versions should receive 100% traffic because
			// the traffic is public in one version and private in another.
			name: "two_versions_same_listener_public_and_private",
			versions: []versionAppTest{
				newAppVersion("v1", 1, []string{"l1.host.com" /*public*/}),
				newAppVersion("v2", 2, []string{"l1" /*private*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l1.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0))),
			},
		},
		{
			// Two versions:
			// One installed and one new; they are exporting partially overlapping listeners.
			//
			// Expected behavior:
			// 1) The listener that exists only in the installed version should
			//    retain 100% traffic share.
			//
			// 2) The listener that exists only in the new version should
			//    immediately get assigned 100% traffic share.
			//
			// 3) The listener that exists in both versions should see its traffic
			//    gradually shift to the new version.
			name: "two_versions_partial_listener_overlap",
			versions: []versionAppTest{
				newAppVersion("v1", 1,
					[]string{"l1.host.com" /*public*/, "l2" /*private*/}),
				newAppVersion("v2", 2,
					[]string{"l2" /*private*/, "l3.host.com" /*public*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.9), share("v2", 0.1)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(10,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.3)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(20,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(30, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.9), share("v2", 0.1)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(14,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.3)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(28,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(42, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
			},
		},
		{
			// Two versions:
			// Deploying at different rates, exporting partially overlapping listeners.
			//
			// Expected behavior:
			// Should be exactly the same as if though the older of the two versions
			// was already fully installed.  (See previous test.)
			name: "two_new_versions_partial_listener_overlap",
			versions: []versionAppTest{
				newAppVersion("v1", 1,
					[]string{"l1.host.com" /*public*/, "l2" /*private*/},
					f(10, 0.1), f(10, 0.3), f(10, 0.8)),
				newAppVersion("v2", 2,
					[]string{"l2" /*private*/, "l3.host.com" /*public*/},
					f(10, 0.2), f(10, 0.4), f(20, 0.7)),
			},
			expect: []hostsState{
				stateAt(-1), // initial state
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.8), share("v2", 0.2)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(10,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.6), share("v2", 0.4)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(20,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.7)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(30, // v1 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.7)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(40, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1), // initial state
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.8), share("v2", 0.2)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(14,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.6), share("v2", 0.4)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(28,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.7)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(42, // v1 fully rolled out
					// NOTE: v2 traffic fraction hasn't changed since the
					// previous state, so v2 won't experience any traffic
					// application delays. This state will therefore take only
					// 10 ticks to be fully applied.
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.7)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
				stateAt(52, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 1.0)),
					h("l3.host.com", true /*public*/, share("v2", 1.0))),
			},
		},
		{
			// Three versions:
			// One installed and two newer ones; exporting partially overlapping listeners.
			//
			// Expected behavior:
			// 1) The listener that exists only in the installed version should
			//    retain 100% traffic share.
			//
			// 2) The listeners that exist in all three versions should get
			//    their traffic share assigned in the order of newest to oldest,
			//    using the corresponding target functions.  All the leftover traffic
			//    share should flow to the older versions.
			//
			// 3) The listeners that exist only in one of the new versions should
			//    immediately get assigned 100% traffic share.
			name: "three_versions_partial_listener_overlap",
			versions: []versionAppTest{
				newAppVersion("v1", 1, []string{
					"l1.host.com", // public
					"l2",          // private
					"l3.host.com", // public
				}),
				newAppVersion("v2", 2, []string{
					"l2",          // private
					"l3.host.com", // public
					"l4",          // private
				}, f(10, 0.1), f(10, 0.3), f(10, 0.8)),
				newAppVersion("v3", 3, []string{
					"l2",          // private
					"l4",          // private
					"l5.host.com", // public
				}, f(10, 0.2), f(10, 0.4), f(20, 0.7)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0)),
					h("l3.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.1), share("v3", 0.2)),
					h("l3.host.com", true /*public*/, share("v1", 0.9), share("v2", 0.1)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.8), share("v3", 0.2)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(10,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.3), share("v3", 0.4)),
					h("l3.host.com", true /*public*/, share("v1", 0.7), share("v2", 0.3)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.6), share("v3", 0.4)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(20,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(30,
					// NOTE: v2 rollout is paused forever at this point because
					// its l2 listener hasn't been getting the expected traffic
					// (i.e., 0.8) to successfully validate, due to v3.
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(40, // v3 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v3", 1.0)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v3", 1.0)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 1.0)),
					h("l3.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.7), share("v2", 0.1), share("v3", 0.2)),
					h("l3.host.com", true /*public*/, share("v1", 0.9), share("v2", 0.1)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.8), share("v3", 0.2)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(14,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v1", 0.3), share("v2", 0.3), share("v3", 0.4)),
					h("l3.host.com", true /*public*/, share("v1", 0.7), share("v2", 0.3)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.6), share("v3", 0.4)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(28,
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(42,
					// NOTE: v2 rollout is paused forever at this point because
					// its l2 listener hasn't been getting the expected traffic
					// (i.e., 0.8) to successfully validate, due to v3.
					// NOTE: v3 traffic fraction hasn't changed since the
					// previous state, so v3 won't experience any traffic
					// application delays. This state will therefore take only
					// 10 ticks to be fully applied.
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v2", 0.3), share("v3", 0.7)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
				stateAt(52, // v3 fully rolled out
					h("l1.host.com", true /*public*/, share("v1", 1.0)),
					h("l2.us-west1.serviceweaver.internal", false /*public*/, share("v3", 1.0)),
					h("l3.host.com", true /*public*/, share("v1", 0.2), share("v2", 0.8)),
					h("l4.us-west1.serviceweaver.internal", false /*public*/, share("v3", 1.0)),
					h("l5.host.com", true /*public*/, share("v3", 1.0))),
			},
		},
		{
			// Three versions that export the same listener. The submission order is
			// different from the version order.
			//
			// Expected behavior:
			// The listener traffic should gradually shift to the newest version.
			name: "three_versions",
			versions: []versionAppTest{
				newAppVersion("v1", 1, []string{"l1.host.com" /*public*/}),
				newAppVersion("v2", 4, []string{"l1.host.com" /*public*/},
					f(10, 0.1), f(10, 0.8)),
				newAppVersion("v3", 2, []string{"l1.host.com" /*public*/},
					f(10, 0.1), f(10, 0.8)),
			},
			expect: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 0.8), share("v3", 0.1), share("v2", 0.1))),
				stateAt(10,
					h("l1.host.com", true /*public*/, share("v3", 0.2), share("v2", 0.8))),
				stateAt(20, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v2", 1.0))),
			},
			expectDelayed: []hostsState{
				stateAt(-1, // initial state
					h("l1.host.com", true /*public*/, share("v1", 1.0))),
				stateAt(0,
					h("l1.host.com", true /*public*/, share("v1", 0.8), share("v3", 0.1), share("v2", 0.1))),
				stateAt(14,
					h("l1.host.com", true /*public*/, share("v3", 0.2), share("v2", 0.8))),
				stateAt(28, // v2 fully rolled out
					h("l1.host.com", true /*public*/, share("v2", 1.0))),
			},
		},
	} {
		c := c
		// We run the test in two modes: one where the traffic changes are
		// immediately materialized and one where they are delayed.
		for _, doDelay := range []bool{false, true} {
			doDelay := doDelay
			testName := c.name
			expect := c.expect
			if doDelay {
				testName += "_delayed"
				expect = c.expectDelayed
			}
			t.Run(testName, func(t *testing.T) {
				ctx := context.Background()
				versions := map[string]versionAppTest{} // versionId -> version
				for _, v := range c.versions {
					versions[v.id.String()] = v
				}

				// Go a little past the last clock tick.
				endTick := expect[len(expect)-1].tick + 10

				var tick int = -1 // Current clock tick

				// (Historic) host states at each tick.
				// actualStates[i] stores the state at tick i-1.
				actualStates := make([][]hostState, endTick+1)

				// Create a distributor for a single app and register test case
				// versions for it.
				distributor, err := Start(ctx,
					http.NewServeMux(), // unused
					store.NewFakeStore(),
					&logging.NewTestLogger(t).FuncLogger,
					&mockManagerClient{nil, nil, nil, nil},
					testRegion,
					nil, // babysitterConstructor
					0,   // manageAppsInterval
					0,   // computeTrafficInterval
					0,   // applyTrafficInterval
					0,   // detectAppliedTrafficInterval
					nil, // applyTraffic
					func(_ context.Context, cfg *config.GKEConfig) ([]*protos.Listener, error) {
						id := runtime.DeploymentID(cfg.Deployment)
						ver, ok := versions[id.String()]
						if !ok {
							panic(fmt.Errorf("unknown version id: %v", id))
						}
						return ver.listeners, nil
					},
					func(ctx context.Context, metric string, labels ...string) ([]MetricCount, error) {
						if metric != "serviceweaver_http_request_count" || len(labels) != 3 || labels[0] != "serviceweaver_app" || labels[1] != "serviceweaver_version" || labels[2] != "host" {
							panic("invalid metric information requested")
						}
						// Get a (possibly) delayed traffic state.
						targetTick := tick
						if doDelay {
							targetTick -= delayTicks
						}
						if targetTick < -1 {
							return nil, nil
						}
						actualState := actualStates[targetTick+1]
						var ret []MetricCount
						for _, hs := range actualState {
							for _, share := range hs.Shares {
								id := getVersionID(share.Ver)
								ret = append(ret, MetricCount{
									LabelVals: []string{"app", id.String(), hs.Host},
									Count:     float64(share.Frac),
								})
							}
						}
						return ret, nil
					},
				)
				if err != nil {
					t.Fatalf("error creating state: %v", err)
				}
				if err := distributor.registerApp(ctx, "app"); err != nil {
					t.Fatalf("error creating app: %v", err)
				}
				// Pick a fixed time in the past for determinism.
				now := time.Date(1977, time.June, 25, 0, 0, 0, 0, time.UTC)
				for _, v := range c.versions {
					id := v.id
					targetFn := &nanny.TargetFn{}
					for _, frac := range v.targetFn {
						targetFn.Fractions = append(targetFn.Fractions,
							&nanny.FractionSpec{
								Duration:        durationpb.New(time.Second * time.Duration(frac.dur)),
								TrafficFraction: frac.frac,
							})
					}
					req := &nanny.ApplicationDistributionRequest{
						AppName: "app",
					}
					req.Requests = append(req.Requests, &nanny.VersionDistributionRequest{
						Config: &config.GKEConfig{
							Deployment: &protos.Deployment{
								Id: id.String(),
								App: &protos.AppConfig{
									Name: "app",
								},
							},
							PublicListener: v.publicListenerConfig,
						},
						TargetFn:     targetFn,
						SubmissionId: int64(v.submissionId),
					})

					if err := distributor.registerVersions(ctx, req, now); err != nil {
						t.Fatalf("error adding app versions %v: %v", req, err)
					}
					if err := distributor.mayDeployApp(ctx, "app"); err != nil {
						t.Fatalf("error managing app versions %v: %v", req, err)
					}
					if err := distributor.getListenerState(ctx, "app"); err != nil {
						t.Fatalf("error getting listener state for app version %v: %v", id, err)
					}
					if len(targetFn.Fractions) <= 0 {
						// The test case wants this deployment already installed:
						// call the update functions right away.
						if _, err := distributor.ComputeTrafficAssignments(ctx, now); err != nil {
							t.Fatalf("testCase(%+v)\nerror applying initial state: %v", c, err)
						}
					}
				}

				// Run through the clock ticks and make sure the traffic assignment
				// matches the test case expectation.
				expectIdx := 0
				for ; tick < endTick; tick++ {
					if expectIdx < len(expect)-1 && tick == (expect[expectIdx+1].tick) {
						expectIdx++
					}
					expect := expect[expectIdx].states

					if tick > -1 {
						if _, err := distributor.ComputeTrafficAssignments(ctx, now); err != nil {
							t.Fatalf("testCase(%+v)\nupdate(%d)\ncannot compute traffic assignments: %v", c, tick, err)
						}
						now = now.Add(time.Second)
					}
					state, _, err := distributor.loadState(ctx)
					if err != nil {
						t.Fatal(err)
					}
					actual := toHostStates(versions, state)
					actualStates[tick+1] = actual // save for later
					if tick > -1 {
						if err := distributor.detectAppliedTraffic(ctx, time.Second); err != nil {
							t.Fatalf("testCase(%+v)\nupdate(%d)\ncannot detect applied traffic: %v", c, tick, err)
						}
					}
					// Check that the application traffic assignment matches the
					// expected values.
					sortFn := func(s []hostState) func(i, j int) bool {
						return func(i, j int) bool {
							return s[i].Host < s[j].Host
						}
					}
					sort.Slice(expect, sortFn(expect))
					sort.Slice(actual, sortFn(actual))
					if diff := cmp.Diff(expect, actual); diff != "" {
						t.Fatalf("bad state @ tick %d: (-want,+got):\n%s", tick, diff)
					}
				}
			})
		}
	}
}
