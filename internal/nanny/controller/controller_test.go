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

package controller

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/pprof/profile"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// toUUID returns a valid version UUID string for a given digit.
func toUUID(d int) string {
	return strings.ReplaceAll(uuid.Nil.String(), "0", fmt.Sprintf("%d", d))
}

func TestRolloutRequests(t *testing.T) {
	// Test plan: Create and start a controller that contains rollout state for
	// 4 versions of an application. Verify that the controller generates a set
	// of rollout requests as expected.
	appName := "app"
	buildConfig := func(id int) *config.GKEConfig {
		return &config.GKEConfig{
			Deployment: &protos.Deployment{
				App: &protos.AppConfig{Name: appName},
				Id:  toUUID(id),
			},
		}
	}
	inputState := &AppState{
		Versions: map[string]*AppVersionState{
			toUUID(1): {
				// Description:
				//   v1 is fully rolled out.
				//
				// Expected behavior:
				//   No new rollout requests for v1 should be computed.
				Config:          buildConfig(1),
				RolloutStrategy: &RolloutStrategy{},
				// RolloutCompleted: true,
				SubmissionId: 0,
			},
			toUUID(2): {
				// Description:
				//   v2 was successfully rolled out in wave 0. However, the validation
				//   step didn't finish yet.
				//
				// Expected behavior:
				//   No new rollout requests for v2 should be computed.
				Config:          buildConfig(2),
				RolloutStrategy: &RolloutStrategy{},
				WaveIdx:         0,
				Distributors: map[string]*AppVersionDistributorState{
					"loc1": {
						Status: AppVersionDistributorStatus_ROLLED_OUT,
					},
				},
				SubmissionId: 1,
			},
			toUUID(3): {
				// Description:
				//   v3 was partially rolled out in wave 0 (i.e., successfully rolled
				//   out in loc1, still pending in loc2).
				//
				// Expected behavior:
				//   A new rollout request should be computed for loc2.
				Config:  buildConfig(3),
				WaveIdx: 0,
				Distributors: map[string]*AppVersionDistributorState{
					"loc1": {
						Status: AppVersionDistributorStatus_ROLLED_OUT,
					},
					"loc2": {
						Status: AppVersionDistributorStatus_STARTING,
					},
				},
				RolloutStrategy: &RolloutStrategy{
					Waves: []*RolloutWave{
						{
							TargetFunctions: map[string]*nanny.TargetFn{
								"loc1": {Fractions: []*nanny.FractionSpec{}},
							},
						},
						{
							TargetFunctions: map[string]*nanny.TargetFn{
								"loc2": {Fractions: []*nanny.FractionSpec{}},
							},
						},
					},
				},
				SubmissionId: 100,
			},
			toUUID(4): {
				// Description:
				//   v4 should roll out in wave 0 next. No locations in wave 0
				//   successfully rolled out v4 yet.
				//
				// Expected behavior:
				//   A new rollout request should be computed for loc2 and loc3.
				Config:  buildConfig(4),
				WaveIdx: 0,
				Distributors: map[string]*AppVersionDistributorState{
					"loc2": {
						Status: AppVersionDistributorStatus_STARTING,
					},
					"loc3": {
						Status: AppVersionDistributorStatus_STARTING,
					},
				},
				RolloutStrategy: &RolloutStrategy{
					Waves: []*RolloutWave{
						{
							TargetFunctions: map[string]*nanny.TargetFn{
								"loc2": {Fractions: []*nanny.FractionSpec{}},
								"loc3": {Fractions: []*nanny.FractionSpec{}},
							},
						},
					},
				},
				SubmissionId: 10,
			},
		},
	}

	// Start the controller.
	ctrl, err := startController(context.Background(), t, func(string) endpoints.Distributor {
		panic("unimplemented")
	})
	if err != nil {
		t.Fatalf("unable to start the controller: %v", err)
	}

	// Update the controller with information regarding the managed distributors
	// and the managed applications.
	ctx := context.Background()
	if err := ctrl.saveState(ctx, &ControllerState{
		Applications: map[string]bool{appName: true},
		Distributors: map[string]*DistributorState{
			"loc1": {
				Location: &RolloutRequest_Location{DistributorAddr: "dist_1"},
			},
			"loc2": {
				Location: &RolloutRequest_Location{DistributorAddr: "dist_2"},
			},
			"loc3": {
				Location: &RolloutRequest_Location{DistributorAddr: "dist_3"},
			},
		},
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Note that the expected requests should contain entries for v4 before v3,
	// because v3 is a newer version than v4.
	v3 := inputState.Versions[toUUID(3)]
	v4 := inputState.Versions[toUUID(4)]
	expectedState := map[string]*nanny.ApplicationDistributionRequest{
		"loc2": {
			AppName: appName,
			Requests: []*nanny.VersionDistributionRequest{
				{
					Config:       v4.Config,
					TargetFn:     v4.RolloutStrategy.Waves[v4.WaveIdx].TargetFunctions["loc2"],
					SubmissionId: 10,
				},
				{
					Config:       v3.Config,
					TargetFn:     v3.RolloutStrategy.Waves[v3.WaveIdx].TargetFunctions["loc2"],
					SubmissionId: 100,
				},
			},
		},
		"loc3": {
			AppName: appName,
			Requests: []*nanny.VersionDistributionRequest{
				{
					Config:       v4.Config,
					TargetFn:     v4.RolloutStrategy.Waves[v4.WaveIdx].TargetFunctions["loc3"],
					SubmissionId: 10,
				},
			},
		},
	}

	// Force the applications to compute new rollout requests (if any).
	gotState := ctrl.createDistributionRequests(appName, inputState)

	// Sort the expected and the actual states.
	sortFn := func(requests map[string]*nanny.ApplicationDistributionRequest) {
		for _, req := range requests {
			sort.Slice(req.Requests, func(i, j int) bool {
				return req.Requests[i].Config.Deployment.Id < req.Requests[j].Config.Deployment.Id
			})
		}
	}
	sortFn(gotState)
	sortFn(expectedState)
	if diff := cmp.Diff(expectedState, gotState, protocmp.Transform()); diff != "" {
		t.Fatalf("bad rollout requests (-want +got):\n%s", diff)
	}
}

func TestUpdateRolloutStatus(t *testing.T) {
	type testCase struct {
		name             string
		versions         map[string]*AppVersionState
		expectedVersions map[string]*AppVersionState
	}
	appName := "app"
	buildConfig := func(id int) *config.GKEConfig {
		return &config.GKEConfig{
			Deployment: &protos.Deployment{
				App: &protos.AppConfig{Name: appName},
				Id:  toUUID(1),
			},
		}
	}
	for _, c := range []testCase{
		{
			// Test plan:
			//   v1 is fully rolled out.
			//
			// Expected behavior:
			//   The rollout status for v1 shouldn't change.
			name: "rollout_complete",
			versions: map[string]*AppVersionState{
				toUUID(1): {
					Config:          buildConfig(1),
					RolloutStrategy: &RolloutStrategy{},
					// RolloutCompleted: true,
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLED_OUT,
						},
					},
				},
			},
			expectedVersions: map[string]*AppVersionState{
				toUUID(1): {
					Config:          buildConfig(1),
					RolloutStrategy: &RolloutStrategy{},
					// RolloutCompleted: true,
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLED_OUT,
						},
					},
				},
			},
		},
		{
			// Test plan:
			//   v2's rollout is currently in progress in wave 0.
			//
			// Expected behavior:
			//   The rollout status for v2 shouldn't change.
			name: "rollout_within_wave_completed_wait",
			versions: map[string]*AppVersionState{
				toUUID(2): {
					Config:          buildConfig(2),
					RolloutStrategy: &RolloutStrategy{},
					WaveIdx:         0,
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLING_OUT,
						},
					},
				},
			},
			expectedVersions: map[string]*AppVersionState{
				toUUID(2): {
					Config:          buildConfig(2),
					RolloutStrategy: &RolloutStrategy{},
					WaveIdx:         0,
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLING_OUT,
						},
					},
				},
			},
		},
		{
			// Test plan:
			//   v3's rollout finished in wave 0, and the validation time also passed.
			//
			// Expected behavior:
			//   The rollout for v3 should advance to the next wave.
			name: "rollout_within_wave_completed_wait_completed_advance_to_next_wave",
			versions: map[string]*AppVersionState{
				toUUID(3): {
					Config:            buildConfig(3),
					WaveIdx:           0,
					TimeWaveRolledOut: timestamppb.Now(),
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLED_OUT,
						},
						"loc2": {
							WaveIdx: 1,
							Status:  AppVersionDistributorStatus_UNKNOWN,
						},
						"loc3": {
							WaveIdx: 1,
							Status:  AppVersionDistributorStatus_UNKNOWN,
						},
					},
					RolloutStrategy: &RolloutStrategy{
						Waves: []*RolloutWave{
							{
								TargetFunctions: map[string]*nanny.TargetFn{
									"loc1": {Fractions: []*nanny.FractionSpec{}},
								},
								WaitTime: durationpb.New(0),
							},
							{
								TargetFunctions: map[string]*nanny.TargetFn{
									"loc2": {Fractions: []*nanny.FractionSpec{}},
									"loc3": {Fractions: []*nanny.FractionSpec{}},
								},
								WaitTime: durationpb.New(0),
							},
						},
					},
				},
			},
			expectedVersions: map[string]*AppVersionState{
				toUUID(3): {
					Config:  buildConfig(3),
					WaveIdx: 1,
					Distributors: map[string]*AppVersionDistributorState{
						"loc1": {
							WaveIdx: 0,
							Status:  AppVersionDistributorStatus_ROLLED_OUT,
						},
						"loc2": {
							WaveIdx: 1,
							Status:  AppVersionDistributorStatus_STARTING,
						},
						"loc3": {
							WaveIdx: 1,
							Status:  AppVersionDistributorStatus_STARTING,
						},
					},
					RolloutStrategy: &RolloutStrategy{
						Waves: []*RolloutWave{
							{
								TargetFunctions: map[string]*nanny.TargetFn{
									"loc1": {Fractions: []*nanny.FractionSpec{}},
								},
								WaitTime: durationpb.New(0),
							},
							{
								TargetFunctions: map[string]*nanny.TargetFn{
									"loc2": {Fractions: []*nanny.FractionSpec{}},
									"loc3": {Fractions: []*nanny.FractionSpec{}},
								},
								WaitTime: durationpb.New(0),
							},
						},
					},
				},
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()

			// Start the controller.
			ctrl, err := startController(ctx, t, func(string) endpoints.Distributor {
				panic("unimplemented")
			})
			if err != nil {
				t.Fatalf("unable to start the controller: %v", err)
			}

			// Update the controller with information regarding the managed distributors
			// and the managed applications.
			if err := ctrl.saveAppState(ctx, appName, &AppState{Versions: c.versions}, nil); err != nil {
				t.Fatal(err)
			}
			if err := ctrl.saveState(ctx, &ControllerState{
				Applications: map[string]bool{appName: true},
				Distributors: map[string]*DistributorState{
					"loc1": {
						Location: &RolloutRequest_Location{DistributorAddr: "dist_1"},
					},
					"loc2": {
						Location: &RolloutRequest_Location{DistributorAddr: "dist_2"},
					},
					"loc3": {
						Location: &RolloutRequest_Location{DistributorAddr: "dist_3"},
					},
				},
			}, nil); err != nil {
				t.Fatal(err)
			}

			// Force the applications to update the rollout status.
			ctrl.advanceRollouts(ctx, appName)

			// Verify that the state modified as expected.
			appState, _, err := ctrl.loadAppState(ctx, appName)
			if err != nil {
				t.Fatal(err)
			}
			gotVersions := appState.Versions
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.IgnoreFields(&AppVersionState{}, "time_wave_started"),
			}
			if diff := cmp.Diff(c.expectedVersions, gotVersions, opts...); diff != "" {
				t.Fatalf("(-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetTrafficAssignment(t *testing.T) {
	type testCase struct {
		name                      string
		versions                  []version
		expectedTrafficAssignment *nanny.TrafficAssignment
	}
	for _, c := range []testCase{
		{
			// Test plan:
			// One app deployed in a single location with a single listener l1.

			// Expected behavior:
			// All the traffic should go to l1.
			name: "one_app_single_location",
			versions: []version{
				newVersion([]string{"loc1"}, "app", toUUID(1), "10m", "l1.host.com" /*public*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app deployed in two locations. The app runs different versions in
			// each location. Among the two locations, it exports one common listener l2.

			// Expected behavior:
			// 1) For the listener exported in both locations (l2), 0.5 traffic should
			//    go to location 1 and 0.5 traffic should go to location 2.
			// 2) For all the other listeners, the whole traffic should go to the
			//    corresponding location.
			name: "one_app_two_locations",
			versions: []version{
				newVersion([]string{"loc1"}, "app", toUUID(1), "10m",
					"l1.host.com" /*public*/, "l2.host.com" /*public*/),
				newVersion([]string{"loc2"}, "app", toUUID(2), "10m",
					"l2.host.com" /*public*/, "l3.host.com" /*public*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l2.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l2"},
							},
							{
								Location:        "loc2",
								AppName:         "app",
								VersionId:       toUUID(2),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l2"},
							},
						},
					},
					"l3.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc2",
								AppName:         "app",
								VersionId:       toUUID(2),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l3"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app deployed in two locations. The app runs different versions in
			// each location. Among the two locations, it exports the same listener l1.
			// However, in each version the listener has a different hostname.

			// Expected behavior:
			// The traffic assignment has two different hostnames. Each of them gets
			// the entire traffic for its corresponding location.
			name: "one_app_two_locations_different_domains",
			versions: []version{
				newVersion([]string{"loc1"}, "app", toUUID(1), "10m", "l1.host1.com" /*public*/),
				newVersion([]string{"loc2"}, "app", toUUID(2), "10m", "l1.host2.com" /*public*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host1.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l1.host2.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc2",
								AppName:         "app",
								VersionId:       toUUID(2),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps in different locations. The apps export the same listener l1.

			// Expected behavior:
			// The listener gets two traffic allocations, 0.5 traffic allocation for
			// app1 in loc1 and 0.5 traffic allocation for app2 in loc2.
			name: "two_apps_non_overlapping_locations_same_listener",
			versions: []version{
				newVersion([]string{"loc1"}, "app1", toUUID(1), "10m", "l1.host.com" /*public*/),
				newVersion([]string{"loc2"}, "app2", toUUID(2), "10m", "l1.host.com" /*public*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l1"},
							},
							{
								Location:        "loc2",
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps running in the same two locations. Each app has one listener
			// exported in both locations (e.g., app1 has l2, app2 has l3). Also, both
			// apps have one listener exported in all the locations (l1).

			// Expected behavior:
			// 1) The listener exported by both apps in all the locations gets two
			//    traffic allocations (1 for each location for the latest version).
			// 2) The listeners exported only by an app (l2, l3), but across multiple
			//    locations, get two allocations in total.
			name: "two_apps_common_listener_between_versions_common_listener_between_apps",
			versions: []version{
				newVersion([]string{"loc1"}, "app1", toUUID(1), "10m",
					"l1.host.com" /*public*/, "l2.host.com" /*public*/),
				newVersion([]string{"loc2"}, "app1", toUUID(2), "10m",
					"l1.host.com" /*public*/, "l2.host.com" /*public*/),
				newVersion([]string{"loc1"}, "app2", toUUID(3), "10m",
					"l1.host.com" /*public*/, "l3.host.com" /*public*/),
				newVersion([]string{"loc2"}, "app2", toUUID(4), "10m",
					"l1.host.com" /*public*/, "l3.host.com" /*public*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app2",
								VersionId:       toUUID(3),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l1"},
							},
							{
								Location:        "loc2",
								AppName:         "app2",
								VersionId:       toUUID(4),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l2.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l2"},
							},
							{
								Location:        "loc2",
								AppName:         "app1",
								VersionId:       toUUID(2),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l2"},
							},
						},
					},
					"l3.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app2",
								VersionId:       toUUID(3),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l3"},
							},
							{
								Location:        "loc2",
								AppName:         "app2",
								VersionId:       toUUID(4),
								TrafficFraction: 0.5,
								Listener:        &nanny.Listener{Name: "l3"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app deployed in a single location with two listeners: one
			// private and the other public.

			// Expected behavior:
			// 1) The public listener gets 1.0 traffic allocation.
			// 2) The private listener is ignored.
			name: "one_app_single_location",
			versions: []version{
				newVersion([]string{"loc1"}, "app", toUUID(1), "10m",
					"l1.host.com" /*public*/, "l2" /*private*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app deployed in two locations with the same listener.
			// In one location, the listener is public; in the other, it is
			// private.

			// Expected behavior:
			// 1) The listener gets 1.0 traffic fraction in the location
			//    where it is public.
			name: "",
			versions: []version{
				newVersion([]string{"loc1"}, "app", toUUID(1), "10m", "l1.host.com" /*public*/),
				newVersion([]string{"loc2"}, "app", toUUID(2), "10m", "l1" /*private*/),
			},
			expectedTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        "loc1",
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1.0,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			versions := map[string]version{} // versionId -> version
			for _, v := range c.versions {
				versions[v.id] = v
			}

			// Compute the traffic assignment at each distributor location,
			// assuming that all versions are fully rolled out.
			type appVersionListener struct {
				app, version string
				lis          *nanny.Listener
			}
			locAssignments := map[string]map[string]appVersionListener{}
			for _, v := range c.versions {
				for _, loc := range v.locations {
					a := locAssignments[loc]
					if a == nil {
						a = map[string]appVersionListener{}
						locAssignments[loc] = a
					}
					for host, lis := range v.listeners {
						a[host] = appVersionListener{v.appName, v.id, lis}
					}
				}
			}

			// Start the distributors (one for each location where an
			// application version should run).
			distributors := map[string]endpoints.Distributor{}
			for loc, a := range locAssignments {
				hosts := map[string]*nanny.HostTrafficAssignment{}
				for host, v := range a {
					hosts[host] = &nanny.HostTrafficAssignment{
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        loc,
								AppName:         v.app,
								VersionId:       v.version,
								TrafficFraction: 1.0,
								Listener:        v.lis,
							},
						},
					}
				}
				distributors[loc] = &mockDistributorClient{
					publicTraffic: &nanny.TrafficAssignment{
						HostAssignment: hosts,
					},
				}
			}

			// Start the controller.
			controller, err := startController(ctx, t, func(addr string) endpoints.Distributor {
				distributor, ok := distributors[addr]
				if !ok {
					panic(fmt.Sprintf("distributor %q not found", addr))
				}
				return distributor
			})
			if err != nil {
				t.Fatalf("unable to start the controller: %v", err)
			}

			// Send the applications versions to the controller.
			for _, v := range c.versions {
				if err := registerNewAppVersion(ctx, controller, v); err != nil {
					t.Fatalf("unable to register app version %v due to %v", v.id, err)
				}
			}

			// Manage the applications state.
			controller.manageState(ctx)

			// Retrieve the latest traffic assignments from the distributors.
			controller.fetchTrafficAssignments(ctx)

			// Compute the merged traffic assignment.
			gotTrafficAssignment, err := controller.traffic(ctx)
			if err != nil {
				t.Fatal(err)
			}

			// Check that the traffic assignment at the controller is as expected.
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.SortRepeatedFields(&nanny.HostTrafficAssignment{}, "allocs"),
			}
			if diff := cmp.Diff(c.expectedTrafficAssignment, gotTrafficAssignment, opts...); diff != "" {
				t.Fatalf("testCase(%s) wrong traffic assignment (-want, +got):\n%s", c.name, diff)
			}
		})
	}
}

func TestRunProfiling(t *testing.T) {
	prof := func(dur time.Duration, numSamples int64) *profile.Profile {
		cpuM := &profile.Mapping{
			ID:              1,
			Start:           0x10000,
			Limit:           0x40000,
			File:            "/bin/foo",
			HasFunctions:    true,
			HasFilenames:    true,
			HasLineNumbers:  true,
			HasInlineFrames: true,
		}
		cpuF := &profile.Function{
			ID:         1,
			Name:       "main",
			SystemName: "main",
			Filename:   "main.go",
		}
		cpuL := &profile.Location{
			ID:      1,
			Mapping: cpuM,
			Address: 0x1000,
			Line: []profile.Line{{
				Function: cpuF,
				Line:     1,
			}},
		}
		return &profile.Profile{
			TimeNanos:     10,
			PeriodType:    &profile.ValueType{Type: "cpu", Unit: "milliseconds"},
			Period:        1,
			DurationNanos: dur.Nanoseconds(),
			SampleType:    []*profile.ValueType{{Type: "cpu", Unit: "milliseconds"}},
			Sample: []*profile.Sample{{
				Location: []*profile.Location{cpuL},
				Value:    []int64{numSamples},
			}},
			Location: []*profile.Location{cpuL},
			Function: []*profile.Function{cpuF},
			Mapping:  []*profile.Mapping{cpuM},
		}
	}

	type testCase struct {
		name      string
		profiles  map[string]*profile.Profile // location -> profile
		expect    *profile.Profile
		expectErr string
	}
	for _, c := range []testCase{
		{
			// Test plan: Single distributor location. The generated profile
			// should be the same as the profile returned by the distributor.
			name: "one_location",
			profiles: map[string]*profile.Profile{
				"loc1": prof(time.Second, 100),
			},
			expect: prof(time.Second, 100),
		},
		{
			// Test plan: Two distributor locations, both returning a full
			// profile. The generated profile should be the combination of
			// the two profiles.
			name: "two_locations",
			profiles: map[string]*profile.Profile{
				"loc1": prof(time.Second, 100),
				"loc2": prof(time.Second, 1000),
			},
			expect: prof(2*time.Second, 1100),
		},
		{
			// Test plan: Two distributor locations and profile collection
			// fails at one of the locations. The returned profile should be the
			// profile of the other group, but with an error.
			name: "two_locations_one_error",
			profiles: map[string]*profile.Profile{
				"loc1": prof(time.Second, 100),
				"loc2": nil,
			},
			expect:    prof(time.Second, 100),
			expectErr: "cannot generate profile",
		},
		{
			// Test plan: Two distributor locations and profile collection
			// fails at both. The returned profile data should be empty, with no
			// errors.
			name: "two_locations_two_errors",
			profiles: map[string]*profile.Profile{
				"loc1": nil,
				"loc2": nil,
			},
			expect:    nil,
			expectErr: "cannot generate profile",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			const appName = "app"
			const versionId = "1"
			ctx := context.Background()
			// Start the controller.
			distributorConstructor := func(addr string) endpoints.Distributor {
				prof, ok := c.profiles[addr]
				if !ok {
					prof = nil
				}
				return &mockDistributorClient{prof: prof}
			}
			controller, err := startController(ctx, t, distributorConstructor)
			if err != nil {
				t.Fatalf("unable to start the controller: %v", err)
			}

			// Initialize controller state.
			state := &ControllerState{
				Applications: map[string]bool{appName: true},
				Distributors: map[string]*DistributorState{},
			}
			for loc := range c.profiles {
				state.Distributors[loc] = &DistributorState{
					Location: &RolloutRequest_Location{DistributorAddr: loc},
				}
			}
			if err := controller.saveState(ctx, state, nil); err != nil {
				t.Fatal(err)
			}

			// Profile the application version.
			v := &AppVersionState{
				Distributors: map[string]*AppVersionDistributorState{},
			}
			for loc := range c.profiles {
				v.Distributors[loc] = &AppVersionDistributorState{
					Status: AppVersionDistributorStatus_ROLLED_OUT,
				}
			}

			p, err := controller.runProfiling(ctx, v, &nanny.GetProfileRequest{
				AppName:   appName,
				VersionId: versionId,
			})
			// TODO(spetrovic): ProfileGroups needs to change to return a nil
			// error (instead of an error with empty string). In the meantime,
			// handle the case of an empty error string as no errors.
			if err != nil && err.Error() == "" {
				err = nil
			}
			if err != nil && c.expectErr == "" {
				t.Fatal(err)
			} else if err == nil && c.expectErr != "" {
				t.Fatal("expecting error, got no errors")
			} else if err != nil {
				if !strings.Contains(err.Error(), c.expectErr) {
					t.Errorf("expecting error string %q, got error: %v", c.expectErr, err)
				}
			}

			var got string
			if p != nil && len(p.Data) > 0 {
				parsed, err := profile.ParseData(p.Data)
				if err != nil {
					t.Fatal(err)
				}
				got = parsed.String()
			}
			var want string
			if c.expect != nil {
				want = c.expect.String()
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("profile diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestControllerDistributorInteraction(t *testing.T) {
	// Test plan: Execute a sequence of rollouts and kills over a chaotic
	// network. Despite the reordering and duplication, if we eventually
	// deliever every request to the distributor, the final controller state
	// will converge to what we expect.

	type rollout struct{ v version }
	type kill struct{ app string }
	for _, test := range []struct {
		name         string
		ops          []any // rollout or kill
		wantState    *ControllerState
		wantAppState *AppState
	}{
		{
			"LaunchThree",
			[]any{
				rollout{newVersion([]string{"loc1", "loc2"}, "app", toUUID(1), "0s")},
				rollout{newVersion([]string{"loc2", "loc3"}, "app", toUUID(2), "0s")},
				rollout{newVersion([]string{"loc1", "loc3"}, "app", toUUID(3), "0s")},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}, "loc2": {}, "loc3": {}},
			},
			&AppState{
				Versions: map[string]*AppVersionState{
					toUUID(1): {},
					toUUID(2): {},
					toUUID(3): {},
				},
			},
		},
		{
			"LaunchOneAndKill",
			[]any{
				rollout{newVersion([]string{"loc1"}, "app", toUUID(1), "0s")},
				kill{"app"},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}},
			},
			&AppState{},
		},
		{
			"LaunchThreeAndKill",
			[]any{
				rollout{newVersion([]string{"loc1", "loc2"}, "app", toUUID(1), "0s")},
				rollout{newVersion([]string{"loc2", "loc3"}, "app", toUUID(2), "0s")},
				rollout{newVersion([]string{"loc1", "loc3"}, "app", toUUID(3), "0s")},
				kill{"app"},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}, "loc2": {}, "loc3": {}},
			},
			&AppState{},
		},
		{
			"LaunchKillLaunch",
			[]any{
				rollout{newVersion([]string{"loc1", "loc2"}, "app", toUUID(1), "0s")},
				kill{"app"},
				rollout{newVersion([]string{"loc2", "loc3"}, "app", toUUID(2), "0s")},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}, "loc2": {}, "loc3": {}},
			},
			&AppState{
				Versions: map[string]*AppVersionState{
					toUUID(2): {},
				},
			},
		},
		{
			"LaunchKillLaunchKill",
			[]any{
				rollout{newVersion([]string{"loc1", "loc2"}, "app", toUUID(1), "0s")},
				kill{"app"},
				rollout{newVersion([]string{"loc2", "loc3"}, "app", toUUID(2), "0s")},
				rollout{newVersion([]string{"loc1", "loc3"}, "app", toUUID(3), "0s")},
				kill{"app"},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}, "loc2": {}, "loc3": {}},
			},
			&AppState{},
		},
		{
			"LaunchKillLaunchKillLaunch",
			[]any{
				rollout{newVersion([]string{"loc1", "loc2"}, "app", toUUID(1), "0s")},
				kill{"app"},
				rollout{newVersion([]string{"loc2", "loc3"}, "app", toUUID(2), "0s")},
				rollout{newVersion([]string{"loc1", "loc3"}, "app", toUUID(3), "0s")},
				kill{"app"},
				rollout{newVersion([]string{"loc1", "loc2", "loc3"}, "app", toUUID(4), "0s")},
				rollout{newVersion([]string{"loc1"}, "app", toUUID(5), "0s")},
			},
			&ControllerState{
				Applications: map[string]bool{"app": true},
				Distributors: map[string]*DistributorState{"loc1": {}, "loc2": {}, "loc3": {}},
			},
			&AppState{
				Versions: map[string]*AppVersionState{
					toUUID(4): {},
					toUUID(5): {},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Start the distributors.
			ctx := context.Background()
			distributors := map[string]*distributor.Distributor{}
			for _, loc := range []string{"loc1", "loc2", "loc3"} {
				getListeners := func(context.Context, *config.GKEConfig) ([]*nanny.Listener, error) { return nil, nil }
				distributor, err := startDistributor(ctx, t, loc, getListeners)
				if err != nil {
					t.Fatalf("unable to start a distributor in %q: %v", loc, err)
				}
				distributors[loc] = distributor
			}

			// Start the controller.
			network := &chaosNetwork{
				distributors: distributors,
				rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
			}
			controller, err := startController(ctx, t, network.client)
			if err != nil {
				t.Fatalf("unable to start the controller: %v", err)
			}

			// Execute the trace.
			for _, op := range test.ops {
				switch x := op.(type) {
				case rollout:
					if err := registerNewAppVersion(ctx, controller, x.v); err != nil {
						t.Fatalf("cannot register version %v: %v", x.v, err)
					}
				case kill:
					if err := controller.kill(ctx, &KillRequest{App: x.app}); err != nil {
						t.Fatalf("cannot kill app %q: %v", x.app, err)
					}
				default:
					t.Fatalf("unexpected op of type %T", op)
				}
			}
			network.deliverAll(ctx)

			// Allow the controller to successfully execute an annealing loop.
			controller.distributor = func(addr string) endpoints.Distributor {
				distributor, ok := distributors[addr]
				if !ok {
					t.Fatalf("distributor %q not found", addr)
				}
				return distributor
			}
			controller.manageState(ctx)

			// Log the execution.
			t.Log("Execution Trace:")
			for i, op := range network.log {
				t.Logf("  %2d: %v", i, op)
			}

			// Verify the controller state.
			state, _, err := controller.loadState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.IgnoreFields(&DistributorState{}, "location", "traffic_assignment"),
			}
			if diff := cmp.Diff(test.wantState, state, opts...); diff != "" {
				t.Fatalf("bad controller state (-want +got):\n%s", diff)
			}

			// Verify the app state.
			appState, _, err := controller.loadAppState(ctx, "app")
			if err != nil {
				t.Fatal(err)
			}
			opts = []cmp.Option{
				protocmp.Transform(),
				protocmp.IgnoreFields(&AppVersionState{},
					"config",
					"rollout_strategy",
					"distributors",
					"submission_id",
					"submission_time",
					"wave_idx",
					"time_wave_started",
					"time_wave_rolled_out"),
			}
			if diff := cmp.Diff(test.wantAppState, appState, opts...); diff != "" {
				t.Fatalf("bad app state (-want +got):\n%s", diff)
			}
		})
	}
}

func startController(ctx context.Context, t *testing.T, distributor func(addr string) endpoints.Distributor) (*controller, error) {
	return Start(ctx,
		http.NewServeMux(),
		store.NewFakeStore(),
		logging.NewTestSlogger(t, testing.Verbose()),
		5*time.Second, // actuationDelay
		distributor,
		0,   // fetchAssignmentsInterval
		0,   // applyAssignmentInterval
		0,   // manageStateInterval
		nil) // applyTraffic
}

func startDistributor(ctx context.Context, t *testing.T, loc string, getListeners func(context.Context, *config.GKEConfig) ([]*nanny.Listener, error)) (*distributor.Distributor, error) {
	return distributor.Start(ctx,
		http.NewServeMux(),
		store.NewFakeStore(),
		logging.NewTestSlogger(t, testing.Verbose()),
		&mockManagerClient{nil, nil, nil},
		loc,
		nil, // babysitterConstructor
		0,   // manageAppsInterval
		0,   // computeTrafficInterval
		0,   // applyTrafficInterval
		0,   // detectAppliedTrafficInterval
		nil, // applyTraffic
		getListeners,
		nil, // getMetricCounts
	)
}

// version represents an app version
type version struct {
	locations    []string                   // locations where this version is deployed
	appName      string                     // application name
	id           string                     // version name
	rollout      string                     // rollout duration hint
	listeners    map[string]*nanny.Listener // hostname -> listener
	listenerOpts map[string]*config.GKEConfig_ListenerOptions
}

// newVersion creates a new app version.
func newVersion(locations []string, appName, id string, rollout string, listener ...string) version {
	v := version{
		locations:    locations,
		appName:      appName,
		id:           id,
		rollout:      rollout,
		listeners:    map[string]*nanny.Listener{},
		listenerOpts: map[string]*config.GKEConfig_ListenerOptions{},
	}
	for _, l := range listener {
		if strings.Contains(l, ".") { // public listener
			name := strings.Split(l, ".")[0]
			v.listeners[l] = &nanny.Listener{Name: name}
			v.listenerOpts[name] = &config.GKEConfig_ListenerOptions{
				PublicHostname: l,
			}
		}
	}
	return v
}

func registerNewAppVersion(ctx context.Context, controller *controller, v version) error {
	rollout, err := time.ParseDuration(v.rollout)
	if err != nil {
		return err
	}
	req := &RolloutRequest{
		Config: &config.GKEConfig{
			Deployment: &protos.Deployment{
				App: &protos.AppConfig{
					Name:         v.appName,
					RolloutNanos: int64(rollout),
				},
				Id: v.id,
			},
			Listeners: v.listenerOpts,
		},
	}
	for _, location := range v.locations {
		req.Locations = append(req.Locations, &RolloutRequest_Location{
			Name:            location,
			DistributorAddr: location,
		})
	}
	return controller.rollout(ctx, req)
}

func TestNextSubmissionID(t *testing.T) {
	// Test plan: Launch a number of goroutines that race on calls to
	// nextSubmissionID. Despite the race, nextSubmissionID should always
	// return a unique and monotonically increasing id.
	const numIds = 1000
	const numThreads = 10

	// Launch the goroutines, which write ids to idChan.
	ctx := context.Background()
	s := store.NewFakeStore()
	idChan := make(chan int, numIds)
	for i := 0; i < numThreads; i++ {
		go func() {
			prevId := math.MinInt
			for j := 0; j < numIds/numThreads; j++ {
				id, err := nextSubmissionID(ctx, s)
				if err != nil {
					panic(err)
				}
				if id <= prevId {
					panic(fmt.Sprintf("non-monotonic id: %d -> %d", prevId, id))
				}
				idChan <- id
				prevId = id
			}
		}()
	}

	// Verify that every id is unique.
	seen := map[int]bool{}
	for i := 0; i < numIds; i++ {
		id := <-idChan
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
}

// mockManagerClient is a mock manager.Client that returns the provided errors.
type mockManagerClient struct {
	deploy error // returned by Deploy
	stop   error // returned by Stop
	delete error // returned by Delete
}

var _ endpoints.Manager = &mockManagerClient{}

// Deploy implements the endpoints.Manager interface.
func (m *mockManagerClient) Deploy(context.Context, *nanny.ApplicationDeploymentRequest) error {
	return m.deploy
}

// Stop implements the endpoints.Manager interface.
func (m *mockManagerClient) Stop(context.Context, *nanny.ApplicationStopRequest) error {
	return m.stop
}

// Delete implements the endpoints.Manager interface.
func (m *mockManagerClient) Delete(context.Context, *nanny.ApplicationDeleteRequest) error {
	return m.delete
}

// GetReplicaSetState implements the endpoints.Manager interface.
func (m *mockManagerClient) GetReplicaSetState(context.Context, *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error) {
	return nil, fmt.Errorf("unimplemented")
}

// ActivateComponent implements the endpoints.Manager interface.
func (m *mockManagerClient) ActivateComponent(context.Context, *nanny.ActivateComponentRequest) error {
	panic("implement me")
}

// RegisterReplica implements the endpoints.Manager interface.
func (m *mockManagerClient) RegisterReplica(context.Context, *nanny.RegisterReplicaRequest) error {
	panic("implement me")
}

// ReportLoad implements the endpoints.Manager interface.
func (m *mockManagerClient) ReportLoad(context.Context, *nanny.LoadReport) error {
	panic("implement me")
}

func (m *mockManagerClient) GetListenerAddress(ctx context.Context, req *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	panic("implement me")
}

// ExportListener implements the endpoints.Manager interface.
func (m *mockManagerClient) ExportListener(context.Context, *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	panic("implement me")
}

// GetRoutingInfo implements the endpoints.Manager interface.
func (m *mockManagerClient) GetRoutingInfo(context.Context, *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	//TODO implement me
	panic("implement me")
}

// GetComponentsToStart implements the endpoints.Manager interface.
func (m *mockManagerClient) GetComponentsToStart(context.Context, *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	panic("implement me")
}

// mockDistributorClient is a mock distributor.Client that returns pre-created
// values for its various methods.
type mockDistributorClient struct {
	publicTraffic  *nanny.TrafficAssignment
	privateTraffic *nanny.TrafficAssignment
	prof           *profile.Profile
}

var _ endpoints.Distributor = &mockDistributorClient{}

// Distribute implements the endpoints.Distributor interface.
func (d *mockDistributorClient) Distribute(context.Context, *nanny.ApplicationDistributionRequest) error {
	return nil
}

// Cleanup implements the endpoints.Distributor interface.
func (d *mockDistributorClient) Cleanup(context.Context, *nanny.ApplicationCleanupRequest) error {
	return fmt.Errorf("unimplemented")
}

// GetApplicationState implements the endpoints.Distributor interface.
func (d *mockDistributorClient) GetApplicationState(context.Context, *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error) {
	return nil, fmt.Errorf("unimplemented")
}

// GetPublicTrafficAssignment implements the endpoints.Distributor interface.
func (d *mockDistributorClient) GetPublicTrafficAssignment(context.Context) (*nanny.TrafficAssignment, error) {
	return d.publicTraffic, nil
}
func (d *mockDistributorClient) GetPrivateTrafficAssignment(context.Context) (*nanny.TrafficAssignment, error) {
	return d.privateTraffic, nil
}

// RunProfiling implements the endpoints.Distributor interface.
func (d *mockDistributorClient) RunProfiling(context.Context, *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	if d.prof == nil {
		return nil, fmt.Errorf("cannot generate profile")
	}
	var buf bytes.Buffer
	if err := d.prof.Write(&buf); err != nil {
		return nil, err
	}
	return &protos.GetProfileReply{Data: buf.Bytes()}, nil
}

// chaosNetwork is a chaotic fake network that randomly duplicates, delays, and
// reorders requests sent between the controller and the distributor.
type chaosNetwork struct {
	distributors map[string]*distributor.Distributor // distributors, by location
	rand         *rand.Rand                          // source of randomness
	buf          []op                                // buffer of pending operations
	log          []op                                // log of executed operations
}

// op represents a Distributor operation. Every Distributor method (e.g.,
// Distribute, Cleanup) has a corresponding op. These ops are duplicated and
// reordered by the chaosDistributorClient.
type op struct {
	kind string        // distribute, cleanup, getApplicationState, or getPublicTrafficAssignment
	addr string        // the distributor's address
	req  proto.Message // the request, or nil if there is no request
}

// chaosDistributorClient is a distributor.Client returned by a chaosNetwork.
type chaosDistributorClient struct {
	parent      *chaosNetwork
	addr        string
	distributor *distributor.Distributor
}

// buffer buffers the provided operation for execution, replicating it a random
// number of times greater than or equal to the provided min replication.
func (c *chaosNetwork) buffer(op op, min int) {
	// Use an exponential distribution so that the number of copies is likely
	// not too big, but still can be big on occasion.
	for i := 0; i < min+int(c.rand.ExpFloat64()*2); i++ {
		c.buf = append(c.buf, op)
	}
}

// execute executes the provided operation against the distributors.
func (c *chaosNetwork) execute(ctx context.Context, op op) {
	distributor, ok := c.distributors[op.addr]
	if !ok {
		return
	}

	switch op.kind {
	case "distribute":
		distributor.Distribute(ctx, op.req.(*nanny.ApplicationDistributionRequest))
	case "cleanup":
		distributor.Cleanup(ctx, op.req.(*nanny.ApplicationCleanupRequest))
	case "getApplicationState":
		distributor.GetApplicationState(ctx, op.req.(*nanny.ApplicationStateAtDistributorRequest))
	case "getPublicTrafficAssignment":
		distributor.GetPublicTrafficAssignment(ctx)
	default:
		panic(fmt.Sprintf("unexpected kind %q\n", op.kind))
	}
	c.log = append(c.log, op)
}

// deliverN unbuffers and executes n randomly chosen buffered operations.
func (c *chaosNetwork) deliverN(ctx context.Context, n int) {
	if n > len(c.buf) {
		n = len(c.buf)
	}
	c.rand.Shuffle(len(c.buf), func(i, j int) {
		c.buf[i], c.buf[j] = c.buf[j], c.buf[i]
	})
	for i := 0; i < n; i++ {
		c.execute(ctx, c.buf[i])
	}
	c.buf = c.buf[n:]
}

// deliverSome unbuffers and executes a random number of buffered operations.
func (c *chaosNetwork) deliverSome(ctx context.Context) {
	n := len(c.buf)
	if n == 0 {
		return
	}
	c.deliverN(ctx, c.rand.Intn(n))
}

// deliverAll unbuffers and executes all buffered operations.
func (c *chaosNetwork) deliverAll(ctx context.Context) {
	c.deliverN(ctx, len(c.buf))
}

// step perform a single step of chaos.
func (c *chaosNetwork) step(ctx context.Context, op op, f func() error) error {
	// Flip a coin to decide if we actually execute this request.
	err := fmt.Errorf("network error")
	if c.rand.Float32() <= 0.5 {
		// We are executing the request. Buffer zero or more copies of the
		// request.
		err = f()
		c.buffer(op, 0)
		c.log = append(c.log, op)
	} else {
		// We aren't executing the request. Buffer one or more copies of the
		// request. We don't want to drop the request completely.
		c.buffer(op, 1)
	}

	// Execute a random number of requests.
	c.deliverSome(ctx)
	return err
}

func (c *chaosNetwork) client(addr string) endpoints.Distributor {
	distributor, ok := c.distributors[addr]
	if !ok {
		panic(fmt.Sprintf("distributor %q not found", addr))
	}
	return &chaosDistributorClient{c, addr, distributor}
}

// Distribute implements the distributor.Client interface.
func (c *chaosDistributorClient) Distribute(ctx context.Context, req *nanny.ApplicationDistributionRequest) error {
	op := op{"distribute", c.addr, req}
	return c.parent.step(ctx, op, func() error {
		return c.distributor.Distribute(ctx, req)
	})
}

// Cleanup implements the distributor.Client interface.
func (c *chaosDistributorClient) Cleanup(ctx context.Context, req *nanny.ApplicationCleanupRequest) error {
	op := op{"cleanup", c.addr, req}
	return c.parent.step(ctx, op, func() error {
		return c.distributor.Cleanup(ctx, req)
	})
}

// GetApplicationState implements the distributor.Client interface.
func (c *chaosDistributorClient) GetApplicationState(ctx context.Context, req *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error) {
	op := op{"getApplicationState", c.addr, req}
	var reply *nanny.ApplicationStateAtDistributor
	err := c.parent.step(ctx, op, func() error {
		var err error
		reply, err = c.distributor.GetApplicationState(ctx, req)
		return err
	})
	return reply, err
}

// GetPublicTrafficAssignment implements the distributor.Client interface.
func (c *chaosDistributorClient) GetPublicTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	op := op{"getPublicTrafficAssignment", c.addr, nil}
	var reply *nanny.TrafficAssignment
	err := c.parent.step(ctx, op, func() error {
		var err error
		reply, err = c.distributor.GetPublicTrafficAssignment(ctx)
		return err
	})
	return reply, err
}

// GetPrivateTrafficAssignment implements the distributor.Client interface.
func (c *chaosDistributorClient) GetPrivateTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	panic("chaosDistributorClient.GetPrivateTrafficAssignment unimplemented")
}

// RunProfiling implements the distributor.Client interface.
func (c *chaosDistributorClient) RunProfiling(ctx context.Context, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (op op) String() string {
	switch op.kind {
	case "distribute":
		req := op.req.(*nanny.ApplicationDistributionRequest)
		versions := make([]string, len(req.Requests))
		for i, version := range req.Requests {
			versions[i] = version.Config.Deployment.Id
		}
		return fmt.Sprintf("Distribute(loc=%q, app=%q, versions=%v)", op.addr, req.AppName, versions)

	case "cleanup":
		req := op.req.(*nanny.ApplicationCleanupRequest)
		return fmt.Sprintf("Cleanup(loc=%q, app=%q, versions=%v)", op.addr, req.AppName, req.Versions)

	case "getApplicationState":
		req := op.req.(*nanny.ApplicationStateAtDistributorRequest)
		return fmt.Sprintf("GetApplicationState(loc=%q, app=%q)", op.addr, req.AppName)

	case "getPublicTrafficAssignment":
		return fmt.Sprintf("GetPublicTrafficAssignment(loc=%q)", op.addr)

	default:
		panic(fmt.Sprintf("unexpected kind %q\n", op.kind))
	}
}
