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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/pprof/profile"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
)

const testRegion = "us-west1"

// toUUID returns a valid version UUID string for a given digit.
func toUUID(d int) string {
	return strings.ReplaceAll(uuid.Nil.String(), "0", fmt.Sprintf("%d", d))
}

// TODO(mwhittaker): Add tests with injected store and manager failures and
// multiple distributors running at once.

func TestDistributeAndCleanup(t *testing.T) {
	// Test Plan: Start a distributor and repeatedly deploy and clean up
	// application versions. Check that the state in the store matches what we
	// expect. For some versions, we inject a faulty manager that refuses to
	// start or stop the versions. The requests should still succeed.
	//
	// Additionally, check the traffic assignment correctly assigns traffic to
	// started apps, but does not route traffic to apps that are starting,
	// deleting, or deleted.
	//
	// TODO(mwhittaker): Dependency inject times and test the deletion path.

	type deploy struct {
		v     version // version to deploy
		start bool    // successfully start the version?
	}

	type cleanup struct {
		app, version string // version to clean up
		stop         bool   // successfully stop the version?
	}

	// traffic returns an assignment that routes to the provided versions.
	traffic := func(versions ...version) *nanny.TrafficAssignment {
		assignment := &nanny.TrafficAssignment{
			HostAssignment: map[string]*nanny.HostTrafficAssignment{},
		}
		for _, v := range versions {
			host := "pub.host.com"
			assignment.HostAssignment[host] = &nanny.HostTrafficAssignment{
				Allocs: []*nanny.TrafficAllocation{
					{
						Location:        testRegion,
						AppName:         v.appName,
						VersionId:       v.id,
						TrafficFraction: 1.0,
						Listener:        &nanny.Listener{Name: "pub"},
					},
				},
			}
		}
		return assignment
	}

	// Start the distributor.
	ctx := context.Background()
	store := store.NewFakeStore()
	manager := &mockManagerClient{nil, nil, nil, nil}
	listeners := []*nanny.Listener{{Name: "pub"}}
	distributor, err := Start(ctx,
		http.NewServeMux(),
		store,
		logging.NewTestSlogger(t, testing.Verbose()),
		manager,
		testRegion,
		nil, // babysitterConstructor
		0,   // manageAppsInterval
		0,   // computeTrafficInterval
		0,   // applyTrafficInterval
		0,   // detectAppliedTrafficInterval
		nil, // applyTraffic
		func(_ context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
			return listeners, nil
		},
		nil, // getMetricCounts
	)
	if err != nil {
		t.Fatal(err)
	}

	a1 := newVersion("a", toUUID(1), 0, "pub.host.com")
	a2 := newVersion("a", toUUID(2), 1, "pub.host.com")
	b3 := newVersion("b", toUUID(3), 2, "pub.host.com")
	a4 := newVersion("a", toUUID(4), 3, "pub.host.com")
	b5 := newVersion("b", toUUID(5), 4, "pub.host.com")
	c6 := newVersion("c", toUUID(6), 5, "pub.host.com")

	for _, test := range []struct {
		op    any                  // operation (deploy or cleanup)
		state *DistributorState    // expected distributor state
		apps  map[string]*AppState // expected app states
	}{
		// Deployments that successfully start.
		{
			deploy{a1, true},
			&DistributorState{
				Applications:            []string{"a"},
				PublicTrafficAssignment: traffic(a1),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
					},
				},
			},
		},
		{
			deploy{a2, true},
			&DistributorState{
				Applications:            []string{"a"},
				PublicTrafficAssignment: traffic(a1, a2),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
						{Order: 1, Status: AppVersionState_STARTED},
					},
				},
			},
		},
		{
			deploy{b3, true},
			&DistributorState{
				Applications:            []string{"a", "b"},
				PublicTrafficAssignment: traffic(a1, a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
						{Order: 1, Status: AppVersionState_STARTED},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
					},
				},
			},
		},

		// Deployments that unsuccessfully start.
		{
			deploy{a4, false},
			&DistributorState{
				Applications:            []string{"a", "b"},
				PublicTrafficAssignment: traffic(a1, a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
						{Order: 1, Status: AppVersionState_STARTED},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
					},
				},
			},
		},
		{
			deploy{b5, false},
			&DistributorState{
				Applications:            []string{"a", "b"},
				PublicTrafficAssignment: traffic(a1, a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
						{Order: 1, Status: AppVersionState_STARTED},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
			},
		},
		{
			deploy{c6, false},
			&DistributorState{
				Applications:            []string{"a", "b", "c"},
				PublicTrafficAssignment: traffic(a1, a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STARTED},
						{Order: 1, Status: AppVersionState_STARTED},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
				"c": {
					Versions: []*AppVersionState{
						{Order: 5, Status: AppVersionState_STARTING},
					},
				},
			},
		},

		// Cleanups that successfully stop.
		{
			cleanup{"a", toUUID(1), true},
			&DistributorState{
				Applications:            []string{"a", "b", "c"},
				PublicTrafficAssignment: traffic(a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STOPPED},
						{Order: 1, Status: AppVersionState_STARTED},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
				"c": {
					Versions: []*AppVersionState{
						{Order: 5, Status: AppVersionState_STARTING},
					},
				},
			},
		},
		{
			cleanup{"c", toUUID(6), true},
			&DistributorState{
				Applications:            []string{"a", "b", "c"},
				PublicTrafficAssignment: traffic(a2, b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STOPPED},
						{Order: 1, Status: AppVersionState_STARTED},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
				"c": {
					Versions: []*AppVersionState{
						{Order: 5, Status: AppVersionState_STOPPED},
					},
				},
			},
		},

		// Cleanups that unsuccessfully stop.
		{
			cleanup{"a", toUUID(2), false},
			&DistributorState{
				Applications:            []string{"a", "b", "c"},
				PublicTrafficAssignment: traffic(b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STOPPED},
						{Order: 1, Status: AppVersionState_STOPPING},
						{Order: 3, Status: AppVersionState_STARTING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
				"c": {
					Versions: []*AppVersionState{
						{Order: 5, Status: AppVersionState_STOPPED},
					},
				},
			},
		},
		{
			cleanup{"a", toUUID(4), false},
			&DistributorState{
				Applications:            []string{"a", "b", "c"},
				PublicTrafficAssignment: traffic(b3),
			},
			map[string]*AppState{
				"a": {
					Versions: []*AppVersionState{
						{Order: 0, Status: AppVersionState_STOPPED},
						{Order: 1, Status: AppVersionState_STOPPING},
						{Order: 3, Status: AppVersionState_STOPPING},
					},
				},
				"b": {
					Versions: []*AppVersionState{
						{Order: 2, Status: AppVersionState_STARTED},
						{Order: 4, Status: AppVersionState_STARTING},
					},
				},
				"c": {
					Versions: []*AppVersionState{
						{Order: 5, Status: AppVersionState_STOPPED},
					},
				},
			},
		},
	} {
		switch x := test.op.(type) {
		case deploy:
			// Possibly inject starting failures.
			if x.start {
				manager.deploy = nil
			} else {
				manager.deploy = fmt.Errorf("refusing to start")
			}

			// Register the version.
			if err := registerNewAppVersion(distributor, x.v); err != nil {
				t.Fatalf("registerNewAppVersion(%s): %v", x.v.id, err)
			}

		case cleanup:
			// Possibly inject stopping failures.
			if x.stop {
				manager.stop = nil
			} else {
				manager.stop = fmt.Errorf("refusing to stop")
			}

			// Clean up the version.
			req := &nanny.ApplicationCleanupRequest{
				AppName:  x.app,
				Versions: []string{x.version},
			}
			if err := distributor.Cleanup(ctx, req); err != nil {
				t.Fatalf("cleanup(%s, %s): %v", x.app, x.version, err)
			}

		default:
			t.Fatalf("unexpected operation of type %T", test.op)
		}

		// Check the distributor's state.
		state, _, err := distributor.loadState(ctx)
		if err != nil {
			t.Fatalf("loadState: %v", err)
		}
		opts := []cmp.Option{
			protocmp.Transform(),
			protocmp.IgnoreFields(&DistributorState{}, "private_traffic_assignment"),
			protocmp.IgnoreFields(&AppVersionState{}, "config", "schedule", "stopped_time", "listeners"),
		}
		if diff := cmp.Diff(test.state, state, opts...); diff != "" {
			t.Fatalf("bad state after op %T%v(-want +got):\n%s", test.op, test.op, diff)
		}

		// Check the app's state.
		for _, app := range state.Applications {
			appState, _, err := distributor.loadAppState(ctx, app)
			if err != nil {
				t.Fatalf("loadAppState(%q): %v", app, err)
			}
			if diff := cmp.Diff(test.apps[app], appState, opts...); diff != "" {
				t.Fatalf("bad %q state (-want +got):\n%s", app, diff)
			}
		}
	}
}

func TestCleanupMissing(t *testing.T) {
	// Test plan: Delete an application version that the distributor doesn't
	// know about. This request should succeed, and the distributor should
	// record that the version should be deleted. Then, try and start the
	// application version. This should succeed but not actually start
	// anything.

	// Start the distributor.
	ctx := context.Background()
	store := store.NewFakeStore()
	distributor, err := Start(ctx,
		http.NewServeMux(),
		store,
		logging.NewTestSlogger(t, testing.Verbose()),
		&mockManagerClient{fmt.Errorf("unimplemented"), nil, nil, nil},
		testRegion,
		nil, // babysitterConstructor
		0,   // manageAppsInterval
		0,   // computeTrafficInterval
		0,   // applyTrafficInterval
		0,   // detectAppliedTrafficInterval
		nil, // applyTraffic
		func(_ context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
			return []*nanny.Listener{}, nil
		},
		nil, // getMetricCounts
	)
	if err != nil {
		t.Fatal(err)
	}

	// Clean up an app version for an app that doesn't exist.
	v1 := newVersion("a", toUUID(1), 1)
	if err := distributor.Cleanup(ctx, &nanny.ApplicationCleanupRequest{
		AppName:  v1.appName,
		Versions: []string{v1.id},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify the resulting state.
	wantState := &DistributorState{Applications: []string{v1.appName}}
	wantAppState := &AppState{DeletedVersions: []string{v1.id}}
	state, _, err := distributor.loadState(ctx)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&DistributorState{}, "public_traffic_assignment"),
		protocmp.IgnoreFields(&DistributorState{}, "private_traffic_assignment"),
		protocmp.IgnoreFields(&AppVersionState{}, "config", "schedule"),
	}
	if diff := cmp.Diff(wantState, state, opts...); diff != "" {
		t.Fatalf("bad state (-want +got):\n%s", diff)
	}
	appState, _, err := distributor.loadAppState(ctx, "a")
	if err != nil {
		t.Fatalf("loadAppState(%q): %v", "a", err)
	}
	if diff := cmp.Diff(wantAppState, appState, opts...); diff != "" {
		t.Fatalf("bad %q state (-want +got):\n%s", "a", diff)
	}

	// Try to deploy the deleted app. Deploy shouldn't be called.
	if err := registerNewAppVersion(distributor, v1); err != nil {
		t.Fatal(err)
	}
}

func TestAnneal(t *testing.T) {
	// Test Plan: Deploy and clean up various application versions. Then run an
	// annealing loop and check that the loop converges the actual state to the
	// desired state. For example, starting apps should be started, and
	// deleting apps should be deleted.

	// Start the distributor.
	ctx := context.Background()
	store := store.NewFakeStore()
	manager := &mockManagerClient{nil, nil, nil, nil}
	distributor, err := Start(ctx,
		http.NewServeMux(),
		store,
		logging.NewTestSlogger(t, testing.Verbose()),
		manager,
		testRegion,
		nil, // babysitterConstructor
		0,   // manageAppsInterval
		0,   // computeTrafficInterval
		0,   // applyTrafficInterval
		0,   // detectAppliedTrafficInterval
		nil, // applyTraffic
		func(_ context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
			return []*nanny.Listener{}, nil
		},
		nil, // getMetricCounts
	)
	if err != nil {
		t.Fatal(err)
	}

	// Register and clean up various app versions.
	v1 := newVersion("a", toUUID(1), 1) // STARTED
	v2 := newVersion("a", toUUID(2), 2) // STARTING
	v3 := newVersion("a", toUUID(3), 3) // STOPPING
	v4 := newVersion("b", toUUID(4), 4) // STOPPED
	if err := registerNewAppVersion(distributor, v1); err != nil {
		t.Fatal(err)
	}
	if err := registerNewAppVersion(distributor, v3); err != nil {
		t.Fatal(err)
	}
	if err := registerNewAppVersion(distributor, v4); err != nil {
		t.Fatal(err)
	}
	if err := distributor.Cleanup(ctx, &nanny.ApplicationCleanupRequest{
		AppName:  "b",
		Versions: []string{toUUID(4)},
	}); err != nil {
		t.Fatal(err)
	}
	manager.deploy = fmt.Errorf("refusing to deploy")
	manager.delete = fmt.Errorf("refusing to delete")
	if err := registerNewAppVersion(distributor, v2); err != nil {
		t.Fatal(err)
	}
	if err := distributor.Cleanup(ctx, &nanny.ApplicationCleanupRequest{
		AppName:  "a",
		Versions: []string{toUUID(3)},
	}); err != nil {
		t.Fatal(err)
	}
	manager.deploy = nil
	manager.delete = nil

	// Run an annealing loop.
	if err := distributor.ManageAppStates(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := distributor.ComputeTrafficAssignments(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Verify the resulting state.
	wantState := &DistributorState{
		Applications: []string{"a", "b"},
	}
	wantApps := map[string]*AppState{
		"a": {
			Versions: []*AppVersionState{
				{Order: 1, Status: AppVersionState_STARTED},
				{Order: 2, Status: AppVersionState_STARTED},
				{Order: 3, Status: AppVersionState_STOPPED},
			},
		},
		"b": {
			Versions: []*AppVersionState{
				{Order: 4, Status: AppVersionState_STOPPED},
			},
		},
	}

	state, _, err := distributor.loadState(ctx)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&DistributorState{}, "public_traffic_assignment"),
		protocmp.IgnoreFields(&DistributorState{}, "private_traffic_assignment"),
		protocmp.IgnoreFields(&AppVersionState{}, "config", "schedule", "stopped_time", "replica_sets"),
	}
	if diff := cmp.Diff(wantState, state, opts...); diff != "" {
		t.Fatalf("bad state (-want +got):\n%s", diff)
	}

	for _, app := range state.Applications {
		appState, _, err := distributor.loadAppState(ctx, app)
		if err != nil {
			t.Fatalf("loadAppState(%q): %v", app, err)
		}
		if diff := cmp.Diff(wantApps[app], appState, opts...); diff != "" {
			t.Fatalf("bad %q state (-want +got):\n%s", app, diff)
		}
	}
}

func TestGetDistributorState(t *testing.T) {
	replicaSets := replicaSetStates{
		"rs1": {
			replicas:   []string{"replica1:healthy"},
			components: []string{"component1", "component2"},
			listeners:  []string{"l1"},
		},
		"rs2": {
			replicas:   []string{"replica1:healthy"},
			components: []string{"component3"},
			listeners:  []string{"l2", "l3"},
		},
	}
	type testCase struct {
		name                             string
		versions                         []version
		expectedPublicTrafficAssignment  *nanny.TrafficAssignment
		expectedPrivateTrafficAssignment *nanny.TrafficAssignment
		expectedState                    map[string]*nanny.ApplicationStateAtDistributor
	}
	for _, c := range []testCase{
		{
			// Test plan:
			// One app with two versions that are exporting partially
			// overlapping listeners, both public and private, and are rolled
			// out entirely at the same time.
			//
			// Expected behavior:
			// 1) The non-overlapping listener should receive 100% traffic share.
			// 2) The overlapping listener should receive 100% traffic share
			//    only in the newest version.
			// 3) The distributor state contains information for two application versions.
			name: "one_app_two_versions_partially_overlapping_listeners",
			versions: []version{
				newVersion("app", toUUID(1), 1, "l1.host.com" /*public*/, "l2" /*private*/),
				newVersion("app", toUUID(2), 2, "l1.host.com" /*public*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l2.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l2"},
							},
						},
					},
				},
			},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app with four versions that are exporting the same public
			// listener, and are rolled out as follows: v2 then v3 then v4 then
			// v1.
			//
			// Expected behavior:
			// 1) The listener should receive 100% traffic share in the newest version v1.
			// 2) The distributor state contains information for all the four application versions.
			name: "one_app_four_versions_same_listener_random_order",
			versions: []version{
				newVersion("app", toUUID(1), 4, "l1.host.com" /*public*/),
				newVersion("app", toUUID(2), 1, "l1.host.com" /*public*/),
				newVersion("app", toUUID(3), 2, "l1.host.com" /*public*/),
				newVersion("app", toUUID(4), 3, "l1.host.com" /*public*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
						{
							VersionId:                  toUUID(3),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
						{
							VersionId:                  toUUID(4),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// One app with two versions that are exporting the same listener,
			// but the listener is declared public in one version and private
			// in the other. The two versions are rolled out at the same time.
			//
			// Expected behavior:
			// 1) Each app listener should get the entire traffic, because the
			//    traffic is public in one version and private in another.
			// 2) The distributor state contains information for two application
			//    versions.
			name: "one_app_two_versions_same_listener_public_and_private",
			versions: []version{
				newVersion("app1", toUUID(1), 1, "l1.host.com" /*public*/),
				newVersion("app1", toUUID(2), 2, "l1" /*private*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app1": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps (with the same domain name) with one version each that
			// is rolled out entirely at once. The apps versions don't share any
			// listeners.
			//
			// Expected behavior:
			// 1) All the traffic should go to each listener.
			// 2) The distributor state contains information for two applications.
			name: "two_apps_one_version_different_listeners",
			versions: []version{
				newVersion("app1", toUUID(1), 1, "l1.host.com" /*public*/),
				newVersion("app2", toUUID(2), 2, "l2.host.com" /*public*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l2.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l2"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app1": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
				"app2": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps and one version each are rolled out entirely at once.
			// The apps versions share one listener l1.
			//
			// Expected behavior:
			// 1) All the traffic for non-overlapping listeners should go
			//    entirely to those listeners (l2 and l3).
			// 2) The overlapping listener should receive 100% traffic share
			//    only in the newest version.
			// 3) The distributor state contains information for two applications.
			name: "two_apps_one_version_partially_overlapping_listeners",
			versions: []version{
				newVersion("app1", toUUID(1), 1, "l1" /*private*/, "l3" /*private*/),
				newVersion("app2", toUUID(2), 2, "l1" /*private*/, "l2" /*private*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l2.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l2"},
							},
						},
					},
					"l3.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l3"},
							},
						},
					},
				},
			},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app1": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
				"app2": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps and one version each are rolled out entirely at once.
			// The apps versions share one listener l1 exported under different
			// hostnames.
			//
			// Expected behavior:
			// 1) Each app listener should get the entire traffic, because they
			//    don't share the same hostname.
			// 2) The distributor state contains information for two applications.
			name: "two_apps_one_version_same_listener_names_different_hostnames",
			versions: []version{
				newVersion("app1", toUUID(1), 1, "l1.host1.com" /*public*/),
				newVersion("app2", toUUID(2), 2, "l1.host2.com" /*public*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host1.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
					"l1.host2.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app1": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
				"app2": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
		{
			// Test plan:
			// Two apps and one version each are rolled out entirely at once.
			// The apps versions share one listener that is public in one
			// version and private in the other.
			//
			// Expected behavior:
			// 1) Each app listener should get the entire traffic, because the
			//    traffic is public in one case and private in another.
			// 2) The distributor state contains information for two applications.
			name: "two_apps_one_version_same_listener_public_and_private",
			versions: []version{
				newVersion("app1", toUUID(1), 1, "l1" /*private*/),
				newVersion("app2", toUUID(2), 2, "l1.host.com" /*public*/),
			},
			expectedPublicTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.host.com": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app2",
								VersionId:       toUUID(2),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedPrivateTrafficAssignment: &nanny.TrafficAssignment{
				HostAssignment: map[string]*nanny.HostTrafficAssignment{
					"l1.us-west1.serviceweaver.internal": {
						Allocs: []*nanny.TrafficAllocation{
							{
								Location:        testRegion,
								AppName:         "app1",
								VersionId:       toUUID(1),
								TrafficFraction: 1,
								Listener:        &nanny.Listener{Name: "l1"},
							},
						},
					},
				},
			},
			expectedState: map[string]*nanny.ApplicationStateAtDistributor{
				"app1": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(1),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
				"app2": {
					VersionState: []*nanny.VersionStateAtDistributor{
						{
							VersionId:                  toUUID(2),
							LastTrafficFractionApplied: 1.0,
							RolloutCompleted:           true,
							IsDeployed:                 true,
							ReplicaSets:                replicaSets.toProto(),
						},
					},
				},
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Test plan: Create a distributor. Create a client that calls the
			// distributor's methods. Verify that the results are as expected.
			versions := map[string]version{} // versionId -> version
			for _, v := range c.versions {
				versions[v.id] = v
			}

			ctx := context.Background()
			mux := http.NewServeMux()

			// Configure the distributor, s.t. the applications check every 10
			// milliseconds whether the traffic-forwarding specification should
			// be updated.
			distributor, err := Start(ctx,
				mux,
				store.NewFakeStore(),
				logging.NewTestSlogger(t, testing.Verbose()),
				&mockManagerClient{nil, nil, nil, replicaSets},
				testRegion,
				nil, // babysitterConstructor
				0,   // manageAppsInterval
				0,   // computeTrafficInterval
				0,   // applyTrafficInterval
				0,   // detectAppliedTrafficInterval
				nil, // applyTraffic
				func(_ context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
					ver, ok := versions[cfg.Deployment.Id]
					if !ok {
						return nil, fmt.Errorf("unknown version id: %v", cfg.Deployment.Id)
					}
					return ver.listeners, nil
				},
				nil, // getMetricCounts
			)
			if err != nil {
				t.Fatal("error starting the distributor service", err)
			}
			for _, v := range c.versions {
				if err := registerNewAppVersion(distributor, v); err != nil {
					t.Fatalf("unable to register app version %v due to %v", v.id, err)
				}
			}

			// Run an annealing loop.
			if err := distributor.ManageAppStates(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := distributor.ComputeTrafficAssignments(ctx, time.Now()); err != nil {
				t.Fatal(err)
			}

			// Verify that the traffic assignment is as expected.
			gotPublicTrafficAssignment, err := distributor.getTrafficAssignment(ctx, true /*public*/)
			if err != nil {
				t.Fatal(err)
			}
			gotPrivateTrafficAssignment, err := distributor.getTrafficAssignment(ctx, false /*public*/)
			if err != nil {
				t.Fatal(err)
			}
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.SortRepeatedFields(&nanny.HostTrafficAssignment{}, "allocs"),
			}
			if diff := cmp.Diff(c.expectedPublicTrafficAssignment, gotPublicTrafficAssignment, opts...); diff != "" {
				t.Fatalf("bad public traffic assignment: (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(c.expectedPrivateTrafficAssignment, gotPrivateTrafficAssignment, opts...); diff != "" {
				t.Fatalf("bad private traffic assignment: (-want +got):\n%s", diff)
			}

			// Get the latest applications state at the distributor.
			apps := map[string]bool{}
			for _, v := range c.versions {
				if _, found := apps[v.appName]; !found {
					apps[v.appName] = true
				}
			}
			gotAppsState := map[string]*nanny.ApplicationStateAtDistributor{}
			for a := range apps {
				reply, err := distributor.GetApplicationState(ctx, &nanny.ApplicationStateAtDistributorRequest{AppName: a})
				if err != nil {
					t.Fatal(err)
				}
				gotAppsState[a] = reply
			}

			// Verify that the rollout status is as expected.
			opts = []cmp.Option{
				protocmp.Transform(),
				protocmp.SortRepeatedFields(&nanny.ReplicaSetState{}, "replica_sets"),
			}
			if diff := cmp.Diff(c.expectedState, gotAppsState, opts...); diff != "" {
				t.Fatalf("bad state (-want, +got):\n%s", diff)
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
		name       string
		procStates replicaSetStates            // proc_name -> ReplicaSet state
		profiles   map[string]*profile.Profile // proc_name:replica -> profile
		expect     *profile.Profile
		expectErr  string
	}
	for _, c := range []testCase{
		{
			// Test plan: Single ReplicaSet with a single healthy replica. The
			// returned profile should be the profile of that single replica.
			name: "one_replica_set",
			procStates: replicaSetStates{
				"rs1": {
					replicas: []string{"replica1:healthy", "replica2:unhealthy"},
				},
			},
			profiles: map[string]*profile.Profile{
				"rs1:replica1": prof(time.Second, 100),
			},
			expect: prof(time.Second, 100),
		},
		{
			// Test plan: Two ReplicaSets with a different number of healthy
			// replicas. The returned profile should be the scaled combination
			// of the two ReplicaSets' profiles.
			name: "two_replica_sets",
			procStates: replicaSetStates{
				"rs1": {
					replicas: []string{
						"replica1:healthy",
						"replica2:unhealthy",
						"replica3:healthy",
					},
				},
				"rs2": {
					replicas: []string{
						"replica1:healthy",
						"replica2:healthy",
						"replica3:healthy",
						"replica4:unhealthy",
					},
				},
			},
			profiles: map[string]*profile.Profile{
				"rs1:replica1": prof(time.Second, 100),
				"rs1:replica3": prof(time.Second, 100),
				"rs2:replica1": prof(time.Second, 1000),
				"rs2:replica2": prof(time.Second, 1000),
				"rs2:replica3": prof(time.Second, 1000),
			},
			expect: prof(2*time.Second, 3200),
		},
		{
			// Test plan: Two ReplicaSets where one has a healthy replica and the
			// other doesn't. The returned profile should be the profile of
			// the healthy ReplicaSet.
			name: "two_replica_sets_one_unhealthy",
			procStates: replicaSetStates{
				"rs1": {replicas: []string{"replica1:healthy"}},
				"rs2": {replicas: []string{"replica1:unhealthy"}},
			},
			profiles: map[string]*profile.Profile{
				"rs1:replica1": prof(time.Second, 100),
			},
			expect: prof(time.Second, 100),
		},
		{
			// Test plan: Two ReplicaSets, both healthy. Profile collection fails
			// at one of the two ReplicaSets. The returned profile should be the
			// profile of the other ReplicaSet, but with an error.
			name: "two_replica_sets_one_failed",
			procStates: replicaSetStates{
				"rs1": {replicas: []string{"replica1:healthy"}},
				"rs2": {replicas: []string{"replica1:healthy"}},
			},
			profiles: map[string]*profile.Profile{
				"rs1:replica1": prof(time.Second, 100),
			},
			expect:    prof(time.Second, 100),
			expectErr: "no profile found",
		},
		{
			// Test plan: Two ReplicaSets, both unhealthy. The returned profile
			// data should be empty, with no errors.
			name: "two_replica_sets_both_unhealthy",
			procStates: replicaSetStates{
				"rs1": {replicas: []string{"replica1:unhealthy"}},
				"rs2": {replicas: []string{"replica1:unhealthy"}},
			},
			expect: nil,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			manager := &mockManagerClient{nil, nil, nil, c.procStates}
			babysitterConstructor := func(addr string) clients.BabysitterClient {
				return &mockBabysitterClient{c.profiles[addr], nil /*metrics*/}
			}
			d, err := Start(ctx,
				http.NewServeMux(),
				store.NewFakeStore(),
				logging.NewTestSlogger(t, testing.Verbose()),
				manager,
				testRegion,
				babysitterConstructor,
				0,   // manageAppsInterval
				0,   // computeTrafficInterval
				0,   // applyTrafficInterval
				0,   // detectAppliedTrafficInterval
				nil, // applyTraffic
				func(_ context.Context, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
					return []*nanny.Listener{{Name: "pub"}}, nil
				},
				nil, // getMetricCounts
			)
			if err != nil {
				t.Fatal(err)
			}
			p, err := d.RunProfiling(ctx, &nanny.GetProfileRequest{
				AppName:   "app",
				VersionId: "1",
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
			if len(p.Data) > 0 {
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

// version represents an app version
type version struct {
	appName      string
	id           string // version name
	submissionId int    // submission id
	listeners    []*nanny.Listener
	listenerOpts map[string]*config.GKEConfig_ListenerOptions
}

// newVersion creates a new app version.
func newVersion(appName, id string, submissionId int, listener ...string) version {
	v := version{
		appName:      appName,
		id:           id,
		submissionId: submissionId,
		listenerOpts: map[string]*config.GKEConfig_ListenerOptions{},
	}
	for _, l := range listener {
		if strings.Contains(l, ".") { // public listener
			name := strings.Split(l, ".")[0]
			v.listenerOpts[name] = &config.GKEConfig_ListenerOptions{
				PublicHostname: l,
			}
			l = name
		}
		v.listeners = append(v.listeners, &nanny.Listener{Name: l})
	}
	return v
}

func registerNewAppVersion(d *Distributor, v version) error {
	req := &nanny.ApplicationDistributionRequest{
		AppName: v.appName,
		Requests: []*nanny.VersionDistributionRequest{
			{
				Config: &config.GKEConfig{
					Deployment: &protos.Deployment{
						App: &protos.AppConfig{
							Name: v.appName,
						},
						Id: v.id,
					},
					Listeners: v.listenerOpts,
				},
				TargetFn:     &nanny.TargetFn{}, // rolls out immediately
				SubmissionId: int64(v.submissionId),
			},
		},
	}
	return d.Distribute(context.Background(), req)
}

type replicaSetStates map[string]struct {
	replicas   []string
	components []string
	listeners  []string
}

func (s replicaSetStates) toProto() *nanny.ReplicaSetState {
	var ret nanny.ReplicaSetState
	for proc, state := range s {
		rs := &nanny.ReplicaSetState_ReplicaSet{Name: proc, Components: state.components}
		for _, replica := range state.replicas {
			parts := strings.Split(replica, ":")
			if len(parts) != 2 {
				panic(fmt.Errorf("invalid replica format %q", replica))
			}
			healthStatus := protos.HealthStatus_UNHEALTHY
			if parts[1] == "healthy" {
				healthStatus = protos.HealthStatus_HEALTHY
			}
			addr := fmt.Sprintf("%s:%s", proc, parts[0])
			rs.Pods = append(rs.Pods, &nanny.ReplicaSetState_ReplicaSet_Pod{
				WeaveletAddr:   addr,
				BabysitterAddr: addr,
				HealthStatus:   healthStatus,
			})
		}
		rs.Listeners = state.listeners
		ret.ReplicaSets = append(ret.ReplicaSets, rs)
	}
	return &ret
}

// mockManagerClient is a mock manager.Client that returns the provided
// errors/values for the corresponding methods.
type mockManagerClient struct {
	deploy      error            // returned by Deploy
	stop        error            // returned by Stop
	delete      error            // returned by Delete
	replicaSets replicaSetStates // returned by GetReplicaSetState
}

var _ clients.ManagerClient = &mockManagerClient{}

// Deploy implements the clients.ManagerClient interface.
func (m *mockManagerClient) Deploy(context.Context, *nanny.ApplicationDeploymentRequest) error {
	return m.deploy
}

// Stop implements the clients.ManagerClient interface.
func (m *mockManagerClient) Stop(context.Context, *nanny.ApplicationStopRequest) error {
	return m.stop
}

// Delete implements the clients.ManagerClient interface.
func (m *mockManagerClient) Delete(context.Context, *nanny.ApplicationDeleteRequest) error {
	return m.delete
}

// GetReplicaSetState implements the clients.ManagerClient interface.
func (m *mockManagerClient) GetReplicaSetState(_ context.Context, req *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error) {
	return m.replicaSets.toProto(), nil
}

// StartComponent implements the clients.ManagerClient interface.
func (m *mockManagerClient) ActivateComponent(context.Context, *nanny.ActivateComponentRequest) error {
	panic("implement me")
}

// RegisterReplica implements the clients.ManagerClient interface.
func (m *mockManagerClient) RegisterReplica(context.Context, *nanny.RegisterReplicaRequest) error {
	panic("implement me")
}

// ReportLoad implements the clients.ManagerClient interface.
func (m *mockManagerClient) ReportLoad(context.Context, *nanny.LoadReport) error {
	panic("implement me")
}

// GetListenerAddress implements the clients.ManagerClient interface.
func (m *mockManagerClient) GetListenerAddress(context.Context, *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	panic("implement me")
}

// ExportListener implements the clients.ManagerClient interface.
func (m *mockManagerClient) ExportListener(context.Context, *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	panic("implement me")
}

// GetRoutingInfo implements the clients.ManagerClient interface.
func (m *mockManagerClient) GetRoutingInfo(context.Context, *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	panic("implement me")
}

// GetComponentsToStart implements the clients.ManagerClient interface.
func (m *mockManagerClient) GetComponentsToStart(context.Context, *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error) {
	panic("implement me")
}

// testBabysitterClient is a mock babysitter.Client that returns the pre-created
// profile, or an error if a profile hasn't been pre-created.
type mockBabysitterClient struct {
	prof    *profile.Profile
	metrics []*protos.MetricValue
}

var _ clients.BabysitterClient = mockBabysitterClient{}

// RunProfiling implements the clients.BabysitterClient interface.
func (b mockBabysitterClient) RunProfiling(_ context.Context, req *protos.GetProfileRequest) (*protos.GetProfileReply, error) {
	if b.prof == nil {
		return nil, fmt.Errorf("no profile found")
	}

	var buf bytes.Buffer
	if err := b.prof.Write(&buf); err != nil {
		return nil, err
	}
	return &protos.GetProfileReply{Data: buf.Bytes()}, nil
}

// CheckHealth implements the clients.BabysitterClient interface.
func (b mockBabysitterClient) CheckHealth(context.Context, *protos.GetHealthRequest) (*protos.GetHealthReply, error) {
	panic("implement me")
}
