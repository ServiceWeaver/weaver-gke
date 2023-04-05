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
	"testing"
	"time"

	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProjectedTraffic(t *testing.T) {
	// This is the full rollout schedule for this test case. We have three
	// versions (v1, v2, v3) and three locations (a, b, c). Every row
	// corresponds to a version's desired traffic fraction in given location (x
	// = 0.2, y = 0.5, z = 1.0). Every column corresponds to a second. To
	// understand what the traffic assignment should be at a given time, look
	// at the time's column.
	//
	//                1         2         3         4         5
	//      0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0
	// v1 a xyz...
	// v1 b       xxyyzz...
	// v1 c                xxxyyyzzz...
	// v2 a           xxyyzz...
	// v2 b                    xxxyyyzzz...
	// v2 c                                xyz...
	// v3 a                     xxxyyyzzz...
	// v3 b                                 xyz...
	// v3 c                                       xxyyzz...

	target := func(d time.Duration) *nanny.TargetFn {
		return &nanny.TargetFn{
			Fractions: []*nanny.FractionSpec{
				{Duration: durationpb.New(d), TrafficFraction: 0.2},
				{Duration: durationpb.New(d), TrafficFraction: 0.5},
				{Duration: durationpb.New(d), TrafficFraction: 1.0},
			},
		}
	}

	strategy := func(a, b, c *nanny.TargetFn) *RolloutStrategy {
		return &RolloutStrategy{
			Waves: []*RolloutWave{
				{
					TargetFunctions: map[string]*nanny.TargetFn{"a": a},
					WaitTime:        durationpb.New(3 * time.Second),
				},
				{
					TargetFunctions: map[string]*nanny.TargetFn{"b": b},
					WaitTime:        durationpb.New(3 * time.Second),
				},
				{
					TargetFunctions: map[string]*nanny.TargetFn{"c": c},
					WaitTime:        durationpb.New(3 * time.Second),
				},
			},
		}
	}

	alloc := func(loc, version string, fraction float32) *nanny.TrafficAllocation {
		return &nanny.TrafficAllocation{
			Location:        loc,
			AppName:         "app",
			VersionId:       version,
			TrafficFraction: fraction,
			Listener:        &nanny.Listener{Name: "app"},
		}
	}

	start := time.Now()
	at := func(seconds int) time.Time {
		return start.Add(time.Duration(seconds) * time.Second)
	}

	short := target(1 * time.Second)
	medium := target(2 * time.Second)
	long := target(3 * time.Second)

	v1 := &AppVersionState{
		Config:          &config.GKEConfig{Deployment: &protos.Deployment{Id: "v1"}},
		RolloutStrategy: strategy(short, medium, long),
		SubmissionId:    0,
	}
	v2 := &AppVersionState{
		Config:          &config.GKEConfig{Deployment: &protos.Deployment{Id: "v2"}},
		RolloutStrategy: strategy(medium, long, short),
		SubmissionId:    1,
	}
	v3 := &AppVersionState{
		Config:          &config.GKEConfig{Deployment: &protos.Deployment{Id: "v3"}},
		RolloutStrategy: strategy(long, short, medium),
		SubmissionId:    2,
	}

	// At Time 0; Projecting Time 5.
	v1.WaveIdx = 0
	v1.TimeWaveStarted = timestamppb.New(at(0))
	got, err := projectedTraffic("app", map[string]*AppVersionState{"v1": v1}, at(5))
	if err != nil {
		t.Fatal(err)
	}
	want := &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v1", 1.0),
				},
			},
		},
	}
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.SortRepeatedFields(&nanny.HostTrafficAssignment{}, "allocs"),
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 0; Projecting Time 8.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1}, at(8))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v1", 1.0/2),
					alloc("b", "v1", 1.0/2),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 0; Projecting Time 20.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1}, at(20))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v1", 1.0/3),
					alloc("b", "v1", 1.0/3),
					alloc("c", "v1", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 15; Projecting Time 16.
	v1.WaveIdx = 2
	v1.TimeWaveStarted = timestamppb.New(at(15))
	v2.WaveIdx = 0
	v2.TimeWaveStarted = timestamppb.New(at(10))
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2}, at(16))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v2", 1.0/3),
					alloc("b", "v1", 1.0/3),
					alloc("c", "v1", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 15; Projecting Time 23.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2}, at(23))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v2", 1.0/3),
					alloc("b", "v1", 0.5/3),
					alloc("b", "v2", 0.5/3),
					alloc("c", "v1", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 15; Projecting Time 32.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2}, at(32))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v2", 1.0/3),
					alloc("b", "v2", 1.0/3),
					alloc("c", "v1", 0.5/3),
					alloc("c", "v2", 0.5/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 20; Projecting Time 26.
	v1.WaveIdx = 2
	v1.TimeWaveStarted = timestamppb.New(at(15))
	v2.WaveIdx = 1
	v2.TimeWaveStarted = timestamppb.New(at(19))
	v3.WaveIdx = 0
	v3.TimeWaveStarted = timestamppb.New(at(20))
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(26))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v2", 1.0/3),
					alloc("c", "v1", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 20; Projecting Time 32.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(32))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v2", 0.8/3),
					alloc("b", "v3", 0.2/3),
					alloc("c", "v1", 0.5/3),
					alloc("c", "v2", 0.5/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 20; Projecting Time 40.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(40))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v3", 1.0/3),
					alloc("c", "v2", 0.5/3),
					alloc("c", "v3", 0.5/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 32; Projecting Time 36.
	v1.WaveIdx = 3
	v1.TimeWaveStarted = nil
	v2.WaveIdx = 2
	v2.TimeWaveStarted = timestamppb.New(at(31))
	v3.WaveIdx = 1
	v3.TimeWaveStarted = timestamppb.New(at(32))
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(36))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v3", 1.0/3),
					alloc("c", "v2", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 32; Projecting Time 40.
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(40))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v3", 1.0/3),
					alloc("c", "v2", 0.5/3),
					alloc("c", "v3", 0.5/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}

	// At Time 38; Projecting Time 46.
	v1.WaveIdx = 2
	v1.TimeWaveStarted = timestamppb.New(at(15))
	v2.WaveIdx = 2
	v2.TimeWaveStarted = timestamppb.New(at(31))
	v3.WaveIdx = 2
	v3.TimeWaveStarted = timestamppb.New(at(38))
	got, err = projectedTraffic("app", map[string]*AppVersionState{"v1": v1, "v2": v2, "v3": v3}, at(46))
	if err != nil {
		t.Fatal(err)
	}
	want = &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{
			"app": {
				Allocs: []*nanny.TrafficAllocation{
					alloc("a", "v3", 1.0/3),
					alloc("b", "v3", 1.0/3),
					alloc("c", "v3", 1.0/3),
				},
			},
		},
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("traffic (-want +got):\n%s", diff)
	}
}
