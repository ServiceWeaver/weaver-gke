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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
)

func TestBasicRolloutProcessor(t *testing.T) {
	type testCase struct {
		name  string
		props rolloutProperties
		err   string
	}
	for _, c := range []testCase{
		{
			name: "one_location",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.5,
				locations:    []string{"loc1"},
			},
			err: "",
		},
		{
			name: "many_locations",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.5,
				locations:    []string{"loc1", "loc2", "loc3", "loc4", "loc5", "loc6"},
			},
			err: "",
		},
		{
			name: "long_rollout",
			props: rolloutProperties{
				durationHint: time.Hour * 24,
				waitTimeFrac: 0.5,
				locations:    []string{"loc1", "loc2", "loc3"},
			},
			err: "",
		},
		{
			name: "short_rollout",
			props: rolloutProperties{
				durationHint: time.Minute * 10,
				waitTimeFrac: 0.5,
				locations:    []string{"loc1", "loc2", "loc3"},
			},
			err: "",
		},
		{
			name: "long_validation",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.7,
				locations:    []string{"loc1", "loc2", "loc3"},
			},
			err: "",
		},
		{
			name: "no_validation",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.0,
				locations:    []string{"loc1", "loc2", "loc3"},
			},
			err: "",
		},
		{
			name: "actuation_delay",
			props: rolloutProperties{
				durationHint:   time.Hour,
				waitTimeFrac:   0.1,
				locations:      []string{"loc1", "loc2", "loc3"},
				actuationDelay: 5 * time.Minute,
			},
		},
		{
			name: "no_location_to_rollout",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.5,
			},
			err: "no rollout locations specified",
		},
		{
			name: "duplicate_locations_to_rollout",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 0.5,
				locations:    []string{"loc1", "loc2", "loc1"},
			},
			err: "specified multiple times",
		},
		{
			name: "invalid_frac_time_to_wait_small",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: -0.5,
				locations:    []string{"loc1"},
			},
			err: "invalid waitTimeFrac",
		},
		{
			name: "invalid_frac_time_to_wait_big",
			props: rolloutProperties{
				durationHint: time.Hour,
				waitTimeFrac: 1.0,
				locations:    []string{"loc1"},
			},
			err: "invalid waitTimeFrac",
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rolloutProc := basicRolloutProcessor{}
			strategy, err := rolloutProc.computeRolloutStrategy(c.props)
			if err != nil {
				if c.err != "" {
					if !strings.Contains(err.Error(), c.err) {
						t.Error(err)
					}
					return
				}
				t.Fatal(err)
			}

			var locs []string
			applyDurationSecs := 0.0
			waitDurationSecs := 0.0
			for _, wave := range strategy.Waves {
				hasComputedApplyTime := false
				waitDurationSecs += wave.WaitTime.AsDuration().Seconds()

				for loc, targetFn := range wave.TargetFunctions {
					locs = append(locs, loc)
					if hasComputedApplyTime {
						continue
					}
					for _, e := range targetFn.Fractions {
						applyDurationSecs += float64(e.Duration.Seconds)
					}
					hasComputedApplyTime = true
				}
			}

			acceptedDiff := float64(10)
			eq := func(a, b float64) (float64, bool) {
				diff := math.Abs(a - b)
				if diff > acceptedDiff {
					return diff, false
				}
				return 0, true
			}
			hint := c.props.durationHint.Seconds()
			actDelay := c.props.actuationDelay.Seconds()

			// Verify that the total time spent to apply traffic is as expected.
			if diff, ok := eq((1-c.props.waitTimeFrac)*hint, applyDurationSecs); !ok {
				t.Errorf("testCase(%s) wrong time to apply traffic (-want, +got):\n%f", c.name, diff)
			}

			// Verify that the total time to wait is as expected.
			if diff, ok := eq(c.props.waitTimeFrac*hint+actDelay, waitDurationSecs); !ok {
				t.Errorf("testCase(%s) wrong time to wait (-want, +got):\n%f", c.name, diff)
			}

			// Verify that the total rollout time is as expected.
			if diff, ok := eq(applyDurationSecs+waitDurationSecs, hint+actDelay); !ok {
				t.Errorf("testCase(%s) wrong rollout time (-want, +got):\n%f", c.name, diff)
			}

			// Verify that the number of rollout locations is as expected.
			if diff := len(locs) - len(c.props.locations); diff != 0 {
				t.Errorf("testCase(%s) wrong number locations to rollout (-want, +got):\n%d", c.name, diff)
			}
		})
	}
}

// target returns a target function with the provided durations. Every traffic
// fraction is 0.5.
func target(durations ...time.Duration) *nanny.TargetFn {
	fractions := make([]*nanny.FractionSpec, len(durations))
	for i, d := range durations {
		fractions[i] = &nanny.FractionSpec{
			Duration:        durationpb.New(d),
			TrafficFraction: 0.5,
		}
	}
	return &nanny.TargetFn{Fractions: fractions}
}

func TestWaveLength(t *testing.T) {
	for _, test := range []struct {
		name string
		wave *RolloutWave
		want time.Duration
	}{
		{
			name: "OneLocation",
			wave: &RolloutWave{
				TargetFunctions: map[string]*nanny.TargetFn{
					"a": target(time.Second, 2*time.Second, 3*time.Second),
				},
				WaitTime: durationpb.New(10 * time.Second),
			},
			want: 16 * time.Second,
		},
		{
			name: "TwoLocations",
			wave: &RolloutWave{
				TargetFunctions: map[string]*nanny.TargetFn{
					"a": target(time.Second, 2*time.Second, 3*time.Second),
					"b": target(5*time.Second, time.Second, 3*time.Second),
				},
				WaitTime: durationpb.New(10 * time.Second),
			},
			want: 19 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := length(test.wave); got != test.want {
				t.Fatalf("wave.Length(): got %v, want %v", got, test.want)
			}
		})
	}
}

func TestRolloutStrategyFraction(t *testing.T) {
	now := time.Now()
	sec := func(i int) time.Duration { return time.Duration(i) * time.Second }
	at := func(seconds int) time.Time { return now.Add(sec(seconds)) }

	// This is the full rollout schedule for this test case. We have three
	// locations (a, b, c). Every row corresponds to a location's target
	// function (x = 0.2, y = 0.5, z = 1.0). Every column corresponds to a
	// second. To understand what the traffic fraction should be at a given
	// time, look at the time's column.
	//
	//             1         2
	//   0 2 4 6 8 0 2 4 6 8 0 2 4 6
	// a xyz...
	// b       xxyyzz...
	// c                xxxyyyzzz...
	target := func(d time.Duration) *nanny.TargetFn {
		return &nanny.TargetFn{
			Fractions: []*nanny.FractionSpec{
				{Duration: durationpb.New(d), TrafficFraction: 0.2},
				{Duration: durationpb.New(d), TrafficFraction: 0.5},
				{Duration: durationpb.New(d), TrafficFraction: 1.0},
			},
		}
	}
	s := &RolloutStrategy{
		Waves: []*RolloutWave{
			{
				TargetFunctions: map[string]*nanny.TargetFn{"a": target(sec(1))},
				WaitTime:        durationpb.New(sec(3)),
			},
			{
				TargetFunctions: map[string]*nanny.TargetFn{"b": target(sec(2))},
				WaitTime:        durationpb.New(sec(3)),
			},
			{
				TargetFunctions: map[string]*nanny.TargetFn{"c": target(sec(3))},
				WaitTime:        durationpb.New(sec(3)),
			},
		},
	}

	for _, test := range []struct {
		location string
		i        int
		start    time.Time
		d        time.Duration
		want     float32
	}{
		// In wave 0.
		{"a", 0, at(0), sec(0), 0.2},
		{"a", 0, at(0), sec(1), 0.5},
		{"a", 0, at(0), sec(2), 1.0},
		{"a", 0, at(0), sec(3), 1.0},
		{"a", 0, at(0), sec(4), 1.0},
		{"a", 0, at(0), sec(5), 1.0},
		{"a", 0, at(0), sec(6), 1.0},
		{"a", 0, at(0), sec(7), 1.0},
		{"a", 0, at(0), sec(8), 1.0},
		{"a", 0, at(0), sec(9), 1.0},
		{"a", 0, at(0), sec(10), 1.0},
		{"a", 0, at(0), sec(11), 1.0},
		{"a", 0, at(0), sec(12), 1.0},
		{"a", 0, at(0), sec(13), 1.0},
		{"a", 0, at(0), sec(14), 1.0},
		{"a", 0, at(0), sec(15), 1.0},
		{"a", 0, at(0), sec(16), 1.0},
		{"a", 0, at(0), sec(17), 1.0},
		{"a", 0, at(0), sec(18), 1.0},
		{"a", 0, at(0), sec(19), 1.0},
		{"a", 0, at(0), sec(20), 1.0},
		{"a", 0, at(0), sec(21), 1.0},
		{"a", 0, at(0), sec(22), 1.0},
		{"a", 0, at(0), sec(23), 1.0},
		{"a", 0, at(0), sec(24), 1.0},
		{"a", 0, at(0), sec(25), 1.0},
		{"a", 0, at(0), sec(26), 1.0},

		{"b", 0, at(0), sec(0), 0.0},
		{"b", 0, at(0), sec(1), 0.0},
		{"b", 0, at(0), sec(2), 0.0},
		{"b", 0, at(0), sec(3), 0.0},
		{"b", 0, at(0), sec(4), 0.0},
		{"b", 0, at(0), sec(5), 0.0},
		{"b", 0, at(0), sec(6), 0.2},
		{"b", 0, at(0), sec(7), 0.2},
		{"b", 0, at(0), sec(8), 0.5},
		{"b", 0, at(0), sec(9), 0.5},
		{"b", 0, at(0), sec(10), 1.0},
		{"b", 0, at(0), sec(11), 1.0},
		{"b", 0, at(0), sec(12), 1.0},
		{"b", 0, at(0), sec(13), 1.0},
		{"b", 0, at(0), sec(14), 1.0},
		{"b", 0, at(0), sec(15), 1.0},
		{"b", 0, at(0), sec(16), 1.0},
		{"b", 0, at(0), sec(17), 1.0},
		{"b", 0, at(0), sec(18), 1.0},
		{"b", 0, at(0), sec(19), 1.0},
		{"b", 0, at(0), sec(20), 1.0},
		{"b", 0, at(0), sec(21), 1.0},
		{"b", 0, at(0), sec(22), 1.0},
		{"b", 0, at(0), sec(23), 1.0},
		{"b", 0, at(0), sec(24), 1.0},
		{"b", 0, at(0), sec(25), 1.0},
		{"b", 0, at(0), sec(26), 1.0},

		{"c", 0, at(0), sec(0), 0.0},
		{"c", 0, at(0), sec(1), 0.0},
		{"c", 0, at(0), sec(2), 0.0},
		{"c", 0, at(0), sec(3), 0.0},
		{"c", 0, at(0), sec(4), 0.0},
		{"c", 0, at(0), sec(5), 0.0},
		{"c", 0, at(0), sec(6), 0.0},
		{"c", 0, at(0), sec(7), 0.0},
		{"c", 0, at(0), sec(8), 0.0},
		{"c", 0, at(0), sec(9), 0.0},
		{"c", 0, at(0), sec(10), 0.0},
		{"c", 0, at(0), sec(11), 0.0},
		{"c", 0, at(0), sec(12), 0.0},
		{"c", 0, at(0), sec(13), 0.0},
		{"c", 0, at(0), sec(14), 0.0},
		{"c", 0, at(0), sec(15), 0.2},
		{"c", 0, at(0), sec(16), 0.2},
		{"c", 0, at(0), sec(17), 0.2},
		{"c", 0, at(0), sec(18), 0.5},
		{"c", 0, at(0), sec(19), 0.5},
		{"c", 0, at(0), sec(20), 0.5},
		{"c", 0, at(0), sec(21), 1.0},
		{"c", 0, at(0), sec(22), 1.0},
		{"c", 0, at(0), sec(23), 1.0},
		{"c", 0, at(0), sec(24), 1.0},
		{"c", 0, at(0), sec(25), 1.0},
		{"c", 0, at(0), sec(26), 1.0},

		// In wave 1.
		{"a", 1, at(6) /*6*/, sec(0), 1.0},
		{"a", 1, at(6) /*7*/, sec(1), 1.0},
		{"a", 1, at(6) /*8*/, sec(2), 1.0},
		{"a", 1, at(6) /*9*/, sec(3), 1.0},
		{"a", 1, at(6) /*10*/, sec(4), 1.0},
		{"a", 1, at(6) /*11*/, sec(5), 1.0},
		{"a", 1, at(6) /*12*/, sec(6), 1.0},
		{"a", 1, at(6) /*13*/, sec(7), 1.0},
		{"a", 1, at(6) /*14*/, sec(8), 1.0},
		{"a", 1, at(6) /*15*/, sec(9), 1.0},
		{"a", 1, at(6) /*16*/, sec(10), 1.0},
		{"a", 1, at(6) /*17*/, sec(11), 1.0},
		{"a", 1, at(6) /*18*/, sec(12), 1.0},
		{"a", 1, at(6) /*19*/, sec(13), 1.0},
		{"a", 1, at(6) /*20*/, sec(14), 1.0},
		{"a", 1, at(6) /*21*/, sec(15), 1.0},
		{"a", 1, at(6) /*22*/, sec(16), 1.0},
		{"a", 1, at(6) /*23*/, sec(17), 1.0},
		{"a", 1, at(6) /*24*/, sec(18), 1.0},
		{"a", 1, at(6) /*25*/, sec(19), 1.0},
		{"a", 1, at(6) /*26*/, sec(20), 1.0},

		{"b", 1, at(6) /*6*/, sec(0), 0.2},
		{"b", 1, at(6) /*7*/, sec(1), 0.2},
		{"b", 1, at(6) /*8*/, sec(2), 0.5},
		{"b", 1, at(6) /*9*/, sec(3), 0.5},
		{"b", 1, at(6) /*10*/, sec(4), 1.0},
		{"b", 1, at(6) /*11*/, sec(5), 1.0},
		{"b", 1, at(6) /*12*/, sec(6), 1.0},
		{"b", 1, at(6) /*13*/, sec(7), 1.0},
		{"b", 1, at(6) /*14*/, sec(8), 1.0},
		{"b", 1, at(6) /*15*/, sec(9), 1.0},
		{"b", 1, at(6) /*16*/, sec(10), 1.0},
		{"b", 1, at(6) /*17*/, sec(11), 1.0},
		{"b", 1, at(6) /*18*/, sec(12), 1.0},
		{"b", 1, at(6) /*19*/, sec(13), 1.0},
		{"b", 1, at(6) /*20*/, sec(14), 1.0},
		{"b", 1, at(6) /*21*/, sec(15), 1.0},
		{"b", 1, at(6) /*22*/, sec(16), 1.0},
		{"b", 1, at(6) /*23*/, sec(17), 1.0},
		{"b", 1, at(6) /*24*/, sec(18), 1.0},
		{"b", 1, at(6) /*25*/, sec(19), 1.0},
		{"b", 1, at(6) /*26*/, sec(20), 1.0},

		{"c", 1, at(6) /*6*/, sec(0), 0.0},
		{"c", 1, at(6) /*7*/, sec(1), 0.0},
		{"c", 1, at(6) /*8*/, sec(2), 0.0},
		{"c", 1, at(6) /*9*/, sec(3), 0.0},
		{"c", 1, at(6) /*10*/, sec(4), 0.0},
		{"c", 1, at(6) /*11*/, sec(5), 0.0},
		{"c", 1, at(6) /*12*/, sec(6), 0.0},
		{"c", 1, at(6) /*13*/, sec(7), 0.0},
		{"c", 1, at(6) /*14*/, sec(8), 0.0},
		{"c", 1, at(6) /*15*/, sec(9), 0.2},
		{"c", 1, at(6) /*16*/, sec(10), 0.2},
		{"c", 1, at(6) /*17*/, sec(11), 0.2},
		{"c", 1, at(6) /*18*/, sec(12), 0.5},
		{"c", 1, at(6) /*19*/, sec(13), 0.5},
		{"c", 1, at(6) /*20*/, sec(14), 0.5},
		{"c", 1, at(6) /*21*/, sec(15), 1.0},
		{"c", 1, at(6) /*22*/, sec(16), 1.0},
		{"c", 1, at(6) /*23*/, sec(17), 1.0},
		{"c", 1, at(6) /*24*/, sec(18), 1.0},
		{"c", 1, at(6) /*25*/, sec(19), 1.0},
		{"c", 1, at(6) /*26*/, sec(20), 1.0},

		// In wave 2.
		{"a", 2, at(15) /*15*/, sec(0), 1.0},
		{"a", 2, at(15) /*16*/, sec(1), 1.0},
		{"a", 2, at(15) /*17*/, sec(2), 1.0},
		{"a", 2, at(15) /*18*/, sec(3), 1.0},
		{"a", 2, at(15) /*19*/, sec(4), 1.0},
		{"a", 2, at(15) /*20*/, sec(5), 1.0},
		{"a", 2, at(15) /*21*/, sec(6), 1.0},
		{"a", 2, at(15) /*22*/, sec(7), 1.0},
		{"a", 2, at(15) /*23*/, sec(8), 1.0},
		{"a", 2, at(15) /*24*/, sec(9), 1.0},
		{"a", 2, at(15) /*25*/, sec(10), 1.0},
		{"a", 2, at(15) /*26*/, sec(11), 1.0},

		{"b", 2, at(15) /*15*/, sec(0), 1.0},
		{"b", 2, at(15) /*16*/, sec(1), 1.0},
		{"b", 2, at(15) /*17*/, sec(2), 1.0},
		{"b", 2, at(15) /*18*/, sec(3), 1.0},
		{"b", 2, at(15) /*19*/, sec(4), 1.0},
		{"b", 2, at(15) /*20*/, sec(5), 1.0},
		{"b", 2, at(15) /*21*/, sec(6), 1.0},
		{"b", 2, at(15) /*22*/, sec(7), 1.0},
		{"b", 2, at(15) /*23*/, sec(8), 1.0},
		{"b", 2, at(15) /*24*/, sec(9), 1.0},
		{"b", 2, at(15) /*25*/, sec(10), 1.0},
		{"b", 2, at(15) /*26*/, sec(11), 1.0},

		{"c", 2, at(15) /*15*/, sec(0), 0.2},
		{"c", 2, at(15) /*16*/, sec(1), 0.2},
		{"c", 2, at(15) /*17*/, sec(2), 0.2},
		{"c", 2, at(15) /*18*/, sec(3), 0.5},
		{"c", 2, at(15) /*19*/, sec(4), 0.5},
		{"c", 2, at(15) /*20*/, sec(5), 0.5},
		{"c", 2, at(15) /*21*/, sec(6), 1.0},
		{"c", 2, at(15) /*22*/, sec(7), 1.0},
		{"c", 2, at(15) /*23*/, sec(8), 1.0},
		{"c", 2, at(15) /*24*/, sec(9), 1.0},
		{"c", 2, at(15) /*25*/, sec(10), 1.0},
		{"c", 2, at(15) /*26*/, sec(11), 1.0},
	} {
		name := fmt.Sprintf("%s@%vin%v", test.location, test.d, test.i)
		t.Run(name, func(t *testing.T) {
			got, err := fraction(s, test.location, test.i, test.start, test.d)
			if err != nil {
				t.Fatalf("s.Fraction: %v", err)
			}
			if got != test.want {
				t.Fatalf("s.Fraction: got %v, want %v", got, test.want)
			}
		})
	}
}

func TestChangeTimes(t *testing.T) {
	strategy := &RolloutStrategy{
		Waves: []*RolloutWave{
			{
				TargetFunctions: map[string]*nanny.TargetFn{
					"a": target(10 * time.Second),
					"b": target(10*time.Second, 10*time.Second),
				},
				WaitTime: durationpb.New(10 * time.Second),
			},
			{
				TargetFunctions: map[string]*nanny.TargetFn{
					"c": target(1*time.Second, 2*time.Second),
					"d": target(3*time.Second, 5*time.Second),
				},
				WaitTime: durationpb.New(10 * time.Second),
			},
			{
				TargetFunctions: map[string]*nanny.TargetFn{
					"e": target(10 * time.Second),
					"f": target(20*time.Second, 30*time.Second),
				},
				WaitTime: durationpb.New(10 * time.Second),
			},
		},
	}

	var now time.Time
	times, err := changeTimes(strategy, 1, now)
	if err != nil {
		t.Fatalf("strategy.ChangeTimes: %v", err)
	}
	got := map[time.Time]bool{}
	for _, t := range times {
		got[t] = true
	}

	want := map[time.Time]bool{
		now.Add(1 * time.Second):  true,
		now.Add(3 * time.Second):  true,
		now.Add(8 * time.Second):  true,
		now.Add(18 * time.Second): true,
		now.Add(28 * time.Second): true,
		now.Add(38 * time.Second): true,
		now.Add(68 * time.Second): true,
		now.Add(78 * time.Second): true,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("strategy.ChangeTimes (-want +got):\n%s", diff)
	}
}
