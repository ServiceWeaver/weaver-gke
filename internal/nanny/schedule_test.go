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
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// schedule returns a schedule with the provided durations. The ith duration is
// assigned a fraction of i.
func schedule(durations ...time.Duration) *Schedule {
	fractions := make([]*FractionSpec, len(durations))
	for i, d := range durations {
		fractions[i] = &FractionSpec{
			Duration:        durationpb.New(d),
			TrafficFraction: float32(i),
		}
	}
	return NewSchedule(&TargetFn{Fractions: fractions})
}

func TestReady(t *testing.T) {
	s := schedule(time.Minute, time.Minute)

	checkReady := func(expect bool, tick int) {
		actual := Ready(s)
		if expect != actual {
			t.Errorf("Ready(%d): got %v; want %v", tick, actual, expect)
		}
	}

	// First fraction.
	for tick := 0; tick < 60; tick++ {
		checkReady(false, tick)
		IncrementAppliedDuration(s, time.Second)
	}
	checkReady(true, 60)

	// Second fraction.
	if err := Advance(s); err != nil {
		t.Fatalf("Advance(60): %v", err)
	}
	for tick := 60; tick < 120; tick++ {
		checkReady(false, tick)
		IncrementAppliedDuration(s, time.Second)
	}
	checkReady(true, 120)

	// Done.
	if err := Advance(s); err != nil {
		t.Fatalf("Advance(120): %v", err)
	}
	for tick := 120; tick < 300; tick++ {
		checkReady(false, tick)
		IncrementAppliedDuration(s, time.Second)
	}
}

func TestAdvance(t *testing.T) {
	s := schedule(time.Minute, time.Minute, time.Minute, time.Minute)
	for tick := 0; tick < 4; tick++ {
		IncrementAppliedDuration(s, time.Minute)
		if err := Advance(s); err != nil {
			t.Fatalf("Advance(%d): %v", tick, err)
		}
	}
}

func TestPrematureAdvance(t *testing.T) {
	s := schedule(time.Minute, time.Minute, time.Minute, time.Minute)
	for tick := 0; tick < 300; tick++ {
		wantAdvanced := tick == 60 || tick == 120 || tick == 180 || tick == 240
		actualAdvanced := Advance(s) == nil
		if wantAdvanced != actualAdvanced {
			t.Errorf("Advance(%d): got %v; want %v", tick, actualAdvanced, wantAdvanced)
		}
		IncrementAppliedDuration(s, time.Second)
	}
}

func TestAdvanceDone(t *testing.T) {
	s := schedule(time.Minute, time.Minute, time.Minute, time.Minute)
	for tick := 0; tick < 4; tick++ {
		IncrementAppliedDuration(s, time.Minute)
		Advance(s)
	}
	if err := Advance(s); err == nil {
		t.Fatalf("s.Advance(4): unexpected success")
	}
}

func TestFraction(t *testing.T) {
	s := schedule(time.Minute, time.Minute, time.Minute)
	for tick := 0; tick < 300; tick++ {
		var want float32
		switch {
		case tick < 60:
			want = 0.0
		case tick < 120:
			want = 1.0
		case tick < 180:
			want = 2.0
		default:
			want = 1.0
		}
		if got := Fraction(s); got != want {
			t.Errorf("Fraction(%d): got %v; want %v", tick, got, want)
		}
		IncrementAppliedDuration(s, time.Second)
		if Ready(s) {
			if err := Advance(s); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestDone(t *testing.T) {
	s := schedule(time.Minute, time.Minute, time.Minute, time.Minute)
	for tick := 0; tick < 10; tick++ {
		wantDone := tick >= 4
		actualDone := Done(s)
		if wantDone != actualDone {
			t.Fatalf("Done(%d): got %v, want %v", tick, actualDone, wantDone)
		}
		IncrementAppliedDuration(s, time.Minute)
		Advance(s)
	}
}
