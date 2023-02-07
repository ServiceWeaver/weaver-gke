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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestTargetFnFraction(t *testing.T) {
	targetfn := &TargetFn{
		Fractions: []*FractionSpec{
			{Duration: durationpb.New(10 * time.Second), TrafficFraction: 0.1},
			{Duration: durationpb.New(5 * time.Second), TrafficFraction: 0.2},
			{Duration: durationpb.New(7 * time.Second), TrafficFraction: 0.3},
		},
	}
	for i, want := range []float32{
		0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
		0.2, 0.2, 0.2, 0.2, 0.2,
		0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3,
		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
	} {
		d := time.Duration(i) * time.Second
		t.Run(fmt.Sprint(d), func(t *testing.T) {
			if got := TargetFraction(targetfn, d); got != want {
				t.Fatalf("targetfn.Fraction(%v): got %f, want %f", d, got, want)
			}
		})
	}
}

func TestLength(t *testing.T) {
	targetfn := &TargetFn{
		Fractions: []*FractionSpec{
			{Duration: durationpb.New(10 * time.Second), TrafficFraction: 0.1},
			{Duration: durationpb.New(5 * time.Second), TrafficFraction: 0.2},
			{Duration: durationpb.New(7 * time.Second), TrafficFraction: 0.3},
			{Duration: durationpb.New(3 * time.Second), TrafficFraction: 0.4},
		},
	}
	if got, want := Length(targetfn), 25*time.Second; got != want {
		t.Fatalf("targetfn.Length(): got %v, want %v", got, want)
	}
}

func TestChangeTimes(t *testing.T) {
	targetfn := &TargetFn{
		Fractions: []*FractionSpec{
			{Duration: durationpb.New(10 * time.Second), TrafficFraction: 0.1},
			{Duration: durationpb.New(5 * time.Second), TrafficFraction: 0.2},
			{Duration: durationpb.New(7 * time.Second), TrafficFraction: 0.3},
			{Duration: durationpb.New(3 * time.Second), TrafficFraction: 0.4},
		},
	}
	now := time.Now()
	got := ChangeTimes(targetfn, now)
	want := []time.Time{
		now.Add(10 * time.Second),
		now.Add(15 * time.Second),
		now.Add(22 * time.Second),
		now.Add(25 * time.Second),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("targetfn.ChangeTimes(%v): (-want got):\n%s", now, diff)
	}
}

func TestChangeTimesFinalFractionOne(t *testing.T) {
	targetfn := &TargetFn{
		Fractions: []*FractionSpec{
			{Duration: durationpb.New(10 * time.Second), TrafficFraction: 0.1},
			{Duration: durationpb.New(5 * time.Second), TrafficFraction: 0.2},
			{Duration: durationpb.New(7 * time.Second), TrafficFraction: 0.3},
			// Because the final fraction is 1.0, now+25 seconds is not an
			// interesting point.
			{Duration: durationpb.New(3 * time.Second), TrafficFraction: 1.0},
		},
	}
	now := time.Now()
	got := ChangeTimes(targetfn, now)
	want := []time.Time{
		now.Add(10 * time.Second),
		now.Add(15 * time.Second),
		now.Add(22 * time.Second),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("targetfn.ChangeTimes(%v): (-want got):\n%s", now, diff)
	}
}
