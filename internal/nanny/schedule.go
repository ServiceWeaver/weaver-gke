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
	"math"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

// NewSchedule returns a new Schedule.
func NewSchedule(target *TargetFn) *Schedule {
	return &Schedule{
		TargetFn:        target,
		Index:           0,
		AppliedDuration: durationpb.New(0),
	}
}

// Ready returns whether the schedule is ready to be advanced. A schedule is
// ready to be advanced if the current traffic fraction has been applied for
// its specified duration.
//
// Example usage:
//
//	if Ready(schedule) {
//	    if err := Advance(schedule); err != nil {
//	        ...
//	    }
//	}
func Ready(s *Schedule) bool {
	return !Done(s) && Remaining(s) <= 0
}

// Advance advances the schedule to the next traffic fraction. A schedule can
// only be advanced if it's ready. See the Ready method.
func Advance(s *Schedule) error {
	if Done(s) {
		return fmt.Errorf("cannot advance done schedule")
	}
	if r := Remaining(s); r > 0 {
		return fmt.Errorf("cannot advance schedule with %v remaining", r)
	}
	s.Index++
	s.AppliedDuration = durationpb.New(0)
	return nil
}

// Fraction returns the current traffic fraction.
func Fraction(s *Schedule) float32 {
	if Done(s) {
		return 1.0
	}
	return s.TargetFn.Fractions[s.Index].TrafficFraction
}

// Remaining returns the remaining duration the current traffic fraction
// should be applied for.
func Remaining(s *Schedule) time.Duration {
	if Done(s) {
		return time.Duration(math.MaxInt64)
	}
	target := s.TargetFn.Fractions[s.Index].Duration.AsDuration()
	actual := s.AppliedDuration.AsDuration()
	if actual >= target {
		return 0
	}
	return target - actual
}

// IncrementAppliedDuration increases the duration the current traffic fraction
// has been applied.
func IncrementAppliedDuration(s *Schedule, d time.Duration) {
	// Increment by hand to avoid allocation a new Duration.
	nanos := d.Nanoseconds()
	secs := nanos / 1e9
	nanos -= secs * 1e9
	s.AppliedDuration.Seconds += secs
	s.AppliedDuration.Nanos += int32(nanos)
}

// Done returns whether the schedule has been fully applied.
func Done(s *Schedule) bool {
	return s.Index >= int64(len(s.TargetFn.Fractions))
}
