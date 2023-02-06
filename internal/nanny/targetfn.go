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

import "time"

// TargetFraction returns the target fraction at the given duration after a target
// function has started to be applied, assuming no delays.
func TargetFraction(t *TargetFn, d time.Duration) float32 {
	var cumsum time.Duration
	for _, f := range t.Fractions {
		cumsum += f.Duration.AsDuration()
		if d < cumsum {
			return f.TrafficFraction
		}
	}
	return 1.0
}

// Length returns the total length of the TargetFn.
func Length(t *TargetFn) time.Duration {
	var d time.Duration
	for _, f := range t.Fractions {
		d += f.Duration.AsDuration()
	}
	return d
}

// ChangeTimes returns the set of times at which the target function changes,
// assuming the target function is started at the provided time.
func ChangeTimes(t *TargetFn, start time.Time) []time.Time {
	var times []time.Time
	for i, f := range t.Fractions {
		if i == len(t.Fractions)-1 && f.TrafficFraction == 1.0 {
			// After a TargetFn is done, the traffic fraction defaults to 1.0.
			// If the final traffic fraction is also 1.0, then the end of its
			// duration is not a change point.
			continue
		}
		start = start.Add(f.Duration.AsDuration())
		times = append(times, start)
	}
	return times
}
