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
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"google.golang.org/protobuf/types/known/durationpb"
)

// basicRolloutProcessor computes a rollout strategy as follows:
//   - the locations are assigned to at most 4 waves;
//   - the first wave contains only one location and is the canary wave;
//   - all other locations are assigned in round-robin order to the remaining
//     waves; next location to assign to a wave is the next entry as specified
//     in the locations' field by the user;
//   - all the waves take the same amount of time to validate that the rollout
//     finished successfully in all the locations within the wave;
//   - for each wave and location in the wave, the target function is the same.
type basicRolloutProcessor struct{}

var _ rolloutProcessor = &basicRolloutProcessor{}

// computeRolloutStrategy implements the rolloutProcessor interface.
func (r *basicRolloutProcessor) computeRolloutStrategy(props rolloutProperties) (
	*RolloutStrategy, error) {
	if err := validateProperties(props); err != nil {
		return nil, err
	}
	numWaves := 4
	if len(props.locations) < 4 {
		numWaves = len(props.locations)
	}

	waveFn := func(d time.Duration, mul float64, div int) time.Duration {
		return time.Duration(float64(d/time.Duration(div)) * mul)
	}
	waveApplyDuration := waveFn(props.durationHint, 1-props.waitTimeFrac, numWaves)
	waveWaitDuration := waveFn(props.durationHint, props.waitTimeFrac, numWaves)

	waves := make([]*RolloutWave, numWaves)
	for i := 0; i < numWaves; i++ {
		waves[i] = &RolloutWave{
			TargetFunctions: map[string]*nanny.TargetFn{},
			WaitTime:        durationpb.New(waveWaitDuration),
		}
	}

	// Wave 0 is the canary wave, and it does the rollout of the target function
	// for the first location.
	waves[0].TargetFunctions[props.locations[0]] = targetFunction(waveApplyDuration)

	if props.actuationDelay > 0 {
		// It takes time for the traffic to take effect after it is applied;
		// we need to account for this delay.
		// One solution is to incur the delay each time a traffic fraction
		// is applied (e.g., after each step of the target function), but that
		// can unnecessarily slow down faster releases.
		// We choose instead to apply this delay only to the end of the last
		// wave. The reasoning for this choice is that the delay is pipelined
		// across target function steps and across waves, so applying it
		// only once makes sense. However, this choice also means that
		// we may move onto the next target function step and wave prematurely,
		// i.e., before the previous step has been fully evaluated.
		// This is something we're willing to live with, for now.
		newWaitTime := waves[len(waves)-1].WaitTime.AsDuration() + props.actuationDelay
		waves[len(waves)-1].WaitTime = durationpb.New(newWaitTime)
	}

	// Compute the target function for all other locations, and assign them in
	// round-robin order to all the other waves.
	for i, loc := range props.locations[1:] {
		waves[i%(numWaves-1)+1].TargetFunctions[loc] = targetFunction(waveApplyDuration)
	}
	return &RolloutStrategy{Waves: waves}, nil
}

// targetFunction computes a target function that rolls out the application
// version fully over the given time duration.
func targetFunction(duration time.Duration) *nanny.TargetFn {
	applyDuration := durationpb.New(duration / 5)
	return &nanny.TargetFn{
		Fractions: []*nanny.FractionSpec{
			{Duration: applyDuration, TrafficFraction: 0.1},
			{Duration: applyDuration, TrafficFraction: 0.25},
			{Duration: applyDuration, TrafficFraction: 0.5},
			{Duration: applyDuration, TrafficFraction: 0.75},
			{Duration: applyDuration, TrafficFraction: 1.0},
		},
	}
}

// validateProperties verifies that the rollout properties are valid.
func validateProperties(props rolloutProperties) error {
	// Verify that the value of wait time fraction is valid.
	if props.waitTimeFrac < 0 || props.waitTimeFrac > 0.8 {
		return fmt.Errorf("rollout error: invalid waitTimeFrac, allowed [0.0, 0.8], got: %f", props.waitTimeFrac)
	}

	// Verify that at least one rollout location was specified.
	if len(props.locations) == 0 {
		return fmt.Errorf("rollout error: no rollout locations specified")
	}

	duplicates := map[string]bool{}
	// Verify that the rollout locations don't contain duplicate entries.
	for _, loc := range props.locations {
		if _, found := duplicates[loc]; found {
			return fmt.Errorf("rollout error: location %s was specified multiple times", loc)
		}
		duplicates[loc] = true
	}
	return nil
}

// length returns the total length of the wave.
func length(w *RolloutWave) time.Duration {
	// The length of a wave is the length of the longest target function, plus
	// the length of the wait time.
	var length time.Duration
	for _, target := range w.TargetFunctions {
		if l := nanny.Length(target); l > length {
			length = l
		}
	}
	return length + w.WaitTime.AsDuration()
}

// fraction returns the target fraction for the given location at the given
// duration after the provided wave has started, assuming no delays.
func fraction(r *RolloutStrategy, location string, i int, start time.Time, d time.Duration) (float32, error) {
	// NOTE(mwhittaker): Fraction would be much simpler if took in the start
	// time of the first wave, rather than the start time of the ith wave.
	// Unfortunately, when a controller rolls out traffic fractions according
	// to a rollout strategy, there are lots of delays introduced. If we used
	// the start time of the first wave, we would ignore all of these delays,
	// and the projected fractions would be significantly skewed from the real
	// fractions. By taking the start time of the ith wave, we can sync the
	// actual fractions with the projected fractions.

	// Double check that the provided location exists in the rollout strategy.
	found := false
	for _, wave := range r.Waves {
		if _, ok := wave.TargetFunctions[location]; ok {
			found = true
			break
		}
	}
	if !found {
		return 0.0, fmt.Errorf("location %q not found", location)
	}

	if i < 0 {
		return 0.0, fmt.Errorf("invalid wave index %d", i)
	}
	if i >= len(r.Waves) {
		// The rollout is complete.
		return 1.0, nil
	}

	at := start.Add(d)
	var cumdur time.Duration
	for j, wave := range r.Waves {
		target, ok := wave.TargetFunctions[location]
		if j < i {
			if ok {
				// The location is in a wave that has already been fully rolled
				// out, so it's traffic fraction is 1.
				return 1.0, nil
			}
			continue
		}

		cumdur += length(wave)
		thisWave := d < cumdur
		if thisWave && ok {
			// The location is in the current wave.
			return nanny.TargetFraction(target, at.Sub(start)), nil
		} else if thisWave && !ok {
			// The location is in a future wave.
			return 0.0, nil
		} else if !thisWave && ok {
			// The traffic fraction is in a previous wave.
			return 1.0, nil
		}
		start = start.Add(length(wave))
	}

	// The rollout is complete.
	return 1.0, nil
}

// changeTimes returns the set of times at which the traffic assignment may
// change, assuming we are in the ith wave, and it started at the provided start
// time.
//
// Note that changeTimes returns an overestimation of the set of times at which
// the traffic assignment can change. Some returned times may not correspond to
// an actual traffic change.
func changeTimes(r *RolloutStrategy, i int, start time.Time) ([]time.Time, error) {
	if i < 0 || i >= len(r.Waves) {
		return nil, fmt.Errorf("invalid wave index %d", i)
	}

	var times []time.Time
	for _, wave := range r.Waves[i:] {
		var length time.Duration
		for _, targetfn := range wave.TargetFunctions {
			times = append(times, nanny.ChangeTimes(targetfn, start)...)
			if l := nanny.Length(targetfn); l > length {
				length = l
			}
		}
		start = start.Add(length).Add(wave.WaitTime.AsDuration())
		times = append(times, start)
	}
	return times, nil
}
