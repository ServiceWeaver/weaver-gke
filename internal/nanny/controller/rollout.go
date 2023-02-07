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
	"time"
)

// rolloutProcessor computes strategies for rolling out application versions.
type rolloutProcessor interface {
	// computeRolloutStrategy returns a new rolloutStrategy for an application
	// version, given the rollout properties.
	computeRolloutStrategy(props rolloutProperties) (*RolloutStrategy, error)
}

// rolloutProperties describes properties for the rollout of an application version.
type rolloutProperties struct {
	// The expected duration for the rollout, and specified by the user.
	//
	// Note that we make the best effort to rollout the application version in
	// this time period, but we do not guarantee it (e.g., due to rollout errors).
	durationHint time.Duration

	// The fraction of time to wait and validate whether the rollout was successful.
	//
	// It is a fraction of the durationHint and takes values between [0, 1).
	waitTimeFrac float64

	// The expected maximum delay from when the traffic is applied to when it
	// takes effect.
	actuationDelay time.Duration

	// List of rollout locations (e.g., cloud regions), as specified by the user.
	locations []string
}
