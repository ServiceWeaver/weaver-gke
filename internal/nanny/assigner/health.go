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

package assigner

import (
	"sync"
	"time"

	protos "github.com/ServiceWeaver/weaver/runtime/protos"
)

const (
	// healthyThreshold and unhealthyThreshold are the thresholds for a
	// healthTracker to transition to the healthy and unhealthy states
	// respectively. The thresholds are chosen based on the following reasons:
	//
	//   - It takes longer for a resource to be declared healthy. Traffic can
	//     be served anyway by other resources, so better make sure the
	//     resource is likely healthy when assigning keys.
	//
	//   - We should declare a resource unhealthy much faster. If at least half
	//     of the entries are unhealthy, we should just stop assigning keys to
	//     the resource.  It is better to be cautious and not assign keys to a
	//     resource, rather than the keys not being able to be served, because
	//     we are not able to react fast enough.
	healthyThreshold   = 0.66
	unhealthyThreshold = 0.5

	// The maximum number of health reports that a healthTracker records. When
	// choosing the value for historySize, we should consider the following:
	//
	//  - A larger historySize means we have more health reports to make a
	//    decision about the health of a resource, so it will take longer to
	//    react to health changes.
	//
	//  - A smaller historySize means we react faster to health changes but can
	//    be more sensitive to "noise" too. For example, if a resource is
	//    slow/unavailable for few seconds, we may declare it unhealthy.
	//
	//  - When choosing the value for historySize, we should also consider how
	//    much of the recent health information we want to capture. HistorySize
	//    might need to be tuned along with other parameters such as
	//    checkInterval.
	historySize = 10
)

// healthTracker tracks the health of a resource, determining if the resource
// (e.g., a replica) is healthy or unhealthy. A healthTracker is safe for
// concurrent use by multiple goroutines.
//
// # Overview
//
// A healthTracker is a simple state machine that begins in one of three states:
// healthy, unhealthy, or unknown. The healthTracker maintains the most recent
// history of health reports for a resource. If the fraction of healthy reports
// is high, the tracker transitions to the healthy state. Conversely, if the
// fraction of unhealthy reports is high, the tracker transitions to the
// unhealthy state. A healthTracker will never transition to the unknown state.
//
// Note that a resource can transition from any state (unknown, healthy, unhealthy)
// to the terminated state when a terminated report is received. Once a resource
// is in the terminated state, it can  never transition to any other state.
//
//	        over unhealthyThreshold% of                     receive
//	         the history is unhealthy                   terminated status
//	      .-----------------------------.                ---------------|
//	     /               /              v                     v         |
//	+---------+    +---------+    +-----------+         +------------+  |
//	| unknown |    | healthy |    | unhealthy |         | terminated |  |
//	+---------+    +---------+    +-----------+         +------------+  |
//	     \            ^   ^             /                     v         |
//	      '-----------'   '------------'                      ---------/
//	         over healthyThreshold% of
//	          the history is healthy
//
// # Advantages
//
// The high-level goal of this health tracker design is to not react to
// instantaneous changes in health reports, which can lead to unnecessary churn
// in the health status.
//
// Some additional benefits:
//
//   - We don't bias the health of a resource neither based on a single entry
//     nor a consecutive sequence of entries with the same status. Sometimes
//     the resource might not be responsive for several seconds, or it takes
//     time for the underlying stub to be ready.
//
//   - We don't bias towards the latest/oldest entries in the historySize
//     entries window. By doing that, the thinking becomes more complicated,
//     and we may end up with a more complex strategy.
//
// TODO(rgrandl): We may want to add time-based information as well; i.e., give
// more weight to latest health reports.
type healthTracker struct {
	mu                   sync.Mutex            // guards the following fields
	state                protos.HealthStatus   // the current health status
	history              []protos.HealthStatus // the most recent health entries
	lastTimeRecvdHealthy time.Time             // last time a healthy report was received
}

// newHealthTracker returns a health tracker with the provided health status.
func newHealthTracker(status protos.HealthStatus) *healthTracker {
	return &healthTracker{
		state:                status,
		history:              []protos.HealthStatus{},
		lastTimeRecvdHealthy: time.Now(),
	}
}

// status returns the current health status.
func (h *healthTracker) status() protos.HealthStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

// report adds a new health report and returns whether the resource's health
// status has changed as a result.
func (h *healthTracker) report(status protos.HealthStatus) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if status == protos.HealthStatus_UNKNOWN {
		panic("cannot report unknown health status")
	}

	// Once terminated, the resource's health can't change.
	if h.state == protos.HealthStatus_TERMINATED {
		return false
	}

	// Update our state.
	oldState := h.state

	if status == protos.HealthStatus_TERMINATED {
		h.state = status
		h.history = []protos.HealthStatus{}
		return oldState != h.state
	}

	if status == protos.HealthStatus_HEALTHY {
		h.lastTimeRecvdHealthy = time.Now()
	}

	// Record the status.
	if len(h.history) == historySize {
		h.history = h.history[1:]
	}
	h.history = append(h.history, status)

	// Count the number of healthy and unhealthy statuses.
	numHealthy := 0
	numUnhealthy := 0
	for _, status := range h.history {
		if status == protos.HealthStatus_HEALTHY {
			numHealthy++
		} else {
			numUnhealthy++
		}
	}

	if float64(numHealthy) >= healthyThreshold*historySize {
		h.state = protos.HealthStatus_HEALTHY
	} else if float64(numUnhealthy) >= unhealthyThreshold*historySize {
		h.state = protos.HealthStatus_UNHEALTHY
	}
	return oldState != h.state
}
