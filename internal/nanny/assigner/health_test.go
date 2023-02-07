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
	"testing"

	"github.com/ServiceWeaver/weaver/runtime/protos"
)

func TestHealth(t *testing.T) {
	for _, c := range []struct {
		name    string
		reports []protos.HealthStatus
		want    protos.HealthStatus
	}{
		{
			// Test plan: Don't send reports. Verify that the status is
			// healthy.
			name: "no_health_reports",
			want: protos.HealthStatus_HEALTHY,
		},
		{
			// Test plan: Send only healthy reports. Verify that the status is
			// healthy.
			name: "all_healthy_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY,
			},
			want: protos.HealthStatus_HEALTHY,
		},
		{
			// Test plan: Send only unhealthy reports. Verify that the status
			// is unhealthy.
			name: "all_unhealthy_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY,
			},
			want: protos.HealthStatus_UNHEALTHY,
		},
		{
			// Test plan: Send equal number of healthy/unhealthy reports.
			// Verify that the status is unhealthy.
			name: "equal_healthy_unhealthy_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
			},
			want: protos.HealthStatus_UNHEALTHY,
		},
		{
			// Test plan: Send more healthy than unhealthy reports. Verify that
			// the status is healthy.
			name: "more_healthy_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
			},
			want: protos.HealthStatus_HEALTHY,
		},
		{
			// Test plan: Send more unhealthy than healthy reports. Verify that
			// the status is unhealthy.
			name: "more_unhealthy_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_UNHEALTHY,
			},
			want: protos.HealthStatus_UNHEALTHY,
		},
		{
			// Test plan: Send unhealthy, healthy and terminated reports. Verify that
			// the status is terminated.
			name: "terminated_reports",
			reports: []protos.HealthStatus{
				protos.HealthStatus_UNHEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_UNHEALTHY,
				protos.HealthStatus_TERMINATED, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
				protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY, protos.HealthStatus_HEALTHY,
			},
			want: protos.HealthStatus_TERMINATED,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			tracker := newHealthTracker(protos.HealthStatus_HEALTHY)
			for _, status := range c.reports {
				tracker.report(status)
			}
			if got := tracker.status(); got != c.want {
				t.Fatalf("bad status: got %v, want %v", got, c.want)
			}
		})
	}
}
