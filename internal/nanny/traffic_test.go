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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
)

func TestCascade(t *testing.T) {
	for _, test := range []struct {
		total   float32
		targets []float32
		want    []float32
	}{
		{1.0, []float32{}, []float32{}},
		{0.0, []float32{1.0}, []float32{0.0}},
		{1.0, []float32{1.0}, []float32{1.0}},
		{2.0, []float32{0.5, 1.5}, []float32{0.5, 1.5}},
		{3.0, []float32{0.5, 0.5, 2.0}, []float32{0.5, 0.5, 2.0}},
		{4.0, []float32{0.5, 0.5, 2.0}, []float32{1.5, 0.5, 2.0}},
	} {
		name := fmt.Sprintf("%f/%v", test.total, test.targets)
		t.Run(name, func(t *testing.T) {
			got, err := cascade(test.total, test.targets)
			if err != nil {
				t.Errorf("cascade(%f, %v): %v", test.total, test.targets, err)
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("cascade(%f, %v): (-want +got):\n%s", test.total, test.targets, diff)
			}
		})
	}
}

func TestCascadeNegativeTotal(t *testing.T) {
	if _, err := cascade(-1.0, []float32{}); err == nil {
		t.Errorf("cascade(0.0, []float32{}): unexpected success")
	}
}

func TestCascadeNegativeTarget(t *testing.T) {
	if _, err := cascade(1.0, []float32{0.5, -1.0, 1.0}); err == nil {
		t.Errorf("cascade(0.0, []float32{}): unexpected success")
	}
}

func TestCascadeTrafficForHost(t *testing.T) {
	// Test plan: Given a set of CascadeTarget(s) and a given host, verify that
	// the generated HostTrafficAssignment is as expected.
	targets := []*CascadeTarget{
		{
			Location:  "host1",
			AppName:   "app",
			VersionId: toUUID(1),
			Listeners: map[string][]*protos.Listener{
				"host1": {
					{Name: "lis1", Addr: "1.1.1.1:1111"},
					{Name: "lis2", Addr: "2.2.2.2:2222"},
					{Name: "lis3", Addr: "3.3.3.3:3333"},
					{Name: "lis4", Addr: "4.4.4.4:4444"},
				},
			},
			TrafficFraction: 1.0,
		},
	}
	host := "host1"
	want := &HostTrafficAssignment{
		Allocs: []*TrafficAllocation{
			{
				Location:        "host1",
				AppName:         "app",
				VersionId:       toUUID(1),
				TrafficFraction: 0.25,
				Listener:        &protos.Listener{Name: "lis1", Addr: "1.1.1.1:1111"},
			},
			{
				Location:        "host1",
				AppName:         "app",
				VersionId:       toUUID(1),
				TrafficFraction: 0.25,
				Listener:        &protos.Listener{Name: "lis2", Addr: "2.2.2.2:2222"},
			},
			{
				Location:        "host1",
				AppName:         "app",
				VersionId:       toUUID(1),
				TrafficFraction: 0.25,
				Listener:        &protos.Listener{Name: "lis3", Addr: "3.3.3.3:3333"},
			},
			{
				Location:        "host1",
				AppName:         "app",
				VersionId:       toUUID(1),
				TrafficFraction: 0.25,
				Listener:        &protos.Listener{Name: "lis4", Addr: "4.4.4.4:4444"},
			},
		},
	}

	got, err := cascadeTrafficForHost(targets, host)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

// toUUID returns a valid version UUID string for a given digit.
func toUUID(d int) string {
	return strings.ReplaceAll(uuid.Nil.String(), "0", fmt.Sprintf("%d", d))
}
