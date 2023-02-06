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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

func TestAssignmentFromProto(t *testing.T) {
	for _, c := range []struct {
		name  string
		proto *protos.Assignment
		want  *Assignment
	}{
		{
			name: "empty_assignment",
			proto: &protos.Assignment{
				App:          "app",
				DeploymentId: "v1",
				Component:    "component",
				Version:      42,
			},
			want: &Assignment{
				App:          "app",
				DeploymentId: "v1",
				Component:    "component",
				Version:      42,
				Constraints:  &AlgoConstraints{},
				Stats:        &Statistics{},
			},
		},
		{
			name: "simple_assignment",
			proto: &protos.Assignment{
				App:          "app",
				DeploymentId: "v1",
				Component:    "component",
				Version:      42,
				Slices: []*protos.Assignment_Slice{
					{
						Start:    minSliceKey,
						Replicas: []string{"resource1", "resource2"},
					},
					{
						Start:    1000,
						Replicas: []string{"resource3"},
					},
				},
			},
			want: &Assignment{
				App:          "app",
				DeploymentId: "v1",
				Component:    "component",
				Version:      42,
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: 1000},
						LoadInfo: &LoadTracker{
							Resources: map[string]bool{
								"resource1": true,
								"resource2": true,
							},
						},
					},
					{
						StartInclusive: &SliceKey{Val: 1000},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo: &LoadTracker{
							Resources: map[string]bool{"resource3": true},
						},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := FromProto(c.proto)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(c.want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("bad assignment (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestAssignmentFromProtoErrors(t *testing.T) {
	for _, c := range []struct {
		name  string
		proto *protos.Assignment
		err   string
	}{
		{
			// Test plan: Build an assignment with a single slice that doesn't
			// cover the entire key range.
			name: "assignment_does_not_cover_entire_key_range",
			proto: &protos.Assignment{
				Slices: []*protos.Assignment_Slice{
					{
						Start:    1000,
						Replicas: []string{"resource3"},
					},
				},
			},
			err: "first slice starts at 1000, not 0",
		},
		{
			// Test plan: Build an assignment where slices are not in
			// increasing order.
			name: "assignment_with_unordered_slices",
			proto: &protos.Assignment{
				Slices: []*protos.Assignment_Slice{
					{
						Start:    minSliceKey,
						Replicas: []string{"resource1", "resource2"},
					},
					{
						Start:    1000,
						Replicas: []string{"resource3"},
					},
					{
						Start:    50,
						Replicas: []string{"resource1"},
					},
				},
			},
			err: "slices 1 and 2 out of order",
		},
		{
			// Test plan: Build an assignment that doesn't start with
			// MinSliceKey.
			name: "assignment_doesnt_start_with_min_slice_key",
			proto: &protos.Assignment{
				Slices: []*protos.Assignment_Slice{
					{
						Start:    1000,
						Replicas: []string{"resource3"},
					},
					{
						Start:    2000,
						Replicas: []string{"resource1"},
					},
				},
			},
			err: "first slice starts at 1000, not 0",
		},
		{
			// Test plan: Build an assignment that doesn't assign resources to
			// slices.
			name: "assignment_with_unassigned_slices",
			proto: &protos.Assignment{
				Slices: []*protos.Assignment_Slice{
					{
						Start: minSliceKey,
					},
				},
			},
			err: "slice 0 has no resources",
		},
		{
			// Test plan: Build an assignment that contains a key outside the
			// allowed key range.
			name: "assignment_with_key_outside_key_range",
			proto: &protos.Assignment{
				Slices: []*protos.Assignment_Slice{
					{
						Start:    minSliceKey,
						Replicas: []string{"resource1"},
					},
					{
						Start:    maxSliceKey,
						Replicas: []string{"resource2"},
					},
				},
			},
			err: "last slice starts at 18446744073709551615 >= 18446744073709551615",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := FromProto(c.proto)
			if err == nil {
				t.Fatal("unexpected success")
			}
			if got, want := err.Error(), c.err; got != want {
				t.Fatalf("bad error: got %q, want %q", got, want)
			}
		})
	}
}

func TestAssignmentFindSlice(t *testing.T) {
	assignmentP := &protos.Assignment{
		Slices: []*protos.Assignment_Slice{
			{
				Start:    minSliceKey,
				Replicas: []string{"resource1"},
			},
			{
				Start:    100,
				Replicas: []string{"resource2"},
			},
			{
				Start:    1000,
				Replicas: []string{"resource3"},
			},
			{
				Start:    1002,
				Replicas: []string{"resource4"},
			},
		},
	}
	assignment, _ := FromProto(assignmentP)

	type testCase struct {
		name           string
		keyToFindSlice *SliceKey
		expectedSlice  *Slice
	}
	for _, c := range []testCase{
		{
			// Test plan:
			//  Lookup the slice for the MinSliceKey.
			//  Verify that the first slice is returned.
			name:           "find_min_slice",
			keyToFindSlice: &SliceKey{Val: minSliceKey},
			expectedSlice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 100},
				LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
			},
		},
		{
			// Test plan:
			//  Lookup the slice for the key 500.
			//  Verify that the second slice is returned.
			name:           "find_slice_500",
			keyToFindSlice: &SliceKey{Val: 500},
			expectedSlice: &Slice{
				StartInclusive: &SliceKey{Val: 100},
				EndExclusive:   &SliceKey{Val: 1000},
				LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
			},
		},
		{
			// Test plan:
			//  Lookup the slice for the key 1001.
			//  Verify that the third slice is returned.
			name:           "find_slice_1001",
			keyToFindSlice: &SliceKey{Val: 1000},
			expectedSlice: &Slice{
				StartInclusive: &SliceKey{Val: 1000},
				EndExclusive:   &SliceKey{Val: 1002},
				LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource3": true}},
			},
		},
		{
			// Test plan:
			//  Lookup the slice for the MaxSliceKey.
			//  Verify that no slice is returned.
			name:           "find_max_slice",
			keyToFindSlice: &SliceKey{Val: maxSliceKey},
			expectedSlice:  nil,
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotSlice, found := findSlice(assignment, c.keyToFindSlice)
			if found {
				if !slicesEqual(gotSlice, c.expectedSlice) {
					t.Fatalf("wrong slice for key:%v; got: %v; wants: %v", c.keyToFindSlice, gotSlice, c.expectedSlice)
				}
			} else {
				if c.expectedSlice != nil {
					t.Fatalf("wrong slice for key:%v; got: %v; wants: nil", c.keyToFindSlice, gotSlice)
				}
			}
		})
	}
}

func TestAssignmentUpdateLoad(t *testing.T) {
	// Test plan:
	// Given an assignment with 2 slices assigned to two resources, update load
	// information for the slices. Verify that the assignment load is updated as
	// expected.
	assignment := &Assignment{
		Slices: []*Slice{
			{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 1000},
				LoadInfo: &LoadTracker{
					Resources: map[string]bool{"resource1": true},
				},
			},
			{
				StartInclusive: &SliceKey{Val: 1000},
				EndExclusive:   &SliceKey{Val: maxSliceKey},
				LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true, "resource2": true}},
			},
		},
		Constraints: &AlgoConstraints{},
		Stats:       &Statistics{},
	}

	// Add a load report for Slice{StartInclusive: MinSliceKey, EndExclusive: 1000}
	report := &protos.WeaveletLoadReport_ComponentLoad{
		Load: []*protos.WeaveletLoadReport_ComponentLoad_SliceLoad{
			{
				Start: minSliceKey,
				End:   1000,
				Load:  50,
			},
		},
	}
	updateLoad(assignment, "resource1", report)
	expectedLoad := 50.0
	if totalLoad(assignment) != expectedLoad {
		t.Fatalf("wrong assignment load; got: %v; wants: %v", totalLoad(assignment), expectedLoad)
	}

	// Add a load report for Slice{StartInclusive: 1000, EndExclusive: MaxSliceKey}
	// on resource1.
	report = &protos.WeaveletLoadReport_ComponentLoad{
		Load: []*protos.WeaveletLoadReport_ComponentLoad_SliceLoad{
			{
				Start: 1000,
				End:   maxSliceKey,
				Load:  50,
			},
		},
	}
	updateLoad(assignment, "resource1", report)

	// Add a load report for Slice{StartInclusive: 1000, EndExclusive: MaxSliceKey}
	// on resource2.
	report = &protos.WeaveletLoadReport_ComponentLoad{
		Load: []*protos.WeaveletLoadReport_ComponentLoad_SliceLoad{
			{
				Start: 1000,
				End:   maxSliceKey,
				Load:  50,
			},
		},
	}
	updateLoad(assignment, "resource2", report)
	expectedLoad = 50 + 50 + 50
	if totalLoad(assignment) != expectedLoad {
		t.Fatalf("wrong assignment load; got: %v; wants: %v", totalLoad(assignment), expectedLoad)
	}

	slice, _ := findSlice(assignment, &SliceKey{Val: 1000})
	expectedLoad = 50 + 50
	if totalLoadSlice(slice) != expectedLoad {
		t.Fatalf("wrong slice load; got: %v; wants: %v", totalLoadSlice(slice), expectedLoad)
	}
}

func TestAssignmentResourcesByLoad(t *testing.T) {
	// Test plan: Create an assignment with slices assigned to several resources.
	// Verify that the resources are sorted by load as expected.
	assignment := &Assignment{
		Slices: []*Slice{
			{
				LoadInfo: &LoadTracker{
					PerResourceLoad: 1,
					Resources:       map[string]bool{"resource1": true},
				},
			},
			{
				LoadInfo: &LoadTracker{
					PerResourceLoad: 25,
					Resources: map[string]bool{
						"resource2": true,
						"resource3": true,
						"resource4": true,
						"resource5": true,
					},
				},
			},
			{
				LoadInfo: &LoadTracker{
					PerResourceLoad: 25,
					Resources: map[string]bool{
						"resource3": true,
						"resource4": true,
						"resource5": true,
					},
				},
			},
			{
				LoadInfo: &LoadTracker{
					PerResourceLoad: 25,
					Resources: map[string]bool{
						"resource4": true,
						"resource5": true,
					},
				},
			},
			{
				LoadInfo: &LoadTracker{
					PerResourceLoad: 25,
					Resources:       map[string]bool{"resource5": true},
				},
			},
		},
	}

	opts := []cmp.Option{
		cmp.Exporter(anyType),
		cmpopts.IgnoreFields(resourceInfo{}, "isHealthy", "slices"),
	}

	// Expected that load is distributed as follows:
	//  resource1: 1
	//  resource2: 25
	//  resource3: 50
	//  resource4: 75
	//  resource5: 100

	want := []*resourceInfo{
		{resource: "resource1", load: 1},
		{resource: "resource2", load: 25},
		{resource: "resource3", load: 50},
		{resource: "resource4", load: 75},
		{resource: "resource5", load: 100},
	}
	got := resourcesByLoad(assignment)
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("invalid resources by load; (-want +got):\n%s", diff)
	}

	// Make resource1 to have the highest load.
	assignment.Slices[0].LoadInfo.PerResourceLoad = 200

	// Expected that load is distributed as follows:
	//  resource1: 200
	//  resource2: 25
	//  resource3: 50
	//  resource4: 75
	//  resource5: 100
	want = []*resourceInfo{
		{resource: "resource2", load: 25},
		{resource: "resource3", load: 50},
		{resource: "resource4", load: 75},
		{resource: "resource5", load: 100},
		{resource: "resource1", load: 200},
	}
	got = resourcesByLoad(assignment)
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("invalid resources by load; (-want +got):\n%s", diff)
	}

	// Add a new slice assigned to resource6 with the lowest load.
	assignment.Slices = append(assignment.Slices, &Slice{
		LoadInfo: &LoadTracker{
			PerResourceLoad: 1,
			Resources:       map[string]bool{"resource6": true},
		},
	})

	// Expected that load is distributed as follows:
	//  resource1: 200
	//  resource2: 25
	//  resource3: 50
	//  resource4: 75
	//  resource5: 100
	//  resource6: 1
	want = []*resourceInfo{
		{resource: "resource6", load: 1},
		{resource: "resource2", load: 25},
		{resource: "resource3", load: 50},
		{resource: "resource4", load: 75},
		{resource: "resource5", load: 100},
		{resource: "resource1", load: 200},
	}
	got = resourcesByLoad(assignment)
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Fatalf("invalid resources by load; (-want +got):\n%s", diff)
	}
}

// anyType is used by the cmp.Exporter to compare the unexported fields.
func anyType(reflect.Type) bool {
	return true
}

func slicesEqual(s *Slice, other *Slice) bool {
	return equalKeyRange(s, other) && sameResources(Replicas(s), Replicas(other))
}
