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

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSliceClone(t *testing.T) {
	// Test plan: Create a slice and clone it. Verify that the clone is the same
	// as the original slice. Next, modify the original slice and verify that the
	// clone didn't change.
	original := &Slice{
		StartInclusive: &SliceKey{Val: minSliceKey},
		EndExclusive:   &SliceKey{Val: 100},
		LoadInfo: &LoadTracker{
			PerResourceLoad: 100,
			Resources:       map[string]bool{"resource1": true},
			Distribution: map[uint64]float64{
				minSliceKey + 10: 40,
				minSliceKey + 40: 40,
				minSliceKey + 80: 20,
			},
		},
	}

	clone1 := clone(original)
	if diff := cmp.Diff(original, clone1, protocmp.Transform()); diff != "" {
		t.Fatalf("slices should be the same; (-want +got):\n%s", diff)
	}

	// Modify the original slice. Verify that the clone didn't change.
	original.LoadInfo.PerResourceLoad = 200
	if diff := cmp.Diff(original, clone1, protocmp.Transform()); diff == "" {
		t.Fatalf("slices should not be the same; (-want +got):\n%s", diff)
	}

	// Verify that cloning the updated original slice lead to a new clone.
	clone2 := clone(original)
	if diff := cmp.Diff(clone1, clone2, protocmp.Transform()); diff == "" {
		t.Fatalf("slices should not be the same; (-want +got):\n%s", diff)
	}
}

func TestSplitSlice(t *testing.T) {
	type testCase struct {
		name           string
		slice          *Slice
		splitThreshold float64
		expected       []*Slice
	}
	for _, c := range []testCase{
		{
			// Test plan: Slice with a single split point that is the start key. Attempt
			// to split the slice where the split threshold is higher than the load.
			// Verify that the result contains the same slice.
			name: "single_split_point_threshold_higher",
			slice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 100},
				LoadInfo: &LoadTracker{
					PerResourceLoad: 100,
					Distribution: map[uint64]float64{
						minSliceKey: 100,
					},
				},
			},
			splitThreshold: 200,
			expected: []*Slice{
				{
					StartInclusive: &SliceKey{Val: minSliceKey},
					EndExclusive:   &SliceKey{Val: 100},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 100,
						Resources:       map[string]bool{},
					},
				},
			},
		},
		{
			// Test plan: Slice with a single split point that is the start key. Attempt
			// to split the slice where the split threshold is smaller than the load.
			// Verify that the result contains the same slice.
			name: "single_split_point_threshold_lower",
			slice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 100},
				LoadInfo: &LoadTracker{
					PerResourceLoad: 100,
					Distribution: map[uint64]float64{
						minSliceKey: 100,
					},
				},
			},
			splitThreshold: 10,
			expected: []*Slice{
				{
					StartInclusive: &SliceKey{Val: minSliceKey},
					EndExclusive:   &SliceKey{Val: 100},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 100,
						Resources:       map[string]bool{},
					},
				},
			},
		},
		{
			// Test plan: Slice with multiple split points. However, the split threshold
			// is smaller than the load of each split point. Verify that the set of
			// returned subslices contain all the split points.
			name: "multiple_split_points_no_splits",
			slice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 400},
				LoadInfo: &LoadTracker{
					PerResourceLoad: 50,
					Distribution: map[uint64]float64{
						minSliceKey: 25,
						100:         25,
					},
				},
			},
			splitThreshold: 10,
			expected: []*Slice{
				{
					StartInclusive: &SliceKey{Val: minSliceKey},
					EndExclusive:   &SliceKey{Val: 100},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 25,
						Resources:       map[string]bool{},
					},
				},
				{
					StartInclusive: &SliceKey{Val: 100},
					EndExclusive:   &SliceKey{Val: 400},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 25,
						Resources:       map[string]bool{},
					},
				},
			},
		},
		{
			// Test plan: Slice with multiple split points. However, the split threshold
			// is higher than the sum of the load of each split point. Verify that the
			// set of returned subslices contain the original slice.
			name: "multiple_split_points_merge_all_splits",
			slice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 100},
				LoadInfo: &LoadTracker{
					PerResourceLoad: 50,
					Distribution: map[uint64]float64{
						minSliceKey: 25,
						750:         25,
					},
				},
			},
			splitThreshold: 100,
			expected: []*Slice{
				{
					StartInclusive: &SliceKey{Val: minSliceKey},
					EndExclusive:   &SliceKey{Val: 100},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 50,
						Resources:       map[string]bool{},
					},
				},
			},
		},
		{
			// Test plan: Slice with multiple split points. However, the split threshold
			// is higher than the load on some consecutive split points. Verify that the
			// set of returned subslices are as expected.
			//
			// Slice:
			//   Range: [MinSliceKey, 100)
			//   Load: 100
			//   Splits: {MinSliceKey: 20}
			//           {20: 5}
			//           {40: 5}
			//           {60: 10}
			//           {70: 50}
			//           {80: 5}
			//           {85: 5}
			// Split Threshold: 10
			// Expected subslices:
			//   [MinSliceKey, 20), [20, 60), [60, 70), [70, 80), [80, 100)
			name: "multiple_split_points_merge_some_split_points",
			slice: &Slice{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: 100},
				LoadInfo: &LoadTracker{
					PerResourceLoad: 100,
					Distribution: map[uint64]float64{
						minSliceKey: 20,
						20:          5,
						40:          5,
						60:          10,
						70:          50,
						80:          5,
						85:          5,
					},
				},
			},
			splitThreshold: 10,
			expected: []*Slice{
				{
					StartInclusive: &SliceKey{Val: minSliceKey},
					EndExclusive:   &SliceKey{Val: 20},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 20,
						Resources:       map[string]bool{},
					},
				},
				{
					StartInclusive: &SliceKey{Val: 20},
					EndExclusive:   &SliceKey{Val: 60},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 10,
						Resources:       map[string]bool{},
					},
				},
				{
					StartInclusive: &SliceKey{Val: 60},
					EndExclusive:   &SliceKey{Val: 70},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 10,
						Resources:       map[string]bool{},
					},
				},
				{
					StartInclusive: &SliceKey{Val: 70},
					EndExclusive:   &SliceKey{Val: 80},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 50,
						Resources:       map[string]bool{},
					},
				},
				{
					StartInclusive: &SliceKey{Val: 80},
					EndExclusive:   &SliceKey{Val: 100},
					LoadInfo: &LoadTracker{
						PerResourceLoad: 10,
						Resources:       map[string]bool{},
					},
				},
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := splitSliceByLoad(c.slice, c.splitThreshold)
			if diff := cmp.Diff(got, c.expected, protocmp.Transform()); diff != "" {
				t.Fatalf("invalid subslices; (-want +got):\n%s", diff)
			}
		})
	}
}
