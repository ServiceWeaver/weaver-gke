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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestLoadBasedAlgorithm(t *testing.T) {
	for _, c := range []struct {
		name               string
		currAssignment     *Assignment
		candidateResources []string
		loadUpdate         map[string]*protos.LoadReport_ComponentLoad

		expectedToInvokeAlgo      bool
		expectedAssignedResources []string
		expectedStats             *Statistics
		expectError               bool
		expectBalancedLoad        bool
	}{
		{
			// Test plan: invoke the algo with no candidate resources. Verify that
			// the algo is not invoked.
			name: "no_candidate_resources",
			currAssignment: &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			expectedToInvokeAlgo: false,
			expectedStats:        &Statistics{},
		},
		{
			// Test plan: invoke the algo with an empty assignment. Verify that the
			// algo is invoked; the load is not expected to be balanced because there
			// is no load information.
			name: "assignment_is_empty",
			currAssignment: &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources:        []string{"resource1", "resource2"},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2"},
			expectedStats:             &Statistics{},
			expectBalancedLoad:        false,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to two healthy resources.
			// The load is heavily imbalanced:
			//  resource1: 80 load
			//  resource2: 20 load
			//
			// However, none of the slices assigned to either resource can be split.
			// Verify that the algo is invoked, however it can't balance the load.
			name: "load_above_limit_cannot_balance_load",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: 1024},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 1024},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource2"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   1024,
							Load:  80,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 80},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 1024,
							End:   maxSliceKey,
							Load:  20,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 1024, Load: 20},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2"},
			expectedStats:             &Statistics{},
			expectBalancedLoad:        false,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to two healthy resources.
			// The load is heavily imbalanced:
			//  resource1: 80 load
			//  resource2: 20 load
			//
			// However, the slice assigned to resource1 can be split.
			// Verify that the algo is invoked, and the load is balanced:
			//  resource1: 40 load
			//  resource2: 60 load
			name: "load_above_limit_should_balance_load",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: 1024},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 1024},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource2"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   1024,
							Load:  80,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 20},
								{Start: 250, Load: 40},
								{Start: 500, Load: 5},
								{Start: 750, Load: 5},
								{Start: 1000, Load: 10},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 1024,
							End:   maxSliceKey,
							Load:  20,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 1024, Load: 20},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2"},
			expectedStats: &Statistics{
				SplitOps:            4, // 4 splits for slice [minSliceKey, 1024)
				MoveDueToBalanceOps: 1, // 1 move for subslice [250, 500) to resource2
			},
			expectBalancedLoad: true,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to four resources.
			// The load is imbalanced:
			//  resource1: 10 load
			//  resource2: 30 load
			//  resource3: 40 load
			//  resource4: 20 load
			//
			// Some healthy resources changed, and the load is splittable.
			// Verify that the algo is invoked, and the load is assigned only to
			// healthy resources, and it is balanced:
			//  resource1: 40
			//  resource4: 30
			//  resource5: 30
			name: "load_above_can_be_balanced_resources_changed",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: 500},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 500},
						EndExclusive:   &SliceKey{Val: 1024},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 1024},
						EndExclusive:   &SliceKey{Val: 2056},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 2056},
						EndExclusive:   &SliceKey{Val: 5012},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource3": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 5012},
						EndExclusive:   &SliceKey{Val: 10000},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource3": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 10000},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource4": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource4", "resource5"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   500,
							Load:  10,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 2},
								{Start: 5, Load: 8},
							},
						},
						{
							Start: 500,
							End:   1024,
							Load:  0,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 500, Load: 0},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 1024,
							End:   2056,
							Load:  30,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 1024, Load: 5},
								{Start: 1400, Load: 10},
								{Start: 1600, Load: 10},
								{Start: 1900, Load: 5},
							},
						},
					},
				},
				"resource3": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 2056,
							End:   5012,
							Load:  30,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 2056, Load: 2},
								{Start: 3200, Load: 8},
								{Start: 4200, Load: 15},
								{Start: 4900, Load: 5},
							},
						},
						{
							Start: 5012,
							End:   10000,
							Load:  10,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 5012, Load: 5},
								{Start: 7500, Load: 5},
							},
						},
					},
				},
				"resource4": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 10000,
							End:   maxSliceKey,
							Load:  20,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 10000, Load: 20},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource4", "resource5"},
			expectedStats: &Statistics{
				SplitOps:              8,
				MoveDueToUnhealthyOps: 3, // due to move slices from unhealthy resource2/3
			},
			expectBalancedLoad: true,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to four healthy resources.
			// The load is heavily imbalanced:
			//  resource1: 40 load
			//  resource2: 40 load
			//  resource3: 10 load
			//  resource4: 10 load
			//
			// Resource1 has a hot key that can't be split, so it has to be replicated
			// instead.
			// Verify that the algo is invoked and the load is balanced (both due to
			// splitting and replicating load):
			//  resource1: 30 load
			//  resource2: 30 load
			//  resource3: 20 load
			//  resource4: 20 load
			name: "load_above_limit_replicate_to_balance_load",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: minSliceKey + 1},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: minSliceKey + 1},
						EndExclusive:   &SliceKey{Val: 2056},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 2056},
						EndExclusive:   &SliceKey{Val: 5012},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource3": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 5012},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource4": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource2", "resource3", "resource4"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   minSliceKey + 1,
							Load:  40,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 40},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey + 1,
							End:   2056,
							Load:  40,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey + 1, Load: 10},
								{Start: 1500, Load: 10},
								{Start: 2000, Load: 20},
							},
						},
					},
				},
				"resource3": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 2056,
							End:   5012,
							Load:  10,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 2056, Load: 10},
							},
						},
					},
				},
				"resource4": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 5012,
							End:   maxSliceKey,
							Load:  10,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 5012, Load: 10},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2", "resource3", "resource4"},
			expectedStats: &Statistics{
				SplitOps:            2, // split slice [minSliceKey + 1, 2056)
				ReplicateOps:        3, // replicate slice [minSliceKey, minSliceKey+1) to resources 2, 3, 4
				MoveDueToBalanceOps: 1, // move to balance load slice [2000, 2056)
			},
			expectBalancedLoad: true,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to three healthy resources. Slice [minSliceKey, minSliceKey+1)
			// is replicated across all the resources. However, its replica load is
			// very small.
			//
			// Verify that the algo is invoked, and does two dereplicate operations.
			name: "dereplicate_slices",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: minSliceKey + 1},
						LoadInfo: &LoadTracker{
							Resources: map[string]bool{
								"resource1": true,
								"resource2": true,
								"resource3": true,
							},
						},
					},
					{
						StartInclusive: &SliceKey{Val: minSliceKey + 1},
						EndExclusive:   &SliceKey{Val: 1024},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 1024},
						EndExclusive:   &SliceKey{Val: 2056},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 2056},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource3": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource2", "resource3"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   minSliceKey + 1,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 1},
							},
						},
						{
							Start: minSliceKey + 1,
							End:   1024,
							Load:  50,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey + 1, Load: 50},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   minSliceKey + 1,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 1},
							},
						},
						{
							Start: 1024,
							End:   2056,
							Load:  30,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 1024, Load: 30},
							},
						},
					},
				},
				"resource3": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   minSliceKey + 1,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 1},
							},
						},
						{
							Start: 2056,
							End:   maxSliceKey,
							Load:  20,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 2056, Load: 20},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2", "resource3"},
			expectedStats:             &Statistics{DereplicateOps: 2},
			expectBalancedLoad:        false,
		},
		{
			// Test plan: invoke the algo with a non-empty assignment with slices
			// assigned to two healthy resources. Slices are assigned to the resources
			// such that:
			// resource1:
			//  slice[10, 20), slice[20, 30) and slice[30, 31) can be merged
			//  slice[31, 32) can't be merged because it's replicated
			//  slice[32, 100) and slice[100, 150) can be merged
			//
			// resource2:
			//  slice[1024, 2056) and slice[2056, 5012) can be merged
			//
			// Verify that the algo is invoked, and does three merge operations.
			name: "merge_slices",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						EndExclusive:   &SliceKey{Val: 10},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 10},
						EndExclusive:   &SliceKey{Val: 20},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 20},
						EndExclusive:   &SliceKey{Val: 30},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 30},
						EndExclusive:   &SliceKey{Val: 31},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 31},
						EndExclusive:   &SliceKey{Val: 32},
						LoadInfo: &LoadTracker{Resources: map[string]bool{
							"resource1": true,
							"resource2": true,
						}},
					},
					{
						StartInclusive: &SliceKey{Val: 32},
						EndExclusive:   &SliceKey{Val: 100},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 100},
						EndExclusive:   &SliceKey{Val: 150},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 150},
						EndExclusive:   &SliceKey{Val: 1024},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 1024},
						EndExclusive:   &SliceKey{Val: 2056},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 2056},
						EndExclusive:   &SliceKey{Val: 5012},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 5012},
						EndExclusive:   &SliceKey{Val: maxSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			candidateResources: []string{"resource1", "resource2"},
			loadUpdate: map[string]*protos.LoadReport_ComponentLoad{
				"resource1": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: minSliceKey,
							End:   10,
							Load:  50,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: minSliceKey, Load: 50},
							},
						},
						{
							Start: 10,
							End:   20,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 10, Load: 1},
							},
						},
						{
							Start: 20,
							End:   30,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 20, Load: 1},
							},
						},
						{
							Start: 30,
							End:   31,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 30, Load: 1},
							},
						},
						{
							Start: 31,
							End:   32,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 31, Load: 2},
							},
						},
						{
							Start: 32,
							End:   100,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 31, Load: 1},
							},
						},
						{
							Start: 100,
							End:   150,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 100, Load: 1},
							},
						},
						{
							Start: 150,
							End:   1024,
							Load:  100,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 150, Load: 100},
							},
						},
					},
				},
				"resource2": {
					Version: 0,
					Load: []*protos.LoadReport_SliceLoad{
						{
							Start: 31,
							End:   32,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 31, Load: 1},
							},
						},
						{
							Start: 1024,
							End:   2056,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 1024, Load: 1},
							},
						},
						{
							Start: 2056,
							End:   5012,
							Load:  1,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 2056, Load: 1},
							},
						},
						{
							Start: 5012,
							End:   maxSliceKey,
							Load:  100,
							Splits: []*protos.LoadReport_SubsliceLoad{
								{Start: 5012, Load: 100},
							},
						},
					},
				},
			},
			expectedToInvokeAlgo:      true,
			expectedAssignedResources: []string{"resource1", "resource2"},
			expectedStats: &Statistics{
				MoveDueToBalanceOps: 0,
				MergeOps:            4,
				DereplicateOps:      1,
			},
			expectBalancedLoad: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			updateCandidateResources(c.currAssignment, c.candidateResources)
			for resource, load := range c.loadUpdate {
				updateLoad(c.currAssignment, resource, load)
			}

			if c.expectedToInvokeAlgo && !shouldChange(c.currAssignment) {
				t.Fatal("expected to invoke the algo")
			}

			// Enforce the algo to merge slices if any opportunities for merging.
			c.currAssignment.Constraints.MaxNumSlicesResourceHint = 1
			newAssignment, err := LoadBasedAlgorithm(c.currAssignment)
			if c.expectError {
				if err == nil {
					t.Fatalf("expected error during algo invocation")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error during algo invocation: %v", err)
			}

			// Validate that the assignment is correct.
			if _, err := FromProto(toProto(newAssignment)); err != nil {
				t.Fatalf("malformed assignment: %v", err)
			}

			// Check that the assigned resources are as expected.
			assignedResources := AssignedResources(newAssignment)
			less := func(x, y string) bool { return x < y }
			if diff := cmp.Diff(c.expectedAssignedResources, assignedResources, cmpopts.SortSlices(less)); diff != "" {
				t.Fatalf("invalid assignment resources; (-want +got):\n%s", diff)
			}

			// Check that the max load across all the resources is below the max load
			// limit, if the load should be balanced.
			resources := resourcesByLoad(newAssignment)
			if len(resources) > 0 {
				gotMaxLoad := resources[len(resources)-1].load
				wantMaxLoad := newAssignment.Constraints.MaxLoadLimitResource
				diff := wantMaxLoad - gotMaxLoad
				if c.expectBalancedLoad {
					if diff < 0 {
						t.Fatalf("load expected to be balanced; max load is %f higher than expected max load", -diff)
					}
				}
			}

			// Verify whether the statistics are as expected.
			if diff := cmp.Diff(c.expectedStats, newAssignment.Stats, protocmp.Transform()); diff != "" {
				t.Fatalf("assignment stats should be the same; (-want +got):\n%s", diff)
			}

			// Check that the version was incremented.
			if got, want := newAssignment.Version, uint64(1); got != want {
				t.Fatalf("bad version: got %d, want %d", got, want)
			}
		})
	}
}

func TestEqualDistributionAlgorithm(t *testing.T) {
	for _, c := range []struct {
		name                      string
		currAssignment            *Assignment
		resources                 map[string]protos.HealthStatus
		assignmentShouldChange    bool
		expectedAssignedResources []string
		expectedError             bool
	}{
		{
			// Test plan: invoke the algo with no candidate resources. Verify
			// that the algo is not invoked.
			name: "no_candidate_resources",
			currAssignment: &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
		},
		{
			// Test plan: invoke the algo with no candidate resources. Verify
			// that the algo is not invoked.
			name: "no_candidate_resources",
			currAssignment: &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
		},
		{
			// Test plan:
			// Invoke the algo with no previous assignment, and one healthy resource.
			// Verify that the algo is invoked and the entire key space is assigned to
			// the healthy resource.
			name: "empty_previous_assignment",
			currAssignment: &Assignment{
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			resources: map[string]protos.HealthStatus{
				"resource1": protos.HealthStatus_HEALTHY,
				"resource2": protos.HealthStatus_UNHEALTHY,
			},
			assignmentShouldChange:    true,
			expectedAssignedResources: []string{"resource1"},
			expectedError:             false,
		},
		{
			// Test plan:
			// Invoke the algo with a valid assignment. However, the set of healthy
			// resources don't change.
			// Verify that the algo is not invoked.
			name: "healthy_resources_set_unchanged",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 100},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			resources: map[string]protos.HealthStatus{
				"resource1": protos.HealthStatus_HEALTHY,
				"resource2": protos.HealthStatus_HEALTHY,
			},
			assignmentShouldChange: false,
			expectedError:          false,
		},
		{
			// Test plan:
			// Invoke the algo with a valid assignment. However, the set of healthy
			// resources changed.
			// Verify that the algo is invoked and the keys are assigned to the updated
			// set of healthy resources.
			name: "healthy_resources_set_changed",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 100},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			resources: map[string]protos.HealthStatus{
				"resource1": protos.HealthStatus_HEALTHY,
				"resource2": protos.HealthStatus_UNHEALTHY,
				"resource3": protos.HealthStatus_HEALTHY,
			},
			assignmentShouldChange:    true,
			expectedAssignedResources: []string{"resource1", "resource3"},
			expectedError:             false,
		},
		{
			// Test plan:
			// Invoke the algo with a valid assignment. However, the number of healthy
			// resources decreased.
			// Verify that the algo is invoked and the new assignment assigns all keys
			// to the new set of healthy resources.
			name: "healthy_resources_set_decreased",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 100},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			resources: map[string]protos.HealthStatus{
				"resource1": protos.HealthStatus_HEALTHY,
				"resource2": protos.HealthStatus_UNHEALTHY,
			},
			assignmentShouldChange:    true,
			expectedAssignedResources: []string{"resource1"},
			expectedError:             false,
		},
		{
			// Test plan:
			// Invoke the algo with a valid assignment. However, the number of healthy
			// resources increased.
			// Verify that the algo is invoked and the new assignment assigns all keys
			// to the new set of healthy resources.
			name: "healthy_resources_set_increased",
			currAssignment: &Assignment{
				Slices: []*Slice{
					{
						StartInclusive: &SliceKey{Val: minSliceKey},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource1": true}},
					},
					{
						StartInclusive: &SliceKey{Val: 100},
						LoadInfo:       &LoadTracker{Resources: map[string]bool{"resource2": true}},
					},
				},
				Constraints: &AlgoConstraints{},
				Stats:       &Statistics{},
			},
			resources: map[string]protos.HealthStatus{
				"resource1": protos.HealthStatus_HEALTHY,
				"resource2": protos.HealthStatus_UNHEALTHY,
				"resource3": protos.HealthStatus_HEALTHY,
				"resource4": protos.HealthStatus_HEALTHY,
			},
			assignmentShouldChange:    true,
			expectedAssignedResources: []string{"resource1", "resource3", "resource4"},
			expectedError:             false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			currAssignment := c.currAssignment
			healthyResources := map[string]bool{}
			for r, s := range c.resources {
				if s == protos.HealthStatus_HEALTHY {
					healthyResources[r] = true
				}
			}
			currAssignment.CandidateResources = healthyResources

			if c.assignmentShouldChange != shouldChange(currAssignment) {
				t.Fatalf("algo to be invoked; want %v, got %v\n",
					c.assignmentShouldChange, shouldChange(currAssignment))
			}
			if !c.assignmentShouldChange {
				return
			}

			newAssignment, err := EqualDistributionAlgorithm(currAssignment)
			if c.expectedError && err == nil {
				t.Fatalf("expected error during algo invocation")
			}
			if err != nil {
				t.Fatalf("unexpected error during algo invocation: %v", err)
			}

			// Validate that the assignment is correct.
			if _, err := FromProto(toProto(newAssignment)); err != nil {
				t.Fatalf("malformed assignment: %v", err)
			}

			assignedResources := AssignedResources(newAssignment)
			sort.Strings(assignedResources)
			sort.Strings(c.expectedAssignedResources)
			if diff := cmp.Diff(assignedResources, c.expectedAssignedResources); diff != "" {
				t.Fatalf("invalid assignment resources; (-want +got):\n%s", diff)
			}

			// Check that the version was incremented.
			if got, want := newAssignment.Version, uint64(1); got != want {
				t.Fatalf("bad version: got %d, want %d", got, want)
			}
		})
	}
}

func TestNextPowerOfTwo(t *testing.T) {
	vals := []int{64, 1, 9, 14, 27, 33}
	expected := []int{64, 1, 16, 16, 32, 64}

	for i := range vals {
		got := nextPowerOfTwo(vals[i])
		if got != expected[i] {
			t.Fatalf("wrong value for the next power of two for: %d; expected: %d; got: %d",
				vals[i], expected[i], got)
		}
	}
}

// FuzzLoadBasedAlgorithm generated random Assignments and passes them to
// LoadBasedAlgorithm.
func FuzzLoadBasedAlgorithm(f *testing.F) {
	for i := int64(0); i < 100; i++ {
		f.Add(i)
	}

	// Currently, go is very limited in the type of arguments we can pass to
	// f.Fuzz (mostly primitives). We can supercharge the fuzzing by fuzzing
	// the seed of a regular random number generator. This makes the fuzz tests
	// deterministic, which is needed for them to work correctly, while
	// allowing us to randomly generate more complex structures like an
	// Assignment.
	f.Fuzz(func(t *testing.T, seed int64) {
		rand := rand.New(rand.NewSource(seed))
		assignment := randAssignment(rand)
		if _, err := LoadBasedAlgorithm(assignment); err != nil {
			t.Fatalf("bad assignment:\n%s\n%v", prototext.Format(assignment), err)
		}
	})
}

// FuzzEqualDistributionAlgorithm generated random Assignments and passes them
// to EqualDistributionAlgorithm.
func FuzzEqualDistributionAlgorithm(f *testing.F) {
	for i := int64(0); i < 100; i++ {
		f.Add(i)
	}

	f.Fuzz(func(t *testing.T, seed int64) {
		rand := rand.New(rand.NewSource(seed))
		assignment := randAssignment(rand)
		if _, err := EqualDistributionAlgorithm(assignment); err != nil {
			t.Fatalf("bad assignment:\n%s\n%v", prototext.Format(assignment), err)
		}
	})
}

// randAssignment returns a randomly generated assignment.
func randAssignment(rand *rand.Rand) *Assignment {
	// Create the resources.
	resources := make([]string, 1+rand.Intn(5))
	resourcesSet := map[string]bool{}
	for i := range resources {
		r := fmt.Sprint(i)
		resources[i] = r
		resourcesSet[r] = true
	}

	// Create the slices.
	slices := make([]*Slice, 1+rand.Intn(100))
	delta := 100000
	var start, end uint64 = 0, 1 + uint64(rand.Intn(delta))
	for i := range slices {
		load := rand.Float64() * 100000

		// Create the subslices.
		numSubslices := 1 + rand.Intn(20)
		if numSubslices > int(end-start) {
			numSubslices = 1
		}
		subslices := splitN(rand, start, end, numSubslices)
		subloads := apportion(rand, load, numSubslices)
		dist := map[uint64]float64{}
		for i := 0; i < len(subslices); i++ {
			dist[subslices[i]] = subloads[i]
		}

		slice := &Slice{
			StartInclusive: &SliceKey{Val: start},
			EndExclusive:   &SliceKey{Val: end},
			LoadInfo: &LoadTracker{
				PerResourceLoad: load,
				Resources:       sample(rand, resources),
				Distribution:    dist,
			},
		}
		if i == len(slices)-1 {
			// The last slice has to end at maxSliceKey.
			slice.EndExclusive = &SliceKey{Val: maxSliceKey}
		}
		slices[i] = slice
		start, end = end, end+1+uint64(rand.Intn(delta))
	}

	assignment := &Assignment{
		Version:            rand.Uint64(),
		Slices:             slices,
		CandidateResources: resourcesSet,
		Constraints:        &AlgoConstraints{},
		Stats:              &Statistics{},
	}
	updateThresholds(assignment)
	return assignment
}

// sample returns a random non-empty subset of the provided list. sample panics
// if the provided list is empty.
func sample[T comparable](rand *rand.Rand, xs []T) map[T]bool {
	if len(xs) == 0 {
		panic(fmt.Errorf("sample: empty list"))
	}

	// Pick a random non-zero length, n.
	n := 1 + rand.Intn(len(xs))

	// Take a sample of length n.
	sampled := map[T]bool{}
	for _, i := range rand.Perm(len(xs))[:n] {
		sampled[xs[i]] = true
	}
	return sampled
}

// splitN divides the interval [start, end) into n contiguous, disjoint,
// non-empty intervals. splitN returns the starting points of these
// subintervals.
func splitN(rand *rand.Rand, start, end uint64, n int) []uint64 {
	if end < start {
		panic(fmt.Errorf("invalid range [%d, %d)", start, end))
	}
	if n <= 0 {
		panic(fmt.Errorf("non-positive n: %d", n))
	}
	if int(end-start) < n {
		panic(fmt.Errorf("cannot split [%d, %d) in %d", start, end, n))
	}

	// Pick the n starting points.
	picked := map[uint64]bool{start: true}
	for len(picked) < n {
		picked[start+uint64(rand.Intn(int(end-start)))] = true
	}

	// Sort the n points in ascending order.
	var sorted []uint64
	for x := range picked {
		sorted = append(sorted, x)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted
}

// apportion returns n non-negative numbers that sum to f.
func apportion(rand *rand.Rand, f float64, n int) []float64 {
	if f < 0 {
		panic(fmt.Errorf("negative float: %f", f))
	}
	if n <= 0 {
		panic(fmt.Errorf("non-positive n: %d", n))
	}

	// Pick n-1 points in the range [0, f).
	picked := map[float64]bool{f: true}
	for len(picked) < n {
		picked[rand.Float64()*f] = true
	}

	// Sort the points in ascending order.
	var sorted []float64
	for x := range picked {
		sorted = append(sorted, x)
	}
	sort.Float64s(sorted)

	// Divide f into n regions based on these points.
	prev := 0.0
	values := make([]float64, n)
	for i, point := range sorted {
		values[i] = point - prev
		prev = point
	}
	return values
}
