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
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/ServiceWeaver/weaver/runtime/protos"
	"google.golang.org/protobuf/proto"
)

const (
	minSliceKey = 0
	maxSliceKey = math.MaxUint64

	imbalanceRatio       = 0.2 // max load imbalance allowed on a resource
	maxNumSlicesResource = 50  // max number of slices on a given resource
)

// resourceInfo contains per resource information. This is needed by the load
// based routing algorithms to track the load and the health status of each resource.
type resourceInfo struct {
	resource  string   // name of the resource
	load      float64  // total load on the resource
	isHealthy bool     // whether the resource is healthy
	slices    []*Slice // set of slices assigned to the resource
}

// findSlice returns the slice corresponding to a given key.
func findSlice(a *Assignment, key *SliceKey) (s *Slice, ok bool) {
	if len(a.Slices) == 0 {
		return nil, false
	}

	// Find the least slice whose end is greater than key (since end is exclusive).
	i := sort.Search(len(a.Slices), func(i int) bool {
		return less(key, a.Slices[i].EndExclusive)
	})
	if i == len(a.Slices) {
		return nil, false
	}
	result := a.Slices[i]
	if !contains(result, key) {
		return
	}
	return result, true
}

// FromProto returns an assignment from an assignment proto.
func FromProto(assignmentP *protos.Assignment) (*Assignment, error) {
	if assignmentP == nil {
		return nil, nil
	}
	if err := validateAssignment(assignmentP); err != nil {
		return nil, err
	}

	var slices []*Slice
	for i, slicep := range assignmentP.Slices {
		startKey := &SliceKey{Val: slicep.Start}
		var endKey *SliceKey
		if i < len(assignmentP.Slices)-1 {
			endKey = &SliceKey{Val: assignmentP.Slices[i+1].Start}
		} else {
			endKey = &SliceKey{Val: maxSliceKey}
		}
		slice := &Slice{
			StartInclusive: startKey,
			EndExclusive:   endKey,
			LoadInfo:       &LoadTracker{Resources: map[string]bool{}},
		}
		for _, resource := range slicep.Replicas {
			slice.LoadInfo.Resources[resource] = true
		}
		slices = append(slices, slice)
	}
	return &Assignment{
		Version:     assignmentP.Version,
		Slices:      slices,
		Constraints: &AlgoConstraints{},
		Stats:       &Statistics{},
	}, nil
}

// toProto returns an assignment proto from an assignment.
func toProto(a *Assignment) *protos.Assignment {
	if a == nil {
		return nil
	}
	var slices []*protos.Assignment_Slice
	for _, slice := range a.Slices {
		slices = append(slices, &protos.Assignment_Slice{
			Start:    slice.StartInclusive.Val,
			Replicas: Replicas(slice),
		})
	}
	return &protos.Assignment{
		Slices:  slices,
		Version: a.Version,
	}
}

// AssignedResources returns all the resources assigned to the entire key space
// in a given assignment.
func AssignedResources(a *Assignment) []string {
	resources := make(map[string]bool)
	for _, slice := range a.Slices {
		if slice.LoadInfo == nil {
			panic(fmt.Errorf("slice %v doesn't have any assigned resources", slice))
		}
		for ep := range slice.LoadInfo.Resources {
			if found := resources[ep]; !found {
				resources[ep] = true
			}
		}
	}
	var result []string
	for e := range resources {
		result = append(result, e)
	}
	return result
}

// updateLoad updates the assignment load information based on a report from
// a given resource.
//
// TODO(rgrandl): handle errors more gracefully.
func updateLoad(a *Assignment, resource string, load *protos.LoadReport_ComponentLoad) error {
	for _, loadReport := range load.Load {
		reportedSlice := &Slice{
			StartInclusive: &SliceKey{Val: loadReport.Start},
			EndExclusive:   &SliceKey{Val: loadReport.End},
		}
		slice, found := findSlice(a, reportedSlice.StartInclusive)
		if !found || !equalKeyRange(slice, reportedSlice) {
			// Slice for which load is reported is not found in the assignment.
			panic(fmt.Errorf("slice %v with load report does not exist in the assignment", reportedSlice))
		}

		if slice.LoadInfo == nil {
			return fmt.Errorf("unable to update load from resource %s for slice: %v", resource, slice)
		}
		if _, found := slice.LoadInfo.Resources[resource]; !found {
			// Load reported from a resource that shouldn't manage the slice as of now.
			return fmt.Errorf("load reported from a wrong resource %v", resource)
		}

		// TODO(rgrandl): should we validate the load report at the assigner too?
		// We already do that at the weavelet.
		slice.LoadInfo.PerResourceLoad = loadReport.Load
		slice.LoadInfo.Distribution = map[uint64]float64{} // clear the old distribution
		for _, split := range loadReport.Splits {
			slice.LoadInfo.Distribution[split.Start] = split.Load
		}
	}
	updateThresholds(a)
	return nil
}

func updateCandidateResources(a *Assignment, resources []string) {
	res := map[string]bool{}
	for _, r := range resources {
		res[r] = true
	}
	a.CandidateResources = res
	updateThresholds(a)
}

func getCandidateResources(a *Assignment) []string {
	var candidates []string
	for r := range a.CandidateResources {
		candidates = append(candidates, r)
	}
	return candidates
}

// resourcesByLoad returns the set of resources in increasing order of load. For
// each resource, the assigned slices are sorted in decreasing order of load.
func resourcesByLoad(a *Assignment) []*resourceInfo {
	result := map[string]*resourceInfo{}
	for _, slice := range a.Slices {
		if slice.LoadInfo == nil {
			panic(fmt.Errorf("slice %v doesn't have load information", slice))
		}
		for r := range slice.LoadInfo.Resources {
			if _, found := result[r]; !found {
				isHealthy := true
				if _, found := a.CandidateResources[r]; !found {
					isHealthy = false // update the health status of the assigned resource
				}
				result[r] = &resourceInfo{resource: r, isHealthy: isHealthy}
			}
			result[r].load += slice.LoadInfo.PerResourceLoad
			result[r].slices = append(result[r].slices, slice)
		}
	}

	// Add the set of candidate resources that are not present in the assignment.
	for r := range a.CandidateResources {
		if _, found := result[r]; !found {
			result[r] = &resourceInfo{resource: r, isHealthy: true}
		}
	}

	// Sort resources in ascending order of load. For each resource, sort slices
	// in descending order of load.
	var resources []*resourceInfo
	for _, r := range result {
		sort.Slice(r.slices, func(i, j int) bool {
			return totalLoadSlice(r.slices[i]) >= totalLoadSlice(r.slices[j])
		})
		resources = append(resources, r)
	}
	sort.Slice(resources, func(i, j int) bool {
		return resources[i].load < resources[j].load
	})
	return resources
}

// isBuiltOnAssignment returns whether assignment a is related to the old assignment.
func isBuiltOnAssignment(a *Assignment, old *Assignment) error {
	// TODO(rgrandl): ensure versions are monotonically increasing instead.
	if a.Version < old.Version {
		return fmt.Errorf("assignment has version %d that is older than %d",
			a.Version, old.Version)
	}
	if !reflect.DeepEqual(a.CandidateResources, old.CandidateResources) {
		return errors.New("assignment has different candidate resources")
	}
	if !proto.Equal(a.Constraints, old.Constraints) {
		return errors.New("assignment has different algo constraints")
	}
	return nil
}

func totalLoad(a *Assignment) float64 {
	var totalLoad float64
	for _, slice := range a.Slices {
		totalLoad += totalLoadSlice(slice)
	}
	return totalLoad
}

func idealLoadPerResource(a *Assignment) float64 {
	if len(a.CandidateResources) == 0 {
		return 0.0
	}
	return totalLoad(a) / float64(len(a.CandidateResources))
}

func updateThresholds(a *Assignment) {
	ideal := idealLoadPerResource(a)
	a.Constraints.MaxLoadLimitResource = (1 + imbalanceRatio) * ideal
	a.Constraints.MinLoadLimitResource = (1 - imbalanceRatio) * ideal
	a.Constraints.SplitThreshold = imbalanceRatio * ideal / 2
	a.Constraints.ReplicateThreshold = a.Constraints.SplitThreshold
	a.Constraints.DereplicateThreshold = 0.05 * ideal
	a.Constraints.MaxNumSlicesResourceHint = maxNumSlicesResource
}

// shouldChange returns whether a new assignment should be generated.
func shouldChange(a *Assignment) bool {
	// Generate a new assignment if the set of resources has changed.
	var candidates []string
	for r := range a.CandidateResources {
		candidates = append(candidates, r)
	}
	if !sameResources(AssignedResources(a), candidates) {
		return true
	}

	// Generate a new assignment if the most loaded resources has the load above
	// the maximum load limit.
	resources := resourcesByLoad(a)
	if len(resources) > 0 &&
		resources[len(resources)-1].load > a.Constraints.MaxLoadLimitResource {
		return true
	}

	// TODO(rgrandl): generate an assignment whether the load on a resource is not
	// within the acceptable min load limits.
	return false
}

func sizeLimit(a *Assignment) int {
	return int(a.Constraints.MaxNumSlicesResourceHint) * len(AssignedResources(a))
}

// validateAssignment validates that an assignment is valid.
func validateAssignment(assignment *protos.Assignment) error {
	if len(assignment.Slices) == 0 {
		// If a component doesn't have any replicas, its assignment doesn't have
		// any slices. This is okay. a component may not have any replicas if the
		// replicas haven't started yet or if all the replicas have failed.
		return nil
	}

	// The first slice should start at minSliceKey.
	if start := assignment.Slices[0].Start; start != minSliceKey {
		return fmt.Errorf("first slice starts at %d, not %d", start, minSliceKey)
	}

	// The last slice should not start at or beyond maxSliceKey.
	n := len(assignment.Slices)
	if start := assignment.Slices[n-1].Start; start >= maxSliceKey {
		return fmt.Errorf("last slice starts at %d >= %d", start, uint64(maxSliceKey))
	}

	// Slices should be in ascending order, and every slice should have at
	// least one resource assigned to it.
	for i := 0; i < n; i++ {
		slice := assignment.Slices[i]
		if len(slice.Replicas) == 0 {
			return fmt.Errorf("slice %d has no resources", i)
		}

		if i < n-1 {
			next := assignment.Slices[i+1]
			if slice.Start >= next.Start {
				return fmt.Errorf("slices %d and %d out of order", i, i+1)
			}
		}
	}
	return nil
}

func sameResources(new []string, old []string) bool {
	if len(new) != len(old) {
		return false
	}
	m := make(map[string]int)
	for _, k := range new {
		m[k]++
	}
	for _, k := range old {
		m[k]--
		if m[k] < 0 {
			return false
		}
	}
	return true
}
