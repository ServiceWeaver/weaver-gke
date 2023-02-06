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
	"math"
	"sort"

	"github.com/ServiceWeaver/weaver/runtime/protomsg"
)

// Algorithm takes the current assignment, and returns a new assignment. The
// returned assignment should have a larger version than the current
// assignment.
type Algorithm func(currAssignment *Assignment) (*Assignment, error)

// LoadBasedAlgorithm is an implementation of a sharding algorithm that
// attempts to minimize the maximum load on any given resource.
func LoadBasedAlgorithm(currAssignment *Assignment) (*Assignment, error) {
	newAssignment := protomsg.Clone(currAssignment)
	newAssignment.Version++

	if len(currAssignment.CandidateResources) == 0 {
		newAssignment.Slices = nil
		updateThresholds(newAssignment)
		return newAssignment, nil
	}

	if len(currAssignment.Slices) == 0 {
		// We don't have any load info, so we assume load is uniformly
		// distributed across the entire key space.
		return EqualDistributionAlgorithm(currAssignment)
	}

	// Step 1: Update the health status of the assigned resources and unassign
	// slices from the unhealthy resources.
	moveSlicesFromUnhealthyResources(newAssignment)

	// Step 2: Dereplicate slices that have more than a replica and the replica
	// load is small. This may create opportunities for the algorithm to merge
	// slices hence control the assignment size.
	mayDereplicateSlices(newAssignment)

	// Step 3: Split slices that are too big. Note that splitting slices that are
	// too big (high load) it creates more opportunities for the algorithm to move
	// slices around in order to balance the load across the resources.
	maySplitSlices(newAssignment)

	// Step 4: Replicate slices that have a single key and their load is too big.
	// This may create further opportunities for the algorithm to move slices
	// around to provide load balancing across the resources.
	mayReplicateSlices(newAssignment)

	// Step 5: Merge slices if their combined load is small. Merging ensures that
	// the assignment size doesn't grow excessively big and the computational cost
	// to generate new assignment is somewhat bounded.
	//
	// Note that dereplicating, splitting and moving slices around may create
	// opportunities for merging.
	//
	// Note that we only merge non-replicated slices. However, this may lead to a
	// scenario where hot keys that become cold will never be able to be merged.
	mayMergeSlices(newAssignment)

	// Step 6: Move slices around to ensure the load is balanced, i.e., no resource
	// has the load above the max load limit. Note that this is not a strict
	// guarantee (e.g., if moving slices around doesn't help to reduce the max load
	// on a resource).
	loadBalance(newAssignment)

	// TODO(rgrandl): ensure that no resource is running cold (i.e., below the
	// minLoadLimitResource).

	// Validate that the new assignment is correct.
	if err := isBuiltOnAssignment(newAssignment, currAssignment); err != nil {
		return nil, err
	}
	diffLoad := math.Abs(totalLoad(newAssignment) - totalLoad(currAssignment))
	if diffLoad >= 1e-3 {
		// Small load differences are acceptable because we deal with floats.
		return nil, fmt.Errorf("assignment load differs by %f from the old assignment", diffLoad)
	}
	return newAssignment, nil
}

// EqualDistributionAlgorithm is an implementation of a sharding algorithm that
// distributes the entire key space approximately equally across all healthy
// resources.
//
// The algorithm is as follows:
// - split the entire key space in a number of slices that is more likely to
// spread uniformly the key space among all healthy resources
//
// - distribute the slices round robin across all healthy resources
func EqualDistributionAlgorithm(currAssignment *Assignment) (*Assignment, error) {
	newAssignment := protomsg.Clone(currAssignment)
	newAssignment.Version++

	candidates := getCandidateResources(currAssignment)
	if len(candidates) == 0 {
		newAssignment.Slices = nil
		updateThresholds(newAssignment)
		return newAssignment, nil
	}

	// If there is only one healthy resource, just assign all the key space to it.
	if len(candidates) == 1 {
		newAssignment.Slices = []*Slice{
			{
				StartInclusive: &SliceKey{Val: minSliceKey},
				EndExclusive:   &SliceKey{Val: maxSliceKey},
				LoadInfo:       &LoadTracker{Resources: map[string]bool{candidates[0]: true}},
			}}
		return newAssignment, nil
	}

	// Partition the entire key range into slices.
	//
	// TODO(rgrandl): For now, we use a simple formula to determine how many
	// slices to generate. In the future, we may want to determine how many slices
	// to generate based on load information or something else.
	numSlices := nextPowerOfTwo(len(candidates))

	// Split slices in equal subslices in order to generate numSlices.
	var slices []*Slice
	slices = append(slices, fullSlice())
	for ok := true; ok; ok = len(slices) != numSlices {
		var currentSlice *Slice
		currentSlice, slices = slices[0], slices[1:]

		midPoint, _ := getSplitPoint(currentSlice, 0.5)
		slicel := clone(currentSlice)
		slicer := clone(currentSlice)
		slicel.EndExclusive = midPoint
		slicer.StartInclusive = midPoint
		slices = append(slices, slicel, slicer)
	}

	// Sort the computed slices in increasing order based on the start key, in
	// order to provide a deterministic assignment across multiple runs, hence to
	// minimize churn.
	sort.Slice(slices, func(i, j int) bool {
		return lessEqual(slices[i].StartInclusive, slices[j].StartInclusive)
	})

	// Note that the healthy resources should be sorted already. This is required
	// because we want to do a deterministic assignment of slices to resources
	// among different invocations, to avoid unnecessary churn while generating
	// new assignments.

	// Assign the computed slices to resources in a round robin fashion.
	rId := 0
	for _, s := range slices {
		s.LoadInfo.Resources[candidates[rId]] = true
		rId = (rId + 1) % len(candidates)
	}
	newAssignment.Slices = slices

	// Validate that the new assignment is correct.
	if err := isBuiltOnAssignment(newAssignment, currAssignment); err != nil {
		return nil, err
	}
	return newAssignment, nil
}

func moveSlicesFromUnhealthyResources(assignment *Assignment) {
	numHealthyResources := len(assignment.CandidateResources)
	if numHealthyResources == 0 {
		panic("no candidate resources to assign slices")
	}

	// Move slices from unhealthy resources to healthy resources.
	for _, slice := range assignment.Slices {
		if slice.LoadInfo == nil {
			continue
		}

		// TODO(rgrandl): if the number of replicas of a slice is higher than the
		// number of candidate resources, we should panic for now. When we support
		// replication, we should decrease the number of replicas instead.
		if len(slice.LoadInfo.Resources) > numHealthyResources {
			panic(fmt.Errorf("slice %s has %d replicas while there are only %d healthy resources",
				slice, len(slice.LoadInfo.Resources), numHealthyResources))
		}

		for fromResource := range slice.LoadInfo.Resources {
			if _, isHealthy := assignment.CandidateResources[fromResource]; !isHealthy {
				// Assign the slice to the healthy resource with the smallest load.
				toAssignResources := resourcesByLoad(assignment)
				for _, toResource := range toAssignResources {
					if toResource.isHealthy {
						delete(slice.LoadInfo.Resources, fromResource)
						slice.LoadInfo.Resources[toResource.resource] = true // isHealthy
						assignment.Stats.MoveDueToUnhealthyOps++
						break
					}
				}
			}
		}
	}
}

// maySplitSlices attempts to split the slices in the assignment, s.t. no
// slice has load above the assignment.constraints.splitThreshold.
func maySplitSlices(assignment *Assignment) {
	var newSlices []*Slice
	for _, slice := range assignment.Slices {
		if !eligibleToSplit(slice) {
			newSlices = append(newSlices, clone(slice))
			continue
		}
		splits := splitSliceByLoad(slice, assignment.Constraints.SplitThreshold)
		newSlices = append(newSlices, splits...)
		assignment.Stats.SplitOps += int64(len(splits) - 1)
	}
	assignment.Slices = newSlices
}

func mayMergeSlices(assignment *Assignment) {
	for {
		if len(assignment.Slices) <= sizeLimit(assignment) {
			// Assignment size is within limits.
			break
		}

		hasMerged := false // whether a merge operation happened
		resources := resourcesByLoad(assignment)

		// Start from the coldest resource, and attempt to merge consecutive slices
		// if their total load is below the split threshold (i.e., these slices would
		// not have been split before).
		//
		// Merging slices on the coldest resources first is most likely to result
		// in merging opportunities because the slices on these resources should be cold.
		//
		// TODO(rgrandl): This can result in excessive merging on the coldest resources.
		// Maybe a better approach is to ensure each resource has a lower bound on
		// the number of slices as well.
		//
		// TODO(rgrandl): merging can be slow because we merge one set of slices at
		// a time; think about doing multiple merges in the same iteration.
		//
		// TODO(rgrandl): merge replicated slices.
		for idxFrom := 0; idxFrom < len(resources); idxFrom++ {
			if hasMerged = tryMerge(resources[idxFrom], assignment); hasMerged {
				break
			}
		}
		if !hasMerged {
			// No merging operation is possible anymore.
			break
		}
	}
}

func tryMerge(resource *resourceInfo, assignment *Assignment) bool {
	// Make sure slices are sorted in increasing order based on the keyspace.
	sort.Slice(resource.slices, func(i, j int) bool {
		return less(resource.slices[i].StartInclusive, resource.slices[j].StartInclusive)
	})

	if len(resource.slices) <= 1 { // nothing to merge
		return false
	}

	for i := 0; i < len(resource.slices)-1; i++ {
		currSlice := resource.slices[i]
		if !eligibleToMerge(currSlice) {
			continue
		}

		slicesToMerge := []*Slice{currSlice}
		currLoad := resource.slices[i].LoadInfo.PerResourceLoad
		numMergeOps := 0

		for j := i + 1; j < len(resource.slices); j++ {
			nextSlice := resource.slices[j]
			if !eligibleToMerge(nextSlice) {
				// Encountered a slice that can't be merged; we can't merge further.
				break
			}
			if !equalSliceKey(nextSlice.StartInclusive, slicesToMerge[len(slicesToMerge)-1].EndExclusive) {
				// nextSlice is not consecutive in the keyspace with the previous slice to merge.
				break
			}
			if len(assignment.Slices)-numMergeOps <= sizeLimit(assignment) {
				// Assignment size is within limits.
				break
			}

			nextLoad := nextSlice.LoadInfo.PerResourceLoad
			if currLoad+nextLoad > assignment.Constraints.SplitThreshold {
				// Merging nextSlice results in a slice whose load is too big.
				break
			}

			numMergeOps += 1
			currLoad += nextLoad
			slicesToMerge = append(slicesToMerge, nextSlice)
		}

		if numMergeOps == 0 {
			// Couldn't merge slices starting with the slice at index i.
			continue
		}

		// Note that the merged slice will have the same resources as the current slice.
		// This is because all the slices that are merged are assigned to the same resources.
		mergedSlice := &Slice{
			StartInclusive: currSlice.StartInclusive,
			EndExclusive:   slicesToMerge[len(slicesToMerge)-1].EndExclusive,
			LoadInfo: &LoadTracker{
				PerResourceLoad: currLoad,
				Resources:       cloneMap(currSlice.LoadInfo.Resources),
			},
		}

		// Update the set of slices in the assignment: remove all the merged slices
		// and add the newly mergedSlice.
		//
		// TODO(rgrandl): implement a more efficient mechanism to remove old slices.
		for curr := 0; curr < len(assignment.Slices); curr++ {
			if equalSliceKey(assignment.Slices[curr].StartInclusive, slicesToMerge[0].StartInclusive) {
				var newSlices []*Slice
				newSlices = append(newSlices, assignment.Slices[:curr]...)
				newSlices = append(newSlices, mergedSlice)
				newSlices = append(newSlices, assignment.Slices[curr+len(slicesToMerge):]...)
				assignment.Slices = newSlices
				assignment.Stats.MergeOps += int64(numMergeOps)
				return true
			}
		}
		panic(fmt.Errorf("unexpected merge failure for slices %v", slicesToMerge))
	}
	return false
}

func mayReplicateSlices(assignment *Assignment) {
	for _, slice := range assignment.Slices {
		if !eligibleToReplicate(slice) {
			continue
		}

		for {
			enoughReplicas := replicaLoad(slice) <= assignment.Constraints.ReplicateThreshold
			maxReplicated := len(slice.LoadInfo.Resources) == len(assignment.CandidateResources)
			if enoughReplicas || maxReplicated {
				break
			}

			// Create a new replica and assign it to the healthy resource with the lowest
			// load that doesn't have already a replica for the slice.
			resources := resourcesByLoad(assignment)
			for _, toResource := range resources {
				if _, found := slice.LoadInfo.Resources[toResource.resource]; found {
					continue
				}
				currLoad := slice.LoadInfo.PerResourceLoad
				numReplicas := float64(len(slice.LoadInfo.Resources))
				slice.LoadInfo.Resources[toResource.resource] = true
				slice.LoadInfo.PerResourceLoad = currLoad * numReplicas / (1 + numReplicas)
				assignment.Stats.ReplicateOps++
				break
			}
		}
	}
}

func mayDereplicateSlices(assignment *Assignment) {
	for _, slice := range assignment.Slices {
		if !eligibleToDereplicate(slice) {
			continue
		}

		for {
			enoughReplicas := replicaLoad(slice) >= assignment.Constraints.DereplicateThreshold
			minReplicated := len(slice.LoadInfo.Resources) == 1
			if enoughReplicas || minReplicated {
				break
			}

			// Remove the slice replica from the hottest resource that has the slice assigned to.
			resources := resourcesByLoad(assignment)
			for idxFrom := len(resources) - 1; idxFrom >= 0; idxFrom-- {
				fromResource := resources[idxFrom]
				if _, found := slice.LoadInfo.Resources[fromResource.resource]; !found {
					continue
				}
				currLoad := slice.LoadInfo.PerResourceLoad
				numReplicas := float64(len(slice.LoadInfo.Resources))
				delete(slice.LoadInfo.Resources, fromResource.resource)
				slice.LoadInfo.PerResourceLoad = currLoad * numReplicas / (numReplicas - 1)
				assignment.Stats.DereplicateOps++
				break
			}
		}
	}
}

func loadBalance(assignment *Assignment) {
	for {
		// Attempt to move load from the hottest resource to the coldest one, s.t.
		// the destination resource doesn't become hotter than the source. Note that,
		// after each operation, we recompute the set of hot/cold resources in order
		// to reflect the load due to slices being moved.
		resources := resourcesByLoad(assignment)

		// Short circuit if we can't really move anything.
		if len(resources) <= 1 {
			break
		}

		hasMoved := false
		for idxFrom := len(resources) - 1; idxFrom >= 0; idxFrom-- {
			fromResource := resources[idxFrom]

			// Short circuit if the source resource has load within the load limits.
			// I.e., all resources are already within the load limits so the load is
			// balanced.
			//
			// TODO(rgrandl): we should still continue if the load on the toResource
			// is lower than the minLoadLimitResource. This is in the case when we
			// want to make sure no resource runs cold. However, it's unclear if we
			// really want to do that, because it incur load churn.
			if fromResource.load <= assignment.Constraints.MaxLoadLimitResource {
				break
			}

			// Iterate over slices in decreasing order based on load.
			for _, slice := range fromResource.slices {
				if totalLoadSlice(slice) == 0 { // we shouldn't move slices with no load
					break
				}

				// Find a resource with the lowest load that doesn't have the slice
				// already assigned.
				var toResource *resourceInfo
				for idxTo := 0; idxTo < len(resources); idxTo++ {
					if idxTo == idxFrom { // is this even possible?
						break
					}
					if _, found := slice.LoadInfo.Resources[resources[idxTo].resource]; !found {
						toResource = resources[idxTo]
						break
					}
				}

				if toResource == nil { // couldn't move slice to any resource
					continue
				}

				newToResourceLoad := replicaLoad(slice) + toResource.load
				if math.Round(newToResourceLoad) < math.Round(fromResource.load) {
					// Round to nearest integer to avoid unnecessary operations. If the
					// load on the toResource is only 0.0001 smaller than on the fromResource,
					// there is no point to move.
					delete(slice.LoadInfo.Resources, fromResource.resource)
					slice.LoadInfo.Resources[toResource.resource] = true // isHealthy
					hasMoved = true
					assignment.Stats.MoveDueToBalanceOps++
					break
				}
			}
			if hasMoved {
				break
			}
		}

		// If no move operation while iterating over all the resources and the assigned
		// slices, it means that this is the best we could do, so we should stop trying
		// to move slices around.
		if !hasMoved {
			break
		}
	}
}

// nextPowerOfTwo returns the next power of 2 that is greater or equal with x.
func nextPowerOfTwo(x int) int {
	// If x is already power of 2, return x.
	if x&(x-1) == 0 {
		return x
	}
	return int(math.Pow(2, math.Ceil(math.Log2(float64(x)))))
}
