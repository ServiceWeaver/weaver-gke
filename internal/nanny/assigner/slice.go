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
)

func Replicas(s *Slice) []string {
	if s.LoadInfo == nil {
		return nil
	}
	var resources []string
	for r := range s.LoadInfo.Resources {
		resources = append(resources, r)
	}
	return resources
}

func HasReplica(s *Slice, resource string) bool {
	if s.LoadInfo == nil {
		return false
	}
	_, found := s.LoadInfo.Resources[resource]
	return found
}

func clone(s *Slice) *Slice {
	clone := &Slice{
		StartInclusive: s.StartInclusive,
		EndExclusive:   s.EndExclusive,
	}
	if s.LoadInfo != nil {
		clone.LoadInfo = &LoadTracker{
			PerResourceLoad: s.LoadInfo.PerResourceLoad,
			Resources:       cloneMap(s.LoadInfo.Resources),
			Distribution:    cloneMap(s.LoadInfo.Distribution),
		}
	}
	return clone
}

func contains(s *Slice, key *SliceKey) bool {
	return lessEqual(s.StartInclusive, key) && less(key, s.EndExclusive)
}

func equalKeyRange(s *Slice, other *Slice) bool {
	return equalSliceKey(s.StartInclusive, other.StartInclusive) &&
		equalSliceKey(s.EndExclusive, other.EndExclusive)
}

func size(s *Slice) uint64 {
	return s.EndExclusive.Val - s.StartInclusive.Val
}

func totalLoadSlice(s *Slice) float64 {
	if s.LoadInfo != nil {
		return s.LoadInfo.PerResourceLoad * float64(len(s.LoadInfo.Resources))
	}
	return 0.0
}

func replicaLoad(s *Slice) float64 {
	if s.LoadInfo != nil {
		return s.LoadInfo.PerResourceLoad
	}
	return 0.0
}

// splitSliceByLoad returns a list of subslices that should cover the entire
// key range in s, based on split points and sliceMaxLoad.
func splitSliceByLoad(s *Slice, sliceMaxLoad float64) []*Slice {
	var splitPoints []*SliceKey
	for point := range s.LoadInfo.Distribution {
		splitPoints = append(splitPoints, &SliceKey{Val: point})
	}
	// Sort split points, s.t., we can merge consecutive split points, if the
	// slice max load constraint is satisfied; also to guarantee that the computed
	// slices cover the entire key range in s.
	sort.Slice(splitPoints, func(i, j int) bool {
		return less(splitPoints[i], splitPoints[j])
	})

	var subSlices []*Slice

	// Start with the first split point, that should also be the slice s start key.
	currStartKey := splitPoints[0]
	currLoad := s.LoadInfo.Distribution[splitPoints[0].Val]

	// Iterate over the remaining split points and attempt to merge split points
	// into the same subslice if the combined load is below sliceMaxLoad.
	for _, splitPoint := range splitPoints[1:] {
		load := s.LoadInfo.Distribution[splitPoint.Val]
		if currLoad+load <= sliceMaxLoad {
			currLoad += load
			continue
		}

		subSlices = append(subSlices, &Slice{
			StartInclusive: currStartKey,
			EndExclusive:   splitPoint,
			LoadInfo: &LoadTracker{
				PerResourceLoad: currLoad,
				Resources:       cloneMap(s.LoadInfo.Resources),
			},
		})
		currStartKey = splitPoint
		currLoad = load
	}

	subSlices = append(subSlices, &Slice{
		StartInclusive: currStartKey,
		EndExclusive:   s.EndExclusive,
		LoadInfo: &LoadTracker{
			PerResourceLoad: currLoad,
			Resources:       cloneMap(s.LoadInfo.Resources),
		},
	})
	validateSubslices(s, subSlices)
	return subSlices
}

func validateSubslices(slice *Slice, subSlices []*Slice) {
	// subSlices should have at least one element.
	if len(subSlices) < 1 {
		panic(fmt.Errorf("slice %v should have at least one subslice, got: %d", slice, len(subSlices)))
	}

	// subSlices should cover the entire key range.
	if !equalSliceKey(subSlices[0].StartInclusive, slice.StartInclusive) {
		panic(fmt.Errorf("subslices don't cover the entire slice key range; missing start key"))
	}
	if !equalSliceKey(subSlices[len(subSlices)-1].EndExclusive, slice.EndExclusive) {
		panic(fmt.Errorf("subslices don't cover the entire slice key range; missing end key"))
	}

	// subSlices should not have overlapping keys and the total load should be
	// approximately equal with the slice load.
	totalLoad := 0.0
	for i := 0; i < len(subSlices)-1; i++ {
		totalLoad += subSlices[i].LoadInfo.PerResourceLoad
		if !equalSliceKey(subSlices[i].EndExclusive, subSlices[i+1].StartInclusive) {
			panic(fmt.Errorf("subslices don't cover the entire slice key range"))
		}
	}
	totalLoad += subSlices[len(subSlices)-1].LoadInfo.PerResourceLoad
	diff := math.Abs(totalLoad - slice.LoadInfo.PerResourceLoad)
	if diff >= 1e-9 {
		panic(fmt.Errorf("subslices load not approx equal with the slice load; diff: %v", diff))
	}
}

func fullSlice() *Slice {
	return &Slice{
		StartInclusive: &SliceKey{Val: minSliceKey},
		EndExclusive:   &SliceKey{Val: maxSliceKey},
		LoadInfo: &LoadTracker{
			Resources:    map[string]bool{},
			Distribution: map[uint64]float64{},
		},
	}
}

func eligibleToReplicate(s *Slice) bool {
	return size(s) == 1
}

func eligibleToDereplicate(s *Slice) bool {
	return size(s) == 1
}

func eligibleToSplit(s *Slice) bool {
	if size(s) == 1 || s.LoadInfo == nil || len(s.LoadInfo.Distribution) == 1 {
		return false
	}
	return true
}

func eligibleToMerge(s *Slice) bool {
	if s.LoadInfo == nil || len(s.LoadInfo.Resources) != 1 {
		return false
	}
	return true
}

func getSplitPoint(s *Slice, fraction float64) (*SliceKey, error) {
	if fraction < 0 || fraction >= 1 {
		return nil, fmt.Errorf("invalid split slice fraction %v", fraction)
	}
	key := s.StartInclusive.Val + uint64(math.Floor(fraction*float64(size(s))))
	return &SliceKey{Val: key}, nil
}

func equalSliceKey(k *SliceKey, x *SliceKey) bool {
	return k.Val == x.Val
}

func less(k *SliceKey, x *SliceKey) bool {
	return k.Val < x.Val
}

func lessEqual(k *SliceKey, x *SliceKey) bool {
	return k.Val <= x.Val
}
