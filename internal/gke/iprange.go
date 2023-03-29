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

package gke

import (
	"context"
	"fmt"
	"sync"

	compute "google.golang.org/api/compute/v1"
)

const (
	// Format of the IP ranges in the proxy ip range set.  These ranges
	// must not intersect with the 10.128.0.0/9 CIDR block used by the
	// "auto mode" VPC networks.
	//
	// The block we currently use (i.e., 10.1.0.0/16) is large enough to cover
	// the current set of known cloud regions (i.e., 29) and has ample room for
	// future growth.
	ipRangeFmt = "10.1.%d.0/24"
)

var (
	// All known cloud regions as of 8/22/2022.  These regions are sorted
	// by their unique id, which matches the order in which the regions
	// have been added to Google Cloud.
	//
	// A region at index i in this list is assigned the i-th ip block from
	// the reserved CIDR block above.
	//
	// To allow Service Weaver applications to work with future regions added to
	// Google Cloud, we employ the following strategy.
	//
	// Let L be the last known region in this list.
	// Let "foo" be the new region added to Google Cloud, and let X be that
	// region's unique ID.  We use the following formula to assign
	// an ip block to "foo":
	//    block = L.block + (X - L.id) / 10
	//
	// NOTE(spetrovic): We should monitor the Google Cloud's regional IDs and
	// adjust this strategy if necessary in the future.
	knownRegionsList = []regionInfo{
		{name: "us-central1", id: 1000, block: 0},
		{name: "us-central2", id: 1050, block: 1},
		{name: "europe-west1", id: 1100, block: 2},
		{name: "us-west1", id: 1210, block: 3},
		{name: "asia-east1", id: 1220, block: 4},
		{name: "us-east1", id: 1230, block: 5},
		{name: "asia-northeast1", id: 1250, block: 6},
		{name: "asia-southeast1", id: 1260, block: 7},
		{name: "us-east4", id: 1270, block: 8},
		{name: "australia-southeast1", id: 1280, block: 9},
		{name: "europe-west2", id: 1290, block: 10},
		{name: "europe-west3", id: 1300, block: 11},
		{name: "southamerica-east1", id: 1310, block: 12},
		{name: "asia-south1", id: 1320, block: 13},
		{name: "northamerica-northeast1", id: 1330, block: 14},
		{name: "europe-west4", id: 1340, block: 15},
		{name: "europe-north1", id: 1350, block: 16},
		{name: "us-west2", id: 1360, block: 17},
		{name: "asia-east2", id: 1370, block: 18},
		{name: "europe-west6", id: 1380, block: 19},
		{name: "asia-northeast2", id: 1390, block: 20},
		{name: "asia-northeast3", id: 1410, block: 21},
		{name: "us-west3", id: 1420, block: 22},
		{name: "us-west4", id: 1430, block: 23},
		{name: "asia-southeast2", id: 1440, block: 24},
		{name: "europe-central2", id: 1450, block: 25},
		{name: "northamerica-northeast2", id: 1460, block: 26},
		{name: "asia-south2", id: 1470, block: 27},
		{name: "australia-southeast2", id: 1480, block: 28},
		{name: "southamerica-west1 1490", id: 1490, block: 29},
		{name: "us-east7", id: 1500, block: 30},
		{name: "europe-west8", id: 1510, block: 31},
		{name: "europe-west9", id: 1520, block: 32},
		{name: "us-east5", id: 1530, block: 33},
		{name: "europe-southwest1", id: 1540, block: 34},
		{name: "us-south1", id: 1550, block: 35},
	}

	knownRegions map[string]regionInfo = toRegionMap(knownRegionsList)

	regionsMu      sync.Mutex
	unknownRegions map[string]regionInfo = map[string]regionInfo{}
)

func toRegionMap(regions []regionInfo) map[string]regionInfo {
	m := make(map[string]regionInfo, len(regions))
	for _, r := range regions {
		m[r.name] = r
	}
	return m
}

// regionInfo stores information about a cloud region.
type regionInfo struct {
	name  string // Region name.
	id    uint64 // Region unique ID.
	block int    // Region range block.
}

type ipRangeOptions struct {
	ipRangeFmt     string
	regionIDGetter regionIDGetter
}

// ipRangeOption configures GetIPRangeForRegion.
type ipRangeOption func(*ipRangeOptions)

// withIPRangeFormat is an option for overriding the default IP range format
// (i.e., "10.1.%d.0/24") used for minting IP ranges.
//
// Each cloud region is assigned a unique IP block number in the range [0,255],
// which is then substituted into the provided format.
func withIPRangeFormat(ipRangeFmt string) ipRangeOption {
	return func(opts *ipRangeOptions) {
		opts.ipRangeFmt = ipRangeFmt
	}
}

// withRegionIDGetter is an option for overriding the default RegionIDGetter
// used to retrieve region IDs for regions not in the known list of regions
// (i.e., 29 regions in use as of 10/06/2021).
//
// Each returned ID is expected to satisfy the following constraints:
//  1. It must be in the range [1490, 3750].
//  2. It must be divisible by 10.
func withRegionIDGetter(getter regionIDGetter) ipRangeOption {
	return func(opts *ipRangeOptions) {
		opts.regionIDGetter = getter
	}
}

// regionIDGetter is an abstract interface for returning unique ids for
// cloud regions.
type regionIDGetter interface {
	// GetRegionID returns a unique id associated with region in project.
	GetRegionID(ctx context.Context, config CloudConfig, region string) (uint64, error)
}

type cloudRegionIDGetter struct{}

var _ regionIDGetter = &cloudRegionIDGetter{}

// GetRegionID implements RegionIDGetter interface.
func (id *cloudRegionIDGetter) GetRegionID(ctx context.Context, config CloudConfig, region string) (uint64, error) {
	service, err := compute.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return 0, fmt.Errorf("error creating compute service: %w", err)
	}
	resp, err := service.Regions.Get(config.Project, region).Context(ctx).Do()
	if err != nil {
		return 0, fmt.Errorf("error getting cloud region information: %w", err)
	}
	return resp.Id, nil
}

// getIPRangeForRegion returns the proxy IP range for the given region.
//
// The IP range assignment must satisfy the following requirements:
//   - It must return the same IP range for the same region, even if
//     called concurrently from different processes.
//   - It must return different IP ranges for different regions.
func getIPRangeForRegion(ctx context.Context, config CloudConfig, region string, opts ...ipRangeOption) (string, error) {
	var options ipRangeOptions
	for _, opt := range []ipRangeOption{ // Default options.
		withIPRangeFormat(ipRangeFmt),
		withRegionIDGetter(&cloudRegionIDGetter{}),
	} {
		opt(&options)
	}
	for _, opt := range opts { // User-provided options.
		opt(&options)
	}

	r, err := getRegionInfo(ctx, config, region, options)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(options.ipRangeFmt, r.block), nil
}

func getRegionInfo(ctx context.Context, config CloudConfig, region string, opts ipRangeOptions) (*regionInfo, error) {
	// Check in knownRegions.
	if r, ok := knownRegions[region]; ok {
		return &r, nil
	}

	// Check in unknownRegions.
	regionsMu.Lock()
	r, ok := unknownRegions[region]
	regionsMu.Unlock()
	if ok {
		return &r, nil
	}

	// Get region's unique ID.
	id, err := opts.regionIDGetter.GetRegionID(ctx, config, region)
	if err != nil {
		return nil, err
	}

	// Compute the ip block using the offset of the region ID from the
	// latest known cloud region.  See comments above knownRegions.
	lastKnown := knownRegionsList[len(knownRegionsList)-1]
	if id <= lastKnown.id || (id%10) != 0 {
		return nil, fmt.Errorf("unexpected id %d for region %q", id, region)
	}
	block := lastKnown.block + int((id-lastKnown.id)/10)
	if block > 255 {
		return nil, fmt.Errorf("block value %d derived from region id %d too large", block, id)
	}
	r = regionInfo{
		name:  region,
		id:    id,
		block: block,
	}

	// Cache in unknownRegions.
	regionsMu.Lock()
	unknownRegions[region] = r
	regionsMu.Unlock()

	return &r, nil
}
