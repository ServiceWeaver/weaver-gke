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

package proto_test

// TODO(rgrandl): modify this test to use runtime protos instead.

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	dproto "github.com/ServiceWeaver/weaver-gke/internal/proto"
	proto_test "github.com/ServiceWeaver/weaver-gke/internal/proto/test"
)

func TestMerge(t *testing.T) {
	type testCase struct {
		desc   string
		dst    proto.Message
		src    proto.Message
		expect proto.Message
	}
	cmpFields := map[string]string{
		"Memory":     "manufacturer",
		"MemoryChip": "manufacturer",
	}
	for _, c := range []testCase{
		{
			desc: "message",
			dst: &proto_test.CPUCore{
				MinMhz: 1000,
				MaxMhz: 2000,
			},
			src: &proto_test.CPUCore{
				MinMhz:         1100,
				CacheSizeBytes: 26214400,
			},
			expect: &proto_test.CPUCore{
				MinMhz:         1100,
				MaxMhz:         2000,
				CacheSizeBytes: 26214400,
			},
		},
		{
			desc: "message unmodified",
			dst: &proto_test.CPUCore{
				MinMhz: 1000,
				MaxMhz: 2000,
			},
			src: &proto_test.CPUCore{
				MinMhz:         1000,
				CacheSizeBytes: 0,
			},
			expect: nil, // dst unmodified
		},
		{
			desc: "message list",
			dst: &proto_test.Memory{
				Manufacturer: "corsair",
				Chips: []*proto_test.MemoryChip{
					{
						Manufacturer:  "samsung",
						CapacityBytes: 8589934592,
					},
					{
						Manufacturer:  "hynix",
						CapacityBytes: 8589934592,
					},
				},
			},
			src: &proto_test.Memory{
				Manufacturer: "g.skill",
				Chips: []*proto_test.MemoryChip{
					{
						Manufacturer:  "samsung",
						CapacityBytes: 4294967296,
					},
					{
						Manufacturer:  "micron",
						CapacityBytes: 8589934592,
					},
				},
			},
			expect: &proto_test.Memory{
				Manufacturer: "g.skill",
				Chips: []*proto_test.MemoryChip{
					{
						Manufacturer:  "samsung",
						CapacityBytes: 4294967296,
					},
					{
						Manufacturer:  "hynix",
						CapacityBytes: 8589934592,
					},
					{
						Manufacturer:  "micron",
						CapacityBytes: 8589934592,
					},
				},
			},
		},
		{
			desc: "message list unmodified",
			dst: &proto_test.Memory{
				Manufacturer: "corsair",
				Chips: []*proto_test.MemoryChip{
					{
						Manufacturer:  "samsung",
						CapacityBytes: 8589934592,
					},
					{
						Manufacturer:  "hynix",
						CapacityBytes: 8589934592,
					},
				},
			},
			src: &proto_test.Memory{
				Manufacturer: "corsair",
				Chips: []*proto_test.MemoryChip{
					{
						Manufacturer:  "samsung",
						CapacityBytes: 8589934592,
					},
				},
			},
			expect: nil, // dst unmodified
		},
		{
			desc: "message map",
			dst: &proto_test.CPU{
				Model: "intel 12900k",
				Cores: map[string]*proto_test.CPUCore{
					"core1": {
						MinMhz: 3700,
						MaxMhz: 5000,
					},
					"core2": {
						MinMhz: 3700,
						MaxMhz: 5000,
					},
				},
			},
			src: &proto_test.CPU{
				Model: "intel 11900k",
				Cores: map[string]*proto_test.CPUCore{
					"core2": {
						MinMhz: 4000,
						MaxMhz: 4800,
					},
					"core3": {
						MinMhz: 3900,
						MaxMhz: 4900,
					},
				},
			},
			expect: &proto_test.CPU{
				Model: "intel 11900k",
				Cores: map[string]*proto_test.CPUCore{
					"core1": {
						MinMhz: 3700,
						MaxMhz: 5000,
					},
					"core2": {
						MinMhz: 4000,
						MaxMhz: 4800,
					},
					"core3": {
						MinMhz: 3900,
						MaxMhz: 4900,
					},
				},
			},
		},
		{
			desc: "complex",
			dst: &proto_test.Computer{
				Motherboard: &proto_test.Motherboard{
					Chipset: "amd-x570",
					MemorySlots: []*proto_test.Memory{
						{
							Manufacturer: "corsair",
							Chips: []*proto_test.MemoryChip{
								{
									Manufacturer:  "samsung",
									CapacityBytes: 4294967296,
								},
							},
						},
						{
							Manufacturer: "g.skill",
							Chips: []*proto_test.MemoryChip{
								{
									Manufacturer:  "hynix",
									CapacityBytes: 8589934592,
								},
							},
						},
					},
					Cpu: &proto_test.CPU{
						Model: "amd 5950x",
						Cores: map[string]*proto_test.CPUCore{
							"core1": {
								MinMhz: 3400,
								MaxMhz: 4900,
							},
							"core2": {
								MinMhz: 3400,
								MaxMhz: 4900,
							},
						},
					},
				},
			},
			src: &proto_test.Computer{
				Motherboard: &proto_test.Motherboard{
					MemorySlots: []*proto_test.Memory{
						{
							Manufacturer: "g.skill",
							Chips: []*proto_test.MemoryChip{
								{
									Manufacturer:  "micron",
									CapacityBytes: 8589934592,
								},
							},
						},
					},
					Cpu: &proto_test.CPU{
						Cores: map[string]*proto_test.CPUCore{
							"core1": {
								MinMhz: 3600,
								MaxMhz: 4800,
							},
						},
					},
				},
			},
			expect: &proto_test.Computer{
				Motherboard: &proto_test.Motherboard{
					Chipset: "amd-x570",
					MemorySlots: []*proto_test.Memory{
						{
							Manufacturer: "corsair",
							Chips: []*proto_test.MemoryChip{
								{
									Manufacturer:  "samsung",
									CapacityBytes: 4294967296,
								},
							},
						},
						{
							Manufacturer: "g.skill",
							Chips: []*proto_test.MemoryChip{
								{
									Manufacturer:  "hynix",
									CapacityBytes: 8589934592,
								},
								{
									Manufacturer:  "micron",
									CapacityBytes: 8589934592,
								},
							},
						},
					},
					Cpu: &proto_test.CPU{
						Model: "amd 5950x",
						Cores: map[string]*proto_test.CPUCore{
							"core1": {
								MinMhz: 3600,
								MaxMhz: 4800,
							},
							"core2": {
								MinMhz: 3400,
								MaxMhz: 4900,
							},
						},
					},
				},
			},
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			modified, err := dproto.Merge(c.dst, c.src, cmpFields)
			if err != nil {
				t.Fatal(err)
			}
			expectModified := c.expect != nil
			if !expectModified {
				c.expect = c.dst
			}
			if diff := cmp.Diff(c.expect, c.dst, protocmp.Transform()); diff != "" {
				t.Errorf("merge diff (-want +got):\n%s", diff)
			}
			if expectModified != modified {
				t.Errorf("modified diff, want %v, got %v", expectModified, modified)
			}
		})
	}
}
