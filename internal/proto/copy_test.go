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

package proto

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

func TestCopy(t *testing.T) {
	type testCase struct {
		desc           string
		dst            proto.Message
		src            proto.Message
		skip           []string
		expect         proto.Message
		expectModified []string
	}
	for _, c := range []testCase{
		{
			desc: "message",
			dst: &protos.Deployment{
				Id: "dep",
			},
			src: &protos.Deployment{
				Id:                "dep",
				ProcessPicksPorts: true,
				NetworkStorageDir: "dir",
			},
			expect: &protos.Deployment{
				Id:                "dep",
				ProcessPicksPorts: true,
				NetworkStorageDir: "dir",
			},
			expectModified: []string{"network_storage_dir", "process_picks_ports"},
		},
		{
			desc: "message unmodified",
			dst: &protos.Deployment{
				Id: "dep",
			},
			src: &protos.Deployment{
				Id: "dep",
			},
			expect: nil, // dst unmodified
		},
		{
			desc: "message skip",
			dst: &protos.Deployment{
				Id: "dep",
			},
			src: &protos.Deployment{
				Id:                "dep",
				NetworkStorageDir: "dir",
			},
			skip: []string{"network_storage_dir"},
			expect: &protos.Deployment{
				Id: "dep",
			},
			expectModified: nil,
		},
		{
			desc: "src nil",
			dst: &protos.Deployment{
				Id: "dep",
			},
			src:    nil,
			expect: nil, // dst unmodified
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			modified, err := Copy(c.dst, c.src, c.skip)
			if err != nil {
				t.Fatal(err)
			}
			if c.expect == nil {
				c.expect = c.dst
			}
			if diff := cmp.Diff(c.expect, c.dst, protocmp.Transform()); diff != "" {
				t.Errorf("copy diff (-want +got):\n%s", diff)
			}

			slices.Sort(modified)
			slices.Sort(c.expectModified)
			if diff := cmp.Diff(c.expectModified, modified); diff != "" {
				t.Errorf("modified diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCopyError(t *testing.T) {
	type testCase struct {
		desc string
		dst  proto.Message
		src  proto.Message
		err  string
	}
	for _, c := range []testCase{

		{
			desc: "dst nil",
			dst:  nil,
			src: &protos.Deployment{
				Id: "dep",
			},
			err: "nil dst",
		},
	} {
		t.Run(c.desc, func(t *testing.T) {
			_, err := Copy(c.dst, c.src, nil)
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("error mismatch, want %v, got %v", c.err, err)
			}
		})
	}
}
