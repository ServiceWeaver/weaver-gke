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

package bench

import "testing"

func TestSizeString(t *testing.T) {
	for _, test := range []struct {
		size Size
		want string
	}{
		{0, "0B"},
		{1000, "1.00KB"},
		{2500, "2.50KB"},
		{1000 * KB, "1.00MB"},
		{2500 * KB, "2.50MB"},
		{1000 * MB, "1.00GB"},
		{2500 * MB, "2.50GB"},
		{1000 * GB, "1.00TB"},
		{2500 * GB, "2.50TB"},
		{10000 * GB, "10.00TB"},
	} {
		t.Run(test.size.String(), func(t *testing.T) {
			if got := test.size.String(); got != test.want {
				t.Fatalf("bad size: got %q, want %q", got, test.want)
			}
		})
	}
}
