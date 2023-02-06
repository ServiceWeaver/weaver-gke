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
	"strings"
	"testing"
)

type testRegionIDGetterImpl struct{}

var _ regionIDGetter = &testRegionIDGetterImpl{}

func (id *testRegionIDGetterImpl) GetRegionID(_ context.Context, _ CloudConfig, region string) (uint64, error) {
	switch region {
	case "unknown_mid":
		return 2500, nil
	case "unknown_low":
		return 1560, nil
	case "unknown_hi":
		return 3750, nil
	case "error_too_low":
		return 1200, nil
	case "error_too_hi":
		return 1000000, nil
	case "error_id_not_mod10":
		return 1501, nil
	}
	return 0, fmt.Errorf("unknown region %q", region)
}

var (
	testIPRangeFmt        = "192.%d.0.0/8"
	testGetIPRangeOptions = []ipRangeOption{
		withIPRangeFormat(testIPRangeFmt),
		withRegionIDGetter(&testRegionIDGetterImpl{}),
	}
)

func TestGetIPRangeForRegion(t *testing.T) {
	type testCase struct {
		region string
		expect string
	}
	for _, c := range []testCase{
		{
			region: "unknown_mid",
			expect: "192.130.0.0/8",
		},
		{
			region: "unknown_low",
			expect: "192.36.0.0/8",
		},
		{
			region: "unknown_hi",
			expect: "192.255.0.0/8",
		},
	} {
		t.Run(c.region, func(t *testing.T) {
			actual, err := getIPRangeForRegion(context.Background(), CloudConfig{}, c.region, testGetIPRangeOptions...)
			if err != nil {
				t.Errorf("expected ip range %q, got error: %v", c.expect, err)
				return
			}
			if c.expect != actual {
				t.Errorf("unexpected ip range: want %q, got %q", c.expect, actual)
			}

		})
	}
}

func TestGetIPRangeForRegionErrors(t *testing.T) {
	type testCase struct {
		region string
		err    string
	}
	for _, c := range []testCase{
		{
			region: "error_too_low",
			err:    "unexpected id",
		},
		{
			region: "error_too_hi",
			err:    "too large",
		},
		{
			region: "error_id_not_mod10",
			err:    "unexpected id",
		},
	} {
		t.Run(c.region, func(t *testing.T) {
			_, err := getIPRangeForRegion(context.Background(), CloudConfig{}, c.region, testGetIPRangeOptions...)
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("want error with substring %q, got %v", c.err, err)
				return
			}
		})
	}
}
