// Copyright 2023 Google LLC
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

package local

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var now = time.Now()

func record(t *testing.T, db *metricdb.T, tick int, m *metrics.MetricSnapshot) {
	if err := db.Record(context.Background(), m, now.Add(time.Duration(tick)*time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func m(id uint64, name string, val float64, labels ...string) *metrics.MetricSnapshot {
	l := make(map[string]string, len(labels)/2)
	for i := 0; i < len(labels); i += 2 {
		l[labels[i]] = labels[i+1]
	}
	return &metrics.MetricSnapshot{
		Id:     id,
		Type:   protos.MetricType_COUNTER,
		Name:   name,
		Labels: l,
		Help:   name,
		Value:  val,
	}
}

func TestGetMetricCounts(t *testing.T) {
	ctx := context.Background()
	fname := filepath.Join(t.TempDir(), "local.metrics_test.db")
	db, err := metricdb.OpenFile(ctx, fname)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Record a bunch of observed metrics.
	m0_0 := m(0, "m1", 0.5, "foo", "1", "bar", "1", "baz", "1")
	m0_1 := m(0, "m1", 1.0, "foo", "1", "bar", "1", "baz", "1")
	m0_2 := m(0, "m1", 2.0, "foo", "1", "bar", "1", "baz", "1")
	m1_0 := m(1, "m1", 2.5, "foo", "1", "bar", "1", "baz", "2")
	m1_1 := m(1, "m1", 3.0, "foo", "1", "bar", "1", "baz", "2")
	m1_2 := m(1, "m1", 4.0, "foo", "1", "bar", "1", "baz", "2")
	m2_0 := m(2, "m1", 0.0, "foo", "1", "bar", "2", "baz", "2")
	m2_1 := m(2, "m1", 1.0, "foo", "1", "bar", "2", "baz", "2")
	m3_0 := m(3, "m1", 0.0, "foo", "2", "bar", "2", "baz", "2")
	m3_1 := m(3, "m1", 1.0, "foo", "2", "bar", "2", "baz", "2")
	m3_2 := m(3, "m1", 2.0, "foo", "2", "bar", "2", "baz", "2")
	// m4_0 intentionally missing
	m4_1 := m(4, "m1", 2.0, "foo", "1")
	record(t, db, 0, m0_0)
	record(t, db, 1, m0_1)
	record(t, db, 2, m0_2)
	record(t, db, 0, m1_0)
	record(t, db, 1, m1_1)
	record(t, db, 2, m1_2)
	record(t, db, 0, m2_0)
	record(t, db, 1, m2_1)
	record(t, db, 0, m3_0)
	record(t, db, 1, m3_1)
	record(t, db, 2, m3_2)
	record(t, db, 1, m4_1)

	for _, tc := range []struct {
		help                            string
		groupBy                         []string
		horizonTick, startTick, endTick int
		expect                          []distributor.MetricCount
	}{
		{
			help:        "infinite time period no grouping",
			horizonTick: -1,
			startTick:   0,
			endTick:     99999, // max
			expect:      []distributor.MetricCount{{Count: 6.0}},
		},
		{
			help:        "infinite time period yes grouping",
			groupBy:     []string{"foo", "bar"},
			horizonTick: -1,
			startTick:   0,
			endTick:     99999, // max
			expect: []distributor.MetricCount{
				{
					LabelVals: []string{"1", "1"},
					Count:     3.0,
				},
				{
					LabelVals: []string{"1", "2"},
					Count:     1.0,
				},
				{
					LabelVals: []string{"2", "2"},
					Count:     2.0,
				},
			},
		},
		{
			help:        "bounded time period no grouping",
			horizonTick: -1,
			startTick:   1,
			endTick:     2,
			expect:      []distributor.MetricCount{{Count: 3.0}},
		},
		{
			help:        "bounded time period yes grouping",
			groupBy:     []string{"foo", "bar"},
			horizonTick: -1,
			startTick:   0,
			endTick:     1,
			expect: []distributor.MetricCount{
				{
					LabelVals: []string{"1", "1"},
					Count:     1.0,
				},
				{
					LabelVals: []string{"1", "2"},
					Count:     1.0,
				},
				{
					LabelVals: []string{"2", "2"},
					Count:     1.0,
				},
			},
		},
		{
			help:        "horizon time no grouping",
			horizonTick: 1,
			startTick:   1,
			endTick:     2,
			expect:      []distributor.MetricCount{{Count: 3.0}},
		},
	} {
		t.Run(tc.help, func(t *testing.T) {
			ctx := context.Background()
			horizonTime := now.Add(time.Duration(tc.horizonTick) * time.Minute)
			startTime := now.Add(time.Duration(tc.startTick) * time.Minute)
			endTime := now.Add(time.Duration(tc.endTick) * time.Minute)
			actual, err := getMetricCounts(ctx, db, horizonTime, startTime, endTime, "m1", tc.groupBy...)
			if err != nil {
				t.Fatal(err)
			}
			opts := []cmp.Option{
				cmpopts.SortSlices(func(x, y distributor.MetricCount) bool {
					if len(x.LabelVals) != len(y.LabelVals) {
						return len(x.LabelVals) < len(y.LabelVals)
					}
					for i := 0; i < len(x.LabelVals); i++ {
						if x.LabelVals[i] == y.LabelVals[i] {
							continue
						}
						return x.LabelVals[i] < y.LabelVals[i]
					}
					return false
				}),
			}
			if diff := cmp.Diff(tc.expect, actual, opts...); diff != "" {
				t.Fatalf("unexpected metric counts (-want +got):\n%s", diff)
			}
		})
	}
}
