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

package metricdb_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
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

func hm(id uint64, name string, bounds []float64, counts []uint64, labels ...string) *metrics.MetricSnapshot {
	l := make(map[string]string, len(labels)/2)
	for i := 0; i < len(labels); i += 2 {
		l[labels[i]] = labels[i+1]
	}
	return &metrics.MetricSnapshot{
		Id:     id,
		Type:   protos.MetricType_HISTOGRAM,
		Name:   name,
		Labels: l,
		Help:   name,
		Bounds: bounds,
		Counts: counts,
	}
}

func TestDB(t *testing.T) {
	ctx := context.Background()
	fname := filepath.Join(t.TempDir(), "metricdb.db_test.db")
	db, err := metricdb.OpenFile(ctx, fname)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Record a bunch of observed metrics.
	m0_0 := m(0, "m1", 0.5, "foo", "1", "bar", "1", "baz", "1")
	m0_1 := m(0, "m1", 1.0, "foo", "1", "bar", "1", "baz", "1")
	m0_2 := m(0, "m1", 2.0, "foo", "1", "bar", "1", "baz", "1")
	m1_0 := m(1, "m1", 3.0, "foo", "1", "bar", "1", "baz", "2")
	m2_0 := m(2, "m1", 1.0, "foo", "1", "bar", "2", "baz", "2")
	m3_0 := m(3, "m1", 1.0, "foo", "2", "bar", "2", "baz", "2")
	m4_0 := m(4, "m2", 2.0, "foo", "1")
	m5_0 := hm(5, "m3", []float64{1.0, 2.0}, []uint64{2, 3, 4}, "foo", "1", "bar", "1", "baz", "1")
	m5_1 := hm(5, "m3", []float64{1.0, 2.0}, []uint64{5, 6, 7}, "foo", "1", "bar", "1", "baz", "1")
	record(t, db, 0, m0_0)
	record(t, db, 1, m0_1)
	record(t, db, 2, m0_2)
	record(t, db, 0, m1_0)
	record(t, db, 0, m2_0)
	record(t, db, 0, m3_0)
	record(t, db, 0, m4_0)
	record(t, db, 0, m5_0)
	record(t, db, 1, m5_1)

	for _, tc := range []struct {
		help               string
		name               string
		labels             []string
		startTick, endTick int
		expect             []*metrics.MetricSnapshot
	}{
		{
			help: "no matching no time period",
			expect: []*metrics.MetricSnapshot{
				m0_2, m1_0, m2_0, m3_0, m4_0, m5_1},
		},
		{
			help:      "no matching yes time period",
			startTick: 1,
			endTick:   3,
			expect:    []*metrics.MetricSnapshot{m0_2, m5_1},
		},
		{
			help: "name matching",
			name: "m1",
			expect: []*metrics.MetricSnapshot{
				m0_2, m1_0, m2_0, m3_0,
			},
		},
		{
			help:   "single label matching",
			labels: []string{"foo", "1"},
			expect: []*metrics.MetricSnapshot{
				m0_2, m1_0, m2_0, m4_0, m5_1,
			},
		},
		{
			help:   "multi label matching",
			labels: []string{"foo", "1", "bar", "2"},
			expect: []*metrics.MetricSnapshot{m2_0},
		},
		{
			help:   "name and label matching",
			name:   "m1",
			labels: []string{"foo", "1", "baz", "2"},
			expect: []*metrics.MetricSnapshot{m1_0, m2_0},
		},
		{
			help:      "name and label matching and time period",
			name:      "m1",
			labels:    []string{"foo", "1"},
			startTick: 1,
			endTick:   2,
			expect:    []*metrics.MetricSnapshot{m0_2},
		},
	} {
		t.Run(tc.help, func(t *testing.T) {
			if tc.endTick == 0 {
				tc.endTick = 99999
			}
			startTime := now.Add(time.Duration(tc.startTick) * time.Minute)
			endTime := now.Add(time.Duration(tc.endTick) * time.Minute)
			var actual []*metrics.MetricSnapshot
			append := func(m *metrics.MetricSnapshot) error {
				actual = append(actual, m.Clone())
				return nil
			}
			l := make(map[string]string, len(tc.labels)/2)
			for i := 0; i < len(tc.labels); i += 2 {
				l[tc.labels[i]] = tc.labels[i+1]
			}
			if err := db.Query(ctx, startTime, endTime, tc.name, l, append); err != nil {
				t.Fatal(err)
			}
			opts := []cmp.Option{
				cmpopts.SortSlices(func(x, y metrics.MetricSnapshot) bool {
					return x.Id < y.Id
				}),
			}
			if diff := cmp.Diff(tc.expect, actual, opts...); diff != "" {
				t.Fatalf("unexpected metrics (-want +got):\n%s", diff)
			}
		})
	}
}
