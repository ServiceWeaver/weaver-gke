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

package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
)

// getMetricCounts returns the list of aggregated counts for a Service
// Weaver metric, grouping the counts using the provided list of metric labels.
// It attempts to account for metric counts accumulated only during the given
// [startTime, endTime] time interval.
//
// In order to do so, it looks at unique metrics ids and for each computes:
//  1. Its latest value A observed during the [horizonTime, startTime]
//     interval.
//  2. Its latest value B observed during the [startTime, endTime] interval.
//
// It then computes the delta (B - A) and groups/aggregates these deltas using
// the provided metric labels.
//
// REQUIRES: The metric is of type COUNTER.
// REQUIRES: horizonTime < startTime < endTime
func getMetricCounts(ctx context.Context, db *metricdb.T, horizonTime, startTime, endTime time.Time, metric string, labelsGroupBy ...string) ([]distributor.MetricCount, error) {
	recordFn := func(storage map[uint64]*metrics.MetricSnapshot) func(*metrics.MetricSnapshot) error {
		return func(m *metrics.MetricSnapshot) error {
			if m.Type != protos.MetricType_COUNTER {
				return fmt.Errorf("metric %s not a counter", m.Name)
			}
			storage[m.Id] = m.Clone()
			return nil
		}
	}

	// Record observations in the time period [horizonTime, startTime] and
	// [startTime, endTime].
	lowerObservations := map[uint64]*metrics.MetricSnapshot{}
	upperObservations := map[uint64]*metrics.MetricSnapshot{}
	if err := db.Query(ctx, horizonTime, startTime, metric, nil /*labels*/, recordFn(lowerObservations)); err != nil {
		return nil, err
	}
	if err := db.Query(ctx, startTime, endTime, metric, nil /*labels*/, recordFn(upperObservations)); err != nil {
		return nil, err
	}

	// Compute deltas between upper and lower observations. This step aims to
	// capture the metric accumulation during the (startTime, endTime) time
	// period. Group and aggregate the metric deltas using their label values.
	counts := map[string]*distributor.MetricCount{}
outer:
	for id, upper := range upperObservations {
		lower, ok := lowerObservations[id]
		if !ok {
			// No lower observation: ignore the metric.
			continue
		}
		delta := upper.Value - lower.Value
		if delta < 0 {
			// This should never happen: drop the metric.
			continue
		}
		var labelVals []string
		for _, label := range labelsGroupBy {
			val, ok := upper.Labels[label]
			if !ok {
				// Metric doesn't have the required label: ignore it.
				continue outer
			}
			labelVals = append(labelVals, val)
		}
		key := hash(labelVals)
		count := counts[key]
		if count == nil {
			count = &distributor.MetricCount{
				LabelVals: labelVals,
			}
			counts[key] = count
		}
		count.Count += delta
	}

	ret := make([]distributor.MetricCount, 0, len(counts))
	for _, c := range counts {
		ret = append(ret, *c)
	}
	return ret, nil
}

func hash(val []string) string {
	enc, _ := json.Marshal(val)
	hash := sha256.Sum256(enc)
	return string(hash[:])
}
