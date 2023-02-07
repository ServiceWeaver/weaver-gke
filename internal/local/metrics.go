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
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
)

// The interval beyond which we ignore metric values in the database.
// This allows Prometheus to distinguish between (1) a metric value not been
// updated and (2) a metric value was updated but remains the same.
const metricFreshness = time.Minute

// metricDBFilename returns the metrics database filename on the local machine.
func metricDBFilename() (string, error) {
	dataDir, err := defaultDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dataDir, "gke_local_metrics.db"), nil
}

// metricExporter is an implementation of a Prometheus Collector that exports
// metrics collectected from all running Service Weaver processes.
type metricExporter struct {
	db     *metricdb.T
	logger *logging.FuncLogger

	mu    sync.Mutex
	descs map[uint64]*metricDescriptor // metric descriptors cached by metric id
}

type metricDescriptor struct {
	def  *protos.MetricDef
	desc *prometheus.Desc

	// Label names and values, split into two parallel arrays, i.e.,
	// labelValues[i] is the value for label labelNames[i].
	labelNames  []string
	labelValues []string
}

var _ prometheus.Collector = &metricExporter{}

func newMetricExporter(db *metricdb.T, logger *logging.FuncLogger) *metricExporter {
	return &metricExporter{
		db:     db,
		logger: logger,
		descs:  map[uint64]*metricDescriptor{},
	}
}

// Describe implements the prometheus.Collector interface.
func (e *metricExporter) Describe(ch chan<- *prometheus.Desc) {
	// Don't emit any descriptors, since the set of all collected metrics
	// isn't known. This is okay, it just means that prometheus won't check
	// reported metrics against a predefined list of descriptors.
}

// Collect implements the prometheus.Collector interface.
func (e *metricExporter) Collect(ch chan<- prometheus.Metric) {
	e.logger.Info("Collecting metrics")
	processMetric := func(m *metrics.MetricSnapshot) error {
		pm, err := e.convert(m.Clone())
		if err != nil {
			return err
		}
		ch <- pm
		return nil
	}
	now := time.Now()
	if err := e.db.Query(context.Background(), now.Add(-metricFreshness), now, "" /*name*/, nil /*labels*/, processMetric); err != nil {
		e.logger.Error("Cannot query metrics database: ", err)
	}
}

// convert converts the given metric into the Prometheus format.
func (e *metricExporter) convert(m *metrics.MetricSnapshot) (prometheus.Metric, error) {
	md := e.getDescriptor(m)
	switch md.def.Typ {
	case protos.MetricType_COUNTER:
		return prometheus.NewConstMetric(md.desc, prometheus.CounterValue, m.Value, md.labelValues...)
	case protos.MetricType_GAUGE:
		return prometheus.NewConstMetric(md.desc, prometheus.GaugeValue, m.Value, md.labelValues...)
	case protos.MetricType_HISTOGRAM:
		// TODO(spetrovic): This creates a lot of overhead for Go garbage
		// collector. See if we can use an alternate API to report
		// metrics.
		buckets := map[float64]uint64{}
		var count uint64
		for i, bound := range md.def.Bounds {
			count += uint64(m.Counts[i])
			buckets[bound] = count // Cumulative counts.
		}
		// Account for the +Inf bucket.
		count += uint64(m.Counts[len(md.def.Bounds)])
		sum := m.Value
		return prometheus.NewConstHistogram(md.desc, count, sum, buckets, md.labelValues...)
	default:
		return nil, fmt.Errorf("metric %v has unsupported type", md.def.Name)
	}
}

func (e *metricExporter) getDescriptor(m *metrics.MetricSnapshot) *metricDescriptor {
	e.mu.Lock()
	defer e.mu.Unlock()
	if md, ok := e.descs[m.Id]; ok { // already added
		return md
	}
	labelNames := maps.Keys(m.Labels)
	sort.Strings(labelNames)
	labelValues := make([]string, len(labelNames))
	for i, name := range labelNames {
		labelValues[i] = m.Labels[name]
	}
	md := &metricDescriptor{
		def:         m.MetricDef(),
		desc:        prometheus.NewDesc(m.Name, m.Help, labelNames, nil /*constLabels*/),
		labelNames:  labelNames,
		labelValues: labelValues,
	}
	e.descs[m.Id] = md
	return md
}

// getMetricCounts returns the list of aggregated counts for the Service
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
