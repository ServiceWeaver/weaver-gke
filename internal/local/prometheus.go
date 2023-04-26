// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
)

// The interval beyond which we ignore metric values in the metric database.
// This allows Prometheus to distinguish between (1) a metric value not been
// updated and (2) a metric value was updated but remains the same.
const freshness = time.Minute

// promCollector is an implementation of a Prometheus Collector that exports
// metrics stored in the given metric database.
type promCollector struct {
	db     *metricdb.T
	logger *slog.Logger

	// If non-empty, only metrics that correspond to the given app/version
	// are collected.
	app     string
	version string

	mu    sync.Mutex
	descs map[uint64]*promDescriptor // metric descriptors cached by metric id
}

func newPromCollector(db *metricdb.T, logger *slog.Logger, app, version string) *promCollector {
	return &promCollector{
		db:      db,
		logger:  logger,
		app:     app,
		version: version,
		descs:   map[uint64]*promDescriptor{},
	}
}

type promDescriptor struct {
	def  *protos.MetricDef
	desc *prometheus.Desc

	// Label names and values, split into two parallel arrays, i.e.,
	// labelValues[i] is the value for label labelNames[i].
	labelNames  []string
	labelValues []string
}

var _ prometheus.Collector = &promCollector{}

// Describe implements the prometheus.Collector interface.
func (e *promCollector) Describe(ch chan<- *prometheus.Desc) {
	// NOTE: We don't emit any descriptors, since the set of all collected
	// metrics isn't known ahead of time. This is okay, it just means that
	// prometheus won't check reported metrics against a predefined list of
	// descriptors.
}

// Collect implements the prometheus.Collector interface.
func (e *promCollector) Collect(ch chan<- prometheus.Metric) {
	processMetric := func(m *metrics.MetricSnapshot) error {
		pm, err := e.convert(m.Clone())
		if err != nil {
			return err
		}
		ch <- pm
		return nil
	}
	now := time.Now()
	labels := map[string]string{}
	if e.app != "" {
		labels["serviceweaver_app"] = e.app
	}
	if e.version != "" {
		labels["serviceweaver_version"] = e.version
	}
	if err := e.db.Query(context.Background(), now.Add(-freshness), now, "" /*name*/, labels, processMetric); err != nil {
		e.logger.Error("Cannot query metrics database: ", "err", err)
	}
}

// convert converts the given metric into the Prometheus format.
func (e *promCollector) convert(m *metrics.MetricSnapshot) (prometheus.Metric, error) {
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

func (e *promCollector) getDescriptor(m *metrics.MetricSnapshot) *promDescriptor {
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
	md := &promDescriptor{
		def:         m.MetricDef(),
		desc:        prometheus.NewDesc(m.Name, m.Help, labelNames, nil /*constLabels*/),
		labelNames:  labelNames,
		labelValues: labelValues,
	}
	e.descs[m.Id] = md
	return md
}

// promHandler is a Prometheus handler that supports some basic query
// parameters.
type promHandler struct {
	db     *metricdb.T
	logger *slog.Logger
}

// NewPrometheusHandler returns a Prometheus HTTP handler that exports metrics
// stored in the given metrics database. The returned handler supports the
// following query parameters:
//   - app=<app>,         which restricts metrics to the given application, and
//   - version=<version>, which restricts metrics to the given app version.
func NewPrometheusHandler(db *metricdb.T, logger *slog.Logger) http.Handler {
	return &promHandler{db: db, logger: logger}
}

func (h *promHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	app := r.URL.Query().Get("app")
	version := r.URL.Query().Get("version")
	reg := prometheus.NewRegistry() // New registry so we're the only collector
	reg.Register(newPromCollector(h.db, h.logger, app, version))
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}
