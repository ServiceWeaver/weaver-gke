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

package store

import (
	"context"
	"errors"
	"time"

	"github.com/ServiceWeaver/weaver/metrics"
)

type storeLabels struct {
	Service   string // System service using the store (e.g., controller, tool)
	Id        string // Unique id for the system service instance
	Op        string // Store operation (e.g., get, put)
	Generated bool   `weaver:"serviceweaver_generated"`
}

var (
	requestCounts = metrics.NewCounterMap[storeLabels](
		"serviceweaver_store_request_count",
		"Number of store requests, by operation",
	)
	requestLatencies = metrics.NewHistogramMap[storeLabels](
		"serviceweaver_store_request_latency_micros",
		"Latency of store requests, in microseconds, by operation",
		generatedBuckets,
	)
	valueSizes = metrics.NewHistogramMap[storeLabels](
		"serviceweaver_store_value_size_bytes",
		"Size of values, in bytes, by operation",
		generatedBuckets,
	)
	staleCounts = metrics.NewCounterMap[storeLabels](
		"serviceweaver_store_stale_count",
		"Number of stale puts",
	)

	// generatedBuckets provides rounded bucket boundaries for histograms
	// that will only store non-negative values.
	generatedBuckets = []float64{
		// Adjacent buckets differ from each other by 2x or 2.5x.
		1, 2, 5,
		10, 20, 50,
		100, 200, 500,
		1000, 2000, 5000,
		10000, 20000, 50000,
		100000, 200000, 500000,
		1000000, 2000000, 5000000,
		10000000, 20000000, 50000000,
		100000000, 200000000, 500000000,
		1000000000, 2000000000, 5000000000, // i.e., 5e9
	}
)

// WithMetrics returns the provided store, but with methods augmented to update
// the appropriate store rtmetrics.
func WithMetrics(service string, id string, store Store) Store {
	labels := func(op string) storeLabels {
		return storeLabels{Service: service, Id: id, Op: op, Generated: true}
	}
	putCount := requestCounts.Get(labels("put"))
	getCount := requestCounts.Get(labels("get"))
	watchCount := requestCounts.Get(labels("watch"))
	deleteCount := requestCounts.Get(labels("delete"))
	putLatency := requestLatencies.Get(labels("put"))
	getLatency := requestLatencies.Get(labels("get"))
	watchLatency := requestLatencies.Get(labels("watch"))
	deleteLatency := requestLatencies.Get(labels("delete"))
	putSize := valueSizes.Get(labels("put"))
	getSize := valueSizes.Get(labels("get"))
	watchSize := valueSizes.Get(labels("watch"))
	staleCount := staleCounts.Get(labels("put"))
	return &metricsWrapper{
		store:         store,
		putCount:      putCount,
		getCount:      getCount,
		watchCount:    watchCount,
		deleteCount:   deleteCount,
		putLatency:    putLatency,
		getLatency:    getLatency,
		watchLatency:  watchLatency,
		deleteLatency: deleteLatency,
		putSize:       putSize,
		getSize:       getSize,
		watchSize:     watchSize,
		staleCount:    staleCount,
	}
}

// metricsWrapper is a wrapper around a Store that tracks various metrics.
type metricsWrapper struct {
	store                                               Store
	putCount, getCount, watchCount, deleteCount         *metrics.Counter
	putLatency, getLatency, watchLatency, deleteLatency *metrics.Histogram
	putSize, getSize, watchSize                         *metrics.Histogram
	staleCount                                          *metrics.Counter
}

var _ Store = &metricsWrapper{}

// Put implements the Store interface.
func (m *metricsWrapper) Put(ctx context.Context, key, value string, version *Version) (*Version, error) {
	start := time.Now()
	defer func() { m.putLatency.Put(float64(time.Since(start).Microseconds())) }()
	m.putCount.Add(1.0)
	m.putSize.Put(float64(len(value)))
	version, err := m.store.Put(ctx, key, value, version)
	if errors.Is(err, Stale{}) {
		m.staleCount.Add(1.0)
	}
	return version, err
}

// Get implements the Store interface.
func (m *metricsWrapper) Get(ctx context.Context, key string, version *Version) (string, *Version, error) {
	var count *metrics.Counter
	var latency, size *metrics.Histogram
	if version == nil {
		count, latency, size = m.getCount, m.getLatency, m.getSize
	} else {
		count, latency, size = m.watchCount, m.watchLatency, m.watchSize
	}

	start := time.Now()
	defer func() { latency.Put(float64(time.Since(start).Microseconds())) }()
	count.Add(1.0)
	value, version, err := m.store.Get(ctx, key, version)
	if err == nil {
		size.Put(float64(len(value)))
	}
	return value, version, err
}

// Delete implements the Store interface.
func (m *metricsWrapper) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() { m.deleteLatency.Put(float64(time.Since(start).Microseconds())) }()
	m.deleteCount.Add(1.0)
	return m.store.Delete(ctx, key)
}

// List implements the Store interface.
func (m *metricsWrapper) List(ctx context.Context, opts ListOptions) ([]string, error) {
	return m.store.List(ctx, opts)
}
