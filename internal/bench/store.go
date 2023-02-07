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

// Package bench contains benchmarking utilities.
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

// StoreConfig configures the benchmark run by BenchStore.
type StoreConfig struct {
	NumKeys      int                 // The number of keys to sample from.
	ValueSize    Size                // The size of values written to keys.
	ReadFraction float32             // The fraction of operations that are reads.
	Duration     time.Duration       // The duration of the benchmark.
	Logger       *logging.FuncLogger // A logger.
	Parallelism  int                 // The number of parallel store operations.
}

// StoreStats contains statistics about a store benchmark.
type StoreStats struct {
	Throughput       float64       // Number of ops/second (reads and writes).
	MeanReadLatency  time.Duration // Mean read latency.
	MeanWriteLatency time.Duration // Mean write latency.
}

// BenchStore benchmarks the provided store by repeatedly performing reads and
// writes against a set of keys sampled uniformly at random.
func BenchStore(ctx context.Context, store store.Store, config StoreConfig) (StoreStats, error) {
	if config.Parallelism < 1 {
		config.Parallelism = 1
	}

	// Populate the keys.
	config.Logger.Debug("Populating keys...")
	val := strings.Repeat("X", config.ValueSize.Bytes())
	for i := 0; i < config.NumKeys; i++ {
		if _, err := store.Put(ctx, key(i), val, nil); err != nil {
			return StoreStats{}, nil
		}
	}

	// Run the benchmark.
	//
	// TODO(mwhittaker): Use versioned gets and puts.
	config.Logger.Debug("Running benchmark...")
	start := time.Now()
	var mu sync.Mutex
	readDur := time.Duration(0)
	writeDur := time.Duration(0)
	numReads := 0
	numWrites := 0
	var errs []error

	updateErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}
	updateReadStats := func(d time.Duration, num int) int {
		mu.Lock()
		defer mu.Unlock()
		readDur += d
		numReads += num
		return numReads
	}
	updateWriteStats := func(d time.Duration, num int) int {
		mu.Lock()
		defer mu.Unlock()
		writeDur += d
		numWrites += num
		return numWrites
	}

	var wait sync.WaitGroup
	wait.Add(config.Parallelism)
	for i := 0; i < config.Parallelism; i++ {
		go func() {
			defer wait.Done()
			rand := rand.New(rand.NewSource(time.Now().UnixNano()))
			for time.Since(start) < config.Duration {
				key := key(rand.Intn(config.NumKeys))
				if rand.Float32() <= config.ReadFraction {
					t := time.Now()
					if _, _, err := store.Get(ctx, key, nil); err != nil {
						updateErr(err)
						continue
					}
					d := time.Since(t)
					num := updateReadStats(d, 1)
					config.Logger.Debug("Read", "count", num, "duration", d)
				} else {
					t := time.Now()
					if _, err := store.Put(ctx, key, val, nil); err != nil {
						updateErr(err)
						continue
					}
					d := time.Since(t)
					num := updateWriteStats(d, 1)
					config.Logger.Debug("Write", "count", num, "duration", d)
				}
			}
		}()
	}
	wait.Wait()
	stats := StoreStats{
		Throughput: float64(numReads+numWrites) / float64(config.Duration.Seconds()),
	}
	if numReads > 0 {
		stats.MeanReadLatency = time.Duration(int(readDur.Nanoseconds()) / numReads)
	}
	if numWrites > 0 {
		stats.MeanWriteLatency = time.Duration(int(writeDur.Nanoseconds()) / numWrites)
	}
	return stats, nil
}

func key(i int) string {
	return fmt.Sprintf("/storebench/key%06d", i)
}
