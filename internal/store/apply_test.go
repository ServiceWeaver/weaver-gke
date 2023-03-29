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
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestApplyIncrement launches n incrementer goroutines that all try to
// increment an integer-valued key. This test tests that Apply atomically
// applies concurrent updates.
func TestApplyIncrement(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeStore()

	increment := func(delta int) func(value string, version *Version) (string, error) {
		return func(value string, version *Version) (string, error) {
			// Perform a small sleep to increase the chances of a collision.
			time.Sleep(5 * time.Millisecond)

			var x int
			if *version != Missing {
				var err error
				if x, err = strconv.Atoi(value); err != nil {
					return "", err
				}
			}

			x += delta
			return strconv.Itoa(x), nil
		}
	}

	n := 10
	var done sync.WaitGroup
	done.Add(n)
	incrementer := func(delta int) error {
		defer done.Done()
		_, _, err := apply(ctx, fake, "key", increment(delta))
		return err
	}

	// Launch the incrementing goroutines and wait for them to finish.
	errs := make(chan error, n)
	for i := 1; i < n+1; i++ {
		i := i
		go pipeErrs(errs, func() error { return incrementer(i) })
	}
	done.Wait()

	// Check for errors.
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Check that the final value is 1 + 2 + ... + n.
	value, _, err := fake.Get(ctx, "key", nil)
	if err != nil {
		t.Fatal(err)
	}

	var x int
	if x, err = strconv.Atoi(value); err != nil {
		t.Fatal(err)
	}

	want := 0
	for i := 1; i < n+1; i++ {
		want += i
	}
	if x != want {
		t.Fatalf("got %d, want %d", x, want)
	}
}

func TestApplyUnchanged(t *testing.T) {
	// Ensure that an ErrUnchanged result suppresses a subsequent write.
	ctx := context.Background()
	fake := NewFakeStore()
	if _, err := fake.Put(ctx, "k", "v1", nil); err != nil {
		t.Fatalf("initial Put failed")
	}
	initialWrites := fake.Writes()

	// v1 => no change
	// other => v2
	_, _, err := apply(ctx, fake, "k", func(v string, version *Version) (string, error) {
		if v == "v1" {
			return "", ErrUnchanged
		}
		return "v2", nil
	})
	if !errors.Is(err, ErrUnchanged) {
		t.Fatalf("unexpected error %v (should be ErrUnchanged)", err)
	}
	if w := fake.Writes(); w != initialWrites {
		t.Fatalf("unexpected write of unchanged value")
	}
	v, _, err := fake.Get(ctx, "k", nil)
	if err != nil {
		t.Fatalf("final Get failed")
	}
	if v != "v1" {
		t.Fatalf("unexpected get result %q, expecting %q", v, "v1")
	}
}

// TestApplyConsensus launches n proposer goroutines p_1, ..., p_n. Every
// proposer p_i tries to propose that value i be chosen for an integer-valued
// key. This test tests that Apply correctly handles the case where some
// proposers don't want to perform an update. It also tests that Apply returns
// the written value.
func TestApplyConsensus(t *testing.T) {
	ctx := context.Background()
	fake := NewFakeStore()

	propose := func(i int) func(value string, version *Version) (string, error) {
		return func(value string, version *Version) (string, error) {
			// Perform a small sleep to increase the chances of a collision.
			time.Sleep(5 * time.Millisecond)

			x := 0
			if *version != Missing {
				var err error
				if x, err = strconv.Atoi(value); err != nil {
					return "", err
				}
			}

			// A value has already been chosen.
			if x != 0 {
				return value, nil
			}

			// We propose our value.
			return strconv.Itoa(i), nil
		}
	}

	n := 10
	var done sync.WaitGroup
	done.Add(n)
	xs := make(chan int, n)

	proposer := func(i int) error {
		defer done.Done()
		value, _, err := apply(ctx, fake, "key", propose(i))
		if err != nil {
			return err
		}

		x, err := strconv.Atoi(value)
		if err != nil {
			return err
		}

		xs <- x
		return nil
	}

	// Launch the proposers and wait for them to finish.
	errs := make(chan error, n)
	for i := 1; i < n+1; i++ {
		i := i
		go pipeErrs(errs, func() error { return proposer(i) })
	}
	done.Wait()

	// Check for errors.
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Check that the final value is in the range [1, n].
	value, _, err := fake.Get(ctx, "key", nil)
	if err != nil {
		t.Fatal(err)
	}

	x, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	if x < 1 || x > n {
		t.Fatalf("got x=%d, want x in range [1, %d]", x, n)
	}

	// Check that every value is the same.
	close(xs)
	for got := range xs {
		if got != x {
			t.Fatalf("proposer got %d, want %d", got, x)
		}
	}
}
