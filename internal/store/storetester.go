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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// StoreTester is a test suite for Store implementations. Imagine you have a
// struct MyStore that implements the Store interface. In a file like
// mystore_test.go, you can use the following code to test that MyStore
// correctly implements Store:
//
//	func TestMyStore(t *testing.T) {
//	    StoreTester{func(*testing.T) Store { return NewMyStore() }}.Run(t)
//	}
//
// A StoreTester is constructed with a factory function Make that returns a new
// instance of a Store every time it is invoked. Make is passed a *testing.T.
// If in the process of constructing a new Store, an error is encountered, you
// can call testing.T.Fatal. For example:
//
//	    func TestMyStore(t *testing.T) {
//	        maker := func(t *testing.T) Store {
//			       store, err := TryToMakeNewMyStore()
//			       if err != nil {
//			           t.Fatal(err)
//			       }
//			       return store
//	        }
//	        StoreTester{maker}.Run(t)
//	    }
type StoreTester struct {
	Make func(t *testing.T) Store
}

func (s StoreTester) Run(t *testing.T) {
	t.Helper()
	t.Run("BasicOps", s.TestBasicOps)
	t.Run("BlindPuts", s.TestBlindPuts)
	t.Run("GetMissing", s.TestGetMissing)
	t.Run("PutConflict", s.TestPutConflict)
	t.Run("CreateConflict", s.TestCreateConflict)
	t.Run("DeleteMissing", s.TestDeleteMissing)
	t.Run("PutChain", s.TestPutChain)
	t.Run("Bytes", s.TestBytes)
	t.Run("List", s.TestList)
	t.Run("ConcurrentIncrements", s.TestConcurrentIncrements)
	t.Run("ConcurrentCreates", s.TestConcurrentCreates)
	t.Run("BlockedVersionedGet", s.TestBlockedVersionedGet)
	t.Run("VersionedGetUpdated", s.TestVersionedGetUpdated)
	t.Run("VersionedGetCreated", s.TestVersionedGetCreated)
	t.Run("VersionedGetDeleted", s.TestVersionedGetDeleted)
	t.Run("VersionedGetMissing", s.TestVersionedGetMissing)
	t.Run("ConcurrentVersionedGets", s.TestConcurrentVersionedGets)
}

func (s StoreTester) TestBasicOps(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	_, err := store.Put(ctx, "key", "value", &Missing)
	if err != nil {
		t.Fatal(err)
	}

	value, version, err := store.Get(ctx, "key", nil)
	if value != "value" {
		t.Errorf("wanted %q, got %q", "value", value)
	}
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "new value", version)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err = store.Get(ctx, "key", nil)
	if value != "new value" {
		t.Errorf("wanted %q, got %q", "new value", value)
	}
	if err != nil {
		t.Fatal(err)
	}

	err = store.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestBlindPuts(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	_, err := store.Put(ctx, "eggs", "green", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "eggs", "scrambled", nil)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := store.Get(ctx, "eggs", nil)
	if value != "scrambled" {
		t.Errorf("wanted %q, got %q", "scrambled", value)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestGetMissing(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	value, version, err := store.Get(ctx, "missing key", nil)
	if value != "" {
		t.Errorf("wanted %q, got %q", "", value)
	}
	if *version != Missing {
		t.Errorf("wanted %q, got %q", Missing, *version)
	}
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "value", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}

	value, version, err = store.Get(ctx, "key", nil)
	if value != "" {
		t.Errorf("wanted %q, got %q", "", value)
	}
	if *version != Missing {
		t.Errorf("wanted %q, got %q", Missing, *version)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestPutConflict(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	_, err := store.Put(ctx, "key", "value", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, version, err := store.Get(ctx, "key", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "successful put", version)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "unsuccessful put", version)
	if !errors.Is(err, Stale{}) {
		t.Fatalf("wanted Stale, got %v", err)
	}
	var staleVersion Stale
	if !errors.As(err, &staleVersion) {
		t.Fatalf("wanted Stale, got %v", err)
	}
}

func (s StoreTester) TestCreateConflict(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	_, err := store.Put(ctx, "key", "successful create", &Missing)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "unsuccessful create", &Missing)
	if !errors.Is(err, Stale{}) {
		t.Fatalf("wanted Stale, got %v", err)
	}
}

func (s StoreTester) TestDeleteMissing(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	err := store.Delete(ctx, "missing key")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "value", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}

	err = store.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestPutChain(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	version, err := store.Put(ctx, "key", "What do", &Missing)
	if err != nil {
		t.Fatal(err)
	}

	version, err = store.Put(ctx, "key", "dentists", version)
	if err != nil {
		t.Fatal(err)
	}

	version, err = store.Put(ctx, "key", "call x-rays?", version)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Put(ctx, "key", "Tooth pics!", version)
	if err != nil {
		t.Fatal(err)
	}

	value, _, err := store.Get(ctx, "key", nil)
	if value != "Tooth pics!" {
		t.Errorf("wanted %q, got %q", "Tooth pics!", value)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestBytes(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	key := string([]byte{1, 2, 3, 4, 5})
	value := string([]byte{101, 102, 103, 104, 105})
	_, err := store.Put(ctx, key, value, &Missing)
	if err != nil {
		t.Fatal(err)
	}

	got, _, err := store.Get(ctx, key, nil)
	if got != value {
		t.Errorf("wanted %q, got %q", value, got)
	}
	if err != nil {
		t.Fatal(err)
	}

	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
}

func (s StoreTester) TestList(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()
	less := func(x, y string) bool { return x < y }

	for i := 0; i < 2; i++ {
		keys := []string{"a", "b", "c", "d"}
		want := []string{}

		for _, key := range keys {
			_, err := store.Put(ctx, key, "value", nil)
			if err != nil {
				t.Fatal(err)
			}

			// Match without prefix.
			want = append(want, key)
			got, err := store.List(ctx, ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
				t.Fatalf("bad List (-want +got):\n%s", diff)
			}

			// Match with prefix.
			got, err = store.List(ctx, ListOptions{Prefix: key})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff([]string{key}, got); diff != "" {
				t.Fatalf("bad List with prefix (-want +got):\n%s", diff)
			}
		}

		for _, key := range keys {
			if err := store.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}

			want = want[1:]
			got, err := store.List(ctx, ListOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(want, got, cmpopts.SortSlices(less)); diff != "" {
				t.Fatalf("bad List (-want +got):\n%s", diff)
			}
		}
	}
}

// TestConcurrentIncrements initializes a key with value 0. It then spawns n
// goroutines that attempt to read, increment, and update the value. The
// goroutines loop until they successfully update the value. After all
// goroutines have terminated, we check to make sure the value of the key is n.
// This test checks that Store implementations are thread-safe.
func (s StoreTester) TestConcurrentIncrements(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	// Initialize the counter to 0.
	key := "counter"
	_, err := store.Put(ctx, key, "0", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Spawn the n goroutines. Every goroutine writes exactly one value into
	// errs. If a goroutine encounters an error, it writes the error to errs
	// and terminates. Otherwise, if a goroutine terminates successfully without
	// encountering an error, it writes nil to errs.
	n := 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			for {
				value, version, err := store.Get(ctx, key, nil)
				if err != nil {
					errs <- err
					return
				}

				x, err := strconv.Atoi(value)
				if err != nil {
					errs <- err
					return
				}

				_, err = store.Put(ctx, key, strconv.Itoa(x+1), version)
				if errors.Is(err, Stale{}) {
					// There was a conflict. Try again.
					continue
				} else if err != nil {
					errs <- err
					return
				}
				errs <- nil
				return
			}
		}()
	}

	// Wait for the n goroutines to terminate.
	for i := 0; i < n; i++ {
		err := <-errs
		if err != nil {
			t.Error(err)
		}
	}

	// Check that we didn't lose any increments.
	value, _, err := store.Get(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}

	x, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}

	if x != n {
		t.Errorf("got %v, wanted %v", x, n)
	}
}

// TestConcurrentCreates spawns n goroutines numbered 0, 1, ..., n-1. The
// goroutines race to reach consensus on the value of a key, with goroutine i
// proposing that value i be chosen. This test makes sure that every goroutine
// reaches consensus on the same value. It also checks that Store
// implementations are thread-safe.
func (s StoreTester) TestConcurrentCreates(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	// Spawn the n goroutines. Every goroutine writes exactly one value into
	// errs and one value into values. If a goroutine encounters an error, it
	// writes the error to errs and -1 to values and terminates. Otherwise, if
	// a goroutine terminates successfully without encountering an error, it
	// writes nil to errs and the chosen value to values.
	n := 10
	errs := make(chan error, n)
	values := make(chan int, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			_, err := store.Put(ctx, "key", strconv.Itoa(i), &Missing)
			if err == nil {
				errs <- nil
				values <- i
				return
			} else if err != nil && !errors.Is(err, Stale{}) {
				errs <- err
				values <- -1
				return
			}

			value, _, err := store.Get(ctx, "key", nil)
			if err != nil {
				errs <- err
				values <- -1
				return
			}

			x, err := strconv.Atoi(value)
			if err != nil {
				errs <- err
				values <- -1
				return
			}
			errs <- nil
			values <- x
		}()
	}

	// Wait for the n goroutines to terminate.
	for i := 0; i < n; i++ {
		err := <-errs
		if err != nil {
			t.Error(err)
		}
	}

	// Check that every thread agrees on the same value.
	var chosen int
	for i := 0; i < n; i++ {
		if i == 0 {
			chosen = <-values
			continue
		}

		x := <-values
		if x != chosen {
			t.Errorf("got %v, wanted %v", x, chosen)
		}
	}
}

// pipeErrs pipes the error returned by f into errs, if the error is not nil.
func pipeErrs(errs chan error, f func() error) {
	if err := f(); err != nil {
		errs <- err
	}
}

// TestBlockedVersionedGet tests that a blocked versioned get respects the
// context passed into it. We start a versioned get that watches a missing key
// that is never updated. This get blocks and should only return when its
// context has expired.
func (s StoreTester) TestBlockedVersionedGet(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 100*time.Microsecond)
	defer cancel()

	_, _, err := store.Get(ctx, "missing key", &Missing)
	if errors.Is(err, Unchanged) {
		// A store is always allowed to return Unchanged.
		return
	}

	if ctx.Err() == nil {
		t.Fatal("context not cancelled")
	}

	if !errors.Is(err, ctx.Err()) {
		t.Fatalf("unexpected err: got %v, want %v", err, ctx.Err())
	}
}

// TestVersionedGetUpdated spawns two goroutines: one getter and one putter.
// The putter repeatedly puts a value to an existing key, and then sends the
// version it put to the getter, waiting for the getter to receive the version.
// The getter repeatedly issues a versioned Get to get a value, receives a
// version from the putter, and then compares the gotten version to the version
// it received from the putter. This test tests that versioned Gets properly
// notice updates.
func (s StoreTester) TestVersionedGetUpdated(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	version, err := store.Put(ctx, "key", "initial value", nil)
	if err != nil {
		t.Fatal(err)
	}

	// The number of puts and gets to do.
	n := 3

	// The putter sends its versions over this channel to the getter. It's
	// unbuffered, so the putter has to wait for the getter before putting
	// again. This is necessary because if a putter puts too quickly, the
	// getter may miss some updates and stall on its versioned Gets.
	putVersions := make(chan *Version)

	// The putter also sends its values, along with its versions.
	putValues := make(chan string)

	var done sync.WaitGroup
	done.Add(2)

	getter := func() error {
		defer done.Done()

		var value string
		version := version
		var err error

		for i := 0; i < n; i++ {
			value, version, err = store.Get(ctx, "key", version)
			if errors.Is(err, Unchanged) {
				cancel()
				return nil
			} else if err != nil {
				cancel()
				return err
			}

			var putVersion *Version
			select {
			case putVersion = <-putVersions:
			case <-ctx.Done():
				return nil
			}

			if *version != *putVersion {
				cancel()
				return fmt.Errorf("got version %v != put version %v", version, putVersion)
			}

			var putValue string
			select {
			case putValue = <-putValues:
			case <-ctx.Done():
				return nil
			}

			if value != putValue {
				cancel()
				return fmt.Errorf("got value %q != put value %q", value, putValue)
			}
		}

		return nil
	}

	putter := func() error {
		defer done.Done()

		for i := 0; i < n; i++ {
			value := fmt.Sprintf("update %v", i)
			version, err := store.Put(ctx, "key", value, nil)
			if err != nil {
				cancel()
				return err
			}

			select {
			case putVersions <- version:
			case <-ctx.Done():
				return nil
			}

			select {
			case putValues <- value:
			case <-ctx.Done():
				return nil
			}
		}

		return nil
	}

	errs := make(chan error, 2)
	go pipeErrs(errs, getter)
	go pipeErrs(errs, putter)
	done.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestVersionedGetCreated spawns two goroutines: one getter and one putter.
// The getter performs a versioned Get on a missing key with version Missing,
// and the putter creates the key. This test tests that versioned Gets properly
// notice the creation of a key.
func (s StoreTester) TestVersionedGetCreated(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	putVersions := make(chan *Version, 1)

	var done sync.WaitGroup
	done.Add(2)

	getter := func() error {
		defer done.Done()

		ready <- struct{}{}
		value, version, err := store.Get(ctx, "key", &Missing)
		if errors.Is(err, Unchanged) {
			return nil
		} else if err != nil {
			return err
		}

		var putVersion *Version
		select {
		case putVersion = <-putVersions:
		case <-ctx.Done():
			return nil
		}

		if *version != *putVersion {
			return fmt.Errorf("got version %v != put version %v", version, putVersion)
		}

		if value != "value" {
			return fmt.Errorf("got value %q, want value %q", value, "value")
		}

		return nil
	}

	putter := func() error {
		defer done.Done()

		// Wait for the getter to call Get. We want to test that the Get
		// notices the creation while it's waiting.
		<-ready
		version, err := store.Put(ctx, "key", "value", nil)
		if err != nil {
			cancel()
			return err
		}
		putVersions <- version
		return nil
	}

	errs := make(chan error, 2)
	go pipeErrs(errs, getter)
	go pipeErrs(errs, putter)
	done.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestVersionedGetDeleted spawns two goroutines: one getter and one putter.
// The getter performs a versioned Get on a key, and the putter deletes the
// key. This test tests that versioned Gets properly notice the deletion of a
// key.
func (s StoreTester) TestVersionedGetDeleted(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	version, err := store.Put(ctx, "key", "value", nil)
	if err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{})
	var done sync.WaitGroup
	done.Add(2)

	getter := func() error {
		defer done.Done()

		ready <- struct{}{}
		_, version, err := store.Get(ctx, "key", version)
		if errors.Is(err, Unchanged) {
			return nil
		} else if err != nil {
			return err
		}

		if *version != Missing {
			return fmt.Errorf("got version %v != Missing", version)
		}

		return nil
	}

	putter := func() error {
		defer done.Done()

		// Wait for the getter to call Get. We want to test that the Get
		// notices the deletion while it's waiting. Though note that ready is
		// best effort. It is still possible that Get is called before Delete.
		<-ready
		return store.Delete(ctx, "key")
	}

	errs := make(chan error, 2)
	go pipeErrs(errs, getter)
	go pipeErrs(errs, putter)
	done.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestVersionedGetMissing tests that a stale versioned get on a value that's
// currently missing returns the value immediately as missing.
func (s StoreTester) TestVersionedGetMissing(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx := context.Background()

	version, err := store.Put(ctx, "key", "value", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}

	_, newVersion, err := store.Get(ctx, "key", version)
	if err != nil {
		t.Fatal(err)
	}
	if *newVersion != Missing {
		t.Errorf("wanted %q, got %q", Missing, *newVersion)
	}
}

// TestConcurrentVersionedGets spawns a number of getter goroutines and a
// single putter goroutine. The putter creates, updates, and deletes a key,
// while the getters watch the changes to the key. The getters and putter
// coordinate using a ready channel to operate in lockstep. This prevents the
// putter from writing too quickly and avoids the getters skipping updates.
// This test tests that multiple goroutines can simultaneously watch the same
// key without any problems.
func (s StoreTester) TestConcurrentVersionedGets(t *testing.T) {
	t.Helper()
	store := s.Make(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numGetters := 5
	var done sync.WaitGroup
	done.Add(numGetters + 1)
	ready := make(chan struct{}, numGetters)

	getter := func() error {
		defer done.Done()

		// Get created.
		select {
		case <-ctx.Done():
			return nil
		case ready <- struct{}{}:
		}
		value, version, err := store.Get(ctx, "key", &Missing)
		if errors.Is(err, Unchanged) {
			cancel()
			return nil
		} else if err != nil {
			cancel()
			return err
		}
		if value != "created" {
			cancel()
			return fmt.Errorf("got value %q, want %q", value, "created")
		}

		// Get updated.
		select {
		case <-ctx.Done():
			return nil
		case ready <- struct{}{}:
		}
		value, version, err = store.Get(ctx, "key", version)
		if errors.Is(err, Unchanged) {
			cancel()
			return nil
		} else if err != nil {
			cancel()
			return err
		}
		if value != "updated" {
			cancel()
			return fmt.Errorf("got value %q, want %q", value, "updated")
		}

		// Get deleted.
		select {
		case <-ctx.Done():
			return nil
		case ready <- struct{}{}:
		}
		_, version, err = store.Get(ctx, "key", version)
		if errors.Is(err, Unchanged) {
			cancel()
			return nil
		} else if err != nil {
			cancel()
			return err
		}
		if *version != Missing {
			cancel()
			return fmt.Errorf("got version %v, want Missing", version)
		}

		return nil
	}

	putter := func() error {
		defer done.Done()

		// Create the key.
		for i := 0; i < numGetters; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-ready:
			}
		}
		version, err := store.Put(ctx, "key", "created", nil)
		if err != nil {
			cancel()
			return err
		}

		// Update the key.
		for i := 0; i < numGetters; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-ready:
			}
		}
		_, err = store.Put(ctx, "key", "updated", version)
		if err != nil {
			cancel()
			return err
		}

		// Delete the key.
		for i := 0; i < numGetters; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-ready:
			}
		}
		err = store.Delete(ctx, "key")
		if err != nil {
			cancel()
			return err
		}

		return nil
	}

	errs := make(chan error, numGetters+1)
	for i := 0; i < numGetters; i++ {
		go pipeErrs(errs, getter)
	}
	go pipeErrs(errs, putter)
	done.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
