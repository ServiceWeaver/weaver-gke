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
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSetAdd(t *testing.T) {
	// Test plan: Add a set of elements in the store under a given key. Verify
	// that the elements are added as expected.

	ctx := context.Background()
	setName := "set"
	s := NewFakeStore()

	var want []string

	for _, x := range []string{"a", "b", "a", "c", "a", "d", "a", "b"} {
		found := false
		for _, elem := range want {
			if elem == x {
				found = true
				break
			}
		}
		if !found {
			want = append(want, x)
		}

		err := AddToSet(ctx, s, setName, x)
		if err != nil && !errors.Is(err, ErrUnchanged) {
			t.Fatal(err)
		}

		got, _, err := GetSet(ctx, s, setName, nil)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Fatalf("(-want +got):\n%s", diff)
		}
	}
}

func TestSetGetEmpty(t *testing.T) {
	// Test plan: Attempt to retrieve the set of elements under an empty key.
	// Verify that no elements exists under the key.

	ctx := context.Background()
	setName := "empty"
	s := NewFakeStore()

	got, _, err := GetSet(ctx, s, setName, nil)
	if err != nil {
		t.Fatal(err)
	}

	var want []string
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestSetConcurrentAdds(t *testing.T) {
	// Test plan: Add a set of elements in the store under the same key but from
	// different goroutines. Verify that all the elements are added as expected.

	ctx := context.Background()
	setName := "concurrent"
	s := NewFakeStore()

	numAdders := 100
	var done sync.WaitGroup
	done.Add(numAdders)
	errs := make(chan error, numAdders)

	// Launch the goroutines.
	for i := 0; i < numAdders; i++ {
		i := i
		go func() {
			defer done.Done()
			err := AddToSet(ctx, s, setName, strconv.Itoa(i))
			if err != nil && !errors.Is(err, ErrUnchanged) {
				errs <- err
			}
		}()
	}

	// Wait for all the goroutines to finish.
	done.Wait()

	// Check for errors.
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Check that all the Adds succeeded.
	got, _, err := GetSet(ctx, s, setName, nil)
	if err != nil {
		t.Fatal(err)
	}

	var want []string
	for i := 0; i < numAdders; i++ {
		want = append(want, strconv.Itoa(i))
	}
	sort.Strings(want)
	sort.Strings(got)
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

func TestSetBytes(t *testing.T) {
	// Test plan: Add an element of type bytes under a given key. Verify that the
	// element was added to the set.

	ctx := context.Background()
	setName := "bytes"
	s := NewFakeStore()

	bytes := make([]byte, 256)
	for i := 0; i < 256; i++ {
		bytes[i] = byte(i)
	}
	want := []string{string(bytes)}
	err := AddToSet(ctx, s, setName, string(bytes))
	if err != nil && !errors.Is(err, ErrUnchanged) {
		t.Fatal(err)
	}
	got, _, err := GetSet(ctx, s, setName, nil)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}
