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
	"strconv"
	"strings"
	"sync"
)

type versioned struct {
	value   string
	version Version
}

// FakeStore is a simple, in-memory, map-backed implementation of a Store.
// FakeStore is a fake test double that is meant for testing.
type FakeStore struct {
	m      sync.Mutex
	global int
	data   map[string]versioned

	// List of channels connected to goroutines waiting for the value to change.
	// We do not bother segregating these by key since the fakestore is used
	// for tests and should not have too many waiters.
	waiters []chan struct{}
}

// Check that FakeStore implements the Store interface.
var _ Store = &FakeStore{}

func NewFakeStore() *FakeStore {
	return &FakeStore{global: 0, data: map[string]versioned{}}
}

func (f *FakeStore) getValue(key string) (string, *Version) {
	old, ok := f.data[key]
	if ok {
		return old.value, &old.version
	} else {
		return "", &Missing
	}
}

func (f *FakeStore) wakeupWaiters() {
	for _, c := range f.waiters {
		close(c)
	}
	f.waiters = f.waiters[:0]
}

// Returns true if the latest version of the key is different from version.
func (f *FakeStore) hasChanged(key string, version *Version) bool {
	_, latest := f.getValue(key)
	return *version != *latest
}

func (f *FakeStore) blindPut(key, value string) *Version {
	version := Version{strconv.Itoa(f.global)}
	f.global += 1
	f.data[key] = versioned{value, version}
	f.wakeupWaiters()
	return &version
}

func (f *FakeStore) Put(_ context.Context, key, value string, version *Version) (*Version, error) {
	f.m.Lock()
	defer f.m.Unlock()

	if version == nil {
		return f.blindPut(key, value), nil
	}

	_, latest := f.getValue(key)
	if *version != *latest {
		return nil, NewStale(*version, latest)
	}

	return f.blindPut(key, value), nil
}

func (f *FakeStore) Get(ctx context.Context, key string, version *Version) (string, *Version, error) {
	f.m.Lock()

	if version != nil {
		// TODO(mwhittaker): spetrovic@ pointed out a subtle flaw in this code. If
		// version is Missing, then we may miss updates. For example,
		//
		//     1. We call Put(key) to write version V1.
		//     2. We call Get(key, &Missing). This ends up waiting on f.changed.
		//     3. Some other thread calls Put(key) to write version V2, then
		//        Delete(key).
		//     4. Thread from step (2) wakes up, notices the version is Missing,
		//        then goes to wait again.
		//
		// Technically, this doesn't break the Get API, as Gets are allowed to skip
		// versions, but it is not ideal. I need to implement some code to avoid
		// it.
		for !f.hasChanged(key, version) {
			c := make(chan struct{})
			f.waiters = append(f.waiters, c)
			f.m.Unlock()
			select {
			case <-ctx.Done():
				return "", nil, ctx.Err()
			case <-c:
			}
			f.m.Lock()
		}
	}

	value, latest := f.getValue(key)
	f.m.Unlock()
	return value, latest, nil
}

func (f *FakeStore) Delete(_ context.Context, key string) error {
	f.m.Lock()
	defer f.m.Unlock()

	delete(f.data, key)
	f.wakeupWaiters()
	return nil
}

func (f *FakeStore) List(_ context.Context, opts ListOptions) ([]string, error) {
	f.m.Lock()
	defer f.m.Unlock()

	keys := []string{}
	for key := range f.data {
		if opts.Prefix != "" && !strings.HasPrefix(string(key), opts.Prefix) {
			continue
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Writes returns the number of modifications made to the store so far.
func (f *FakeStore) Writes() int {
	f.m.Lock()
	defer f.m.Unlock()
	return f.global
}
