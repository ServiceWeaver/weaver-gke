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
)

// A Store is a handle to a versioned, linearizable key-value store. When a
// value is put in the store, the store automatically assigns the value a
// version.
//
// There are two types of Puts: versioned and unversioned. An unversioned Put,
// e.g. Put(ctx, key, value, nil), is a blind put that can be used to
// unilaterally create or update a key. A versioned Put, e.g. Put(ctx, key,
// value, version), succeeds only if the latest version of the key is equal to
// the provided version. Versioned Puts can be used to avoid lost updates, a
// phenomenon when multiple clients concurrently write to the same key, and all
// but one of the updates is overwritten. Versioned Puts can be viewed as a
// form of optimistic concurrency control or a form of compare-and-swap.
//
// A special Missing version represents a value that is not present in the
// store. For example, a Put(ctx, key, value, Missing) writes a value to a key,
// but only if the key does not exist in the store. Versioned Puts with the
// Missing version can be used to implement consensus. When multiple clients
// concurrently perform the Put, only one will succeed.
//
// There are also versioned and unversioned Gets. An unversioned Get, e.g.,
// Get(ctx, key, nil), gets the latest value of a key, along with its version.
// A versioned Get on the other hand, e.g. Get(ctx, key, version), blocks until
// the latest version of the key is newer than the provided version. Once it
// is, the value is returned. Versioned Gets allow you to watch the value of a
// key and get notified when it changes.
//
// Deletes, unlike Puts and Gets, are always unversioned.
//
// Example usage:
//
//	// Get, Put, Put, Put, and Delete.
//	value, version, err := store.Get(ctx, "key", nil)
//	version, err = store.Put(ctx, "key", "new value", version)
//	version, err = store.Put(ctx, "key", "newer value", version)
//	version, err = store.Put(ctx, "key", "newest value", version)
//	err = store.Delete(ctx, "key")
//
// Example usage:
//
//	// Create if not exists.
//	version, err := store.Put(ctx, "key", "this create succeeds", &Missing)
//	version, err := store.Put(ctx, "key", "this one doesn't", &Missing)
//
// Example usage:
//
//	// Blind Puts.
//	_, err := store.Put(ctx, "eggs", "green", nil)
//	_, err = store.Put(ctx, "ham", "green", nil)
//	_, err = store.Put(ctx, "sam", "not happy", nil)
//
// Example usage:
//
//	// Watch.
//	value, version, err := store.Get(ctx, "key", nil)
//	for {
//	    value, version, err = store.Get(ctx, "key", version)
//	    // Do something with value...
//	}
//
// Example usage:
//
//	// Stale write.
//	value, version, err := store.Get(ctx, "key", nil)
//	_, err = store.Put(ctx, "key", "this put succeeds", version)
//	_, err = store.Put(ctx, "key", "this one doesn't", version)
//
// An implementation of the Store interface should be thread-safe. You
// should be able to use a single Store from multiple goroutines.
type Store interface {
	// Put writes a value to a key.
	//
	// If version is nil, then the Put is blind (i.e. the write is performed
	// without any version checks). If version is not nil, then the Put
	// succeeds only if the latest version of the key is equal to the provided
	// version. In either case, if the Put succeeds (i.e. the returned error is
	// nil), then the returned version is the version of the value that was
	// just written.
	//
	// If a versioned Put fails because the provided version is stale, then a
	// Stale error is returned.
	Put(ctx context.Context, key, value string, version *Version) (*Version, error)

	// Get gets the value and version associated with a key.
	//
	// If version is nil, then Get simply gets the latest value of the key,
	// along with its version. This is called an unversioned Get. Note that if
	// the key is not present in the store, the returned value is empty, the
	// returned version is Missing, and the returned error is nil.
	//
	// If version is not nil, called an unversioned Get, then Get blocks until
	// the latest value of the key is newer than the provided version. Once it
	// is, Get returns the value and its version. More precisely, calling
	// store.Get(ctx, key, version) is equivalent to running the following
	// code:
	//
	//     for {
	//         // Note the nil. This is an unversioned Get.
	//         value, latest, err := store.Get(ctx, key, nil)
	//         if err != nil {
	//		       return "", Version{}, err
	//         }
	//         if *version != *latest {
	//             return value, latest, nil
	//         }
	//     }
	//
	// That is, performing a versioned Get is logically equivalent to
	// repeatedly performing a unversioned Get until the returned version is
	// different (and therefore newer because a Store is linearizable). Note
	// that this code demonstrates the semantics of a versioned Get, but is
	// clearly inefficient. Actual implementations of a versioned Get preserve
	// these semantics but are significantly more efficient.
	//
	// Also beware that the version provided to Get and the version returned by
	// Get may not be contiguous. The key may have taken on different values
	// with different versions between the two.
	//
	// A store is permitted to prematurely cancel a versioned Get (e.g., if the
	// store has too many hanging gets). In this case, an error that wraps
	// Unchanged is returned.
	Get(ctx context.Context, key string, version *Version) (string, *Version, error)

	// Delete deletes a value from the store. Unlike Puts, Deletes are always
	// unversioned. Deleting a key that is already missing is a noop and
	// doesn't return an error.
	Delete(ctx context.Context, key string) error

	// List returns the set of keys currently stored in the store. Keys can be
	// returned in any order, and every key appears exactly once in the
	// returned list. For example, store.List(ctx) can return ["a", "b"] or
	// ["b", "a"] but not ["a", "a", "b"]. List is linearizable.
	//
	// Unlike Put, Get, and Delete which are core Store operations, List should
	// only be used for debugging and introspection.
	//
	// TODO(mwhittaker): sanjay@ pointed out that List requires all keys to be
	// present in memory. One alternative he suggested is to provide
	// start/limit keys and constrain the returned keys to this range.
	//
	// TODO(mwhittaker): Implementing List in a linearizable way is
	// challenging. Because List is only for debugging, we may want to
	// guarantee a weaker form of consistency to make it easier to implement.
	List(ctx context.Context) ([]string, error)
}

// Version is the version associated with a value in the store. Versions are
// opaque entities and should not be inspected or interpreted. Versions can be
// compared for equality (e.g., version == Missing), but otherwise do not
// satisfy any properties. For example, they shouldn't be compared with one
// another using an inequality (e.g., version1 < version2). Versions may also
// not be unique across keys. Versions should only ever be constructed by
// calling Get and should only ever be used by passing them to Put.
type Version struct {
	Opaque string
}

// Missing is the version associated with a value that does not exist in the
// store.
var Missing Version = Version{"__tombstone__"}

// Stale is the error returned when a versioned Put fails because of a stale
// version. You can check to see if an error is Stale using errors.Is:
//
//	    value, version, err := store.Get(ctx, "key", nil)
//	    version, err = store.Put(ctx, "key", "value", version)
//	    if errors.Is(err, Stale{}) {
//		       // Do something with err.
//	    }
type Stale struct {
	// stale and newer are not exported because users should not be getting
	// versions from errors. If a Put leads to a Stale, a user should issue
	// another Get.

	// stale is the stale version the user expected to see.
	stale Version

	// newer is the newer version that was encountered. If newer is nil, then
	// we don't know the newer version; all we know is the stale version.
	newer *Version
}

// NewStale returns a new Stale error with stale (the stale version the user
// expected to see) and newer (the newer version that was encountered). If the
// newer version is unknown, nil can be passed for newer.
func NewStale(stale Version, newer *Version) Stale {
	return Stale{stale, newer}
}

// Error implements the error interface.
func (b Stale) Error() string {
	if b.newer == nil {
		return fmt.Sprintf("stale version %v", b.stale)
	}
	return fmt.Sprintf("stale version %v has newer version %v", b.stale, *b.newer)
}

// Is returns true iff target is a Stale error.
func (b Stale) Is(target error) bool {
	_, ok := target.(Stale)
	return ok
}

// Unchanged is returned when a versioned Get is cancelled prematurely. A store
// may cancel a versioned Get, for example, as a form of admission control if
// the store has too many hanging gets.
//
//lint:ignore ST1012 Unchanged is an expected outcome and so we do not prefix with Err
var Unchanged = errors.New("unchanged versioned get")
