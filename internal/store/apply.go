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

	"github.com/ServiceWeaver/weaver/runtime/retry"
)

// apply atomically applies the read-modify-write operation f, retrying if
// there are collisions. You can think of f as a very basic type of
// transaction. For example, we can atomically increment a counter like this:
//
//	increment := func(ctx context.Context, value string, version *Version) (string, error) {
//	    var x int
//	    if *version != store.Missing {
//	        var err error
//	        if x, err = strconv.Atoi(value); err != nil {
//	            return "", err
//	        }
//	    }
//
//	    x += 1
//	    return strconv.Itoa(x), nil
//	}
//
//	// x is the latest value of the counter.
//	value, version, err := apply(ctx, store, key, increment)
//	x, err := strconv.Atoi(value)
//
// Specifically, apply will repeatedly perform the following:
//
//  1. Get the current value and version of key. If key is currently
//     missing, the value is "" and the version is Missing.
//  2. apply f to the current value and version to get a new value. If f
//     returns an error matching ErrUnchanged, apply returns
//     the current value and version and the ErrUnchanged error
//     without writing the result of f. If f returns another error
//     apply aborts and returns the error.
//  3. Perform a versioned Put to atomically write the new value returned by
//     f. If the write succeeds, apply returns the written value and
//     version. If the write failed because of a stale version, apply
//     retries, starting again at Step 1. If the write fails with some other
//     error, apply aborts and returns the error.
func apply(ctx context.Context, s Store, key string,
	f func(value string, version *Version) (string, error)) (string, *Version, error) {
	for r := retry.Begin(); r.Continue(ctx); {
		value, version, err := s.Get(ctx, key, nil)
		if err != nil {
			return "", nil, err
		}

		newValue, err := f(value, version)
		if err != nil {
			if errors.Is(err, ErrUnchanged) {
				return value, version, err
			}
			return "", nil, err
		}
		newVersion, err := s.Put(ctx, key, newValue, version)
		if errors.Is(err, Stale{}) {
			continue
		} else if err != nil {
			return "", nil, err
		}
		return newValue, newVersion, nil
	}
	return "", nil, ctx.Err()
}

type errUnchanged uint8

func (e errUnchanged) Error() string { return "value not changed" }

// ErrUnchanged is an error that indicates that a requested change was not necessary.
const ErrUnchanged errUnchanged = 0
