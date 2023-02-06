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

	"google.golang.org/protobuf/proto"
)

// PutProto calls proto.Marshal on a value and then puts it into the underlying
// store. See Store.Put for details.
func PutProto(ctx context.Context, store Store, key string,
	value proto.Message, version *Version) (*Version, error) {
	bytes, err := proto.Marshal(value)
	if err != nil {
		return nil, err
	}
	return store.Put(ctx, key, string(bytes), version)
}

// GetProto gets a value from the store and then calls proto.Unmarshal on it.
// If the value is missing, value is left untouched. See Store.Get for details.
func GetProto(ctx context.Context, store Store, key string, value proto.Message,
	version *Version) (*Version, error) {
	bytes, version, err := store.Get(ctx, key, version)
	if err != nil {
		return nil, err
	}
	// If the value is missing, we don't have anything to unmarshal.
	if *version == Missing {
		return version, nil
	}
	if err = proto.Unmarshal([]byte(bytes), value); err != nil {
		return nil, err
	}
	return version, nil
}

// UpdateProto atomically applies the read-modify-write operation edit,
// retrying if there are collisions. You can think of edit as a very basic type
// of transaction. For example, we can atomically increment a counter up to a
// limit of 10 like this:
//
//	var counter &Counter{Val: 0}
//	increment := func(*Version) error {
//		if counter.Val >= 10 {
//			return ErrUnchanged
//		}
//		counter.Val++
//		return nil
//	}
//	version, err := UpdateProto(ctx, store, key, &counter, increment)
//	fmt.Println(counter.Val) // The incremented value of the counter.
//
// Specifically, UpdateProto will repeatedly perform the following:
//
//  1. Get the current value and version of key and unmarshal it into the
//     provided proto. If key is currently missing, nothing is unmarshalled,
//     and the version is Missing.
//  2. Execute edit, which may update the provided proto. If edit returns a
//     non-nil error, then UpdateProto returns immediately with the error.
//  3. Otherwise, UpdateProto performs a versioned Put to atomically write the
//     new value of the proto. If the write succeeds, UpdateProto returns the
//     new version. If the write failed because of a stale version,
//     UpdateProto retries, starting again at Step 1. If the write fails with
//     some other error, apply aborts and returns the error.
func UpdateProto(ctx context.Context, store Store, key string, obj proto.Message, edit func(version *Version) error) (*Version, error) {
	// A function that wraps edit with conversion to/from obj.
	wrapper := func(value string, version *Version) (string, error) {
		if err := proto.Unmarshal([]byte(value), obj); err != nil {
			return "", err
		}
		if err := edit(version); err != nil { // May often by ErrUnchanged
			return "", err
		}
		bytes, err := proto.Marshal(obj)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	}
	_, version, err := apply(ctx, store, key, wrapper)
	return version, err
}
