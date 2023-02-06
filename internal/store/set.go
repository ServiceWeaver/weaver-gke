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

// AddToSet adds an element to the set name in the store.
func AddToSet(ctx context.Context, store Store, name string, element string) error {
	f := func(value string, version *Version) (string, error) {
		var info SetProto
		if *version != Missing {
			if err := proto.Unmarshal([]byte(value), &info); err != nil {
				return "", err
			}
		}

		// x is already in the set.
		for _, elem := range info.Elements {
			if element == string(elem) {
				return value, ErrUnchanged
			}
		}

		// x is not already in the set.
		info.Elements = append(info.Elements, []byte(element))
		bytes, err := proto.Marshal(&info)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	}

	_, _, err := apply(ctx, store, name, f)
	return err
}

// GetSet returns the elements of a set name from the store.
//
// Notably, if version is nil, then the Get is a non-blocking Get, and if
// version is not nil, then the Get is a blocking watch.
func GetSet(ctx context.Context, store Store, name string, version *Version) (
	[]string, *Version, error) {
	var info SetProto
	version, err := GetProto(ctx, store, name, &info, version)
	if err != nil {
		return nil, version, err
	}
	var result []string
	for _, elem := range info.Elements {
		result = append(result, string(elem))
	}
	return result, version, nil
}
