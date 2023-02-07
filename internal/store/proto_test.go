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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestUpdateProto(t *testing.T) {
	// Test plan: use a sequence of UpdateProto calls where each one
	// adds a new value to a SetProto stored under key "k". Check
	// at the end that the store contents contain all stored values.
	ctx := context.Background()
	fake := NewFakeStore()
	var data SetProto
	values := []string{"apple", "banana", "canteloupe"}
	for _, v := range values {
		_, err := UpdateProto(ctx, fake, "k", &data, func(*Version) error {
			data.Elements = append(data.Elements, []byte(v))
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected UpdateProto error: %v", err)
		}
	}

	// Call UpdateProto again, but this time do not make any changes
	// so that we can verify the handling of ErrUnchanged.
	_, err := UpdateProto(ctx, fake, "k", &data, func(*Version) error {
		return ErrUnchanged
	})
	if !errors.Is(err, ErrUnchanged) {
		t.Fatalf("unexpected error %v, expecting ErrUnchanged", err)
	}

	// Read final value and check.
	var result SetProto
	if _, err := GetProto(ctx, fake, "k", &result, nil); err != nil {
		t.Fatalf("unexpected GetProto error: %v", err)
	}
	var got []string
	for _, element := range result.Elements {
		got = append(got, string(element))
	}
	if diff := cmp.Diff(values, got); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}
