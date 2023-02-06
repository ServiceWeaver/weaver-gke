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

package gke

import (
	"context"
	"errors"
	"testing"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
)

func TestKubeStore(t *testing.T) {
	maker := func(_ *testing.T) store.Store {
		return newKubeStore(newFakeConfigMapper())
	}
	store.StoreTester{Make: maker}.Run(t)
}

func TestTooBig(t *testing.T) {
	store := newKubeStore(newFakeConfigMapper())
	ctx := context.Background()

	// The limit for len(key) + len(value) is 1 MiB. Here, len(key) +
	// len(value) is exactly 1 MiB, so it's okay.
	key := ""
	value := [1024 * 1024]byte{}
	_, err := store.Put(ctx, key, string(value[:]), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now, we're one byte over the limit.
	key = "k"
	_, err = store.Put(ctx, key, string(value[:]), nil)
	if !errors.Is(err, tooBigError{}) {
		t.Fatalf("wanted TooBigError, got %v", err)
	}
}
