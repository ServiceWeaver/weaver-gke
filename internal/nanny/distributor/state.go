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

package distributor

import (
	"context"
	"path"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
)

// This file contains code to read from and write to the store.

// distributorStateKey is the store key that stores a distributor's state.
const distributorStateKey = "distributor_state"

// loadState loads the distributor's state from the store.
func (d *Distributor) loadState(ctx context.Context) (*DistributorState, *store.Version, error) {
	var state DistributorState
	key := store.GlobalKey(distributorStateKey)
	version, err := store.GetProto(ctx, d.store, key, &state, nil)
	return &state, version, err
}

// saveState saves the distributor's state in the store.
func (d *Distributor) saveState(ctx context.Context, state *DistributorState, version *store.Version) error {
	key := store.GlobalKey(distributorStateKey)
	_, err := store.PutProto(ctx, d.store, key, state, version)
	return err
}

// appStateKey returns the key into which we persist the provided app's state.
func appStateKey(app string) string {
	return path.Join("/", "distributor", "application", app)
}

// loadAppState loads an application's state from the store.
func (d *Distributor) loadAppState(ctx context.Context, app string) (*AppState, *store.Version, error) {
	var state AppState
	version, err := store.GetProto(ctx, d.store, appStateKey(app), &state, nil)
	return &state, version, err
}

// saveAppState saves an application's state in the store.
func (d *Distributor) saveAppState(ctx context.Context, app string, state *AppState, version *store.Version) (*store.Version, error) {
	return store.PutProto(ctx, d.store, appStateKey(app), state, version)
}
