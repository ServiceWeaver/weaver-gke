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

package controller

import (
	"context"
	"path"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
)

// This file contains code to read from and write to the store.

const (
	// controllerStateKey is the store key that stores a controller's state.
	controllerStateKey = "controller_state"

	// submissionIdKey is the store key where we store a counter to generate
	// unique, monotonically increasing submission ids.
	submissionIdKey = "submission_ids"
)

// loadState loads the distributor's state from the store.
func (c *controller) loadState(ctx context.Context) (*ControllerState, *store.Version, error) {
	var state ControllerState
	key := store.GlobalKey(controllerStateKey)
	version, err := store.GetProto(ctx, c.store, key, &state, nil)
	if err != nil {
		return nil, nil, err
	}
	if state.Applications == nil {
		state.Applications = map[string]bool{}
	}
	if state.Distributors == nil {
		state.Distributors = map[string]*DistributorState{}
	}
	return &state, version, err
}

// saveState saves the distributor's state in the store.
func (c *controller) saveState(ctx context.Context, state *ControllerState, version *store.Version) error {
	key := store.GlobalKey(controllerStateKey)
	_, err := store.PutProto(ctx, c.store, key, state, version)
	return err
}

// appStateKey returns the key into which we persist the provided app's state.
func appStateKey(app string) string {
	return path.Join("/", "controller", "application", app)
}

// loadAppState loads an application's state from the store.
func (c *controller) loadAppState(ctx context.Context, app string) (*AppState, *store.Version, error) {
	var state AppState
	version, err := store.GetProto(ctx, c.store, appStateKey(app), &state, nil)
	if err != nil {
		return nil, nil, err
	}
	if state.Versions == nil {
		state.Versions = map[string]*AppVersionState{}
	}
	return &state, version, nil
}

// saveAppState saves an application's state in the store.
func (c *controller) saveAppState(ctx context.Context, app string, state *AppState, version *store.Version) error {
	_, err := store.PutProto(ctx, c.store, appStateKey(app), state, version)
	return err
}

// nextSubmissionID generates a unique, monotonically increasing id. Every
// invocation of nextSubmissionID returns a unique id, even invocations in
// different controller instances.
func nextSubmissionID(ctx context.Context, s store.Store) (int, error) {
	id := &SubmissionId{}
	edit := func(version *store.Version) error {
		id.Id++
		return nil
	}
	key := store.GlobalKey(submissionIdKey)
	_, err := store.UpdateProto(ctx, s, key, id, edit)
	if err != nil {
		return -1, err
	}
	return int(id.Id), nil
}
