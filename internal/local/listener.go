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

package local

import (
	"context"
	"errors"
	"fmt"

	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// RecordListener records the network listener exported by an application
// version.
func RecordListener(ctx context.Context, s store.Store, cfg *config.GKEConfig, lis *nanny.Listener) error {
	// Record the listener by adding a serialized version of the listener
	// to internal.ListenState, stored under an application-version-scoped key
	// "listeners" (e.g. /app/todo/deployment/a47e1a97/listeners).
	state := &ListenState{}
	addListener := func(*store.Version) error {
		// Add spec into state.Listeners, but only if it doesn't already exist.
		for _, existing := range state.Listeners {
			if proto.Equal(lis, existing) {
				return store.ErrUnchanged
			}
		}
		state.Listeners = append(state.Listeners, lis)
		return nil
	}

	dep := cfg.Deployment
	key := store.DeploymentKey(dep.App.Name, uuid.Must(uuid.Parse(dep.Id)), "listeners")
	histKey := store.DeploymentKey(dep.App.Name, uuid.Must(uuid.Parse(dep.Id)), store.HistoryKey)
	err := store.AddToSet(ctx, s, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		// Track the key in the store under histKey.
		return fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = store.UpdateProto(ctx, s, key, state, addListener)
	if errors.Is(err, store.ErrUnchanged) {
		err = nil
	}
	return err
}

// getListeners returns network listeners in the given store associated with
// a given application version.
//
// If a replica that exports a listener dies, we don't update the traffic rules
// to reflect the new set of listeners in case of a gke-local deployer. Note
// that this is intentional, as we don't worry about processes deaths.
func getListeners(ctx context.Context, s store.Store, cfg *config.GKEConfig) ([]*nanny.Listener, error) {
	dep := cfg.Deployment
	key := store.DeploymentKey(dep.App.Name, uuid.Must(uuid.Parse(dep.Id)), "listeners")
	var state ListenState
	if _, err := store.GetProto(ctx, s, key, &state, nil); err != nil {
		return nil, err
	}
	return state.Listeners, nil
}
