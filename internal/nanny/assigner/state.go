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

package assigner

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// assignerStateKey is the store key that stores an assigner's state.
const assignerStateKey = "assigner_state"

// loadState loads the assigner's state from the store.
func (a *Assigner) loadState(ctx context.Context) (*AssignerState, *store.Version, error) {
	var state AssignerState
	key := store.GlobalKey(assignerStateKey)
	version, err := store.GetProto(ctx, a.store, key, &state, nil)
	return &state, version, err
}

// applyState applies a read-modify-write operation to the assigner's state.
// The current state is read from the store and passed to the provided apply
// function. This function modifies the state and returns true, or it leaves
// the state unchanged and returns false. If apply returns true, the state is
// then written back to the store. The read-modify-write operation is retried
// if there are conflicting writes.
func (a *Assigner) applyState(ctx context.Context, apply func(*AssignerState) bool) (*AssignerState, *store.Version, error) {
	return applyProto(ctx, a.store, store.GlobalKey(assignerStateKey), apply)
}

// appStateKey returns the key into which we persist the provided app's state.
func appStateKey(app string) string {
	return path.Join("/", "assigner", "application", app)
}

// loadAppState loads an application's state from the store.
func (a *Assigner) loadAppState(ctx context.Context, app string) (*AppState, *store.Version, error) {
	var state AppState
	version, err := store.GetProto(ctx, a.store, appStateKey(app), &state, nil)
	return &state, version, err
}

// saveAppState saves an application's state in the store.
func (a *Assigner) saveAppState(ctx context.Context, app string, state *AppState, version *store.Version) (*store.Version, error) {
	return store.PutProto(ctx, a.store, appStateKey(app), state, version)
}

// applyAppState applies a read-modify-write operation to the provided app's
// state. The current state is read from the store and passed to the provided
// apply function. This function modifies the state and returns true, or it
// leaves the state unchanged and returns false. If apply returns true, the
// state is then written back to the store. The read-modify-write operation is
// retried if there are conflicting writes.
func (a *Assigner) applyAppState(ctx context.Context, app string, apply func(*AppState) bool) (*AppState, *store.Version, error) {
	return applyProto(ctx, a.store, appStateKey(app), apply)
}

// replicaSetInfoKey returns the key into which we persist the ReplicaSetInfo.
func replicaSetInfoKey(rid *ReplicaSetId) string {
	// TODO(mwhittaker): Remove the requirement that all deployment ids are
	// uuids. We may want to, for example, use more human readable deployment
	// ids. This also makes it easier to write tests.
	id, err := uuid.Parse(rid.Id)
	if err != nil {
		panic(fmt.Sprintf("invalid deployment id %q: %v", rid.Id, err))
	}
	return store.ReplicaSetKey(rid.App, id, rid.Name, "replica_set_info")
}

// loadReplicaSetInfo loads a Kubernetes ReplicaSet's info from the store.
func (a *Assigner) loadReplicaSetInfo(ctx context.Context, rid *ReplicaSetId) (*ReplicaSetInfo, *store.Version, error) {
	rs := &ReplicaSetInfo{}
	version, err := store.GetProto(ctx, a.store, replicaSetInfoKey(rid), rs, nil)
	if err != nil {
		return nil, nil, err
	}
	if rs.Components == nil {
		rs.Components = map[string]*Assignment{}
	}
	if rs.Pods == nil {
		rs.Pods = map[string]*Pod{}
	}
	return rs, version, err
}

// applyReplicaSetInfo applies a read-modify-write operation to the provided
// Kubernetes ReplicaSet's state. The current state is read from the store and
// passed to the provided apply function. This function modifies the state and
// returns true, or it leaves the state unchanged and returns false. If apply
// returns true, the state is then written back to the store.
// The read-modify-write operation is retried if there are conflicting writes.
func (a *Assigner) applyReplicaSetInfo(ctx context.Context, rid *ReplicaSetId, apply func(*ReplicaSetInfo) bool) (*ReplicaSetInfo, *store.Version, error) {
	return applyProto(ctx, a.store, replicaSetInfoKey(rid), apply)
}

// protoPointer[T] is an interface which asserts that *T is a proto.Message.
// See [1] for an overview of this idiom.
//
// [1]: https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type protoPointer[T any] interface {
	*T
	proto.Message
}

// applyProto is a thin wrapper around store.UpdateProto. The apply function is
// passed in the current state of the proto. It should make any necessary
// changes and then return whether the proto was changed.
func applyProto[T any, P protoPointer[T]](ctx context.Context, s store.Store, key string, apply func(P) bool) (P, *store.Version, error) {
	var state T
	version, err := store.UpdateProto(ctx, s, key, P(&state), func(*store.Version) error {
		if apply(&state) {
			return nil
		}
		return store.ErrUnchanged
	})
	if errors.Is(err, store.ErrUnchanged) {
		err = nil
	}
	return &state, version, err
}
