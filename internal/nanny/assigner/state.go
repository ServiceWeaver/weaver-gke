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
	"path"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"google.golang.org/protobuf/proto"
)

// replicaSetStateKey returns the key into which we persist the ReplicaSetState.
func replicaSetStateKey(cfg *config.GKEConfig, replicaSet string) string {
	app := cfg.Deployment.App.Name
	version := cfg.Deployment.Id
	return path.Join("/assigner", "app", app, "version", version, "replica_set", replicaSet, "state")
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
func applyProto[T any, P protoPointer[T]](ctx context.Context, s store.Store, key string, apply func(P, *store.Version) bool) (P, *store.Version, error) {
	var state T
	version, err := store.UpdateProto(ctx, s, key, P(&state), func(v *store.Version) error {
		if apply(&state, v) {
			return nil
		}
		return store.ErrUnchanged
	})
	if errors.Is(err, store.ErrUnchanged) {
		err = nil
	}
	return &state, version, err
}
