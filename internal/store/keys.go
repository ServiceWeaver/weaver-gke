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
	"strings"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
)

const (
	// HistoryKey is the key where we store the set of keys belonging to a
	// particular application version. The key should be scoped to an
	// application version. For example:
	//
	//     /app/collatz/version/a47e1a97/key_history
	//     /app/todo/version/fd578a20/key_history
	HistoryKey = "key_history"
)

// The following Key functions provide a weak form of isolation between
// applications, allowing them to use arbitrary keys without fear of
// collisions. For example, a todo app and chat app can both use the same key
// "status" without a collision:
//
//     // "status" gets converted to "/app/todo/version/123/status"
//     id := uuid.MustParse("123")
//     key := AppVersionKey("todo", id, "status")
//     store.Put(ctx, key, "running", nil)
//
//     // "status" gets converted to "/app/chat/version/456/status"
//     id := uuid.MustParse("456")
//     key := AppVersionKey("chat", id, "status")
//     store.Put(ctx, key, "terminating", nil)
//
// We have the following Key functions. Assume an app named collatz, an
// application version 123, and a ReplicaSet named OddEven.
//
//     1. GlobalKey:      "/key".
//     2. AppKey:         "/app/collatz/key".
//     3. AppVersionKey:  "/app/collatz/version/123/key".
//     4. ReplicaSetKey:  "/app/collatz/version/123/replica_set/OddEven/key".

func join(elements ...string) string {
	// Note that we don't use path.Join because we allow empty keys and
	// path.Join does not. For example, path.Join("/", "", "a") is "/a", but we
	// want it to be "//a".
	//
	// TODO(mwhittaker): Disallow empty names in Service Weaver.
	return "/" + strings.Join(elements, "/")
}

// GlobalKey returns keys in the format "/key".
func GlobalKey(key string) string {
	return join(key)
}

// AppKey returns keys in the format "/app/collatz/key", where "collatz" is the
// application name.
func AppKey(app string, key string) string {
	return join("app", app, key)
}

// AppVersionKey returns keys in the format "/app/collatz/version/123/key",
// where "collatz" is the application name and 123 is the application version id.
func AppVersionKey(cfg *config.GKEConfig, key string) string {
	dep := cfg.Deployment
	return join("app", dep.App.Name, "version", dep.Id, key)
}

// ReplicaSetKey returns keys in the format
// "/app/collatz/version/123/replica_set/OddEven/key", where "collatz" is the
// application name, 123 is the application version id, and OddEven is the
// Kubernetes ReplicaSet name.
func ReplicaSetKey(cfg *config.GKEConfig, replicaSet string, key string) string {
	dep := cfg.Deployment
	return join("app", dep.App.Name, "version", dep.Id, "replica_set", replicaSet, key)
}
