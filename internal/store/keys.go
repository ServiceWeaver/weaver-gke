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

	"github.com/google/uuid"
)

const (
	// HistoryKey is the key where we store the set of keys belonging to a
	// particular deployment. The key should be scoped to a deployment. For
	// example:
	//
	//     /app/collatz/deployment/a47e1a97/key_history
	//     /app/todo/deployment/fd578a20/key_history
	HistoryKey = "key_history"
)

// The following Key functions provide a weak form of isolation between
// applications, allowing them to use arbitrary keys without fear of
// collisions. For example, a todo app and chat app can both use the same key
// "status" without a collision:
//
//     // "status" gets converted to "/app/todo/deployment/123/status"
//     id := uuid.MustParse("123")
//     key := DeploymentKey("todo", id, "status")
//     store.Put(ctx, key, "running", nil)
//
//     // "status" gets converted to "/app/chat/deployment/456/status"
//     id := uuid.MustParse("456")
//     key := DeploymentKey("chat", id, "status")
//     store.Put(ctx, key, "terminating", nil)
//
// We have the following Key functions. Assume an app named collatz, a
// deployment id 123, and a ReplicaSet named OddEven.
//
//     1. GlobalKey:      "/key".
//     2. ApplicationKey: "/app/collatz/key".
//     3. DeploymentKey:  "/app/collatz/deployment/123/key".
//     4. ReplicaSetKey:  "/app/collatz/deployment/123/replica_set/OddEven/key".

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

// DeploymentKey returns keys in the format "/app/collatz/deployment/123/key",
// where "collatz" is the application name and 123 is the deployment id.
func DeploymentKey(app string, deploymentID uuid.UUID, key string) string {
	return join("app", app, "deployment", deploymentID.String(), key)
}

// ReplicaSetKey returns keys in the format
// "/app/collatz/deployment/123/replica_set/OddEven/key", where "collatz" is the
// application name, 123 is the deployment id, and OddEven is the Kubernetes
// ReplicaSet name.
func ReplicaSetKey(app string, deploymentID uuid.UUID, replicaSet string, key string) string {
	return join("app", app, "deployment", deploymentID.String(), "replica_set", replicaSet, key)
}
