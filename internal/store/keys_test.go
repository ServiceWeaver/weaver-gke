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
	"testing"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

func cfg(app, version string) *config.GKEConfig {
	return &config.GKEConfig{
		Deployment: &protos.Deployment{
			App: &protos.AppConfig{
				Name: app,
			},
			Id: version,
		},
	}
}

func TestKeys(t *testing.T) {
	const id = "11111111-1111-1111-1111-111111111111"
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "Global",
			got:  GlobalKey("key"),
			want: "/key",
		},
		{
			name: "Application",
			got:  AppKey("collatz", "key"),
			want: "/app/collatz/key",
		},
		{
			name: "Deployment",
			got:  AppVersionKey(cfg("collatz", id), "key"),
			want: "/app/collatz/version/11111111-1111-1111-1111-111111111111/key",
		},
		{
			name: "ReplicaSet",
			got:  ReplicaSetKey(cfg("collatz", id), "OddEven", "key"),
			want: "/app/collatz/version/11111111-1111-1111-1111-111111111111/replica_set/OddEven/key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("got %q, want %q", test.got, test.want)
			}
		})
	}

}
