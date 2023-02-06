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

package tool

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
)

func parseGKEConfig(name string, config string) (*config.GKEConfig, error) {
	app, err := runtime.ParseConfig(name, config, codegen.ComponentConfigValidator)
	if err != nil {
		return nil, err
	}
	return makeGKEConfig(app)
}

func TestMakeGKEConfig(t *testing.T) {
	type testCase struct {
		name   string
		config string
		expect *config.GKEConfig
	}
	for _, c := range []testCase{
		{
			name:   "empty",
			config: ``,
			expect: &config.GKEConfig{},
		},
		{
			name: "simple",
			config: `
[gke]
project = "foo"
account = "bar"
`,
			expect: &config.GKEConfig{Project: "foo", Account: "bar"},
		},
		{
			name: "long-key",
			config: `
["github.com/ServiceWeaver/weaver-gke/internal/gke"]
project = "bar"
`,
			expect: &config.GKEConfig{Project: "bar"},
		},
		{
			name: "listeners",
			config: `
[gke]
public_listener = [
  {name="a", hostname="a.com"},
  {name="b", hostname="b.com"},
]
`,
			expect: &config.GKEConfig{
				PublicListener: []*config.GKEConfig_PublicListener{
					&config.GKEConfig_PublicListener{
						Name:     "a",
						Hostname: "a.com",
					},
					&config.GKEConfig_PublicListener{
						Name:     "b",
						Hostname: "b.com",
					},
				},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			text := "[serviceweaver]\n\n" + c.config
			cfg, err := parseGKEConfig(c.name, text)
			if err != nil {
				t.Fatal(err)
			}
			opts := []cmp.Option{
				protocmp.Transform(),
				protocmp.IgnoreFields(&config.GKEConfig{}, "deployment"),
			}
			if diff := cmp.Diff(c.expect, cfg, opts...); diff != "" {
				t.Fatalf("bad GKE config: (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBadGKEConfig(t *testing.T) {
	type testCase struct {
		name          string
		cfg           string
		expectedError string
	}

	for _, c := range []testCase{
		{
			name: "missing-hostname",
			cfg: `
[serviceweaver]
name = "test"

[gke]
public_listener = [{name = "test"}]

`,
			expectedError: "empty hostname",
		},
		// XXX Bad rollout
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseGKEConfig(c.name, c.cfg)
			if err == nil || !strings.Contains(err.Error(), c.expectedError) {
				t.Fatalf("error %v does not contain %q in\n%s", err, c.expectedError, c.cfg)
			}
		})
	}
}
