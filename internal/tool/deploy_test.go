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
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
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
			name: "basic",
			config: `
[gke]
regions = ["us-central1"]`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 1,
				Regions:     []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "simple",
			config: `
[gke]
mtls = true
minreplicas = 2
regions = ["us-central1"]
`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 2,
				Mtls:        true,
				Regions:     []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "custom image",
			config: `
[gke]
image = "custom-image"
regions = ["us-central1"]
`,
			expect: &config.GKEConfig{
				Image:       "custom-image",
				MinReplicas: 1,
				Regions:     []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "long-key",
			config: `
["github.com/ServiceWeaver/weaver-gke/internal/gke"]
mtls = true
regions = ["us-central1"]
`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 1,
				Mtls:        true,
				Regions:     []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "public-listeners",
			config: `
[gke]
listeners.a = {is_public=true, hostname="a.com"}
listeners.b = {is_public=true, hostname="b.com"}
regions = ["us-central1"]
`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 1,
				Listeners: map[string]*config.GKEConfig_ListenerOptions{
					"a": {IsPublic: true, Hostname: "a.com"},
					"b": {IsPublic: true, Hostname: "b.com"},
				},
				Regions: []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "public-and-private-listeners",
			config: `
[gke]
listeners.a = {is_public=true, hostname="a.com"}
listeners.b = {hostname="b.com"}
listeners.c = {}
regions = ["us-central1"]
`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 1,
				Listeners: map[string]*config.GKEConfig_ListenerOptions{
					"a": {IsPublic: true, Hostname: "a.com"},
					"b": {IsPublic: false, Hostname: "b.com"},
					"c": {IsPublic: false, Hostname: ""},
				},
				Regions: []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{ExportInterval: durationpb.New(30 * time.Second)},
				},
			},
		},
		{
			name: "metrics",
			config: `
[gke]
regions = ["us-central1"]
telemetry.metrics = {auto_generate_metrics = true, export_interval = "1h"}
`,
			expect: &config.GKEConfig{
				Image:       defaultBaseImage,
				MinReplicas: 1,
				Regions:     []string{"us-central1"},
				Telemetry: &config.Telemetry{
					Metrics: &config.MetricsOptions{
						AutoGenerateMetrics: true,
						ExportInterval:      durationpb.New(1 * time.Hour),
					},
				},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := t.TempDir()
			binary := filepath.Join(d, "bin")
			cmd := exec.Command("go", "build", "-o", binary, "./testprogram")
			err := cmd.Run()
			if err != nil {
				t.Fatal(err)
			}
			text := fmt.Sprintf(`
[serviceweaver]
binary = "%s"

%s`, binary, c.config)
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
			name: "missing_listeners",
			cfg: `
[gke]
listeners.d = {hostname="d.com"}
regions = ["us-central1"]
`,
			expectedError: "not found in the binary",
		},
		{
			name: "public_listener_no_hostname",
			cfg: `
[gke]
listeners.c = {is_public=true}
regions = ["us-central1"]
`,
			expectedError: "no hostname specified",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := t.TempDir()
			binary := filepath.Join(d, "bin")
			cmd := exec.Command("go", "build", "-o", binary, "./testprogram")
			err := cmd.Run()
			if err != nil {
				t.Fatal(err)
			}
			text := fmt.Sprintf(`
[serviceweaver]
binary = "%s"

%s`, binary, c.cfg)
			_, err = parseGKEConfig(c.name, text)
			if err == nil || !strings.Contains(err.Error(), c.expectedError) {
				t.Fatalf("error %v does not contain %q in\n%s", err, c.expectedError, c.cfg)
			}
		})
	}
}
