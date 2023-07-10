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

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

var deploySpec = tool.DeploySpec{
	Tool: "weaver gke",
	Controller: func(ctx context.Context, cfg *config.GKEConfig) (string, *http.Client, error) {
		config, err := gke.SetupCloudConfig(cfg.Project, cfg.Account)
		if err != nil {
			return "", nil, err
		}
		return gke.Controller(ctx, config)
	},
	PrepareRollout: func(ctx context.Context, cfg *config.GKEConfig) (*controller.RolloutRequest, error) {
		config, err := gke.SetupCloudConfig(cfg.Project, cfg.Account)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "Using account %s in project %s\n",
			config.Account, config.Project)
		toolBinVersion, err := getToolVersion()
		if err != nil {
			return nil, fmt.Errorf("error extracting the tool binary version: %w", err)
		}
		return gke.PrepareRollout(ctx, config, cfg, toolBinVersion)
	},
	Source: func(ctx context.Context, cfg *config.GKEConfig) (logging.Source, error) {
		config, err := gke.SetupCloudConfig(cfg.Project, cfg.Account)
		if err != nil {
			return nil, err
		}
		return gke.LogSource(config)
	},
}

// getToolVersion returns the version of the tool binary.
func getToolVersion() (string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		// Should never happen.
		return "", fmt.Errorf("tool binary must be built from a module")
	}
	version := info.Main.Version
	const develToolVersion = "(devel)"
	if version == develToolVersion {
		// Locally-compiled tool binary. Return a version that's guaranteed to
		// be lexicographically greater than any previously returned version
		// (e.g., v0.10.0).
		return fmt.Sprintf("vdevel%d", time.Now().Unix()), nil
	}
	return info.Main.Version, nil
}
