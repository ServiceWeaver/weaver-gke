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
	"flag"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

var deploySpec = tool.DeploySpec{
	Tool:  "weaver gke-local",
	Flags: flag.NewFlagSet("deploy", flag.ContinueOnError),
	Controller: func(ctx context.Context, _ *config.GKEConfig) (string, *http.Client, error) {
		return local.Controller(ctx)
	},
	PrepareRollout: local.PrepareRollout,
	Source: func(context.Context, *config.GKEConfig) (logging.Source, error) {
		return logging.FileSource(local.LogDir), nil
	},
}
