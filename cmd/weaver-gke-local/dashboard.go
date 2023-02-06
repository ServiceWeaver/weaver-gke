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
	"fmt"

	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

var dashboardSpec = tool.DashboardSpec{
	Tool:       "weaver gke-local",
	Flags:      flag.NewFlagSet("dashboard", flag.ContinueOnError),
	Controller: local.Nanny,
	AppLinks: func(ctx context.Context, app string) (tool.Links, error) {
		// TODO(mwhittaker): Add a metrics and tracing link, to /metrics and
		// perfetto respectively.
		return tool.Links{}, nil
	},
	DeploymentLinks: func(ctx context.Context, depId string) (tool.Links, error) {
		// TODO(mwhittaker): Add a metrics and tracing link, to /metrics and
		// perfetto respectively.
		return tool.Links{}, nil
	},
	AppCommands: func(app string) []tool.Command {
		return []tool.Command{
			{Label: "status", Command: "weaver gke-local status"},
			{Label: "cat logs", Command: fmt.Sprintf("weaver gke-local logs 'app==%q'", app)},
			{Label: "follow logs", Command: fmt.Sprintf("weaver gke-local logs --follow 'app==%q'", app)},
			{Label: "kill", Command: fmt.Sprintf("weaver gke-local kill %s", app)},
		}
	},
	DeploymentCommands: func(id string) []tool.Command {
		return []tool.Command{
			{Label: "status", Command: "weaver gke-local status"},
			{Label: "cat logs", Command: fmt.Sprintf("weaver gke-local logs 'version==%q'", logging.Shorten(id))},
			{Label: "follow logs", Command: fmt.Sprintf("weaver gke-local logs --follow 'version==%q'", logging.Shorten(id))},
			{Label: "profile", Command: fmt.Sprintf("weaver multi profile --duration=30s %s", id)},
		}
	},
}
