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
	"net/http"
	"net/url"
	"os"

	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/perfetto"
)

var dashboardSpec = tool.DashboardSpec{
	Tool:       "weaver gke-local",
	Flags:      flag.NewFlagSet("dashboard", flag.ContinueOnError),
	Controller: local.Nanny,
	Init: func(ctx context.Context, mux *http.ServeMux) error {
		logger := logging.StderrLogger(logging.Options{})

		// Add the Prometheus handler.
		metricDB, err := metricdb.Open(ctx, local.MetricsFile)
		if err != nil {
			return err
		}
		mux.Handle("/metrics", local.NewPrometheusHandler(metricDB, logger))

		// Start a separate Perfetto server, which has to run on
		// a specific port.
		db, err := perfetto.Open(ctx, "gke-local")
		if err != nil {
			return err
		}
		go func() {
			defer db.Close()
			if db.Serve(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "Error serving local traces", err)
			}
		}()
		return nil
	},
	AppLinks: func(ctx context.Context, app string) (tool.Links, error) {
		v := url.Values{}
		v.Set("app", app)
		tracerURL := url.QueryEscape("http://127.0.0.1:9001?" + v.Encode())
		return tool.Links{
			Traces:  "https://ui.perfetto.dev/#!/?url=" + tracerURL,
			Metrics: "/metrics?" + v.Encode(),
		}, nil
	},
	DeploymentLinks: func(ctx context.Context, app, version string) (tool.Links, error) {
		// TODO(mwhittaker): Add a metrics link to /metrics.
		v := url.Values{}
		v.Set("app", app)
		v.Set("version", version)
		tracerURL := url.QueryEscape("http://127.0.0.1:9001?" + v.Encode())
		return tool.Links{
			Traces:  "https://ui.perfetto.dev/#!/?url=" + tracerURL,
			Metrics: "/metrics?" + v.Encode(),
		}, nil
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
