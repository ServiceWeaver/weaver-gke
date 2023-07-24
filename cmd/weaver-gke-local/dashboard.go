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
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/perfetto"
	"github.com/ServiceWeaver/weaver/runtime/traces"
)

var (
	//go:embed templates/traces.html
	tracesHTML     string
	tracesTemplate = template.Must(template.New("traces").Funcs(template.FuncMap{
		"sub": func(endTime, startTime time.Time) string {
			return endTime.Sub(startTime).String()
		},
	}).Parse(tracesHTML))
)

var dashboardSpec = tool.DashboardSpec{
	Tool:       "weaver gke-local",
	Flags:      flag.NewFlagSet("dashboard", flag.ContinueOnError),
	Controller: local.Controller,
	Init: func(ctx context.Context, mux *http.ServeMux) error {
		logger := logging.StderrLogger(logging.Options{})

		// Add the Prometheus handler.
		metricDB, err := metricdb.Open(ctx, local.MetricsFile)
		if err != nil {
			return err
		}
		mux.Handle("/metrics", local.NewPrometheusHandler(metricDB, logger))

		// Add the trace handlers.
		traceDB, err := traces.OpenDB(ctx, local.TracesFile)
		if err != nil {
			return err
		}
		mux.HandleFunc("/traces", func(w http.ResponseWriter, r *http.Request) {
			handleTraces(w, r, traceDB)
		})
		mux.HandleFunc("/tracefetch", func(w http.ResponseWriter, r *http.Request) {
			handleTraceFetch(w, r, traceDB)
		})

		return nil
	},
	AppLinks: func(ctx context.Context, app string) (tool.Links, error) {
		v := url.Values{}
		v.Set("app", app)
		return tool.Links{
			Traces:  "/traces?" + v.Encode(),
			Metrics: "/metrics?" + v.Encode(),
		}, nil
	},
	DeploymentLinks: func(ctx context.Context, app, version string) (tool.Links, error) {
		// TODO(mwhittaker): Add a metrics link to /metrics.
		v := url.Values{}
		v.Set("app", app)
		v.Set("version", version)
		return tool.Links{
			Traces:  "/traces?" + v.Encode(),
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

// handleTraces handles requests to /traces?app=<app>&version=<app_version>
func handleTraces(w http.ResponseWriter, r *http.Request, db *traces.DB) {
	app := r.URL.Query().Get("app")
	version := r.URL.Query().Get("version")
	if app == "" && version == "" {
		http.Error(w, "neither application name or version id provided", http.StatusBadRequest)
	}
	parseDuration := func(arg string) (time.Duration, bool) {
		str := r.URL.Query().Get(arg)
		if str == "" {
			return 0, true
		}
		dur, err := time.ParseDuration(str)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid duration %q", str), http.StatusBadRequest)
			return 0, false
		}
		return dur, true
	}
	latencyLower, ok := parseDuration("lat_low")
	if !ok {
		return
	}
	latencyUpper, ok := parseDuration("lat_hi")
	if !ok {
		return
	}
	onlyErrors := r.URL.Query().Get("errs") != ""

	// Weavelets export traces every 5 seconds. In order to (semi-)guarantee
	// that the database contains all spans for the selected traces, we only
	// fetch traces that ended more than 5+ seconds ago (all spans for such
	// traces should have been exported to the database by now).
	const exportInterval = 5 * time.Second
	const gracePeriod = time.Second
	endTime := time.Now().Add(-1 * (exportInterval + gracePeriod))

	const maxNumTraces = 100
	ts, err := db.QueryTraces(r.Context(), app, version, time.Time{} /*startTime*/, endTime, latencyLower, latencyUpper, onlyErrors, maxNumTraces)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot query trace database: %v", err), http.StatusInternalServerError)
		return
	}

	content := struct {
		Tool    string
		App     string
		Version string
		Traces  []traces.TraceSummary
	}{
		Tool:    "gke-local",
		App:     app,
		Version: version,
		Traces:  ts,
	}
	if err := tracesTemplate.Execute(w, content); err != nil {
		http.Error(w, fmt.Sprintf("cannot display traces: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleTraceFetch handles requests to /tracefetch?trace_id=<trace_id>.
func handleTraceFetch(w http.ResponseWriter, r *http.Request, db *traces.DB) {
	traceID := r.URL.Query().Get("trace_id")
	if traceID == "" {
		http.Error(w, fmt.Sprintf("invalid trace id %q", traceID), http.StatusBadRequest)
		return
	}
	spans, err := db.FetchSpans(r.Context(), traceID)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot fetch spans: %v", err), http.StatusInternalServerError)
		return
	}
	if len(spans) == 0 {
		http.Error(w, "no matching spans", http.StatusNotFound)
		return
	}
	data, err := perfetto.EncodeSpans(spans)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot encode spans: %v", err), http.StatusInternalServerError)
		return
	}
	w.Write(data) //nolint:errcheck // response write error
}
