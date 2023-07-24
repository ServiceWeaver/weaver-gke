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
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

var (
	dashboardFlags = newCloudFlagSet("dashboard", flag.ContinueOnError)
)

var dashboardSpec = tool.DashboardSpec{
	Tool:  "weaver gke",
	Flags: dashboardFlags.FlagSet,
	Controller: func(ctx context.Context) (string, *http.Client, error) {
		config, err := dashboardFlags.CloudConfig()
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(os.Stderr, "Using project %s", config.Project)
		return gke.Controller(ctx, config)
	},
	AppLinks: func(ctx context.Context, app string) (tool.Links, error) {
		return links(app, "" /*version*/)
	},
	DeploymentLinks: func(ctx context.Context, app, version string) (tool.Links, error) {
		return links(app, version)
	},
	AppCommands: func(app string) []tool.Command {
		return []tool.Command{
			{Label: "status", Command: "weaver gke status"},
			{Label: "cat logs", Command: fmt.Sprintf("weaver gke logs 'app==%q'", app)},
			{Label: "follow logs", Command: fmt.Sprintf("weaver gke logs --follow 'app==%q'", app)},
			{Label: "kill", Command: fmt.Sprintf("weaver gke kill %s", app)},
		}
	},
	DeploymentCommands: func(id string) []tool.Command {
		return []tool.Command{
			{Label: "status", Command: "weaver gke status"},
			{Label: "cat logs", Command: fmt.Sprintf("weaver gke logs 'version==%q'", logging.Shorten(id))},
			{Label: "follow logs", Command: fmt.Sprintf("weaver gke logs --follow 'version==%q'", logging.Shorten(id))},
			{Label: "profile", Command: fmt.Sprintf("weaver multi profile --duration=30s %s", id)},
		}
	},
}

// links returns links for the provided app or app version. If version is empty,
// links are provided for the app.
func links(app, version string) (tool.Links, error) {
	// Get the account and project.
	config, err := dashboardFlags.CloudConfig()
	if err != nil {
		return tool.Links{}, err
	}
	authuser := "0"
	if config.Account != "" {
		authuser = url.QueryEscape(config.Account)
	}
	project := url.QueryEscape(config.Project)

	// Form a Google Cloud Logging query that matches this version's logs.
	//
	// TODO(mwhittaker): Translate produces ugly queries because it's
	// output was never intended to be shown to humans. Now that we are, we
	// might want to prettify it.
	var logQuery string
	if version == "" { // app
		logQuery = fmt.Sprintf(`app == %q`, app)
	} else { // app version
		logQuery = fmt.Sprintf(`full_version == %q`, version)
	}
	logQuery += ` && !("serviceweaver/system" in attrs)`
	logQuery, err = gke.Translate(config.Project, logQuery)
	if err != nil {
		return tool.Links{}, err
	}
	logQuery = url.QueryEscape(logQuery)

	var traceFilter string
	if version == "" { // app
		traceFilter, err = encodeTraceFilter("serviceweaver.app", app)
	} else { // app version
		traceFilter, err = encodeTraceFilter("serviceweaver.version", version)
	}
	if err != nil {
		return tool.Links{}, err
	}
	traceQuery := url.QueryEscape(fmt.Sprintf(`("traceFilter":("chips":"%s"))`, traceFilter))
	return tool.Links{
		Metrics: fmt.Sprintf("https://console.cloud.google.com/monitoring/metrics-explorer?authuser=%s&project=%s", authuser, project),
		Logs:    fmt.Sprintf("https://console.cloud.google.com/logs/query?authuser=%s&project=%s&query=%s", authuser, project, logQuery),
		Traces:  fmt.Sprintf("https://console.cloud.google.com/traces/list?authuser=%s&project=%s&pageState=%s", authuser, project, traceQuery),
	}, nil
}

// NOTE: A bit of reverse-engineered magic below to encode the trace filter into
// the Google Cloud Tracing URL.

const (
	urlSafeColon = "_3A"
	urlSafeQuote = "_22"
	urlSafeComma = "_2C"
	urlSafeSlash = "_5C"
)

type traceFilterData struct {
	K string `json:"k"`
	T int    `json:"t"`
	V string `json:"v"`
}

func encodeTraceFilter(key, value string) (string, error) {
	safe := func(val string) string {
		val = strings.ReplaceAll(val, "\"", urlSafeQuote)
		val = strings.ReplaceAll(val, ":", urlSafeColon)
		val = strings.ReplaceAll(val, ",", urlSafeComma)
		return strings.ReplaceAll(val, "\\", urlSafeSlash)
	}
	b, err := json.Marshal([]traceFilterData{{
		K: key,
		T: 10,
		V: safe(fmt.Sprintf(`\"%s\"`, value)),
	}})
	if err != nil {
		return "", err
	}
	return url.QueryEscape(safe(string(b))), nil
}
