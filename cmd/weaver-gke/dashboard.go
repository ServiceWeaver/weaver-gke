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
	"sync"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

var (
	dashboardFlags   = flag.NewFlagSet("dashboard", flag.ContinueOnError)
	dashboardProject = dashboardFlags.String("project", "", "Google Cloud project")
	dashboardAccount = dashboardFlags.String("account", "", "Google Cloud user account")

	// See setupCloudConfig.
	cloudConfig    gke.CloudConfig
	cloudConfigErr error
	configInit     sync.Once
)

// setupCloudConfig caches and returns the cloud config returned by
// gke.SetupCloudConfig called on dashboardProject and dashboardAccount.
func setupCloudConfig() (gke.CloudConfig, error) {
	configInit.Do(func() {
		flag.Parse()
		cloudConfig, cloudConfigErr = gke.SetupCloudConfig(*dashboardProject, *dashboardAccount)
	})
	return cloudConfig, cloudConfigErr
}

var dashboardSpec = tool.DashboardSpec{
	Tool:  "weaver gke",
	Flags: dashboardFlags,
	Controller: func(ctx context.Context) (string, *http.Client, error) {
		config, err := setupCloudConfig()
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(os.Stderr, "Using account %s in project %s", config.Account, config.Project)
		return gke.Controller(ctx, config)
	},
	AppLinks: func(ctx context.Context, app string) (tool.Links, error) {
		return links(app, "")
	},
	DeploymentLinks: func(ctx context.Context, depId string) (tool.Links, error) {
		return links("", depId)
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

// links returns links for the provided app or version. Only one of app or
// version should be non-empty.
func links(app, version string) (tool.Links, error) {
	// Get the account and project.
	config, err := setupCloudConfig()
	if err != nil {
		return tool.Links{}, err
	}
	account := url.QueryEscape(config.Account)
	project := url.QueryEscape(config.Project)

	// Form a Google Cloud Logging query that matches this version's logs.
	//
	// TODO(mwhittaker): Translate produces ugly queries because it's
	// output was never intended to be shown to humans. Now that we are, we
	// might want to prettify it.
	var query string
	if app != "" {
		query = fmt.Sprintf(`app == %q`, app)
	} else {
		query = fmt.Sprintf(`full_version == %q`, version)
	}
	query += ` && !("serviceweaver/system" in attrs)`
	query, err = gke.Translate(config.Project, query)
	if err != nil {
		return tool.Links{}, err
	}
	query = url.QueryEscape(query)

	return tool.Links{
		Metrics: fmt.Sprintf("https://console.cloud.google.com/monitoring/metrics-explorer?authuser=%s&project=%s", account, project),
		Logs:    fmt.Sprintf("https://console.cloud.google.com/logs/query?authuser=%s&project=%s&query=%s", account, project, query),
		Traces:  fmt.Sprintf("https://console.cloud.google.com/traces/list?authuser=%s&project=%s", account, project),
	}, nil
}
