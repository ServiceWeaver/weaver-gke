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
	"os"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

var (
	logsFlags = newCloudFlagSet("logs", flag.ContinueOnError)

	// NOTE(mwhittaker): The Cloud Logging team informed us that they're
	// working on speeding up queries over large time ranges, so in the future,
	// we may be able to remove this flag.
	//
	// TODO(mwhittaker): Have a flag to disable the freshness flag?
	// Alternatively, the user can pass in a very large value for freshness.
	logsFreshness = logsFlags.String("freshness", "24h", "Only show logs that are this fresh (e.g., 300ms, 1.5h, 2h45m)")
	logsSpec      = tool.LogsSpec{
		Tool:  "weaver gke",
		Flags: logsFlags.FlagSet,
		Rewrite: func(q logging.Query) (logging.Query, error) {
			hasTime, err := gke.HasTime(q)
			if err != nil {
				return "", err
			}
			if hasTime {
				return q, nil
			}

			freshnessFlagSet := false
			logsFlags.Visit(func(f *flag.Flag) {
				if f.Name == "freshness" {
					freshnessFlagSet = true
				}
			})

			if !freshnessFlagSet {
				fmt.Fprintf(os.Stderr, `Fetching entries newer than %s ago. Set the --freshness flag or include
a time based predicate in your query to override this behavior.
`, *logsFreshness)
			}
			freshness, err := time.ParseDuration(*logsFreshness)
			if err != nil {
				return "", err
			}
			cutoff := time.Now().Add(-freshness)
			q += fmt.Sprintf(" && time >= timestamp(%q)", cutoff.Format(time.RFC3339))
			return q, nil
		},
		Source: func(context.Context) (logging.Source, error) {
			config, err := logsFlags.CloudConfig()
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(os.Stderr, "Using project %s\n", config.Project)
			return gke.LogSource(config)
		},
	}
)
