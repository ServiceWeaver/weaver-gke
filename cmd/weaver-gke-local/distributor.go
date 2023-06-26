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

	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

var (
	distributorFlags       = flag.NewFlagSet("distributor", flag.ContinueOnError)
	distributorRegion      = distributorFlags.String("region", "us-west1", "Simulated GKE region")
	distributorPort        = distributorFlags.Int("port", 0, "Distributor port")
	distributorManagerPort = distributorFlags.Int("manager_port", 0, "Local manager port")
)

var distributorCmd = tool.Command{
	Name:        "distributor",
	Flags:       distributorFlags,
	Description: "The gke-local distributor",
	Help: `Usage:
  weaver gke-local distributor

Flags:
  -h, --help   Print this help message.`,
	Fn: func(ctx context.Context, args []string) error {
		return local.RunDistributor(ctx, *distributorRegion, *distributorPort, *distributorManagerPort)
	},
	Hidden: true,
}
