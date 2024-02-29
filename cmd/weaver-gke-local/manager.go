// Copyright 2023 Google LLC
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
	managerFlags     = flag.NewFlagSet("manager", flag.ContinueOnError)
	managerId        = managerFlags.String("id", "", "manager unique id")
	managerRegion    = managerFlags.String("region", "", "Simulated GKE region")
	managerPort      = managerFlags.Int("port", 0, "Manager port")
	managerProxyPort = managerFlags.Int("proxy_port", 0, "Proxy port")
)

var managerCmd = tool.Command{
	Name:        "manager",
	Flags:       managerFlags,
	Description: "The GKE manager",
	Help: `Usage:
  weaver gke-local manager

Flags:
  -h, --help   Print this help message.`,
	Fn: func(ctx context.Context, args []string) error {
		return local.RunManager(ctx, *managerId, *managerRegion, *managerPort, *managerProxyPort)
	},
	Hidden: true,
}
