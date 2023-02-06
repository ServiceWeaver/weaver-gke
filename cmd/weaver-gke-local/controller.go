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
	controllerFlags  = flag.NewFlagSet("controller", flag.ContinueOnError)
	controllerRegion = controllerFlags.String("region", "us-central1", "Simulated GKE region")
	controllerPort   = controllerFlags.Int("port", 0, "Controller port")
)

var controllerCmd = tool.Command{
	Name:        "controller",
	Flags:       controllerFlags,
	Description: "The gke-local controller",
	Help: `Usage:
  weaver gke-local controller

Flags:
  -h, --help   Print this help message.`,
	Fn: func(ctx context.Context, args []string) error {
		opts := local.NannyOptions{
			Region:          *controllerRegion,
			StartController: true,
			Port:            *controllerPort,
		}
		return local.RunNanny(ctx, opts)
	},
	Hidden: true,
}
