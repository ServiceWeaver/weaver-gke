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

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

var (
	controllerFlags = flag.NewFlagSet("controller", flag.ContinueOnError)
	controllerPort  = controllerFlags.Int("port", 0, "Controller port")
)

var controllerCmd = tool.Command{
	Name:        "controller",
	Flags:       controllerFlags,
	Description: "The GKE controller",
	Help: `Usage:
  weaver gke controller

Flags:
  -h, --help   Print this help message.`,
	Fn: func(ctx context.Context, args []string) error {
		return gke.RunController(ctx, *controllerPort)
	},
	Hidden: true,
}
