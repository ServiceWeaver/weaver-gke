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
	proxyFlags = flag.NewFlagSet("proxy", flag.ContinueOnError)
	proxyPort  = proxyFlags.Int("port", 0, "proxy port")
)

var proxyCmd = tool.Command{
	Name:        "proxy",
	Description: "The gke-local proxy",
	Help: `Usage:
  weaver gke-local proxy --port=<port>

Flags:
  -h, --help   Print this help message.`,
	Flags: proxyFlags,
	Fn: func(ctx context.Context, _ []string) error {
		return local.RunProxy(ctx, *proxyPort)
	},
	Hidden: true,
}
