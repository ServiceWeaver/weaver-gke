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
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

var (
	purgeFlags = newCloudFlagSet("purge", flag.ContinueOnError)
	purgeForce = purgeFlags.Bool("force", false, "Purge without prompt")
)

var purgeCmd = tool.Command{
	Name:        "purge",
	Flags:       purgeFlags.FlagSet,
	Description: "Purge cloud resources",
	Help: fmt.Sprintf(`Usage:
  weaver gke purge [--force]

Flags:
  -h, --help	Print this help message.
%s

Description:
  "weaver gke purge" deletes all cloud resources created by "weaver gke
  deploy". This terminates all running jobs and deletes all data.`,
		tool.FlagsHelp(purgeFlags.FlagSet)),

	Fn: purge,
}

func purge(ctx context.Context, _ []string) error {
	config, err := purgeFlags.CloudConfig()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Using project %s\n", config.Project)

	if !*purgeForce {
		fmt.Printf(`WARNING: You are about to kill every active Service Weaver application and
delete all cloud resources created by "weaver gke". This cannot be undone. Are
you sure you want to proceed?

Enter (y)es to continue: `)
		reader := bufio.NewReader(os.Stdin)
		text, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		text = text[:len(text)-1] // strip the trailing "\n"
		text = strings.ToLower(text)
		if !(text == "y" || text == "yes") {
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "Purge aborted.")
			return nil
		}
	}
	return gke.Purge(ctx, config)
}
