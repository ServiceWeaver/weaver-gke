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
	"os"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
)

var (
	profileFlags   = flag.NewFlagSet("profile", flag.ContinueOnError)
	profileProject = profileFlags.String("project", "", "Google Cloud project")
	profileAccount = profileFlags.String("account", "", "Google Cloud user account")
)

var profileSpec = tool.ProfileSpec{
	Tool:  "weaver gke",
	Flags: profileFlags,
	Controller: func(ctx context.Context) (string, *http.Client, error) {
		config, err := gke.SetupCloudConfig(*profileProject, *profileAccount)
		if err != nil {
			return "", nil, err
		}
		fmt.Fprintf(os.Stderr, "Using account %s in project %s\n", config.Account, config.Project)
		return gke.Controller(ctx, config)
	},
}
