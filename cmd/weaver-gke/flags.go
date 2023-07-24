// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"sync"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
)

type cloudFlagSet struct {
	*flag.FlagSet

	// Predefined cloud flags.
	projectFlag, accessTokenFlag, accountFlag *string

	// Initialized lazily.
	configInit sync.Once
	config     gke.CloudConfig
	configErr  error
}

func newCloudFlagSet(name string, errorHandling flag.ErrorHandling) *cloudFlagSet {
	fset := &cloudFlagSet{FlagSet: flag.NewFlagSet(name, errorHandling)}
	fset.projectFlag = fset.String("project", "", `
Google Cloud project. If empty, the command will use the project name in the
active gcloud configuration on the local machine.`)
	fset.accessTokenFlag = fset.String("access_token", "", `
Google Cloud access token. If empty, the command will acquire cloud access
by using the account name.`)
	fset.accountFlag = fset.String("account", "", `
Google Cloud user account. If empty, the command will use the account name
in the active gcloud configuration on the local machine.`)
	return fset
}

// CloudConfig returns the cloud configuration file that corresponds to the
// predefined cloud flag values.
func (fs *cloudFlagSet) CloudConfig() (gke.CloudConfig, error) {
	fs.configInit.Do(func() {
		flag.Parse()
		fs.config, fs.configErr = gke.SetupCloudConfig(
			*fs.projectFlag, *fs.accessTokenFlag, *fs.accountFlag)
	})
	return fs.config, fs.configErr
}
