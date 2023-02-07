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

package gke

import (
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// CloudConfig stores the configuration information for a cloud project.
type CloudConfig struct {
	Project       string // Cloud project.
	Account       string // Cloud user account.
	ProjectNumber string // Cloud project number

	// TokenSource associated with the account and project.
	tokenSource oauth2.TokenSource
}

// ClientOptions returns the client options that should be passed to
// cloud clients for proper authentication.
func (c *CloudConfig) ClientOptions() []option.ClientOption {
	var opt []option.ClientOption
	if c.tokenSource != nil {
		opt = append(opt, option.WithTokenSource(c.tokenSource))
	}
	return opt
}

// SetupCloudConfig sets up the cloud configuration with the given project and
// account names. If a project or an account name is empty, their values
// are retrieved from the active gcloud configuration on the local
// machine.
// REQUIRES: The caller is running on a user machine.
func SetupCloudConfig(project, account string) (CloudConfig, error) {
	// Check that google cloud SDK is installed.
	if err := checkSDK(); err != nil {
		return CloudConfig{}, err
	}

	// Retrieve the values for project/account from the active gcloud
	// configuration, if necessary.
	if project == "" {
		var err error
		project, err = runCmd(
			"", cmdOptions{}, "gcloud", "config", "get-value", "project")
		if err != nil {
			return CloudConfig{}, fmt.Errorf("couldn't find an active cloud project: "+
				"please run \"gcloud init\": %w", err)
		}
		project = strings.TrimSuffix(project, "\n") // remove trailing newline
	}
	if account == "" {
		var err error
		account, err = runCmd(
			"", cmdOptions{}, "gcloud", "config", "get-value", "account")
		if err != nil {
			return CloudConfig{}, fmt.Errorf(
				"couldn't find an active cloud account: please run "+
					"\"gcloud init\": %w", err)
		}
		account = strings.TrimSuffix(account, "\n") // remove trailing newline
	}
	number, err := runCmd(
		"", cmdOptions{}, "gcloud", "projects", "describe", project,
		"--format=value(projectNumber)", "--project", project, "--account",
		account)
	if err != nil {
		return CloudConfig{}, fmt.Errorf("couldn't find the numeric project "+
			"id for project %q: %w", project, err)
	}
	number = strings.TrimSuffix(number, "\n") // remove trailing newline
	return CloudConfig{
		Project:       project,
		Account:       account,
		ProjectNumber: number,
		tokenSource:   &tokenSource{project: project, account: account},
	}, nil
}

// checkSDK checks that Google Cloud SDK is installed.
func checkSDK() error {
	_, err := runCmd("", cmdOptions{}, "gcloud", "--version")
	if err != nil {
		return errors.New(`gcloud not installed.
Follow these installation instructions:
	https://cloud.google.com/sdk/docs/install`)
	}
	return nil
}

// inCloudConfig returns the cloud configuration that the caller is running in.
// REQUIRES: The caller is running inside a cloud project.
func inCloudConfig() (CloudConfig, error) {
	project, err := metadata.ProjectID()
	if err != nil {
		return CloudConfig{}, err
	}
	account, err := metadata.Email("") // default service account
	if err != nil {
		return CloudConfig{}, err
	}
	number, err := metadata.NumericProjectID()
	if err != nil {
		return CloudConfig{}, err
	}
	return CloudConfig{
		Project:       project,
		Account:       account,
		ProjectNumber: number,
		// Leave the tokenSource as nil, which will cause all cloud API calls
		// to use the default service account.
	}, nil
}
