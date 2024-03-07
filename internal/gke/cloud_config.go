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
	"os"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

const cloudTokenEnvVar = "CLOUDSDK_AUTH_ACCESS_TOKEN"

// CloudConfig stores the configuration information for a cloud project.
type CloudConfig struct {
	Project       string // Cloud project.
	ProjectNumber string // Cloud project number

	// TokenSource used for accessing the cloud project.
	TokenSource oauth2.TokenSource

	// Account name associated with the project. This value is filled as
	// best-effort and may be empty (e.g., if token is used for access).
	Account string
}

// ClientOptions returns the client options that should be passed to
// cloud clients for proper authentication.
func (c *CloudConfig) ClientOptions() []option.ClientOption {
	var opt []option.ClientOption
	if c.TokenSource != nil {
		opt = append(opt, option.WithTokenSource(c.TokenSource))
	}
	return opt
}

// SetupCloudConfig sets up the cloud configuration for the given project,
// using the provided account name and access token. If the project name is
// empty, its value is retrieved from the active gcloud configuration on the
// local machine. If access token is empty, and if the
// CLOUDSDK_AUTH_ACCESS_TOKEN environment variable is set, the environment
// variable value is used. Otherwise, a new access token is generated
// for the provided account name. If the account name is also empty, its
// value is retrieved from the active gcloud configuration on the local machine.
//
// REQUIRES: The caller is running on a user machine.
func SetupCloudConfig(project, token, account string) (CloudConfig, error) {
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
			return CloudConfig{}, fmt.Errorf("couldn't find an active cloud "+
				"project: please run \"gcloud init\": %w", err)
		}
		project = strings.TrimSuffix(project, "\n") // remove trailing newline
		if project == "" {
			return CloudConfig{}, fmt.Errorf(
				"empty active cloud project: please run \"gcloud init\"")
		}
	}
	var accountErr error
	if account == "" {
		account, accountErr = runCmd(
			"", cmdOptions{}, "gcloud", "config", "get-value", "account")
		if accountErr != nil {
			account = ""
		} else {
			account = strings.TrimSuffix(account, "\n") // remove trailing newline
			if account == "" {
				accountErr = fmt.Errorf("empty account name in gcloud configuration")
			}
		}
	}

	// Create the token source.
	var source oauth2.TokenSource
	if token != "" {
		source = &fixedTokenSource{token: token}
	} else if os.Getenv(cloudTokenEnvVar) != "" {
		source = &fixedTokenSource{token: os.Getenv(cloudTokenEnvVar)}
	} else if account != "" {
		source = &refreshingTokenSource{account: account}
	} else {
		return CloudConfig{}, fmt.Errorf("empty token and account error: %w", accountErr)
	}
	config := CloudConfig{
		Project:     project,
		Account:     account,
		TokenSource: source,
	}

	number, err := runGcloud(
		config, "", cmdOptions{}, "projects", "describe", project,
		"--format=value(projectNumber)")
	if err != nil {
		return CloudConfig{}, fmt.Errorf("couldn't find the numeric project "+
			"id for project %q: %w", project, err)
	}
	number = strings.TrimSuffix(number, "\n") // remove trailing newline
	config.ProjectNumber = number
	return config, nil
}

// checkSDK checks that Google Cloud SDK is installed.
func checkSDK() error {
	version, err := runCmd("", cmdOptions{}, "gcloud", "version", "--format=value(core)")
	if err != nil {
		return errors.New(`gcloud not installed.
Follow these installation instructions:
	https://cloud.google.com/sdk/docs/install`)
	}

	// Check the version.
	const minVersion = "2023.04.25"
	if version < minVersion {
		return fmt.Errorf(
			`unsupported gcloud version %q. Please run "gcloud components 
update" to update to at least version %q`, version, minVersion)
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
