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
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

// cmdOptions holds options for running commands on the user machine.
type cmdOptions struct {
	Stdin        []byte
	EnvOverrides []string
}

// runGcloud prints msg to stderr and then runs the "gcloud" command with
// the given arguments, using the account and the project specified
// in the given config.
func runGcloud(config CloudConfig, msg string, opts cmdOptions, args ...string) (string, error) {
	token, err := config.TokenSource.Token()
	if err != nil {
		return "", fmt.Errorf("cannot get cloud access token: %w", err)
	}
	opts.EnvOverrides = append(opts.EnvOverrides, fmt.Sprintf("CLOUDSDK_AUTH_ACCESS_TOKEN=%s", token.AccessToken))
	return runCmd(msg, opts, "gcloud", args...)
}

// runCmd prints msg to stderr and then runs cmd with the given arguments.
func runCmd(msg string, opts cmdOptions, cmd string, args ...string) (string, error) {
	if msg != "" {
		msg += "... "
		fmt.Fprint(os.Stderr, msg)
	}
	c := exec.Command(cmd, args...)
	if opts.Stdin != nil {
		c.Stdin = bytes.NewReader(opts.Stdin)
	}
	if opts.EnvOverrides != nil {
		c.Env = append(os.Environ(), opts.EnvOverrides...)
	}
	var outBuf, errBuf bytes.Buffer
	c.Stdout = &outBuf
	c.Stderr = &errBuf
	if err := c.Run(); err != nil {
		if msg != "" {
			fmt.Fprintln(os.Stderr, "Error")
		}
		return "", fmt.Errorf("%s: %w", errBuf.String(), err)
	}
	if msg != "" {
		fmt.Fprintln(os.Stderr, "Done")
	}
	return outBuf.String(), nil
}
