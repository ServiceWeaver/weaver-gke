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
	"fmt"
	"regexp"

	"github.com/ServiceWeaver/weaver"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/metrics"
)

// stringLength is a histogram that tracks the length of strings passed to the
// Echo method.
var stringLength = metrics.NewHistogram(
	"echo_string_length",
	"The length of strings passed to the Echo method",
	store.GeneratedBuckets,
)

type echoOptions struct {
	// If non-empty, echo only strings that match the given regexp pattern,
	// returning an error for the rest.
	Pattern string
}

// Echoer component.
type Echoer interface {
	Echo(context.Context, string) (string, error)
}

// Implementation of the Echoer component.
type echoer struct {
	weaver.Implements[Echoer]
	weaver.WithConfig[echoOptions]
}

// Echo implements the Echoer interface.
func (e echoer) Echo(ctx context.Context, s string) (string, error) {
	e.Logger(ctx).Debug("echo", "value", s)
	stringLength.Put(float64(len(s))) // Update the stringLength metric.
	pattern := e.Config().Pattern
	if pattern == "" {
		return s, nil
	}
	matched, err := regexp.Match(pattern, []byte(s))
	if err != nil {
		return "", err
	}
	if !matched {
		return "", fmt.Errorf("Input %q doesn't match the pattern %q", s, pattern)
	}
	return s, nil
}
