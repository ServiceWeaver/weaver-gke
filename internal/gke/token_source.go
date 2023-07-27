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
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

// refreshingTokenSource is an oauth2.TokenSource that generates the access
// token using the gcloud command and refreshes the token if needed.
type refreshingTokenSource struct {
	// These fields are immutable after construction.
	account string

	mu     sync.Mutex
	token  *oauth2.Token
	expiry time.Time
}

var _ oauth2.TokenSource = &refreshingTokenSource{}

func (s *refreshingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	const tokenMinValidity = 1 * time.Minute
	if s.token != nil && time.Until(s.expiry) > tokenMinValidity {
		return s.token, nil
	}

	// Refresh the token.
	token, err := runCmd(
		"", cmdOptions{},
		"gcloud", "auth", "--format", "value(token)", "print-access-token",
		s.account,
	)
	if err != nil {
		return nil, err
	}
	token = strings.TrimSuffix(token, "\n") // Trim newline.

	// Trim token padding. Google seems to be increasing [1] the size of
	// some tokens, and they are padding smaller tokens in the meantime.
	//
	// [1] http://github.com/oauth2-proxy/oauth2-proxy/issues/1218
	token = strings.TrimRight(token, ".")

	const tokenValidity = time.Hour
	s.token = &oauth2.Token{AccessToken: token}
	s.expiry = time.Now().Add(tokenValidity)
	return s.token, nil
}

// fixedTokenSource is a oauth2.TokenSource that returns a fixed token
// value.
type fixedTokenSource struct {
	token string
}

var _ oauth2.TokenSource = &fixedTokenSource{}

func (s *fixedTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: s.token}, nil
}
