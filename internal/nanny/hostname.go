// Copyright 2024 Google LLC
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

package nanny

import (
	"fmt"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
)

// Internal domain name for applications' private listeners.
const InternalDNSDomain = "serviceweaver.internal"

// Hostname returns the hostname for the given listener, and the boolean value
// indicating whether the given listener is public.
func Hostname(lis string, loc string, cfg *config.GKEConfig) (string, bool) {
	defaultHostname := fmt.Sprintf("%s.%s.%s", lis, loc, InternalDNSDomain)
	opts := cfg.Listeners[lis]
	if opts == nil {
		// Private listener that doesn't have a hostname specified by the user.
		return defaultHostname, false
	}
	if opts.IsPublic {
		return opts.Hostname, true
	}
	if opts.Hostname != "" {
		// Private listener that has a hostname specified by the user.
		return fmt.Sprintf("%s.%s", opts.Hostname, defaultHostname), false
	}
	return "", false
}
