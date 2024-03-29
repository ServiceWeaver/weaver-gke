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

package local

import (
	"path/filepath"

	"github.com/ServiceWeaver/weaver/runtime"
)

var (
	// The directories and files where "weaver gke-local" stores data.
	LogDir       = filepath.Join(runtime.LogsDir(), "gke-local")
	DataDir      = filepath.Join(must(runtime.DataDir()), "gke-local")
	MetricsFile  = filepath.Join(DataDir, "metrics.db")
	TracesFile   = filepath.Join(DataDir, "traces.db")
	caCertFile   = filepath.Join(DataDir, "ca_cert.pem")
	caKeyFile    = filepath.Join(DataDir, "ca_key.pem")
	toolCertFile = filepath.Join(DataDir, "tool_cert.pem")
	toolKeyFile  = filepath.Join(DataDir, "tool_key.pem")
)

func must[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}
