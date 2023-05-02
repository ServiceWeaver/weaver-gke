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

package local

import (
	"os"
	"path/filepath"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
)

// defaultDataDir returns the default directory, $XDG_DATA_HOME/serviceweaver, in
// which the gke-local data should be stored. If XDG_DATA_HOME is not set,
// then ~/.local/share is used. See [1] for more information.
//
// [1]: https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
func defaultDataDir() (string, error) {
	dataDir := os.Getenv("XDG_DATA_HOME")
	if dataDir == "" {
		// Default to ~/.local/share
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dataDir = filepath.Join(home, ".local", "share")
	}
	dataDir = filepath.Join(dataDir, "serviceweaver")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return "", err
	}
	return dataDir, nil
}

// Store returns a store that persists data in the given directory. If the
// directory does not exist, it is created.
func Store(region string) (store.Store, error) {
	dataDir, err := defaultDataDir()
	if err != nil {
		return nil, err
	}
	storeDir := filepath.Join(dataDir, region)
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return nil, err
	}
	return store.NewSQLStore(filepath.Join(storeDir, "gke_local_store.db"))
}
