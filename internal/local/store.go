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

// Store returns a store that persists data in the given directory. If the
// directory does not exist, it is created.
func Store(region string) (store.Store, error) {
	storeDir := filepath.Join(DataDir, region)
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return nil, err
	}
	return store.NewSQLStore(filepath.Join(storeDir, "store.db"))
}
