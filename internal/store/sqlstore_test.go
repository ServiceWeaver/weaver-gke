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

package store

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestSQLStore(t *testing.T) {
	maker := func(t *testing.T) Store {
		store, err := NewSQLStore(path.Join(t.TempDir(), "weaver.db"))
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	StoreTester{maker}.Run(t)
}

func TestIsLocked(t *testing.T) {
	type testCase struct {
		name     string
		err      error
		isLocked bool
	}
	for _, c := range []testCase{
		{"nil", nil, false},
		{"sqlite3-busy", sqlite3.Error{Code: sqlite3.ErrBusy}, true},
		{"sqlite3-locked", sqlite3.Error{Code: sqlite3.ErrLocked}, true},
		{"sqlite3-other", sqlite3.Error{Code: sqlite3.ErrPerm}, false},
		{"other", os.ErrNotExist, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := isLocked(c.err); got != c.isLocked {
				t.Errorf("IsLocked(%v) = %v, expecting %v", c.err, got, c.isLocked)
			}
			if c.err == nil {
				return
			}
			// Check that we get the same result when wrapped.
			err := fmt.Errorf("wrap %w", c.err)
			if got := isLocked(err); got != c.isLocked {
				t.Errorf("IsLocked(%v) = %v, expecting %v", err, got, c.isLocked)
			}
		})
	}
}
