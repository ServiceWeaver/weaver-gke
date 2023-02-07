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

package errlist

import (
	"fmt"
	"strings"
	"testing"
)

func ExampleErrList() {
	var errs ErrList
	errs = append(errs, fmt.Errorf("We looked!"))
	errs = append(errs, fmt.Errorf("And we saw him!"))
	errs = append(errs, fmt.Errorf("The Cat in the Hat!"))
	var err error = errs.ErrorOrNil()
	fmt.Println(err)

	// Output:
	// We looked!
	// And we saw him!
	// The Cat in the Hat!
}

func TestFromStrings(t *testing.T) {
	for name, list := range map[string][]string{
		"empty":  nil,
		"single": []string{"foo"},
		"multi":  []string{"foo", "bar"},
	} {
		t.Run(name, func(t *testing.T) {
			err := FromStrings(list)
			if len(list) == 0 {
				if err != nil {
					t.Errorf("unexpected non-nil error %v for empty list", err)
				}
				return
			}
			msg := err.Error()
			for _, expect := range list {
				if !strings.Contains(msg, expect) {
					t.Errorf("did not find %q in %q", expect, msg)
				}
			}
		})
	}

}
