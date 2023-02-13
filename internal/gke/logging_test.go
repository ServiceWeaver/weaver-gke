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
	"testing"
)

func TestTranslate(t *testing.T) {
	const project = "p"
	for _, test := range []struct {
		query   string
		want    string
		hasTime bool
	}{
		{`msg == "todo"`, `(textPayload="todo")`, false},
		{`version == "v1"`, `(labels."serviceweaver/version"="v1")`, false},
		{`full_version == "v1"`, `(labels."serviceweaver/full_version"="v1")`, false},
		{`app == "todo"`, `(labels."serviceweaver/app"="todo")`, false},
		{`app != "todo"`, `(labels."serviceweaver/app"!="todo")`, false},
		{`app.contains("todo")`, `(labels."serviceweaver/app":"todo")`, false},
		{`app.matches("todo")`, `(labels."serviceweaver/app"=~"todo")`, false},
		{`attrs["name"] == "foo"`, `(labels."name"="foo")`, false},
		{`attrs["name"].contains("foo")`, `(labels."name":"foo")`, false},
		{`"foo" in attrs`, `(labels."foo":"")`, false},
		{`source.contains("foo")`, `(labels."serviceweaver/source":"foo")`, false},
		{`time < timestamp("1972-01-01T10:00:20Z")`, `(timestamp<"1972-01-01T10:00:20Z")`, true},
		{`app == "todo" && component == "a"`, `((labels."serviceweaver/app"="todo") AND (labels."serviceweaver/component"="a"))`, false},
		{`app == "todo" || app == "collatz"`, `((labels."serviceweaver/app"="todo") OR (labels."serviceweaver/app"="collatz"))`, false},
		{`!(app == "todo")`, `(NOT (labels."serviceweaver/app"="todo"))`, false},
	} {
		t.Run(test.query, func(t *testing.T) {
			got, err := Translate(project, test.query)
			if err != nil {
				t.Fatalf("translate(%s): %v", test.query, err)
			}
			hasTime, err := HasTime(test.query)
			if err != nil {
				t.Fatalf("HasTime(%s): %v", test.query, err)
			}
			if got, want := hasTime, test.hasTime; got != want {
				t.Fatalf("HasTime(%s): got %t, want %t", test.query, got, want)
			}
			want := test.want
			if !hasTime {
				want = want + ` AND timestamp>="1900-01-01T00:00:00Z"`
			}
			// Only check a prefix because the query has a long suffix of
			// filters restricting to Service Weaver logs.
			if !strings.HasPrefix(got, want) {
				t.Fatalf("translate(%s): got %s, want %s", test.query, got, want)
			}
		})
	}
}
