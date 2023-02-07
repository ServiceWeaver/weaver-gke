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
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestDNSLabelName(t *testing.T) {
	type testCase struct {
		Description string
		Components  []string
		Expect      string
	}
	id := uuid.New().String()[:8]
	for _, c := range []testCase{
		{
			Description: "zero",
			Expect:      "",
		},
		{
			Description: "single",
			Components:  []string{"123$$abc"},
			Expect:      "abc",
		},
		{
			Description: "short",
			Components: []string{
				"us-west1",
				"123.abc#$%d--abc$^%--%",
				"123abcd",
				id,
			},
			Expect: fmt.Sprintf("us-west1-123-abcd-abc-123abcd-%s", id),
		},
		{
			Description: "long",
			Components: []string{
				"australia-southeast1",
				"#$%123.abcd.Efgh_ijklmnopqrs^*#$tuvwhXyZ%%---%%--",
				"$%^012345--6789Abcdefg@#$)!hi--jklmnOpqrstUv123349897..--",
				id,
			},
			Expect: fmt.Sprintf("tralia-southeast1-rstuvwhxyz-uv123349897-%s", id),
		},
		{
			Description: "all non alphanumeric",
			Components:  []string{"%^%$^%$^%", "#$%#$%"},
			Expect:      "a-a",
		},
		{
			Description: "empty names",
			Components:  []string{"us-east1", "", "abcdAbcdEfg", ""},
			Expect:      "us-east1-abcdabcdefg",
		},
	} {
		t.Run(c.Description, func(t *testing.T) {
			var name name
			copy(name[:], c.Components)
			actual := name.DNSLabel()
			var prefix string
			lastDashIdx := strings.LastIndex(actual, "-")
			if lastDashIdx != -1 {
				prefix = actual[:lastDashIdx]
			}
			if prefix != c.Expect {
				t.Errorf("different DNS label prefixes, want %q, got %q", c.Expect, prefix)
			}
			if errs := validation.IsDNS1035Label(actual); len(errs) > 0 {
				t.Fatalf("name %s is not a valid DNS label: %v", []byte(actual), errs)
			}
		})
	}
}

func TestDNSSubdomainName(t *testing.T) {
	type testCase struct {
		Description string
		Components  []string
		Expect      string
	}
	id := uuid.New().String()[:8]
	for _, c := range []testCase{
		{
			Description: "zero",
			Expect:      "",
		},
		{
			Description: "single component",
			Components:  []string{"123$$Abc"},
			Expect:      "123abc",
		},
		{
			Description: "short",
			Components: []string{
				"us-west1",
				"123.abc#$%d--abc$^%--%",
				"#$#%---123abcd--..__123",
				id,
			},
			Expect: fmt.Sprintf("us-west1-123.abcd-abc-123abcd-123-%s", id),
		},
		{
			Description: "long",
			Components: []string{
				"australia-southeast1",
				"$%^012345--6789abcdefg@#$)!hi--jklmnopqrstuv123349897..--$%^012345--6789abCDefg@#$)!hi--jklmnopqrStuV123349897..--$%^012345--6789abCDefg@#$)!hi--jklmnopqrSTuv123349897..--$%^012345--6789aBcdefg@#$)!hi--jklmNopqrstuv123349897..--$%^012345--6789abcdeFg@#$)!hi--jklmnopqrstUv123349897..--",
				id,
			},
			Expect: fmt.Sprintf("australia-southeast1-789abcdefghi-jklmnopqrstuv123349897.012345-6789abcdefghi-jklmnopqrstuv123349897.012345-6789abcdefghi-jklmnopqrstuv123349897.012345-6789abcdefghi-jklmnopqrstuv123349897.012345-6789abcdefghi-jklmnopqrstuv123349897-%s", id),
		},
		{
			Description: "all non alphanumeric",
			Components:  []string{"%^%$^%$^%", "#$%#$%"},
			Expect:      "a-a",
		},
		{
			Description: "empty names",
			Components:  []string{"us-east1", "", "abcdAbcdEfg", ""},
			Expect:      "us-east1-abcdabcdefg",
		},
	} {
		t.Run(c.Description, func(t *testing.T) {
			var name name
			copy(name[:], c.Components)
			actual := name.DNSSubdomain()
			var prefix string
			lastDashIdx := strings.LastIndex(actual, "-")
			if lastDashIdx != -1 {
				prefix = actual[:lastDashIdx]
			}
			if prefix != c.Expect {
				t.Errorf("different DNS subdomain prefixes, want %q, got %q", c.Expect, prefix)
			}
			if errs := validation.IsDNS1123Subdomain(actual); len(errs) > 0 {
				t.Fatalf("name %s is not a valid DNS subdomain name: %v", []byte(actual), errs)
			}
		})
	}
}

func TestRandomDNSLabelName(t *testing.T) {
	rand.Seed(2021)
	const maxLen = 50
	const numIters = 10000
	for i := 0; i < numIters; i++ {
		var name name
		num := rand.Intn(maxNumComponents)
		for j := 0; j < num; j++ {
			name[j] = randString(maxLen)
		}
		label := name.DNSLabel()
		if errs := validation.IsDNS1035Label(label); len(errs) > 0 {
			t.Fatalf("label %x is not a valid DNS label: %v; %q", []byte(label), errs, label)
		}
	}
}

func TestRandomDNSSubdomainName(t *testing.T) {
	rand.Seed(2021)
	const maxLen = 160
	const numIters = 10000
	for i := 0; i < numIters; i++ {
		var name name
		num := rand.Intn(maxNumComponents)
		for j := 0; j < num; j++ {
			name[j] = randString(maxLen)
		}
		domain := name.DNSSubdomain()
		if errs := validation.IsDNS1123Subdomain(domain); len(errs) > 0 {
			t.Fatalf("domain %x is not a valid DNS subdomain: %v; %q", []byte(domain), errs, domain)
		}
	}
}

func randString(maxLen int) string {
	len := rand.Intn(maxLen)
	b := make([]byte, len)
	for i := 0; i < len; i++ {
		b[i] = byte(rand.Intn(128))
	}
	return string(b)
}
