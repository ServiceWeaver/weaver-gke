// Copyright 2023 Google LLC
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

package local

import (
	"crypto/x509"
	"testing"

	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
)

func TestGenerateSignedCert(t *testing.T) {
	caCert, caKey, err := generateLeafCert(true /*isCA*/, "ca")
	if err != nil {
		t.Fatal(err)
	}
	cert, _, err := generateSignedCert(caCert, caKey, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the signed certificate.
	name, err := mtls.VerifyCertificateChain("local", caCert, []*x509.Certificate{cert})
	if err != nil {
		t.Fatal(err)
	}
	if name != "test" {
		t.Errorf("unexpected certificate name, want %q, got %q", "test", name)
	}
}
