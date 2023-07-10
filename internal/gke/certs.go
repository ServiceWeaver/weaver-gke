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

package gke

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

const (
	// Local directory that contains the certificates for this Pod.
	certsLocalDir = "/var/run/secrets/workload-spiffe-credentials"

	// Local Files that store the Certificate Authority certificate, the Pod
	// certificate, and the Pod certificate's matching private key.
	caCertFile  = certsLocalDir + "/ca_certificates.pem"
	podCertFile = certsLocalDir + "/certificates.pem"
	podKeyFile  = certsLocalDir + "/private_key.pem"
)

// getPodCerts returns the CA certificate stored on the Pod, as well as a
// function that returns the latest Pod certificates.
func getPodCerts() (*x509.Certificate, func() ([]byte, []byte, error), error) {
	// Load the CA certificate.
	caCertPEM, err := loadPEM(caCertFile)
	if err != nil {
		return nil, nil, err
	}
	caCertDER, _ := pem.Decode(caCertPEM)
	if caCertDER == nil || caCertDER.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("cannot decode the PEM block containing the CA certificate")
	}
	caCert, err := x509.ParseCertificate(caCertDER.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse the CA certificate: %w", err)
	}

	// Create a function that loads the self certificate from the file.
	// Note that the certificate may get periodically re-written by the
	// Certificate Authority Service.
	certGetter := func() ([]byte, []byte, error) {
		selfCertPEM, err := loadPEM(podCertFile)
		if err != nil {
			return nil, nil, err
		}
		selfKeyPEM, err := loadPEM(podKeyFile)
		if err != nil {
			return nil, nil, err
		}
		return selfCertPEM, selfKeyPEM, nil
	}
	return caCert, certGetter, nil
}

func loadPEM(fname string) ([]byte, error) {
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty PEM block in file %s", fname)
	}
	return data, nil
}
