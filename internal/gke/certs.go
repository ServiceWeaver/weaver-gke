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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	privateca "cloud.google.com/go/security/privateca/apiv1"
	"cloud.google.com/go/security/privateca/apiv1/privatecapb"
	"google.golang.org/protobuf/types/known/durationpb"
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

// createToolCertificate creates a new certificate that the tool can use to
// authenticate itself with the controller. It returns the CA root certificate,
// as well as the newly minted tool certificate and its private key.
// REQUIRES: Called by the tool on the user machine.
func createToolCertificate(ctx context.Context, config CloudConfig) (*x509.Certificate, []byte, []byte, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, err
	}
	var publicKeyPEM bytes.Buffer
	if err := pem.Encode(&publicKeyPEM, &pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(&privKey.PublicKey),
	}); err != nil {
		return nil, nil, nil, err
	}

	client, err := privateca.NewCertificateAuthorityClient(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create PrivateCA client: %w", err)
	}
	defer client.Close()
	certProto, err := client.CreateCertificate(ctx, &privatecapb.CreateCertificateRequest{
		Parent:                        fmt.Sprintf("projects/%s/locations/%s/caPools/%s", config.Project, caLocation, caPoolName),
		IssuingCertificateAuthorityId: caName,
		Certificate: &privatecapb.Certificate{
			CertificateConfig: &privatecapb.Certificate_Config{
				Config: &privatecapb.CertificateConfig{
					PublicKey: &privatecapb.PublicKey{
						Key:    publicKeyPEM.Bytes(),
						Format: privatecapb.PublicKey_PEM,
					},
					SubjectConfig: &privatecapb.CertificateConfig_SubjectConfig{
						Subject: &privatecapb.Subject{
							CountryCode: "US",
							Province:    "California",
						},
						SubjectAltName: &privatecapb.SubjectAltNames{
							Uris: []string{
								// Match the URI for Pod-generated certificates.
								fmt.Sprintf("spiffe://%s.svc.id.goog/ns/%s/sa/tool", config.Project, namespaceName),
							},
						},
					},
					X509Config: &privatecapb.X509Parameters{
						KeyUsage: &privatecapb.KeyUsage{
							BaseKeyUsage: &privatecapb.KeyUsage_KeyUsageOptions{
								DigitalSignature: true,
								KeyEncipherment:  true,
							},
							ExtendedKeyUsage: &privatecapb.KeyUsage_ExtendedKeyUsageOptions{
								ServerAuth: true,
								ClientAuth: true,
							},
						},
					},
				},
			},
			Lifetime: &durationpb.Duration{
				Seconds: 3600, // 1h
			},
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Encode the private key.
	keyDER, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, nil, nil, err
	}
	var keyOut bytes.Buffer
	if err := pem.Encode(&keyOut, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyDER,
	}); err != nil {
		return nil, nil, nil, err
	}

	// Parse the root CA certificate from the response.
	parse := func(certPEM, name string) (*x509.Certificate, error) {
		certDER, _ := pem.Decode([]byte(certPEM))
		if certDER == nil {
			return nil, fmt.Errorf("cannot decode %s certificate DER block from PEM data", name)
		}
		cert, err := x509.ParseCertificate(certDER.Bytes)
		if err != nil {
			return nil, fmt.Errorf("cannot parse certificate: %w", err)
		}
		return cert, err
	}
	n := len(certProto.PemCertificateChain)
	if n == 0 {
		return nil, nil, nil, fmt.Errorf("empty certificate verification chain")
	}
	caCert, err := parse(certProto.PemCertificateChain[n-1], "CA")
	if err != nil {
		return nil, nil, nil, err
	}

	return caCert, []byte(certProto.PemCertificate), keyOut.Bytes(), nil
}

// getPodCerts returns the CA certificate stored on the Pod, as well as a
// function that returns the latest Pod certificates.
// REQUIRES: Called from inside a Pod.
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
