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
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"time"
)

// generateCACert generates a self-signed CA certificate and a corresponding
// private key.
//
// The returned certificate has a one-year validity and is attributed to a fake
// authority. As such, it should only ever be used on a temporary basis and for
// in-process certificate signing.
func generateCACert() (*x509.Certificate, crypto.PrivateKey, error) {
	return generateLeafCert(true /*isCA*/, "ca")
}

// GenerateSignedCert generates a certificate for the given DNS names, signed
// by the given Certificate Authority, and a corresponding private key.
//
// The returned certificate has a one-year validity and should only ever
// be used on a temporary basis.
func generateSignedCert(ca *x509.Certificate, caKey crypto.PrivateKey, names ...string) (*x509.Certificate, crypto.PrivateKey, error) {
	// Create an unsigned certificate.
	unsigned, certKey, err := generateLeafCert(false /*isCA*/, names...)
	if err != nil {
		return nil, nil, err
	}
	certDER, err := x509.CreateCertificate(rand.Reader, unsigned, ca, unsigned.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}
	return cert, certKey, nil
}

func generateLeafCert(isCA bool, names ...string) (*x509.Certificate, crypto.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}

	uris := make([]*url.URL, len(names))
	for i, name := range names {
		// Match the URI GKE uses.
		uris[i], err = url.Parse(fmt.Sprintf("spiffe://%s.svc.id.goog/%s", projectName, name))
		if err != nil {
			return nil, nil, err
		}
	}

	keyUsage := x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	if isCA {
		keyUsage |= x509.KeyUsageCertSign
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{Organization: []string{"ACME Co."}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     keyUsage,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		URIs:                  uris,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}
	return cert, priv, nil
}

// pemEncode returns the PEM-encoded blocks for the given certificate and
// private key.
func pemEncode(cert *x509.Certificate, key crypto.PrivateKey) ([]byte, []byte, error) {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	var certOut bytes.Buffer
	if err := pem.Encode(&certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	}); err != nil {
		return nil, nil, err
	}
	var keyOut bytes.Buffer
	if err := pem.Encode(&keyOut, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyDER,
	}); err != nil {
		return nil, nil, err
	}
	return certOut.Bytes(), keyOut.Bytes(), nil
}
