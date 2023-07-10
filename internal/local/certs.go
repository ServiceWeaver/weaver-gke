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
	"os"
	"path/filepath"
	"time"
)

// ensureCACert ensures that a CA certificate and the corresponding private key
// have been generated and stored in caCertFile and caKeyFile, respectively.
// If those files already exists, it simply reads the certificates from them.
func ensureCACert() (*x509.Certificate, crypto.PrivateKey, error) {
	if err := os.MkdirAll(filepath.Dir(caCertFile), 0700); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(caKeyFile), 0700); err != nil {
		return nil, nil, err
	}
	exists := func(file string) bool {
		_, err := os.Stat(file)
		return err == nil
	}
	if exists(caCertFile) && exists(caKeyFile) {
		// Files exist: read from them.
		certPEM, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, nil, err
		}
		keyPEM, err := os.ReadFile(caKeyFile)
		if err != nil {
			return nil, nil, err
		}
		return pemDecode(certPEM, keyPEM)
	}

	// Generate a new cert and write it to a file.
	cert, key, err := generateLeafCert(true /*isCA*/, "ca")
	if err != nil {
		return nil, nil, err
	}
	certPEM, keyPEM, err := pemEncode(cert, key)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(caCertFile, certPEM, 0755); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(caKeyFile, keyPEM, 0600); err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// ensureToolCert ensures that a tool certificate and the corresponding private
// key have been generated and stored in toolCertFile and toolKeyFile,
// respectively. If those files already exists, it simply reads the certificates
// from them.
func ensureToolCert(caCert *x509.Certificate, caKey crypto.PrivateKey) ([]byte, []byte, error) {
	if err := os.MkdirAll(filepath.Dir(toolCertFile), 0700); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(toolKeyFile), 0700); err != nil {
		return nil, nil, err
	}
	exists := func(file string) bool {
		_, err := os.Stat(file)
		return err == nil
	}
	if exists(toolCertFile) && exists(toolKeyFile) {
		// Files exist: read from them.
		certPEM, err := os.ReadFile(toolCertFile)
		if err != nil {
			return nil, nil, err
		}
		keyPEM, err := os.ReadFile(toolKeyFile)
		if err != nil {
			return nil, nil, err
		}
		return certPEM, keyPEM, nil
	}
	// Generate a new cert and write it to a file.
	cert, key, err := generateSignedCert(caCert, caKey, "tool")
	if err != nil {
		return nil, nil, nil
	}
	certPEM, keyPEM, err := pemEncode(cert, key)
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(toolCertFile, certPEM, 0755); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(toolKeyFile, keyPEM, 0400); err != nil {
		return nil, nil, err
	}
	return certPEM, keyPEM, nil
}

// generateNannyCert generates a signed nanny certificate with the given name.
func generateNannyCert(name string) (*x509.Certificate, []byte, []byte, error) {
	// Load the CA certificate and private key from the local files.
	caCert, caKey, err := ensureCACert()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot get CA certificate and key: %w", err)
	}

	// Generate the certificate with the given name.
	cert, key, err := generateSignedCert(caCert, caKey, name)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot generate a cert for %q: %w", name, err)
	}

	// Create the TLS certificate.
	certPEM, keyPEM, err := pemEncode(cert, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot PEM-encode a cert for %q: %w", name, err)
	}
	return caCert, certPEM, keyPEM, nil
}

// generateSignedCert generates a certificate for the given DNS names, signed
// by the given Certificate Authority, and a corresponding private key.
//
// The returned certificate has a one-year validity and should only ever
// be used on a temporary basis.
func generateSignedCert(ca *x509.Certificate, caKey crypto.PrivateKey, name string) (*x509.Certificate, crypto.PrivateKey, error) {
	// Create an unsigned certificate.
	unsigned, certKey, err := generateLeafCert(false /*isCA*/, name)
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

func generateLeafCert(isCA bool, name string) (*x509.Certificate, crypto.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}

	// NOTE: We match the URI that GKE uses.
	uri, err := url.Parse(fmt.Sprintf("spiffe://%s.svc.id.goog/ns/serviceweaver/sa/%s", projectName, name))
	if err != nil {
		return nil, nil, err
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
		URIs:                  []*url.URL{uri},
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

// pemDecode returns the certificate and the private key encoded in the given
// PEM blocks.
func pemDecode(certPEM, keyPEM []byte) (*x509.Certificate, crypto.PrivateKey, error) {
	certDER, _ := pem.Decode(certPEM)
	if certDER == nil {
		return nil, nil, fmt.Errorf("cannot decode certificate DER block from PEM data")
	}
	keyDER, _ := pem.Decode(keyPEM)
	if keyDER == nil {
		return nil, nil, fmt.Errorf("cannot decode private key DER block from PEM data")
	}
	cert, err := x509.ParseCertificate(certDER.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.ParsePKCS8PrivateKey(keyDER.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}
