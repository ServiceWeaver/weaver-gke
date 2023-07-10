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

package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

// VerifyRawCertificateChain verifies the given DER-encoded certificate chain,
// returning the name encoded in its leaf certificate.
func VerifyRawCertificateChain(project string, caCert *x509.Certificate, certsDER [][]byte) (string, error) {
	if len(certsDER) == 0 {
		return "", errors.New("tls: empty peer certificate chain")
	}

	certs := make([]*x509.Certificate, len(certsDER))
	for i, certDER := range certsDER {
		cert, err := x509.ParseCertificate(certDER)
		if err != nil {
			return "", errors.New("tls: bad peer certificate chain")
		}
		certs[i] = cert
	}
	return VerifyCertificateChain(project, caCert, certs)
}

// VerifyCertificateChain verifies the given certificate chain, returning the
// name encoded in its leaf certificate.
func VerifyCertificateChain(project string, caCert *x509.Certificate, certs []*x509.Certificate) (string, error) {
	if len(certs) == 0 {
		return "", errors.New("tls: empty peer certificate chain")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AddCert(caCert)

	intermediates := x509.NewCertPool()
	for i := 1; i < len(certs); i++ {
		intermediates.AddCert(certs[i])
	}
	opts := x509.VerifyOptions{
		Roots:         caCertPool,
		CurrentTime:   time.Now(),
		Intermediates: intermediates,
	}
	verifiedChains, err := certs[0].Verify(opts)
	if err != nil {
		return "", fmt.Errorf("tls: couldn't verify certificate chain: %w", err)
	}
	if len(verifiedChains) != 1 {
		return "", fmt.Errorf("tls: expected a single verified chain, got %d", len(verifiedChains))
	}
	if len(verifiedChains[0]) < 1 {
		return "", fmt.Errorf("tls: empty verified chain")
	}
	verifiedLeaf := verifiedChains[0][0]
	if len(verifiedLeaf.URIs) != 1 {
		return "", fmt.Errorf("tls: expected a single peer URI, got %d", len(verifiedLeaf.URIs))
	}
	uri := verifiedLeaf.URIs[0]
	if uri.Scheme != "spiffe" || uri.Host == "" || uri.Path == "" {
		return "", fmt.Errorf(`invalid peer identity URI, want "spiffe://<host>/<name>", got %q`, uri)
	}
	expectedHost := fmt.Sprintf("%s.svc.id.goog", project)
	if uri.Host != expectedHost {
		return "", fmt.Errorf("invalid host in peer identity, want %q, got %q", expectedHost, uri.Host)
	}
	const nsPrefix = "/ns/serviceweaver/sa/"
	if !strings.HasPrefix(uri.Path, nsPrefix) {
		return "", fmt.Errorf("invalid path in peer identity, want prefix %q, got %q", nsPrefix, uri.Path)
	}
	return strings.TrimPrefix(uri.Path, nsPrefix), nil
}

// ClientTLSConfig returns the TLS configuration that a HTTP client should use
// to communicate using mTLS.
func ClientTLSConfig(project string, caCert *x509.Certificate, getSelfCert func() ([]byte, []byte, error), expectedPeer string) *tls.Config {
	return &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			certPEM, keyPEM, err := getSelfCert()
			if err != nil {
				return nil, err
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
		InsecureSkipVerify: true, // verified by VerifyPeerCertificate
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			peer, err := VerifyRawCertificateChain(project, caCert, rawCerts)
			if err != nil {
				return err
			}
			if peer != expectedPeer {
				return fmt.Errorf("unexpected server: got %q, want %q.", peer, expectedPeer)
			}
			return nil
		},
	}
}

// ServerTLSConfig returns the TLS configuration that a HTTP server should use
// to communicate using mTLS.
func ServerTLSConfig(project string, caCert *x509.Certificate, getSelfCert func() ([]byte, []byte, error), expectedPeers ...string) *tls.Config {
	config := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certPEM, keyPEM, err := getSelfCert()
			if err != nil {
				return nil, err
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
		ClientAuth: tls.RequireAnyClientCert,
	}

	if expectedPeers == nil {
		// Verification happens in the HTTP handler: skip it here.
		return config
	}
	config.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		peer, err := VerifyRawCertificateChain(project, caCert, rawCerts)
		if err != nil {
			return err
		}
		if !slices.Contains(expectedPeers, peer) {
			return fmt.Errorf(`unauthorized client: got %q, want %v`, peer, expectedPeers)
		}
		return nil
	}
	return config
}
