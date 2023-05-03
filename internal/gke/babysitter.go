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
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	traceexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	// Local directory that contains the certificates for this Pod.
	// NOTE: This directory will only exist if the MTLS protocol has been
	// enabled for the deployment.
	certsLocalDir = "/var/run/secrets/workload-spiffe-credentials"

	// Local Files that store the Certificate Authority certificate, the Pod
	// certificate, and the Pod certificate's matching private key.
	caCertFile  = certsLocalDir + "/ca_certificates.pem"
	podCertFile = certsLocalDir + "/certificates.pem"
	podKeyFile  = certsLocalDir + "/private_key.pem"
)

// RunBabysitter creates and runs a GKE babysitter.
//
// The GKE babysitter starts and manages a weavelet. If the weavelet crashes,
// the babysitter will crash as well, causing the kubernetes manager to re-run
// the container.
//
// The following environment variables must be set before this method is called:
//   - configEnvKey
//   - replicaSetEnvKey
//   - containerMetadataEnvKey
//   - nodeNameEnvKey
//   - podNameEnvKey
//
// This call blocks until the provided context is canceled or an error is
// encountered.
func RunBabysitter(ctx context.Context) error {
	for _, v := range []string{
		configEnvKey,
		replicaSetEnvKey,
		containerMetadataEnvKey,
		nodeNameEnvKey,
		podNameEnvKey,
	} {
		val, ok := os.LookupEnv(v)
		if !ok {
			return fmt.Errorf("environment variable %q not set", v)
		}
		if val == "" {
			return fmt.Errorf("empty value for environment variable %q", v)
		}
	}

	// Parse the config.GKEConfig
	cfg, err := gkeConfigFromEnv()
	if err != nil {
		return err
	}
	replicaSet := os.Getenv(replicaSetEnvKey)

	// Parse the ContainerMetadata.
	meta := &ContainerMetadata{}
	metaStr := os.Getenv(containerMetadataEnvKey)
	if err := proto.FromEnv(metaStr, meta); err != nil {
		return fmt.Errorf("proto.FromEnv(%q): %w", metaStr, err)
	}
	meta.NodeName = os.Getenv(nodeNameEnvKey)
	meta.PodName = os.Getenv(podNameEnvKey)

	// Unset some environment variables, so that we don't pollute the Service Weaver
	// application's environment.
	for _, v := range []string{containerMetadataEnvKey, nodeNameEnvKey, podNameEnvKey} {
		if err := os.Unsetenv(v); err != nil {
			return err
		}
	}

	// Load certificates, if MTLS was enabled for the deployment.
	var caCert *x509.Certificate
	var selfCertPEM, selfKeyPEM []byte
	if cfg.UseMtls {
		caCertPEM, err := loadPEM(caCertFile)
		if err != nil {
			return err
		}
		caCertDER, _ := pem.Decode(caCertPEM)
		if caCertDER == nil || caCertDER.Type != "CERTIFICATE" {
			return fmt.Errorf("cannot decode the PEM block containing the CA certificate")
		}
		if caCert, err = x509.ParseCertificate(caCertDER.Bytes); err != nil {
			return fmt.Errorf("cannot parse the CA certificate: %w", err)
		}

		if selfCertPEM, err = loadPEM(podCertFile); err != nil {
			return err
		}
		if selfKeyPEM, err = loadPEM(podKeyFile); err != nil {
			return err
		}
	}

	// Babysitter logs are written to the same log store as the application logs.
	logClient, err := newCloudLoggingClient(ctx, meta)
	if err != nil {
		return err
	}
	defer logClient.Close()

	// Create the Google Cloud metrics exporter.
	metricsExporter, err := newMetricExporter(ctx, meta)
	if err != nil {
		return err
	}
	defer metricsExporter.Close()

	// Create a trace exporter.
	traceExporter, err := traceexporter.New(traceexporter.WithProjectID(meta.Project))
	if err != nil {
		return err
	}
	defer traceExporter.Shutdown(ctx)

	logSaver := logClient.Log
	traceSaver := func(spans []trace.ReadOnlySpan) error {
		return traceExporter.ExportSpans(ctx, spans)
	}
	metricSaver := func(metrics []*metrics.MetricSnapshot) error {
		return metricsExporter.Export(ctx, metrics)
	}

	m := &manager.HttpClient{Addr: cfg.ManagerAddr} // connection to the manager
	b, err := babysitter.NewBabysitter(ctx, cfg, replicaSet, meta.PodName, false /*useLocalhost*/, caCert, selfCertPEM, selfKeyPEM, m, logSaver, traceSaver, metricSaver)
	if err != nil {
		return err
	}

	return b.Run()
}

// gkeConfigFromEnv reads config.GKEConfig from the Service Weaver internal
// environment variable. It returns an error if the environment variable isn't
// set.
func gkeConfigFromEnv() (*config.GKEConfig, error) {
	str := os.Getenv(configEnvKey)
	if str == "" {
		return nil, fmt.Errorf("%q environment variable not set", configEnvKey)
	}
	cfg := &config.GKEConfig{}
	if err := proto.FromEnv(str, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
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
