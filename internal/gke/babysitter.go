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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	traceexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/traces"
	"go.opentelemetry.io/otel/sdk/trace"
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
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        cfg.Deployment.App.Name,
			Deployment: cfg.Deployment.Id,
			Component:  "Babysitter",
			Weavelet:   meta.PodName,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: logSaver,
	})
	traceSaver := func(spans *protos.TraceSpans) error {
		if len(spans.Span) == 0 {
			return nil
		}
		s := make([]trace.ReadOnlySpan, len(spans.Span))
		for i, span := range spans.Span {
			s[i] = &traces.ReadSpan{Span: span}
		}
		return traceExporter.ExportSpans(ctx, s)
	}
	metricSaver := func(metrics []*metrics.MetricSnapshot) error {
		return metricsExporter.Export(ctx, metrics, cfg.Telemetry.Metrics.AutoGenerateMetrics)
	}

	caCert, getSelfCert, err := getPodCerts()
	if err != nil {
		return err
	}

	// Create an unique http client to the manager, that will be reused across all
	// the http requests to the manager.
	m := &manager.HttpClient{
		Addr:   cfg.ManagerAddr,
		Client: makeHttpClient(mtls.ClientTLSConfig(meta.Project, caCert, getSelfCert, "manager")),
	}
	mux := http.NewServeMux()
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	internalAddress := fmt.Sprintf("%s:%d", host, weaveletPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, babysitterPort))
	if err != nil {
		return err
	}
	selfAddr := fmt.Sprintf("https://%s", lis.Addr())
	_, err = babysitter.Start(ctx, logger, cfg, replicaSet, meta.Project, meta.PodName, internalAddress, mux, selfAddr, m, caCert, getSelfCert, logSaver, traceSaver, metricSaver)
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(meta.Project, caCert, getSelfCert, "manager", "distributor"),
	}
	return server.ServeTLS(lis, "", "")
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
