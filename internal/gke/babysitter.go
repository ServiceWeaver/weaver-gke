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
	"os"

	traceexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"go.opentelemetry.io/otel/sdk/trace"
)

// RunBabysitter creates and runs a GKE babysitter.
//
// A GKE nanny does not start a container for each individual Service Weaver process.
// Instead, it starts a container for each colocation group that runs the
// babysitter binary. The babysitter binary in turn starts processes in the
// local container.
//
// The babysitter binary creates pipes to the Service Weaver binary subprocesses, and the
// subprocesses are configured to write their log entries and metrics as encoded
// strings to the pipes.
//
// The following environment variables must be set before this method is called:
//   - configEnvKey
//   - colocationGroupEnvKey
//   - containerMetadataEnvKey
//   - nodeNameEnvKey
//   - podNameEnvKey
//
// This call blocks until the provided context is canceled or an error is
// encountered.
func RunBabysitter(ctx context.Context) error {
	for _, v := range []string{
		configEnvKey,
		colocationGroupEnvKey,
		containerMetadataEnvKey,
		nodeNameEnvKey,
		podNameEnvKey,
	} {
		if _, ok := os.LookupEnv(v); !ok {
			return fmt.Errorf("environment variable %q not set", v)
		}
	}

	// Parse the config.GKEConfig
	cfg, err := gkeConfigFromEnv()
	if err != nil {
		return err
	}
	colocGroup, err := colocationGroupFromEnv()
	if err != nil {
		return err
	}

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
	traceSaver := func(spans []trace.ReadOnlySpan) error {
		return traceExporter.ExportSpans(ctx, spans)
	}
	metricSaver := func(metrics []*metrics.MetricSnapshot) error {
		return metricsExporter.Export(ctx, metrics)
	}

	m := &manager.HttpClient{Addr: cfg.ManagerAddr} // connection to the manager
	b, err := babysitter.NewBabysitter(ctx, cfg, colocGroup, meta.PodName, false /*useLocalhost*/, m, logSaver, traceSaver, metricSaver, envelope.Options{})
	if err != nil {
		return err
	}

	return b.Run()
}

// gkeConfigFromEnv reads config.GKEConfig from the Service Weaver internal environment
// variable. It returns an error if the environment variable isn't set.
func gkeConfigFromEnv() (*config.GKEConfig, error) {
	str := os.Getenv(configEnvKey)
	if str == "" {
		return nil, fmt.Errorf("%q environment variable not set", configEnvKey)
	}
	cfg := &config.GKEConfig{}
	if err := proto.FromEnv(str, cfg); err != nil {
		return nil, err
	}
	if err := runtime.CheckDeployment(cfg.Deployment); err != nil {
		return nil, err
	}
	return cfg, nil
}

// colocationGroupFromEnv reads ColocationGroup from the Service Weaver internal
// environment variable. It returns an error if the environment variable isn't
// set.
func colocationGroupFromEnv() (*protos.ColocationGroup, error) {
	str := os.Getenv(colocationGroupEnvKey)
	if str == "" {
		return nil, fmt.Errorf("%q environment variable not set", colocationGroupEnvKey)
	}
	g := &protos.ColocationGroup{}
	err := proto.FromEnv(str, g)
	if err := checkColocationGroup(g); err != nil {
		return nil, err
	}
	return g, err
}

// checkColocationGroup checks that a colocation group is well-formed.
func checkColocationGroup(g *protos.ColocationGroup) error {
	if g == nil {
		return fmt.Errorf("nil colocation group")
	}
	if g.Name == "" {
		return fmt.Errorf("empty colocation group name")
	}
	return nil
}
