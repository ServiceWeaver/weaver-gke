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

package local

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/exporters/jaeger"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
)

// createBabysitter creates a babysitter in a gke-local deployment.
func createBabysitter(ctx context.Context, cfg *config.GKEConfig,
	group *protos.ColocationGroup, logDir string) (*babysitter.Babysitter, error) {
	groupReplicaID := uuid.New().String()

	ls, err := logging.NewFileStore(logDir)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}
	defer ls.Close()
	logSaver := ls.Add

	// Export traces to the Jaeger collector. By default, traces are exported to
	// http://localhost:14268/api/traces. The user can override the exporting
	// URL by setting the OTEL_EXPORTER_JAEGER_ENDPOINT environment variable.
	//
	// Traces are exported without any user or password options. The user
	// can override the username and password by setting the
	// OTEL_EXPORTER_JAEGER_USER and OTEL_EXPORTER_JAEGER_PASSWORD environment
	// variables.
	traceExporter, err := jaeger.New(jaeger.WithCollectorEndpoint())
	if err != nil {
		return nil, err
	}
	defer traceExporter.Shutdown(ctx)
	const traceMinLogInterval = 5 * time.Second
	const traceMaxLogInterval = 5 * time.Minute
	var traceLogInterval time.Duration
	var traceLastLogTime time.Time
	traceSaver := func(spans []sdktrace.ReadOnlySpan) error {
		err := traceExporter.ExportSpans(ctx, spans)
		if err == nil {
			traceLogInterval = 0 // reset the log interval
			return nil
		}
		if time.Since(traceLastLogTime) < traceLogInterval {
			// Swallow the error to avoid spamming the logs.
			return nil
		}
		traceLastLogTime = time.Now()
		traceLogInterval *= 2
		if traceLogInterval < traceMinLogInterval {
			traceLogInterval = traceMinLogInterval
		}
		if traceLogInterval > traceMaxLogInterval {
			traceLogInterval = traceMaxLogInterval
		}
		return fmt.Errorf("Cannot save traces. Note: To force Service Weaver to"+
			"export traces to a different Jaeger endpoint, restart your "+
			"application with the OTEL_EXPORTER_JAEGER_ENDPOINT set: %w", err)
	}

	metricDBFile, err := metricDBFilename()
	if err != nil {
		return nil, err
	}
	metricDB, err := metricdb.Open(ctx, metricDBFile)
	if err != nil {
		return nil, err
	}
	metricExporter := func(metrics []*metrics.MetricSnapshot) error {
		now := time.Now()
		for _, m := range metrics {
			if err := metricDB.Record(ctx, m, now); err != nil {
				return err
			}
		}
		return nil
	}

	opts := envelope.Options{
		Restart: envelope.OnFailure,
		Retry:   retry.DefaultOptions,
	}

	m := &manager.HttpClient{Addr: cfg.ManagerAddr} // connection to the manager
	return babysitter.NewBabysitter(ctx, cfg, group, groupReplicaID, m, logSaver, traceSaver, metricExporter, opts)
}
