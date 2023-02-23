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

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/perfetto"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

	// Setup trace recording.
	traceDB, err := perfetto.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot open Perfetto database: %w", err)
	}
	traceSaver := func(spans []sdktrace.ReadOnlySpan) error {
		return traceDB.Store(ctx, cfg.Deployment.App.Name, cfg.Deployment.Id, spans)
	}

	// Setup metrics recording.
	metricDB, err := metricdb.Open(ctx)
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
