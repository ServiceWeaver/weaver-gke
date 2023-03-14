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

package babysitter

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	// URL suffixes for various HTTP endpoints exported by the babysitter.
	healthURL       = "/healthz"
	runProfilingURL = "/run_profiling"
	fetchMetricsURL = "/fetch_metrics"
)

// Babysitter starts and manages weavelets belonging to a single colocation
// group for a single application version, on the local machine.
type Babysitter struct {
	ctx            context.Context
	opts           envelope.Options
	cfg            *config.GKEConfig
	group          *protos.ColocationGroup
	envelope       *envelope.Envelope
	podName        string
	lis            net.Listener // listener to serve /healthz and /run_profiling
	manager        clients.ManagerClient
	logger         *logging.FuncLogger
	logSaver       func(*protos.LogEntry)
	traceSaver     func(spans []trace.ReadOnlySpan) error
	metricExporter func(metrics []*metrics.MetricSnapshot) error

	mu sync.RWMutex

	// Internal IP address on which the weavelet can be reached by other
	// weavelets.
	weaveletAddr string

	// Previously used internal weavelet IP addresses that are no longer
	// valid (e.g., envelope restarts the weavelet and it acquires a different
	// IP address).
	defunctWeaveletAddrs map[string]struct{}
}

var _ clients.BabysitterClient = &Babysitter{}

// NewBabysitter returns a new babysitter.
func NewBabysitter(
	ctx context.Context,
	cfg *config.GKEConfig,
	group *protos.ColocationGroup,
	podName string,
	useLocalhost bool,
	manager clients.ManagerClient,
	logSaver func(*protos.LogEntry),
	traceSaver func(spans []trace.ReadOnlySpan) error,
	metricExporter func(metrics []*metrics.MetricSnapshot) error,
	opts envelope.Options) (*Babysitter, error) {
	// Get a dialable address to serve http requests at the babysitter (e.g.,
	// health checks, profiling information).
	//
	// TODO(mwhittaker): Right now, we resolve our hostname to get a dialable
	// IP address. Double check that this always works.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:0", hostname))
	if err != nil {
		return nil, err
	}
	logger := &logging.FuncLogger{
		Opts: logging.Options{
			App:        cfg.Deployment.App.Name,
			Deployment: cfg.Deployment.Id,
			Component:  "Babysitter",
			Weavelet:   podName,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: logSaver,
	}

	// Create the babysitter.
	b := &Babysitter{
		ctx:                  ctx,
		opts:                 opts,
		cfg:                  cfg,
		group:                group,
		podName:              podName,
		lis:                  lis,
		manager:              manager,
		logger:               logger,
		logSaver:             logSaver,
		traceSaver:           traceSaver,
		metricExporter:       metricExporter,
		defunctWeaveletAddrs: map[string]struct{}{},
	}

	// Create the envelope.
	//
	// We use the PodName as a unique group replica id for the following
	// reasons:
	//   * It is derived from a unique 63-bit value, so collisions are
	//     unlikely.
	//   * It may be useful to be associated with a Pod for debugging etc.
	//   * It allows the manager to quickly check if the group replica is still
	//     active by asking the Kubernetes API if the Pod with a given name
	//     exists.
	wlet := &protos.WeaveletInfo{
		App:                cfg.Deployment.App.Name,
		DeploymentId:       cfg.Deployment.Id,
		Group:              group,
		GroupId:            podName,
		Id:                 uuid.New().String(),
		SameProcess:        cfg.Deployment.App.SameProcess,
		Sections:           cfg.Deployment.App.Sections,
		UseLocalhost:       useLocalhost,
		WeaveletPicksPorts: false,
	}
	handler := &Handler{
		Ctx:            ctx,
		Logger:         logger,
		Config:         cfg,
		Group:          group,
		Manager:        manager,
		PodName:        podName,
		BabysitterAddr: lis.Addr().String(),
		LogSaver:       logSaver,
		TraceSaver:     traceSaver,
		ReportWeaveletAddr: func(addr string) error {
			b.mu.Lock()
			defer b.mu.Unlock()
			// Remember the old weavelet address.
			b.defunctWeaveletAddrs[b.weaveletAddr] = struct{}{}
			b.weaveletAddr = addr
			// Cleanup in case the weavelet started with an old IP address.
			//
			// TODO(rgrandl): use weavelet ids instead of addresses to uniquely
			// identify the weavelets.
			delete(b.defunctWeaveletAddrs, addr)
			b.logger.Info("Process (re)started with new address",
				"group", logging.ShortenComponent(group.Name),
				"address", addr)
			return nil
		},
	}
	e, err := envelope.NewEnvelope(wlet, cfg.Deployment.App, handler, opts)
	if err != nil {
		return nil, err
	}
	b.envelope = e

	return b, nil
}

// Run runs the babysitter. This call will block until the context passed to
// NewBabysitter is canceled.
func (b *Babysitter) Run() error {
	if b.lis != nil {
		go func() {
			if err := b.runHTTP(); err != nil {
				b.logger.Error("Error starting the HTTP server", err)
			}
		}()
	}

	// Start a goroutine to periodically export metrics.
	go b.exportMetrics()

	// Run the envelope.
	return b.envelope.Run(b.ctx)

}

// Stop terminates the envelope managed by the babysitter.
func (b *Babysitter) Stop() error {
	return b.envelope.Stop()
}

// runHTTP runs the babysitter HTTP server.
func (b *Babysitter) runHTTP() error {
	// Start the server.
	mux := http.NewServeMux()
	mux.HandleFunc(healthURL, protomsg.HandlerFunc(b.logger, b.CheckHealth))
	mux.HandleFunc(runProfilingURL, protomsg.HandlerFunc(b.logger, b.RunProfiling))
	server := http.Server{Handler: mux}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(b.lis) }()

	// Wait for the server to abort or for the context to be cancelled.
	select {
	case err := <-errs:
		return err
	case <-b.ctx.Done():
		return server.Shutdown(b.ctx)
	}
}

// exportMetrics periodically exports metrics.
func (b *Babysitter) exportMetrics() {
	// Time interval at which metrics are exported.
	const metricExportInterval = 15 * time.Second
	ticker := time.NewTicker(metricExportInterval)
	for {
		select {
		case <-ticker.C:
			snaps := metrics.Snapshot() // babysitter metrics
			em, err := b.envelope.ReadMetrics()
			if err != nil {
				b.logger.Error("cannot get envelope metrics", err)
				continue
			}
			snaps = append(snaps, em...)

			// Export.
			if err := b.metricExporter(snaps); err != nil {
				b.logger.Error("cannot export metrics", err)
			}

		case <-b.ctx.Done():
			b.logger.Debug("exportMetrics cancelled")
			return
		}
	}
}

// CheckHealth implements the clients.BabysitterClient interface.
func (b *Babysitter) CheckHealth(_ context.Context, req *clients.HealthCheck) (*protos.HealthReport, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var healthStatus protos.HealthStatus
	if b.weaveletAddr != req.Addr {
		if _, found := b.defunctWeaveletAddrs[req.Addr]; found {
			// The address belongs to a weavelet that has been terminated.
			healthStatus = protos.HealthStatus_TERMINATED
		} else {
			// The address belongs to an unknown weavelet. This should never happen.
			healthStatus = protos.HealthStatus_UNKNOWN
		}
	} else {
		// The address belongs to an active weavelet: get its status.
		healthStatus = b.envelope.HealthStatus()
	}
	return &protos.HealthReport{Status: healthStatus}, nil
}

// RunProfiling implements the clients.BabysitterClient interface.
func (b *Babysitter) RunProfiling(_ context.Context, req *protos.RunProfiling) (*protos.Profile, error) {
	b.logger.Info(
		"Profiling",
		"profile", req.ProfileType,
		"version", req.VersionId,
		"application", req.AppName,
		"group", logging.ShortenComponent(req.Group),
	)
	prof, err := b.envelope.RunProfiling(b.ctx, req)
	if err != nil {
		return nil, fmt.Errorf("unable to profile %s for version %s of application %s and process %s: %v",
			req.ProfileType, req.VersionId, req.AppName, logging.ShortenComponent(req.Group), err)
	}
	return prof, nil
}
