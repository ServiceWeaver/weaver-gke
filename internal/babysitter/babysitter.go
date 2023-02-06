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

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
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
	podName        string
	lis            net.Listener // listener to serve /healthz and /run_profiling
	manager        clients.ManagerClient
	logger         *logging.FuncLogger
	logSaver       func(*protos.LogEntry)
	traceSaver     func(spans []trace.ReadOnlySpan) error
	metricExporter func(metrics []*metrics.MetricSnapshot) error

	mu sync.RWMutex
	// Envelopes managed by the babysitter, keyed by the Service Weaver process they
	// correspond to.
	envelopes map[string]*envelopeState
}

var _ clients.BabysitterClient = &Babysitter{}

// envelopeState contains information about an envelope managed by the
// babysitter.
type envelopeState struct {
	envelope *envelope.Envelope

	// Internal IP address on which the weavelet can be reached by other
	// weavelets.
	weaveletAddr string

	// Previously used internal weavelet IP addresses that are no longer
	// valid (e.g., envelope restarts the weavelet and it acquires a different
	// IP address).
	defunctWeaveletAddrs map[string]struct{}
}

// NewBabysitter returns a new babysitter.
func NewBabysitter(
	ctx context.Context,
	cfg *config.GKEConfig,
	group *protos.ColocationGroup,
	podName string,
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

	return &Babysitter{
		ctx:     ctx,
		opts:    opts,
		cfg:     cfg,
		group:   group,
		podName: podName,
		lis:     lis,
		manager: manager,
		logger: &logging.FuncLogger{
			Opts: logging.Options{
				App:        cfg.Deployment.App.Name,
				Deployment: cfg.Deployment.Id,
				Component:  "Babysitter",
				Weavelet:   podName,
				Attrs:      []string{"serviceweaver/system", ""},
			},
			Write: logSaver,
		},
		logSaver:       logSaver,
		traceSaver:     traceSaver,
		metricExporter: metricExporter,
		envelopes:      map[string]*envelopeState{},
	}, nil
}

// Run runs the Envelope management loop. This call will block until the
// context passed to NewBabysitter is canceled.
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

	var version string
	for r := retry.Begin(); r.Continue(b.ctx); {
		procsToStart, newVersion, err := b.getProcessesToStart(b.ctx, version)
		if err != nil {
			continue
		}
		version = newVersion
		for _, proc := range procsToStart {
			if err := b.startProcess(proc); err != nil {
				b.logger.Error("Error starting process", err, "process", proc)
			}
		}
		r.Reset()
	}
	return b.ctx.Err()
}

// Stop terminates all envelopes managed by the babysitter.
func (b *Babysitter) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range b.envelopes {
		if p.envelope == nil {
			continue
		}
		if err := p.envelope.Stop(); err != nil {
			return err
		}
	}
	b.envelopes = map[string]*envelopeState{}
	return nil
}

func (b *Babysitter) getProcessesToStart(ctx context.Context, version string) ([]string, string, error) {
	request := &protos.GetProcessesToStartRequest{
		App:             b.cfg.Deployment.App.Name,
		DeploymentId:    b.cfg.Deployment.Id,
		ColocationGroup: b.group.Name,
		Version:         version,
	}
	reply, err := b.manager.GetProcessesToStart(ctx, request)
	if err != nil {
		return nil, "", err
	}
	if reply.Unchanged {
		return nil, "", fmt.Errorf("no new processes to start")
	}
	return reply.Processes, reply.Version, nil
}

func (b *Babysitter) startProcess(proc string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.envelopes[proc]; ok {
		// Already started.
		return nil
	}

	// Start the weavelet and capture its logs, traces, and metrics.
	wlet := &protos.Weavelet{
		Id:             uuid.New().String(),
		Dep:            b.cfg.Deployment,
		Group:          b.group,
		GroupReplicaId: b.podName,
		Process:        proc,
	}
	handler := &Handler{
		Ctx:            b.ctx,
		Config:         b.cfg,
		Manager:        b.manager,
		PodName:        b.podName,
		BabysitterAddr: b.lis.Addr().String(),
		LogSaver:       b.logSaver,
		TraceSaver:     b.traceSaver,
		ReportWeaveletAddr: func(addr string) error {
			b.mu.Lock()
			defer b.mu.Unlock()
			e := b.envelopes[proc]
			if e == nil { // NOTE: should never happen.
				return fmt.Errorf("unable to register the weavelet addr %s for a process %s that hasn't yet started", addr, proc)
			}
			// Remember the old weavelet address.
			e.defunctWeaveletAddrs[e.weaveletAddr] = struct{}{}
			e.weaveletAddr = addr
			// Cleanup in case the weavelet started with an old IP address.
			//
			// TODO(rgrandl): use weavelet ids instead of addresses to uniquely
			// identify the weavelets.
			delete(e.defunctWeaveletAddrs, addr)
			b.logger.Info("Process (re)started with new address",
				"process", logging.ShortenComponent(e.envelope.Weavelet().Process),
				"address", e.weaveletAddr)
			return nil
		},
	}
	e, err := envelope.NewEnvelope(wlet, handler, b.opts)
	if err != nil {
		return err
	}
	go func() {
		if err := e.Run(b.ctx); err != nil {
			b.logger.Error("Error running process", err, "process", logging.ShortenComponent(e.Weavelet().Process))
		}
	}()
	b.envelopes[proc] = &envelopeState{envelope: e, defunctWeaveletAddrs: map[string]struct{}{}}
	return nil
}

// getEnvelopes return the set of envelopes managed by the babysitter.
func (b *Babysitter) getEnvelopes() []*envelope.Envelope {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var envelopes []*envelope.Envelope
	for _, e := range b.envelopes {
		if e.envelope != nil {
			envelopes = append(envelopes, e.envelope)
		}
	}
	return envelopes
}

func (b *Babysitter) getEnvelopeState(proc string) *envelopeState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.envelopes[proc]
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

			// Gather envelope metrics.
			for _, e := range b.getEnvelopes() {
				ms, err := e.ReadMetrics()
				if err != nil {
					b.logger.Error("cannot get envelope metrics", err)
					continue
				}
				snaps = append(snaps, ms...)
			}

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
	e := b.getEnvelopeState(req.Process)
	if e == nil || e.envelope == nil {
		return nil, fmt.Errorf("process %s is not ready for health checks", req.Process)
	}
	var healthStatus protos.HealthStatus
	if e.weaveletAddr != req.Addr {
		if _, found := e.defunctWeaveletAddrs[req.Addr]; found {
			// The address belongs to a weavelet that has been terminated.
			healthStatus = protos.HealthStatus_TERMINATED
		} else {
			// The address belongs to an unknown weavelet. This should never happen.
			healthStatus = protos.HealthStatus_UNKNOWN
		}
	} else {
		// The address belongs to an active weavelet: get its status.
		healthStatus = e.envelope.HealthStatus()
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
		"process", logging.ShortenComponent(req.Process),
	)
	e := b.getEnvelopeState(req.Process)
	if e == nil || e.envelope == nil {
		return nil, fmt.Errorf("process %s is not ready to be profiled yet", logging.ShortenComponent(req.Process))
	}
	prof, err := e.envelope.RunProfiling(b.ctx, req)
	if err != nil {
		return nil, fmt.Errorf("unable to profile %s for version %s of application %s and process %s: %v",
			req.ProfileType, req.VersionId, req.AppName, logging.ShortenComponent(req.Process), err)
	}
	return prof, nil
}
