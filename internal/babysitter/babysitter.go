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
	"crypto/x509"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/exp/slog"
)

const (
	// URL suffixes for various HTTP endpoints exported by the babysitter.
	healthURL       = "/healthz"
	runProfilingURL = "/run_profiling"
	fetchMetricsURL = "/fetch_metrics"
)

// Babysitter starts and manages a weavelet inside the Pod.
type Babysitter struct {
	ctx            context.Context
	mux            *http.ServeMux
	selfAddr       string // HTTP address for the listener
	cfg            *config.GKEConfig
	replicaSet     string
	podName        string
	envelope       *envelope.Envelope
	manager        endpoints.Manager
	caCert         *x509.Certificate
	selfCertGetter func() ([]byte, []byte, error)
	logger         *slog.Logger
	logSaver       func(*protos.LogEntry)
	traceSaver     func(spans []trace.ReadOnlySpan) error
	metricExporter func(metrics []*metrics.MetricSnapshot) error

	mu                  sync.Mutex
	watchingRoutingInfo map[string]struct{}
}

var _ envelope.EnvelopeHandler = &Babysitter{}
var _ endpoints.Babysitter = &Babysitter{}

// Start creates and starts a new babysitter.
func Start(
	ctx context.Context,
	cfg *config.GKEConfig,
	replicaSet string,
	podName string,
	useLocalhost bool,
	mux *http.ServeMux,
	selfAddr string,
	manager endpoints.Manager,
	caCert *x509.Certificate,
	selfCertGetter func() ([]byte, []byte, error),
	logSaver func(*protos.LogEntry),
	traceSaver func(spans []trace.ReadOnlySpan) error,
	metricExporter func(metrics []*metrics.MetricSnapshot) error,
) (*Babysitter, error) {
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        cfg.Deployment.App.Name,
			Deployment: cfg.Deployment.Id,
			Component:  "Babysitter",
			Weavelet:   podName,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: logSaver,
	})

	// Create the envelope.
	//
	// We use the PodName as a unique weavelet id for the following reasons:
	//   * It is derived from a unique 63-bit value, so collisions are
	//     unlikely.
	//   * It may be useful to be associated with a Pod for debugging etc.
	//   * It allows the manager to quickly check if the weavelet is still
	//     active by asking the Kubernetes API if the Pod with a given name
	//     exists.
	info := &protos.EnvelopeInfo{
		App:          cfg.Deployment.App.Name,
		DeploymentId: cfg.Deployment.Id,
		Id:           podName,
		Sections:     cfg.Deployment.App.Sections,
		RunMain:      replicaSet == runtime.Main,
		Mtls:         cfg.Mtls,
	}
	e, err := envelope.NewEnvelope(ctx, info, cfg.Deployment.App)
	if err != nil {
		return nil, err
	}

	// Create the babysitter.
	b := &Babysitter{
		ctx:            ctx,
		mux:            mux,
		cfg:            cfg,
		replicaSet:     replicaSet,
		podName:        podName,
		envelope:       e,
		selfAddr:       selfAddr,
		manager:        manager,
		caCert:         caCert,
		selfCertGetter: selfCertGetter,
		logger:         logger,
		logSaver:       logSaver,
		traceSaver:     traceSaver,
		metricExporter: metricExporter,
	}

	// Register babysitter handlers.
	mux.HandleFunc(healthURL, protomsg.HandlerFunc(b.logger, b.CheckHealth))
	mux.HandleFunc(runProfilingURL, protomsg.HandlerFunc(b.logger, b.RunProfiling))

	// Register the weavelet.
	if err := b.registerReplica(); err != nil {
		return nil, err
	}

	// Start a goroutine to periodically export metrics.
	if b.metricExporter != nil {
		go b.exportMetrics()
	}

	// Start a goroutine to periodically report components' load.
	go b.reportLoad()

	// Start the envelope.
	go b.envelope.Serve(b)

	return b, nil
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
			em, err := b.envelope.GetMetrics()
			if err != nil {
				b.logger.Error("cannot get envelope metrics", "err", err)
				continue
			}
			snaps = append(snaps, em...)

			// Export.
			if err := b.metricExporter(snaps); err != nil {
				b.logger.Error("cannot export metrics", "err", err)
			}

		case <-b.ctx.Done():
			b.logger.Debug("exportMetrics cancelled")
			return
		}
	}
}

// reportLoad periodically exports components' load information.
func (b *Babysitter) reportLoad() error {
	// pick samples a time uniformly from [0.95i, 1.05i] where i is
	// LoadReportInterval. We introduce jitter to avoid processes that start
	// around the same time from storming to update their load.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	pick := func() time.Duration {
		const i = float64(endpoints.LoadReportInterval)
		const low = int64(i * 0.95)
		const high = int64(i * 1.05)
		return time.Duration(r.Int63n(high-low+1) + low)
	}

	ticker := time.NewTicker(pick())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ticker.Reset(pick())
			load, err := b.envelope.GetLoad()
			if err != nil {
				b.logger.Error("Get weavelet load", "err", err)
				continue
			}
			if err := b.manager.ReportLoad(b.ctx, &nanny.LoadReport{
				Load:         load,
				ReplicaSet:   b.replicaSet,
				PodName:      b.podName,
				WeaveletAddr: b.envelope.WeaveletInfo().DialAddr,
				Config:       b.cfg,
			}); err != nil {
				b.logger.Error("ReportLoad", "err", err)
			}
		case <-b.ctx.Done():
			return b.ctx.Err()
		}
	}
}

// CheckHealth implements the endpoints.Babysitter interface.
func (b *Babysitter) CheckHealth(_ context.Context, req *protos.GetHealthRequest) (*protos.GetHealthReply, error) {
	return &protos.GetHealthReply{Status: b.envelope.GetHealth()}, nil
}

// RunProfiling implements the endpoints.Babysitter interface.
func (b *Babysitter) RunProfiling(_ context.Context, req *protos.GetProfileRequest) (*protos.GetProfileReply, error) {
	prof, err := b.envelope.GetProfile(req)
	if err != nil {
		return nil, fmt.Errorf("unable to profile %s for version %s of application %s and process %s: %v",
			req.ProfileType, b.cfg.Deployment.Id, b.cfg.Deployment.App.Name, logging.ShortenComponent(b.replicaSet), err)
	}
	return &protos.GetProfileReply{Data: prof}, nil
}

// ActivateComponent implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) ActivateComponent(ctx context.Context, req *protos.ActivateComponentRequest) (*protos.ActivateComponentReply, error) {
	if err := b.manager.ActivateComponent(ctx, &nanny.ActivateComponentRequest{
		Component: req.Component,
		Routed:    req.Routed,
		Config:    b.cfg,
	}); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.watchingRoutingInfo == nil {
		b.watchingRoutingInfo = map[string]struct{}{}
	}
	if _, ok := b.watchingRoutingInfo[req.Component]; !ok {
		b.watchingRoutingInfo[req.Component] = struct{}{}
		go b.watchRoutingInfo(req.Component)
	}
	return &protos.ActivateComponentReply{}, nil
}

func (b *Babysitter) watchRoutingInfo(component string) {
	version := ""
	for r := retry.Begin(); r.Continue(b.ctx); {
		routing, newVersion, err := b.getRoutingInfo(component, version)
		if err != nil {
			b.logger.Error("cannot get routing info; will retry", "err", err, "component", component)
			continue
		}
		if err := b.envelope.UpdateRoutingInfo(routing); err != nil {
			b.logger.Error("cannot update routing info; will retry", "err", err, "component", component)
			continue
		}
		version = newVersion
		if routing.Local {
			// If the routing is local, it will never change. There is no need
			// to watch.
			return
		}
		r.Reset()
	}
}

func (b *Babysitter) getRoutingInfo(component string, version string) (*protos.RoutingInfo, string, error) {
	reply, err := b.manager.GetRoutingInfo(b.ctx, &nanny.GetRoutingRequest{
		Component: component,
		Version:   version,
		Config:    b.cfg,
	})
	if err != nil {
		return nil, "", err
	}
	return reply.Routing, reply.Version, nil
}

// registerReplica registers the information about a colocation group replica
// (i.e., a weavelet).
func (b *Babysitter) registerReplica() error {
	if err := b.manager.RegisterReplica(b.ctx, &nanny.RegisterReplicaRequest{
		ReplicaSet:        b.replicaSet,
		PodName:           b.podName,
		BabysitterAddress: b.selfAddr,
		WeaveletAddress:   b.envelope.WeaveletInfo().DialAddr,
		Config:            b.cfg,
	}); err != nil {
		return err
	}

	go b.watchComponents()
	return nil
}

func (b *Babysitter) watchComponents() {
	version := ""
	for r := retry.Begin(); r.Continue(b.ctx); {
		components, newVersion, err := b.getComponentsToStart(version)
		if err != nil {
			b.logger.Error("cannot get components to start; will retry", "err", err)
			continue
		}
		version = newVersion
		if err := b.envelope.UpdateComponents(components); err != nil {
			b.logger.Error("cannot update components to start; will retry", "err", err)
			continue
		}
		r.Reset()
	}
}

func (b *Babysitter) getComponentsToStart(version string) ([]string, string, error) {
	reply, err := b.manager.GetComponentsToStart(b.ctx, &nanny.GetComponentsRequest{
		ReplicaSet: b.replicaSet,
		Version:    version,
		Config:     b.cfg,
	})
	if err != nil {
		return nil, "", err
	}
	return reply.Components, reply.Version, nil
}

// GetListenerAddress implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) GetListenerAddress(ctx context.Context, req *protos.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	return b.manager.GetListenerAddress(ctx, &nanny.GetListenerAddressRequest{
		ReplicaSet: b.replicaSet,
		Listener:   req.Name,
		Config:     b.cfg,
	})
}

// ExportListener implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) ExportListener(ctx context.Context, req *protos.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	return b.manager.ExportListener(ctx, &nanny.ExportListenerRequest{
		ReplicaSet: b.replicaSet,
		Listener:   &nanny.Listener{Name: req.Listener, Addr: req.Address},
		Config:     b.cfg,
	})
}

func (b *Babysitter) GetSelfCertificate(context.Context, *protos.GetSelfCertificateRequest) (*protos.GetSelfCertificateReply, error) {
	certPEM, keyPEM, err := b.selfCertGetter()
	if err != nil {
		return nil, err
	}
	return &protos.GetSelfCertificateReply{
		Cert: certPEM,
		Key:  keyPEM,
	}, nil
}

// VerifyClientCertificate implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) VerifyClientCertificate(_ context.Context, req *protos.VerifyClientCertificateRequest) (*protos.VerifyClientCertificateReply, error) {
	identity, err := mtls.VerifyRawCertificateChain(b.cfg.Project, b.caCert, req.CertChain)
	if err != nil {
		return nil, err
	}
	allowlist := b.cfg.IdentityAllowlist[identity]
	return &protos.VerifyClientCertificateReply{Components: allowlist.Component}, nil
}

// VerifyServerCertificate implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) VerifyServerCertificate(_ context.Context, req *protos.VerifyServerCertificateRequest) (*protos.VerifyServerCertificateReply, error) {
	actual, err := mtls.VerifyRawCertificateChain(b.cfg.Project, b.caCert, req.CertChain)
	if err != nil {
		return nil, err
	}

	expected, ok := b.cfg.ComponentIdentity[req.TargetComponent]
	if !ok {
		return nil, fmt.Errorf("unknown identity for component %q", req.TargetComponent)
	}
	if expected != actual {
		return nil, fmt.Errorf("invalid server identity for target component %s: want %q, got %q", req.TargetComponent, expected, actual)
	}
	return &protos.VerifyServerCertificateReply{}, nil
}

// HandleLogEntry implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) HandleLogEntry(_ context.Context, entry *protos.LogEntry) error {
	b.logSaver(entry)
	return nil
}

// HandleTraceSpans implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) HandleTraceSpans(_ context.Context, traces []trace.ReadOnlySpan) error {
	if b.traceSaver == nil {
		return nil
	}
	return b.traceSaver(traces)
}
