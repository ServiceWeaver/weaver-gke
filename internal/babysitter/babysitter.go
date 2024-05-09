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
	"log/slog"
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
)

const (
	// URL suffixes for various HTTP endpoints exported by the babysitter.
	loadURL         = "/load"
	runProfilingURL = "/run_profiling"
)

// Babysitter starts and manages a weavelet inside the Pod.
type Babysitter struct {
	ctx            context.Context
	mux            *http.ServeMux
	selfAddr       string // HTTP address for the listener
	cfg            *config.GKEConfig
	replicaSet     string
	projectName    string
	podName        string
	envelope       *envelope.Envelope
	manager        endpoints.Manager
	caCert         *x509.Certificate
	getSelfCert    func() ([]byte, []byte, error)
	logger         *slog.Logger
	logSaver       func(*protos.LogEntry)
	traceSaver     func(spans *protos.TraceSpans) error
	metricExporter func(metrics []*metrics.MetricSnapshot) error

	mu                  sync.Mutex
	watchingRoutingInfo map[string]struct{}
}

var _ envelope.EnvelopeHandler = &Babysitter{}
var _ endpoints.Babysitter = &Babysitter{}

// Start creates and starts a new babysitter.
func Start(
	ctx context.Context,
	logger *slog.Logger,
	cfg *config.GKEConfig,
	replicaSet string,
	projectName string,
	podName string,
	internalAddress string,
	mux *http.ServeMux,
	selfAddr string,
	manager endpoints.Manager,
	caCert *x509.Certificate,
	getSelfCert func() ([]byte, []byte, error),
	logSaver func(*protos.LogEntry),
	traceSaver func(spans *protos.TraceSpans) error,
	metricExporter func(metrics []*metrics.MetricSnapshot) error,
) (*Babysitter, error) {
	// Create the envelope.
	//
	// We use the PodName as a unique weavelet id for the following reasons:
	//   * It is derived from a unique 63-bit value, so collisions are
	//     unlikely.
	//   * It may be useful to be associated with a Pod for debugging etc.
	//   * It allows the manager to quickly check if the weavelet is still
	//     active by asking the Kubernetes API if the Pod with a given name
	//     exists.
	args := &protos.WeaveletArgs{
		App:             cfg.Deployment.App.Name,
		DeploymentId:    cfg.Deployment.Id,
		Id:              podName,
		RunMain:         replicaSet == runtime.Main,
		Mtls:            cfg.Mtls,
		InternalAddress: internalAddress,
		// ControlSocket, Redirects filled by envelope.NewEnvelope
	}
	e, err := envelope.NewEnvelope(ctx, args, cfg.Deployment.App, envelope.Options{
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}

	// Create the babysitter.
	b := &Babysitter{
		ctx:            ctx,
		mux:            mux,
		cfg:            cfg,
		replicaSet:     replicaSet,
		projectName:    projectName,
		podName:        podName,
		envelope:       e,
		selfAddr:       selfAddr,
		manager:        manager,
		caCert:         caCert,
		getSelfCert:    getSelfCert,
		logger:         logger,
		logSaver:       logSaver,
		traceSaver:     traceSaver,
		metricExporter: metricExporter,
	}

	// Register babysitter handlers.
	mux.HandleFunc(loadURL, protomsg.HandlerFunc(b.logger, b.GetLoad))
	mux.HandleFunc(runProfilingURL, protomsg.HandlerFunc(b.logger, b.RunProfiling))

	// Start a goroutine to periodically export metrics.
	if b.metricExporter != nil {
		go b.exportMetrics()
	}

	// Start the envelope.
	go b.envelope.Serve(b)

	// Watch for components to start.
	go b.watchComponentsToStart()

	return b, nil
}

// WeaveletAddress returns the address that other components should dial to communicate with the
// weavelet.
func (b *Babysitter) WeaveletAddress() string { return b.envelope.WeaveletAddress() }

// SelfAddr returns the address on which the babysitter is listening on
// for incoming requests.
func (b *Babysitter) SelfAddr() string { return b.selfAddr }

// exportMetrics periodically exports metrics.
func (b *Babysitter) exportMetrics() {
	// Time interval at which metrics are exported.
	ticker := time.NewTicker(b.cfg.Telemetry.Metrics.ExportInterval.AsDuration())
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

// GetLoad implements the endpoints.Babysitter interface.
func (b *Babysitter) GetLoad(_ context.Context, req *endpoints.GetLoadRequest) (*endpoints.GetLoadReply, error) {
	status := b.envelope.GetHealth()
	if status != protos.HealthStatus_HEALTHY {
		return nil, fmt.Errorf("weavelet unhealthy")
	}
	load, err := b.envelope.GetLoad()
	if err != nil {
		return nil, fmt.Errorf("couldn't get weavelet load")
	}
	return &endpoints.GetLoadReply{
		Load:         load,
		WeaveletAddr: b.WeaveletAddress(),
	}, nil
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

// LogBatch implements the control.DeployerControl interface.
func (b *Babysitter) LogBatch(ctx context.Context, batch *protos.LogEntryBatch) error {
	for _, entry := range batch.Entries {
		b.logSaver(entry)
	}
	return nil
}

// ActivateComponent implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) ActivateComponent(ctx context.Context, req *protos.ActivateComponentRequest) (*protos.ActivateComponentReply, error) {
	targetReplicaSet := config.ReplicaSetForComponent(req.Component, b.cfg)
	if err := b.manager.ActivateComponent(ctx, &nanny.ActivateComponentRequest{
		Component:  req.Component,
		Routed:     req.Routed,
		ReplicaSet: targetReplicaSet,
		Config:     b.cfg,
	}); err != nil {
		return nil, err
	}

	// Continuously collect routing info the activated component.
	local := targetReplicaSet == b.replicaSet
	if local && !req.Routed {
		// Local non-routed component. The routing will never change and hence
		// we don't need to watch it.
		for r := retry.Begin(); r.Continue(ctx); {
			if err := b.envelope.UpdateRoutingInfo(&protos.RoutingInfo{
				Component: req.Component,
				Local:     true,
			}); err != nil {
				b.logger.Error("cannot update routing info; will retry", "err", err, "component", req.Component)
				continue
			}
			break
		}
		return &protos.ActivateComponentReply{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.watchingRoutingInfo == nil {
		b.watchingRoutingInfo = map[string]struct{}{}
	}
	if _, ok := b.watchingRoutingInfo[req.Component]; !ok {
		b.watchingRoutingInfo[req.Component] = struct{}{}
		go b.watchRoutingInfo(b.ctx, req.Component, targetReplicaSet)
	}
	return &protos.ActivateComponentReply{}, nil
}

// watchRoutingInfo watches and updates the routing information for a given
// non-local component.
func (b *Babysitter) watchRoutingInfo(ctx context.Context, targetComponent, targetReplicaSet string) {
	version := ""
	for r := retry.Begin(); r.Continue(ctx); {
		reply, err := b.manager.GetRoutingInfo(ctx, &nanny.GetRoutingRequest{
			Component:  targetComponent,
			Version:    version,
			ReplicaSet: targetReplicaSet,
			Config:     b.cfg,
		})
		if err != nil {
			b.logger.Error("cannot get routing info for a component; will retry", "err", err, "component", targetComponent)
			continue
		}
		if err := b.envelope.UpdateRoutingInfo(reply.Routing); err != nil {
			b.logger.Error("cannot update routing info for a component; will retry", "err", err, "component", targetComponent)
			continue
		}
		version = reply.Version
		r.Reset()
	}
}

// watchComponentsToStart watches and updates the set of components the local
// weavelet should start.
func (b *Babysitter) watchComponentsToStart() {
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
	for r := retry.Begin(); r.Continue(ctx); {
		reply, err := b.manager.GetListenerAddress(ctx, &nanny.GetListenerAddressRequest{
			ReplicaSet: b.replicaSet,
			Listener:   req.Name,
			Config:     b.cfg,
		})
		if err == nil {
			return reply, nil
		}
	}
	return nil, ctx.Err()
}

// ExportListener implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) ExportListener(ctx context.Context, req *protos.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	return b.manager.ExportListener(ctx, &nanny.ExportListenerRequest{
		ReplicaSet: b.replicaSet,
		Listener:   &nanny.Listener{Name: req.Listener, Addr: req.Address},
		Config:     b.cfg,
	})
}

// GetSelfCertificate implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) GetSelfCertificate(context.Context, *protos.GetSelfCertificateRequest) (*protos.GetSelfCertificateReply, error) {
	certPEM, keyPEM, err := b.getSelfCert()
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
	identity, err := mtls.VerifyRawCertificateChain(b.projectName, b.caCert, req.CertChain)
	if err != nil {
		return nil, err
	}
	allowlist := b.cfg.IdentityAllowlist[identity]
	return &protos.VerifyClientCertificateReply{Components: allowlist.Component}, nil
}

// VerifyServerCertificate implements the envelope.EnvelopeHandler interface.
func (b *Babysitter) VerifyServerCertificate(_ context.Context, req *protos.VerifyServerCertificateRequest) (*protos.VerifyServerCertificateReply, error) {
	actual, err := mtls.VerifyRawCertificateChain(b.projectName, b.caCert, req.CertChain)
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
func (b *Babysitter) HandleTraceSpans(_ context.Context, traces *protos.TraceSpans) error {
	if b.traceSaver == nil {
		return nil
	}
	return b.traceSaver(traces)
}
