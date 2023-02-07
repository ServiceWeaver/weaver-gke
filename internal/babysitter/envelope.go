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

	"go.opentelemetry.io/otel/sdk/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// Handler is an EnvelopeHandler that issues all requests to the manager.
type Handler struct {
	Ctx            context.Context        // context for all operations
	Config         *config.GKEConfig      // GKE config for the handler
	Manager        clients.ManagerClient  // connection to the manager
	PodName        string                 // Pod hosting babysitter/envelope
	BabysitterAddr string                 // IP address of the babysitter
	LogSaver       func(*protos.LogEntry) // called on every log entry

	TraceSaver func(spans []trace.ReadOnlySpan) error // called on every trace

	// Used for reporting the current internal IP address of the weavelet.
	// May be called multiple times, with same or different IP addresses, in
	// case the weavelet gets restarted.
	ReportWeaveletAddr func(addr string) error
}

var _ envelope.EnvelopeHandler = &Handler{}

// StartComponent implements the protos.EnvelopeHandler interface.
func (h *Handler) StartComponent(request *protos.ComponentToStart) error {
	return h.Manager.StartComponent(h.Ctx, request)
}

// StartColocationGroup implements the protos.EnvelopeHandler interface.
func (h *Handler) StartColocationGroup(request *protos.ColocationGroup) error {
	req := &nanny.ColocationGroupStartRequest{
		Config: h.Config,
		Group:  request,
	}
	return h.Manager.StartColocationGroup(h.Ctx, req)
}

// RegisterReplica implements the protos.EnvelopeHandler interface.
func (h *Handler) RegisterReplica(replica *protos.ReplicaToRegister) error {
	if err := h.ReportWeaveletAddr(replica.Address); err != nil {
		return err
	}
	return h.Manager.RegisterReplica(h.Ctx, &nanny.ReplicaToRegister{
		PodName:           h.PodName,
		BabysitterAddress: h.BabysitterAddr,
		Replica:           replica,
	})
}

// ReportLoad implements the protos.EnvelopeHandler interface.
func (h *Handler) ReportLoad(request *protos.WeaveletLoadReport) error {
	return h.Manager.ReportLoad(h.Ctx, request)
}

// ExportListener implements the protos.EnvelopeHandler interface.
func (h *Handler) ExportListener(request *protos.ListenerToExport) (*protos.ExportListenerReply, error) {
	return h.Manager.ExportListener(h.Ctx, &nanny.ExportListenerRequest{
		AppName:  h.Config.Deployment.App.Name,
		Listener: request,
		Config:   h.Config,
	})
}

// GetRoutingInfo implements the protos.EnvelopeHandler interface.
func (h *Handler) GetRoutingInfo(request *protos.GetRoutingInfo) (*protos.RoutingInfo, error) {
	return h.Manager.GetRoutingInfo(h.Ctx, request)
}

// GetComponentsToStart implements the protos.EnvelopeHandler interface.
func (h *Handler) GetComponentsToStart(request *protos.GetComponentsToStart) (*protos.ComponentsToStart, error) {
	return h.Manager.GetComponentsToStart(h.Ctx, request)
}

// RecvLogEntry implements the protos.EnvelopeHandler interface.
func (h *Handler) RecvLogEntry(entry *protos.LogEntry) {
	h.LogSaver(entry)
}

// RecvTraceSpans implements the protos.EnvelopeHandler interface.
func (h *Handler) RecvTraceSpans(traces []trace.ReadOnlySpan) error {
	if h.TraceSaver == nil {
		return nil
	}
	return h.TraceSaver(traces)
}
