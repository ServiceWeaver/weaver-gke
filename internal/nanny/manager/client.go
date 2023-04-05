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

package manager

import (
	"context"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// HttpClient is a Client that executes requests over HTTP.
type HttpClient struct {
	Addr string // manager address
}

var (
	_ clients.ManagerClient = &manager{}
	_ clients.ManagerClient = &HttpClient{}
)

// Deploy implements the clients.ManagerClient interface.
func (h *HttpClient) Deploy(ctx context.Context, req *nanny.ApplicationDeploymentRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: deployURL,
		Request: req,
	})
}

// Stop implements the clients.ManagerClient interface.
func (h *HttpClient) Stop(ctx context.Context, req *nanny.ApplicationStopRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: stopURL,
		Request: req,
	})
}

// Delete implements the clients.ManagerClient interface.
func (h *HttpClient) Delete(ctx context.Context, req *nanny.ApplicationDeleteRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: deleteURL,
		Request: req,
	})
}

// GetReplicaSetState implements the clients.ManagerClient interface.
func (h *HttpClient) GetReplicaSetState(ctx context.Context, req *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error) {
	reply := &nanny.ReplicaSetState{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: getReplicaSetStateURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}

// ActivateComponent implements the clients.ManagerClient interface.
func (h *HttpClient) ActivateComponent(ctx context.Context, req *nanny.ActivateComponentRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: activateComponentURL,
		Request: req,
	})
}

// RegisterReplica implements the clients.ManagerClient interface.
func (h *HttpClient) RegisterReplica(ctx context.Context, req *nanny.RegisterReplicaRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: registerReplicaURL,
		Request: req,
	})
}

// ReportLoad implements the clients.ManagerClient interface.
func (h *HttpClient) ReportLoad(ctx context.Context, req *nanny.LoadReport) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: reportLoadURL,
		Request: req,
	})
}

// GetListenerAddress implements the clients.ManagerClient interface.
func (h *HttpClient) GetListenerAddress(ctx context.Context, req *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error) {
	reply := &protos.GetListenerAddressReply{}
	if err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: getListenerAddressURL,
		Request: req,
		Reply:   reply,
	}); err != nil {
		return nil, err
	}
	return reply, nil
}

// ExportListener implements the clients.ManagerClient interface.
func (h *HttpClient) ExportListener(ctx context.Context, req *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error) {
	reply := &protos.ExportListenerReply{}
	if err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: exportListenerURL,
		Request: req,
		Reply:   reply,
	}); err != nil {
		return nil, err
	}
	return reply, nil
}

// GetRoutingInfo implements the clients.ManagerClient interface.
func (h *HttpClient) GetRoutingInfo(ctx context.Context, req *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error) {
	reply := &nanny.GetRoutingReply{}
	if err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: getRoutingInfoURL,
		Request: req,
		Reply:   reply,
	}); err != nil {
		return nil, err
	}
	return reply, nil
}

// GetComponentsToStart implements the clients.ManagerClient interface.
func (h *HttpClient) GetComponentsToStart(ctx context.Context, req *nanny.GetComponentsRequest) (
	*nanny.GetComponentsReply, error) {
	reply := &nanny.GetComponentsReply{}
	if err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: getComponentsToStartURL,
		Request: req,
		Reply:   reply,
	}); err != nil {
		return nil, err
	}
	return reply, nil
}
