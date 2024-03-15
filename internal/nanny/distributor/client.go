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

package distributor

import (
	"context"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

var _ endpoints.Distributor = &Distributor{}

// HttpClient is a Client that executes requests over HTTP.
type HttpClient struct {
	Addr   string       // babysitter address
	Client *http.Client // The HTTP client to use to make requests.
}

var _ endpoints.Distributor = &HttpClient{}

// Distribute implements the endpoints.Distributor interface.
func (h *HttpClient) Distribute(ctx context.Context, req *nanny.ApplicationDistributionRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: distributeURL,
		Request: req,
	})
}

// Cleanup implements the endpoints.Distributor interface.
func (h *HttpClient) Cleanup(ctx context.Context, req *nanny.ApplicationCleanupRequest) error {
	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: cleanupURL,
		Request: req,
	})
}

// GetApplicationState implements the endpoints.Distributor interface.
func (h *HttpClient) GetApplicationState(ctx context.Context, req *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error) {
	reply := &nanny.ApplicationStateAtDistributor{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: getApplicationStateURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}

// GetPublicTrafficAssignment implements the endpoints.Distributor interface.
func (h *HttpClient) GetPublicTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	reply := &nanny.TrafficAssignment{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: getPublicTrafficAssignmentURL,
		Request: nil,
		Reply:   reply,
	})
	return reply, err
}

// GetPrivateTrafficAssignment implements the endpoints.Distributor interface.
func (h *HttpClient) GetPrivateTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error) {
	reply := &nanny.TrafficAssignment{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: getPrivateTrafficAssignmentURL,
		Request: nil,
		Reply:   reply,
	})
	return reply, err
}

// RunProfiling implements the endpoints.Distributor interface.
func (h *HttpClient) RunProfiling(ctx context.Context, req *nanny.GetProfileRequest) (*protos.GetProfileReply, error) {
	reply := &protos.GetProfileReply{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.Client,
		Addr:    h.Addr,
		URLPath: runProfilingURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}
