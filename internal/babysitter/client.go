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
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// HttpClient is a Client that executes requests over HTTP.
type HttpClient struct {
	Addr string // babysitter address
}

var (
	_ clients.BabysitterClient = &Babysitter{}
	_ clients.BabysitterClient = &HttpClient{}
)

// RunProfiling implements the clients.BabysitterClient interface.
func (h *HttpClient) RunProfiling(ctx context.Context, req *protos.RunProfiling) (*protos.Profile, error) {
	reply := &protos.Profile{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  http.DefaultClient,
		Addr:    h.Addr,
		URLPath: runProfilingURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}

// CheckHealth implements the clients.BabysitterClient interface.
func (h *HttpClient) CheckHealth(ctx context.Context, req *clients.HealthCheck) (*protos.HealthReport, error) {
	reply := &protos.HealthReport{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  &http.Client{Timeout: req.Timeout.AsDuration()},
		Addr:    h.Addr,
		URLPath: healthURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}
