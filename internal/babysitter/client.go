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
	"crypto/tls"
	"net/http"

	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// HttpClient is a Client that executes requests over HTTP.
type HttpClient struct {
	Addr      string      // babysitter address
	TLSConfig *tls.Config // TLS config, possibly nil.
}

var (
	_ endpoints.Babysitter = &Babysitter{}
	_ endpoints.Babysitter = &HttpClient{}
)

// client returns the HTTP client to use to make requests.
func (h *HttpClient) client() *http.Client {
	if h.TLSConfig == nil {
		return http.DefaultClient
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: h.TLSConfig,
		},
	}
}

// RunProfiling implements the endpoints.Babysitter interface.
func (h *HttpClient) RunProfiling(ctx context.Context, req *protos.GetProfileRequest) (*protos.GetProfileReply, error) {
	reply := &protos.GetProfileReply{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.client(),
		Addr:    h.Addr,
		URLPath: runProfilingURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}

// CheckHealth implements the endpoints.Babysitter interface.
func (h *HttpClient) CheckHealth(ctx context.Context, req *protos.GetHealthRequest) (*protos.GetHealthReply, error) {
	reply := &protos.GetHealthReply{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.client(),
		Addr:    h.Addr,
		URLPath: healthURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}
