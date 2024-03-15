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
	addr   string       // babysitter address
	client *http.Client // The HTTP client to use to make requests.
}

var (
	_ endpoints.Babysitter = &Babysitter{}
	_ endpoints.Babysitter = &HttpClient{}
)

func NewHttpClient(addr string, tlsConfig *tls.Config) *HttpClient {
	client := http.DefaultClient
	if tlsConfig != nil {
		client = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	}
	return &HttpClient{
		addr:   addr,
		client: client,
	}
}

// RunProfiling implements the endpoints.Babysitter interface.
func (h *HttpClient) RunProfiling(ctx context.Context, req *protos.GetProfileRequest) (*protos.GetProfileReply, error) {
	reply := &protos.GetProfileReply{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.client,
		Addr:    h.addr,
		URLPath: runProfilingURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}

// GetLoad implements the endpoints.Babysitter interface.
func (h *HttpClient) GetLoad(ctx context.Context, req *endpoints.GetLoadRequest) (*endpoints.GetLoadReply, error) {
	reply := &endpoints.GetLoadReply{}
	err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  h.client,
		Addr:    h.addr,
		URLPath: loadURL,
		Request: req,
		Reply:   reply,
	})
	return reply, err
}
