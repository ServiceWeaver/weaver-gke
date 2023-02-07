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

// Package proxy provides an implementation of a simple proxying service.
//
// TODO(mwhittaker): Merge into gke/internal/local.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"

	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
)

const (
	// URLs for various HTTP endpoints exported by the proxy. These endpoints
	// are only accessible when using the hostname 'serviceweaver.internal'.
	RouteURL    = "/proxy/route"
	notFoundURL = "/proxy/404"
	statusURL   = "/statusz"

	// Host used to update proxy state.
	Host = "serviceweaver.internal"
)

type backendGroup struct {
	clump    string     // the clump this group belongs to (see route)
	sum      float32    // the sum of weights in backends
	backends []*Backend // the backends
}

// proxy is an HTTP proxy that forwards traffic, based on the Host header, to a
// set of backends. Every backend, for a given host, is associated with a
// weight. HTTP requests are forwarded at random to the backends with
// probability proportional to the backends' weights.
//
// Example usage:
//
//	// Create a proxy on port 10000 that forwards HTTP requests with Host
//	// header "foo.local" to "localhost:2000" with 20% probability and
//	// "localhost:8000" with 80% probability. Run
//	//
//	//     curl --header "Host: foo.local" localhost:10000
//	//
//	// to send a request to the proxy.
//	proxy := newProxy()
//	routes := map[string][]*Backend{
//		"foo.local": {
//			&Backend{Address:"localhost:2000", Weight:0.2},
//			&Backend{Address:"localhost:8000", Weight:0.8},
//		},
//	}
//	err := proxy.Route(routes)
//	lis, err := net.Listen("tcp", ":10000")
//	err := proxy.Serve(context.Background(), lis)
type proxy struct {
	addr    string                  // dialable server address
	logger  *logging.FuncLogger     // logger
	rand    func() float32          // dependency injected rand.Float32
	reverse httputil.ReverseProxy   // underlying proxy
	mu      sync.RWMutex            // guards groups
	groups  map[string]backendGroup // a map from host header to backends
}

// NewProxy returns a new proxy.
func NewProxy(logger *logging.FuncLogger) *proxy {
	p := &proxy{
		logger: logger,
		rand:   rand.Float32,
		groups: map[string]backendGroup{},
	}
	p.reverse = httputil.ReverseProxy{Director: p.director}
	return p
}

// handleRoute is an HTTP interface for Route().
func (p *proxy) handleRoute(ctx context.Context, req *RouteRequest) error {
	rules := map[string][]*Backend{}
	for host, b := range req.HostBackends {
		rules[host] = b.Backends
	}
	return p.route(req.Clump, rules)
}

// handleStatusz is a handler for the /statusz endpoint that returns a json
// serialization of the proxy's current routing rules.
func (p *proxy) handleStatusz(w http.ResponseWriter, _ *http.Request) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	type Group struct {
		Clump    string     `json:"clump"`
		Backends []*Backend `json:"backends"`
	}
	backends := map[string]Group{}
	for host, group := range p.groups {
		backends[host] = Group{group.clump, group.backends}
	}
	const prefix = ""
	const indent = "    "
	bytes, err := json.MarshalIndent(backends, prefix, indent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%s\n", string(bytes))
}

// handleNotFound is a handler for the /proxy/404?host=<host> endpoint that
// returns an error describing that the provided host is not found.
func (p *proxy) handleNotFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
	fmt.Fprintf(w, "host %q not found\n", r.URL.Query().Get("host"))
}

// director implements a ReverseProxy.Director function [1].
//
// [1]: https://pkg.go.dev/net/http/httputil#ReverseProxy
func (p *proxy) director(r *http.Request) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	host := r.Host
	group, ok := p.groups[host]
	if !ok {
		// The host is not found. Ideally, we could directly return an error
		// message to the user right here, but the ReverseProxy doesn't make
		// this easy. We instead have to jump through some hoops and redirect
		// the request to our own /proxy/404 endpoint, which will generate the
		// error response.
		p.logger.Error("proxy.director", errors.New("unknown host"), "host", r.Host)
		r.URL.Scheme = "http"
		r.Host = Host
		r.URL.Host = p.addr
		r.URL.Path = notFoundURL
		r.URL.RawQuery = fmt.Sprintf("host=%s", host)
		return
	}

	backend := p.pick(group)
	r.URL.Scheme = "http" // TODO(mwhittaker): Support HTTPS.
	r.URL.Host = backend.Address
}

// pick picks a backend from a set of backends with probability proportional to
// the backends' weights.
func (p *proxy) pick(group backendGroup) *Backend {
	f := p.rand() * group.sum
	var cumsum float32 = 0.0
	for _, backend := range group.backends {
		cumsum += backend.Weight
		if f < cumsum {
			return backend
		}
	}
	return group.backends[len(group.backends)-1]
}

// group groups together a set of backends into a backendGroup.
func (p *proxy) group(clump string, backends []*Backend) (backendGroup, error) {
	if len(backends) == 0 {
		return backendGroup{}, fmt.Errorf("proxy.group(%v): no backends", backends)
	}

	var sum float32 = 0.0
	copied := make([]*Backend, len(backends))
	for i, backend := range backends {
		if backend.Weight <= 0.0 {
			err := fmt.Errorf(
				"proxy.group(%v): backend %d has non-positive weight %f",
				backends, i, backend.Weight)
			return backendGroup{}, err
		}
		sum += backend.Weight
		copied[i] = backend
	}
	return backendGroup{clump: clump, sum: sum, backends: copied}, nil
}

// route atomically registers a set of forwarding rules.
//
// A set of forwarding rules maps a hostname to a list of backends. All HTTP
// requests with the provided Host header are forwarded to one of the backends,
// chosen at random with probability proportional to the backends' weights. The
// provided backends must not be empty. The weight of every backend must be
// positive, but the sum of all weights does not have to equal 1. route is
// thread-safe and can be called either before or after Serve has been called.
//
// A proxy partitions the set of forwarding rules into disjoint partitions
// called clumps. That is, every forwarding rule belongs to exactly one clump.
// When a new set of forwarding rules is installed for a clump, all previous
// rules in the clump are overwritten (or deleted if not present in the new
// clump). This update is atomic; the clump will either be fully updated or
// completely unchanged.
func (p *proxy) route(clump string, rules map[string][]*Backend) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Massage the backends into groups.
	groups := make(map[string]backendGroup, len(rules))
	for host, backends := range rules {
		group, err := p.group(clump, backends)
		if err != nil {
			return err
		}
		groups[host] = group
	}

	// Check that the hosts in the new set of forwarding rules don't already
	// belong to a different clump.
	for host := range rules {
		if group, ok := p.groups[host]; ok && group.clump != clump {
			return fmt.Errorf("proxy.route: host %q already has clump %q (want %q)", host, group.clump, clump)
		}
	}

	// Overwrite the clump.
	for host, group := range p.groups {
		if group.clump == clump {
			delete(p.groups, host)
		}
	}
	for host, group := range groups {
		p.groups[host] = group
	}
	return nil
}

// Serve runs the proxy. It should only be called once. Cancel the provided
// context to shut down the proxy. addr is a dialable version of lis.Addr().
func (p *proxy) Serve(ctx context.Context, lis net.Listener, addr string) error {
	p.addr = addr
	mux := http.NewServeMux()
	mux.HandleFunc(Host+RouteURL, protomsg.HandlerDo(p.logger, p.handleRoute))
	mux.HandleFunc(Host+statusURL, p.handleStatusz)
	mux.HandleFunc(Host+notFoundURL, p.handleNotFound)
	mux.HandleFunc(Host+"/", http.NotFound)
	mux.Handle("/", &p.reverse)
	server := http.Server{Handler: mux}
	errs := make(chan error, 1)
	go func() { errs <- server.Serve(lis) }()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return server.Shutdown(ctx)
	}
}
