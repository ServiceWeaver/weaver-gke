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

package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/ServiceWeaver/weaver/runtime/logging"
)

func backend(addr string, weight float32) *Backend {
	return &Backend{
		Address: addr,
		Weight:  weight,
	}
}

func TestProxyPick(t *testing.T) {
	group := backendGroup{
		sum: 100.0,
		backends: []*Backend{
			backend("0", 10.0),
			backend("1", 30.0),
			backend("2", 20.0),
			backend("3", 40.0),
		},
	}
	for _, test := range []struct {
		val  float32
		want string
	}{
		{0.0, "0"},
		{0.1, "1"}, {0.2, "1"}, {0.3, "1"},
		{0.4, "2"}, {0.5, "2"},
		{0.6, "3"}, {0.7, "3"}, {0.8, "3"}, {0.9, "3"}, {1.0, "3"},
	} {
		t.Run(fmt.Sprintf("%f", test.val), func(t *testing.T) {
			proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)
			proxy.rand = func() float32 { return test.val }
			if got, want := proxy.pick(group).Address, test.want; got != want {
				t.Fatalf("proxy.pick(%v): got %q, want %q", group, got, want)
			}
		})
	}
}

func TestProxyGroup(t *testing.T) {
	proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)
	backends := []*Backend{backend("a", 1.0), backend("b", 2.0), backend("c", 3.0)}
	got, err := proxy.group("clump", backends)
	if err != nil {
		t.Fatalf("proxy.group(%v): %v", backends, err)
	}

	want := backendGroup{clump: "clump", sum: 6.0, backends: backends}
	if got.sum != want.sum {
		t.Fatalf("bad sum: got %f, want %f", got.sum, want.sum)
	}

	if diff := cmp.Diff(want.backends, got.backends, protocmp.Transform()); diff != "" {
		t.Fatalf("bad backendGroup: (-want +got)\n%s", diff)
	}
}

func TestProxyGroupErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		backends []*Backend
	}{
		{"empty", []*Backend{}},
		{"zero", []*Backend{backend("a", 0.0)}},
		{"negative", []*Backend{backend("a", -1.0)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)
			_, err := proxy.group("", test.backends)
			if err == nil {
				t.Fatalf("proxy.group(%v): unexpected success", test.backends)
			}
		})
	}
}

func TestGoodClumping(t *testing.T) {
	a0 := backend("a0", 1.0)
	b0 := backend("b0", 2.0)
	b1 := backend("b1", 3.0)
	b2 := backend("b2", 4.0)
	c0 := backend("c0", 5.0)
	c1 := backend("c1", 6.0)

	proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)
	for _, test := range []struct {
		clump string
		rules map[string][]*Backend
	}{
		{"a", map[string][]*Backend{"a0": {a0}}},
		{"b", map[string][]*Backend{"b0": {b0}, "b1": {b1}}},
		{"c", map[string][]*Backend{"c0": {c0}, "c1": {c1}}},
		{"b", map[string][]*Backend{"b1": {b1}, "b2": {b2}}},
		{"a", map[string][]*Backend{}},
	} {
		if err := proxy.route(test.clump, test.rules); err != nil {
			t.Fatalf("proxy.route(%v, %q): %v", test.rules, test.clump, err)
		}
	}

	want := map[string]backendGroup{
		"b1": {clump: "b", sum: 3.0, backends: []*Backend{b1}},
		"b2": {clump: "b", sum: 4.0, backends: []*Backend{b2}},
		"c0": {clump: "c", sum: 5.0, backends: []*Backend{c0}},
		"c1": {clump: "c", sum: 6.0, backends: []*Backend{c1}},
	}
	opts := []cmp.Option{protocmp.Transform(), cmp.AllowUnexported(backendGroup{})}
	if diff := cmp.Diff(want, proxy.groups, opts...); diff != "" {
		t.Fatalf("bad groups: (-want +got)\n%s", diff)
	}
}

func TestBadClumping(t *testing.T) {
	proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)
	rules := map[string][]*Backend{"host": {backend("addr", 1.0)}}
	if err := proxy.route("a", rules); err != nil {
		t.Fatalf("proxy.route(%q, %v): %v", "a", rules, err)
	}
	if err := proxy.route("b", rules); err == nil {
		t.Fatalf("proxy.route(%q, %v): unexpected success", "b", rules)
	}
}

// server returns a new httptest.Server that replies to all requests with the
// provided name. The returned server is closed when unit test t ends.
func server(t *testing.T, name string) *httptest.Server {
	who := func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, name)
	}
	server := httptest.NewServer(http.HandlerFunc(who))
	t.Cleanup(func() { server.Close() })
	return server
}

// get issues a GET / request to addr with the provided host header.
func get(t *testing.T, addr, host string) string {
	t.Helper()

	client := http.Client{}
	url := fmt.Sprintf("http://%s", addr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatalf("http.NewRequest(%q, %q, nil): %v", "GET", url, err)
	}
	req.Host = host
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do(%v): %v", req, err)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll(%v): %v", resp.Body, err)
	}
	return string(b)
}

func TestProxyServe(t *testing.T) {
	a0 := server(t, "a0")
	b0, b1 := server(t, "b0"), server(t, "b1")
	c0, c1, c2 := server(t, "c0"), server(t, "c1"), server(t, "c2")

	addr := func(s *httptest.Server) string { return s.Listener.Addr().String() }
	proxy := NewProxy(&logging.NewTestLogger(t).FuncLogger)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("net.Listen(%s, %s): %v", "tcp", "localhost:0", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Serve(ctx, lis, lis.Addr().String())

	// Register a rule for host a. Verify that the rule is set.
	rules := map[string][]*Backend{
		"a": {backend(addr(a0), 1.0)},
	}
	if err := proxy.route("", rules); err != nil {
		t.Fatalf("proxy.route: %v", err)
	}
	// GET / HTTP/1.1
	// Host: a
	if got, want := get(t, lis.Addr().String(), "a"), "a0"; got != want {
		t.Errorf("get(%q): got %q, want %q", "a", got, want)
	}

	// Overwrite the rules with a set of rules for hosts b and c.
	rules = map[string][]*Backend{
		"b": {backend(addr(b0), 0.5), backend(addr(b1), 0.5)},
		"c": {backend(addr(c0), 0.33), backend(addr(c1), 0.33), backend(addr(c2), 0.33)},
	}
	if err := proxy.route("", rules); err != nil {
		t.Fatalf("proxy.route: %v", err)
	}

	// Verify that the proxy contains only entries for hosts b and c.
	if _, found := proxy.groups["a"]; found {
		t.Fatalf("found unexpected rule for host a")
	}

	// Get requests for hosts b and c. Verify that corresponding rules exists.
	// GET / HTTP/1.1
	// Host: b
	switch got := get(t, lis.Addr().String(), "b"); got {
	case "b0", "b1":
	default:
		t.Errorf("get(%q): got %q, want %q or %q", "b", got, "b0", "b1")
	}

	// GET / HTTP/1.1
	// Host: c
	switch got := get(t, lis.Addr().String(), "c"); got {
	case "c0", "c1", "c2":
	default:
		t.Errorf("get(%q): got %q, want %q, %q, or %q", "c", got, "c0", "c1", "c2")
	}

	// Register a rule for host a in a different clump.
	rules = map[string][]*Backend{
		"a": {backend(addr(a0), 1.0)},
	}
	if err := proxy.route("a", rules); err != nil {
		t.Fatalf("proxy.route: %v", err)
	}

	// GET / HTTP/1.1
	// Host: a
	if got, want := get(t, lis.Addr().String(), "a"), "a0"; got != want {
		t.Errorf("get(%q): got %q, want %q", "a", got, want)
	}

	// GET / HTTP/1.1
	// Host: b
	switch got := get(t, lis.Addr().String(), "b"); got {
	case "b0", "b1":
	default:
		t.Errorf("get(%q): got %q, want %q or %q", "b", got, "b0", "b1")
	}

	// GET / HTTP/1.1
	// Host: c
	switch got := get(t, lis.Addr().String(), "c"); got {
	case "c0", "c1", "c2":
	default:
		t.Errorf("get(%q): got %q, want %q, %q, or %q", "c", got, "c0", "c1", "c2")
	}
}
