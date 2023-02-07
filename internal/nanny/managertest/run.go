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

// Package managertest provides a weavertest like testing framework for testing
// the gke and gke-local manager.
package managertest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/trace"
	weaver "github.com/ServiceWeaver/weaver"
	"github.com/ServiceWeaver/weaver-gke/internal"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/clients"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/envelope"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// Run creates a brand new gke-local execution environment that places every
// component in its own colocation group and executes the provided function on
// the root component of the new application.
func Run(t testing.TB, f func(weaver.Instance)) {
	// There are three distinct types of execution in a manager test: (1) the
	// unit test, (2) the main Service Weaver process, and (3) every other Service Weaver
	// process.
	//
	// When you run "go test", Init (1) runs a manager in process. Then, it
	// launches itself (i.e. the test binary) in a subprocess as the main Service Weaver
	// process (2). The main Service Weaver process executes the provided function f. f
	// either fails (e.g., by calling t.Fatal) or passes. The root unit test
	// (1) monitors the main Service Weaver process (2). If the main Service Weaver process
	// fails, then the unit test fails; otherwise, it passes. All other Service Weaver
	// processes (3) are launched via the manager and do not execute f. They
	// block on a call to Init.

	t.Helper()
	bootstrap, err := runtime.GetBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.HasPipes() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "panic in Service Weaver sub-process: %v\n", r)
			} else {
				fmt.Fprintf(os.Stderr, "Service Weaver sub-process exiting\n")
			}
		}()
		// If this is the main Service Weaver process, weaver.Init will return and f will
		// execute. If this is a different Service Weaver process, weaver.Init blocks.
		f(weaver.Init(context.Background()))
		return
	}

	// Start the manager.
	ctx, cancel := context.WithCancel(context.Background())
	mux := http.NewServeMux()
	server := httptest.NewUnstartedServer(mux)
	store := store.NewFakeStore()
	depid := uuid.New().String()
	starter := local.NewStarter(store)
	logger := logging.NewTestLogger(t)
	addr := internal.ToHTTPAddress(server.Listener.Addr().String())
	babysitterConstructor := func(addr string) clients.BabysitterClient {
		return &babysitter.HttpClient{Addr: internal.ToHTTPAddress(addr)}
	}
	replicaExists := func(context.Context, string) (bool, error) { return true, nil }
	manager.Start(ctx, mux, store, &logger.FuncLogger, addr, babysitterConstructor, replicaExists, recordListener, starter.Start, nil, nil)
	server.Start()
	defer func() {
		// Silence the logger to ignore all the junk that gets printed when we
		// cancel the context and shut down the manager.
		logger.Silence()
		cancel()
		starter.Stop(ctx, []string{depid})
		server.Close()
	}()

	// Start the main Service Weaver process as a subprocess.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("error fetching binary path: %v", err)
	}
	weavelet := &protos.Weavelet{
		Id: uuid.New().String(),
		Dep: &protos.Deployment{
			Id: depid,
			App: &protos.AppConfig{
				Name:   strings.ReplaceAll(t.Name(), "/", "_"),
				Binary: exe,
				// TODO: Forward os.Args[1:] as well?
				Args: []string{"-test.run", regexp.QuoteMeta(t.Name())},
			},
			UseLocalhost:      true,
			ProcessPicksPorts: true,
			NetworkStorageDir: t.TempDir(),
		},
		Group: &protos.ColocationGroup{
			Name: "main",
		},
		GroupReplicaId: uuid.New().String(),
		Process:        "main",
	}
	cfg := &config.GKEConfig{
		ManagerAddr: addr,
		Deployment:  weavelet.Dep,
	}
	handler := &babysitter.Handler{
		Ctx:      ctx,
		Config:   cfg,
		Manager:  &manager.HttpClient{Addr: addr},
		LogSaver: logger.Log,
		TraceSaver: func([]trace.ReadOnlySpan) error {
			return nil
		},
		ReportWeaveletAddr: func(addr string) error { return nil },
	}
	env, err := envelope.NewEnvelope(weavelet, handler, envelope.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer env.Stop()
	if err := env.Run(ctx); err != nil {
		// If the main subprocess fails---by calling t.Fatal for example---then
		// env.Run will return an error.
		t.Fatal(err)
	}
}

func recordListener(_ context.Context, _ *config.GKEConfig, _ *protos.ColocationGroup, _ *protos.Listener) (*protos.ExportListenerReply, error) {
	return &protos.ExportListenerReply{}, nil
}
