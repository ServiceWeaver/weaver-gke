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

package local

import (
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/metricdb"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	protos "github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/traces"
	"github.com/google/uuid"
)

// startBabysitter creates and starts a babysitter in a gke-local deployment.
func startBabysitter(ctx context.Context, cfg *config.GKEConfig, s *Starter, replicaSet string, logDir string, caCert *x509.Certificate, caKey crypto.PrivateKey) (*babysitter.Babysitter, error) {
	podName := uuid.New().String()
	ls, err := logging.NewFileStore(logDir)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}
	defer ls.Close()
	logSaver := ls.Add
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        cfg.Deployment.App.Name,
			Deployment: cfg.Deployment.Id,
			Component:  "Babysitter",
			Weavelet:   podName,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: logSaver,
	})

	// Setup trace recording.
	traceDB, err := traces.OpenDB(ctx, TracesFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open Perfetto database: %w", err)
	}
	traceSaver := func(spans *protos.TraceSpans) error {
		return traceDB.Store(ctx, cfg.Deployment.App.Name, cfg.Deployment.Id, spans)
	}

	// Setup metrics recording.
	metricDB, err := metricdb.Open(ctx, MetricsFile)
	if err != nil {
		return nil, err
	}
	metricExporter := func(metrics []*metrics.MetricSnapshot) error {
		now := time.Now()
		for _, m := range metrics {
			if err := metricDB.Record(ctx, m, now); err != nil {
				return err
			}
		}
		return nil
	}

	// Generate a self certificate, used by the weavelet to communicate with its
	// peers, as well as the babysitter to communicate with the manager.
	var selfCertPEM, selfKeyPEM []byte
	selfIdentity, ok := cfg.ComponentIdentity[replicaSet]
	if !ok { // should never happen
		return nil, fmt.Errorf("unknown identity for replica set %q", replicaSet)
	}
	selfCert, selfKey, err := generateSignedCert(caCert, caKey, selfIdentity)
	if err != nil {
		return nil, fmt.Errorf("cannot generate self cert: %w", err)
	}
	selfCertPEM, selfKeyPEM, err = pemEncode(selfCert, selfKey)
	if err != nil {
		return nil, fmt.Errorf("cannot PEM-encode cert: %w", err)
	}
	getSelfCert := func() ([]byte, []byte, error) {
		return selfCertPEM, selfKeyPEM, nil
	}

	getReplicaWatcher := func(ctx context.Context, replicaSet string) (babysitter.ReplicaWatcher, error) {
		return &replicaWatcher{
			cfg:        cfg,
			replicaSet: replicaSet,
			s:          s,
		}, nil
	}

	// Connection to the manager.
	m := &manager.HttpClient{
		Addr:      cfg.ManagerAddr,
		TLSConfig: mtls.ClientTLSConfig(projectName, caCert, getSelfCert, "manager"),
	}
	mux := http.NewServeMux()
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:0", hostname))
	if err != nil {
		return nil, err
	}
	selfAddr := fmt.Sprintf("https://%s", lis.Addr().String())
	b, err := babysitter.Start(ctx, logger, cfg, replicaSet, projectName, podName, "localhost:0", mux, selfAddr, m, caCert, getSelfCert, getReplicaWatcher, logSaver, traceSaver, metricExporter)
	if err != nil {
		return nil, err
	}

	// Start the server without blocking.
	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(projectName, caCert, getSelfCert, "manager", "distributor"),
	}
	go server.ServeTLS(lis, "", "")

	return b, nil
}

// replicaWatcher is an implementation of the babysitter.ReplicaWatcher
// interface for the GKE local deployer.
// The implementation is not thread safe.
type replicaWatcher struct {
	cfg        *config.GKEConfig
	replicaSet string
	s          *Starter
	called     bool
}

var _ babysitter.ReplicaWatcher = &replicaWatcher{}

// GetReplicas implements the babysitter.ReplicaWatcher interface.
func (w *replicaWatcher) GetReplicas(ctx context.Context) ([]string, error) {
	if w.called {
		// Local replicas don't change, so we can just block until the context
		// is canceled.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	w.called = true
	return w.s.getReplicas(ctx, w.cfg, w.replicaSet) // blocks
}
