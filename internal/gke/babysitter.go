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

package gke

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	traceexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/mtls"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/manager"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/traces"
	"go.opentelemetry.io/otel/sdk/trace"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// RunBabysitter creates and runs a GKE babysitter.
//
// The GKE babysitter starts and manages a weavelet. If the weavelet crashes,
// the babysitter will crash as well, causing the kubernetes manager to re-run
// the container.
//
// The following environment variables must be set before this method is called:
//   - configEnvKey
//   - replicaSetEnvKey
//   - containerMetadataEnvKey
//   - nodeNameEnvKey
//   - podNameEnvKey
//
// This call blocks until the provided context is canceled or an error is
// encountered.
func RunBabysitter(ctx context.Context) error {
	for _, v := range []string{
		configEnvKey,
		replicaSetEnvKey,
		containerMetadataEnvKey,
		nodeNameEnvKey,
		podNameEnvKey,
	} {
		val, ok := os.LookupEnv(v)
		if !ok {
			return fmt.Errorf("environment variable %q not set", v)
		}
		if val == "" {
			return fmt.Errorf("empty value for environment variable %q", v)
		}
	}

	// Parse the config.GKEConfig
	cfg, err := gkeConfigFromEnv()
	if err != nil {
		return err
	}
	replicaSet := os.Getenv(replicaSetEnvKey)

	// Parse the ContainerMetadata.
	meta := &ContainerMetadata{}
	metaStr := os.Getenv(containerMetadataEnvKey)
	if err := proto.FromEnv(metaStr, meta); err != nil {
		return fmt.Errorf("proto.FromEnv(%q): %w", metaStr, err)
	}
	meta.NodeName = os.Getenv(nodeNameEnvKey)
	meta.PodName = os.Getenv(podNameEnvKey)

	// Get the clientset to access Kubernetes APIs.
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("cannot get kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("cannot get kubernetes clientset: %w", err)
	}

	// Unset some environment variables, so that we don't pollute the Service Weaver
	// application's environment.
	for _, v := range []string{containerMetadataEnvKey, nodeNameEnvKey, podNameEnvKey} {
		if err := os.Unsetenv(v); err != nil {
			return err
		}
	}

	// Babysitter logs are written to the same log store as the application logs.
	logClient, err := newCloudLoggingClient(ctx, meta)
	if err != nil {
		return err
	}
	defer logClient.Close()

	// Create the Google Cloud metrics exporter.
	metricsExporter, err := newMetricExporter(ctx, meta)
	if err != nil {
		return err
	}
	defer metricsExporter.Close()

	// Create a trace exporter.
	traceExporter, err := traceexporter.New(traceexporter.WithProjectID(meta.Project))
	if err != nil {
		return err
	}
	defer traceExporter.Shutdown(ctx)

	logSaver := logClient.Log
	logger := slog.New(&logging.LogHandler{
		Opts: logging.Options{
			App:        cfg.Deployment.App.Name,
			Deployment: cfg.Deployment.Id,
			Component:  "Babysitter",
			Weavelet:   meta.PodName,
			Attrs:      []string{"serviceweaver/system", ""},
		},
		Write: logSaver,
	})
	traceSaver := func(spans *protos.TraceSpans) error {
		if len(spans.Span) == 0 {
			return nil
		}
		s := make([]trace.ReadOnlySpan, len(spans.Span))
		for i, span := range spans.Span {
			s[i] = &traces.ReadSpan{Span: span}
		}
		return traceExporter.ExportSpans(ctx, s)
	}
	metricSaver := func(metrics []*metrics.MetricSnapshot) error {
		return metricsExporter.Export(ctx, metrics)
	}

	caCert, getSelfCert, err := getPodCerts()
	if err != nil {
		return err
	}

	getReplicaWatcher := func(ctx context.Context, replicaSet string) (babysitter.ReplicaWatcher, error) {
		return newReplicaWatcher(ctx, clientset, logger, cfg.Deployment, replicaSet)
	}

	m := &manager.HttpClient{
		Addr:      cfg.ManagerAddr,
		TLSConfig: mtls.ClientTLSConfig(meta.Project, caCert, getSelfCert, "manager"),
	}
	mux := http.NewServeMux()
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	internalAddress := fmt.Sprintf("%s:%d", host, weaveletPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:0", host))
	if err != nil {
		return err
	}
	selfAddr := fmt.Sprintf("https://%s", lis.Addr())
	_, err = babysitter.Start(ctx, logger, cfg, replicaSet, meta.Project, meta.PodName, internalAddress, mux, selfAddr, m, caCert, getSelfCert, getReplicaWatcher, logSaver, traceSaver, metricSaver)
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:   mux,
		TLSConfig: mtls.ServerTLSConfig(meta.Project, caCert, getSelfCert, "manager", "distributor"),
	}
	return server.ServeTLS(lis, "", "")
}

// replicaWatcher is an implementation of the babysitter.ReplicaWatcher
// interface for the GKE deployer.
// The implementation is not thread safe.
type replicaWatcher struct {
	replicaSet string
	logger     *slog.Logger
	addrs      map[string]string // pod name -> weavelet IP address
	watcher    watch.Interface
}

var _ babysitter.ReplicaWatcher = &replicaWatcher{}

func newReplicaWatcher(ctx context.Context, clientset *kubernetes.Clientset, logger *slog.Logger, dep *protos.Deployment, replicaSet string) (*replicaWatcher, error) {
	selector := labels.SelectorFromSet(labels.Set{
		appKey:        name{dep.App.Name}.DNSLabel(),
		versionKey:    name{dep.Id}.DNSLabel(),
		replicaSetKey: name{replicaSet}.DNSLabel(),
	})
	opts := metav1.ListOptions{LabelSelector: selector.String()}
	watcher, err := clientset.CoreV1().Pods(namespaceName).Watch(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("cannot create watcher for replica set %s: %w", replicaSet, err)
	}
	return &replicaWatcher{
		replicaSet: replicaSet,
		logger:     logger,
		addrs:      map[string]string{},
		watcher:    watcher,
	}, nil
}

// GetReplicas implements the babysitter.ReplicaWatcher interface.
func (w *replicaWatcher) GetReplicas(ctx context.Context) ([]string, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-w.watcher.ResultChan():
			if !ok { // channel closed
				return nil, fmt.Errorf("watcher closed")
			}

			// Read all events available on the channel to avoid incremental
			// replica changes.
			events := []watch.Event{event}
		inner:
			for {
				select {
				case event, ok := <-w.watcher.ResultChan():
					if !ok { // channel closed
						return nil, fmt.Errorf("watcher closed")
					}
					events = append(events, event)
				default:
					// Channel empty.
					break inner
				}
			}

			// Process available channel events.
			changed := false
			for _, event := range events {
				switch event.Type {
				case watch.Added, watch.Modified:
					pod := event.Object.(*v1.Pod)
					if pod.Status.PodIP != "" && w.addrs[pod.Name] != pod.Status.PodIP {
						w.addrs[pod.Name] = pod.Status.PodIP
						changed = true
					}
				case watch.Deleted:
					pod := event.Object.(*v1.Pod)
					if _, ok := w.addrs[pod.Name]; ok {
						delete(w.addrs, pod.Name)
						changed = true
					}
				case watch.Error:
					w.logger.Error("watch error for replica set: will retry", "err", event, "replica set", w.replicaSet)
					continue
				}
			}
			if !changed {
				continue
			}
			replicas := []string{}
			for _, addr := range w.addrs {
				replicas = append(replicas, fmt.Sprintf("tcp://%s:%d", addr, weaveletPort))
			}
			return replicas, nil
		}
	}
}

// gkeConfigFromEnv reads config.GKEConfig from the Service Weaver internal
// environment variable. It returns an error if the environment variable isn't
// set.
func gkeConfigFromEnv() (*config.GKEConfig, error) {
	str := os.Getenv(configEnvKey)
	if str == "" {
		return nil, fmt.Errorf("%q environment variable not set", configEnvKey)
	}
	cfg := &config.GKEConfig{}
	if err := proto.FromEnv(str, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
