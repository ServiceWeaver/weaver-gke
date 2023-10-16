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
	"log/slog"
	"sync"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/endpoints"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
)

const (
	// The default replication factor for Service Weaver ReplicaSets.
	defaultReplication = 2
)

// Starter starts ReplicaSets on the local machine.
type Starter struct {
	caCert *x509.Certificate
	caKey  crypto.PrivateKey
	logger *slog.Logger

	mu          sync.Mutex
	appVersions map[string]*appVersion
}

type appVersion struct {
	replicaSets map[string]*replicaSet
}

type replicaSet struct {
	pods []pod
}

type pod struct {
	cancel context.CancelFunc
	b      *babysitter.Babysitter
}

// NewStarter creates a starter that runs all replicas of a colocation
// group as OS processes on the local machine.
func NewStarter(logger *slog.Logger) (*Starter, error) {
	caCert, caKey, err := ensureCACert()
	if err != nil {
		return nil, err
	}
	s := &Starter{
		caCert:      caCert,
		caKey:       caKey,
		logger:      logger,
		appVersions: map[string]*appVersion{},
	}
	return s, nil
}

// Start starts the ReplicaSet for a given deployment.
func (s *Starter) Start(ctx context.Context, cfg *config.GKEConfig, replicaSetName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.appVersions[cfg.Deployment.Id]
	if !ok {
		v = &appVersion{
			replicaSets: map[string]*replicaSet{},
		}
		s.appVersions[cfg.Deployment.Id] = v
	}
	rs, ok := v.replicaSets[replicaSetName]
	if !ok {
		rs = &replicaSet{}
		v.replicaSets[replicaSetName] = rs
	}

	if rs.pods != nil {
		// Already started.
		return nil
	}

	// Create replicas.
	for i := 0; i < defaultReplication; i++ {
		ctx, cancel := context.WithCancel(ctx)
		b, err := startBabysitter(ctx, cfg, s, replicaSetName, LogDir, s.caCert, s.caKey)
		if err != nil {
			cancel()
			return err
		}
		rs.pods = append(rs.pods, pod{
			cancel: cancel,
			b:      b,
		})
	}

	return nil
}

// getHealthyPods returns information for all healthy simulated pods in a given
// replica set.
func (s *Starter) getHealthyPods(ctx context.Context, cfg *config.GKEConfig, replicaSet string, newBabysitter func(string) endpoints.Babysitter) ([]*nanny.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.appVersions[cfg.Deployment.Id]
	if !ok { // app version hasn't yet started
		return nil, nil
	}
	rs, ok := v.replicaSets[replicaSet]
	if !ok { // replica set hasn't yet started
		return nil, nil
	}
	var healthyPods []*nanny.Pod
	for _, r := range rs.pods {
		babysitterAddr := r.b.SelfAddr()
		weaveletAddr := r.b.WeaveletInfo().DialAddr
		// NOTE: we could call r.b.GetLoad() directly here, but are instead
		// choosing to exercise the same call path as GKE proper.
		b := newBabysitter(babysitterAddr)
		reply, err := b.GetLoad(ctx, &endpoints.GetLoadRequest{})
		if err != nil {
			s.logger.Debug("cannot get weavelet load: treating as unhealthy", "replica_set", replicaSet, "addr", weaveletAddr)
			continue
		}
		healthyPods = append(healthyPods, &nanny.Pod{
			BabysitterAddr: babysitterAddr,
			WeaveletAddr:   weaveletAddr,
			Load:           reply.Load,
		})
	}
	return healthyPods, nil
}

// Stop kills processes in all of the versions' replica sets, blocking until all
// of the processes are killed.
func (s *Starter) Stop(_ context.Context, versions []*config.GKEConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range versions {
		d := s.appVersions[v.Deployment.Id]
		if d == nil {
			// Already stopped.
			return nil
		}
		for _, rs := range d.replicaSets {
			for _, r := range rs.pods {
				r.cancel() // Stops the babysitter process.
			}
		}
		delete(s.appVersions, v.Deployment.Id)
	}
	return nil
}
