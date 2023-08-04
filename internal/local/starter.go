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
	"sync"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/local/cond"
)

const (
	// The default replication factor for Service Weaver ReplicaSets.
	defaultReplication = 2
)

// Starter starts ReplicaSets on the local machine.
type Starter struct {
	caCert *x509.Certificate
	caKey  crypto.PrivateKey

	mu          sync.Mutex
	changed     *cond.Cond // broadcast on every change in deployments
	deployments map[string]*deployment
}

type deployment struct {
	replicaSets map[string]*replicaSet
}

type replicaSet struct {
	replicas []replica
}

type replica struct {
	cancel context.CancelFunc
	b      *babysitter.Babysitter
}

// NewStarter creates a starter that runs all replicas of a colocation
// group as OS processes on the local machine.
func NewStarter() (*Starter, error) {
	caCert, caKey, err := ensureCACert()
	if err != nil {
		return nil, err
	}
	s := &Starter{
		deployments: map[string]*deployment{},
		caCert:      caCert,
		caKey:       caKey,
	}
	s.changed = cond.NewCond(&s.mu)
	return s, nil
}

// Start starts the ReplicaSet for a given deployment.
func (s *Starter) Start(ctx context.Context, cfg *config.GKEConfig, replicaSetName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deployments[cfg.Deployment.Id]
	if !ok {
		d = &deployment{
			replicaSets: map[string]*replicaSet{},
		}
		s.deployments[cfg.Deployment.Id] = d
	}
	rs, ok := d.replicaSets[replicaSetName]
	if !ok {
		rs = &replicaSet{}
		d.replicaSets[replicaSetName] = rs
	}

	if rs.replicas != nil {
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
		rs.replicas = append(rs.replicas, replica{
			cancel: cancel,
			b:      b,
		})
	}

	s.changed.Broadcast()
	return nil
}

// getReplicas returns addresses for all the replicas in a given replica set.
// If the replica set hasn't yet been started, this method will block.
func (s *Starter) getReplicas(ctx context.Context, cfg *config.GKEConfig, replicaSetName string) ([]string, error) {
	// NOTE(spetrovic): The algorithm below may lead to a lot of unnecessary
	// wakeups, but that's okay for GKE local.
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		d, ok := s.deployments[cfg.Deployment.Id]
		if !ok {
			if err := s.changed.Wait(ctx); err != nil { // context canceled
				return nil, err
			}
			continue
		}
		rs, ok := d.replicaSets[replicaSetName]
		if !ok {
			if err := s.changed.Wait(ctx); err != nil { // context canceled
				return nil, err
			}
			continue
		}
		addrs := make([]string, len(rs.replicas))
		for i, r := range rs.replicas {
			addrs[i] = r.b.WeaveletInfo().DialAddr
		}
		return addrs, nil
	}
}

// Stop kills processes in all of the versions' replica sets, blocking until all
// of the processes are killed.
func (s *Starter) Stop(_ context.Context, versions []*config.GKEConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range versions {
		d := s.deployments[v.Deployment.Id]
		if d == nil {
			// Already stopped.
			return nil
		}
		for _, rs := range d.replicaSets {
			for _, r := range rs.replicas {
				r.cancel() // Stops the babysitter process.
			}
		}
		delete(s.deployments, v.Deployment.Id)
	}
	return nil
}
