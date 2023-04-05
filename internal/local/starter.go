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
	"errors"
	"fmt"
	"sync"

	"github.com/ServiceWeaver/weaver-gke/internal/babysitter"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/google/uuid"
)

const (
	// The default replication factor for Service Weaver ReplicaSets.
	defaultReplication = 2
)

// Starter starts ReplicaSets on the local machine.
type Starter struct {
	store store.Store

	mu          sync.Mutex
	babysitters map[string][]bsitter // babysitters, by deployment id
}

type bsitter struct {
	cancel context.CancelFunc
	b      *babysitter.Babysitter
}

// NewStarter creates a starter that runs all replicas of a colocation
// group as OS processes on the local machine.
func NewStarter(s store.Store) *Starter {
	return &Starter{
		store:       s,
		babysitters: map[string][]bsitter{},
	}
}

// Start starts the ReplicaSet for a given deployment.
func (s *Starter) Start(ctx context.Context, cfg *config.GKEConfig, replicaSet string) error {
	// Determine if the colocation group has already been started.
	shouldStart, err := s.shouldStart(ctx, cfg, replicaSet)
	if err != nil {
		return err
	}
	if !shouldStart {
		return nil
	}

	// Create and run one babysitter per colocation group replica.
	babysitters := make([]bsitter, defaultReplication)
	for i := 0; i < defaultReplication; i++ {
		ctx, cancel := context.WithCancel(ctx)
		b, err := createBabysitter(ctx, cfg, replicaSet, LogDir)
		if err != nil {
			// TODO(mwhittaker): Should we stop the previously started
			// babysitters?
			cancel()
			return err
		}
		go b.Run()
		babysitters[i] = bsitter{cancel: cancel, b: b}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	id := cfg.Deployment.Id
	s.babysitters[id] = append(s.babysitters[id], babysitters...)
	return nil
}

// Stop kills processes in all of the deployments' colocation groups,
// blocking until all of the processes are killed.
func (s *Starter) Stop(_ context.Context, deployments []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, dep := range deployments {
		for _, b := range s.babysitters[dep] {
			b.cancel() // cancels the context the babysitter started with

		}
		// TODO(spetrovic): Wait for running babysitters to finish.
		delete(s.babysitters, dep)
	}
	return nil
}

// Babysitters returns a snapshot of all currently running babysitters.
func (s *Starter) Babysitters() []*babysitter.Babysitter {
	s.mu.Lock()
	defer s.mu.Unlock()
	var babysitters []*babysitter.Babysitter
	for _, bs := range s.babysitters {
		for _, b := range bs {
			babysitters = append(babysitters, b.b)
		}
	}
	return babysitters
}

// shouldStart returns true iff the caller should start the given ReplicaSet.
func (s *Starter) shouldStart(ctx context.Context, cfg *config.GKEConfig, replicaSet string) (bool, error) {
	// Use the store to coordinate the starting of processes. The process
	// that ends up creating the key "started_by" is the deployer.
	dep := cfg.Deployment
	key := store.ReplicaSetKey(dep.App.Name, uuid.Must(uuid.Parse(dep.Id)), replicaSet, "started_by")
	histKey := store.DeploymentKey(dep.App.Name, uuid.Must(uuid.Parse(dep.Id)), store.HistoryKey)
	err := store.AddToSet(ctx, s.store, histKey, key)
	if err != nil && !errors.Is(err, store.ErrUnchanged) {
		// Track the key in the store under histKey.
		return false, fmt.Errorf("unable to record key %q under %q: %w", key, histKey, err)
	}
	_, err = s.store.Put(ctx, key, "" /*value*/, &store.Missing)
	if errors.Is(err, store.Stale{}) {
		// Some other process created the key first.
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
