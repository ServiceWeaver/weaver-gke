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
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"syscall"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/bin"
)

const (
	// The gke-local deployer spawns one controller and one distributor/manager
	// per region. The controller and distributors all listen on different
	// ports. The store is used to assign ports to regions.
	proxyPort      = 8000                 // proxy port
	controllerPort = 8001                 // controller port
	portsKey       = "/distributor_ports" // mapping from region to distributor port
	projectName    = "local"              // fake name for the local GCP "project"
)

// PrepareRollout returns a new rollout request for the given application
// version, along with the HTTP client that should be used to reach it.
// May mutate the passed-in run.
func PrepareRollout(ctx context.Context, cfg *config.GKEConfig) (*controller.RolloutRequest, *http.Client, error) {
	cfg.Project = projectName

	// Ensure all Service Weaver service processes (i.e., controller,
	// distributor/manager) are running.
	distributorPorts, err := ensureWeaverServices(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	// Generate per-component identities and use the call graph to compute
	// the set of components each identity is allowed to invoke methods on.
	callGraph, err := bin.ReadComponentGraph(cfg.Deployment.App.Binary)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read the call graph from the application binary: %w", err)
	}
	cfg.ComponentIdentity = map[string]string{}
	cfg.IdentityAllowlist = map[string]*config.GKEConfig_Components{}
	addIdentity := func(component string) string {
		// NOTE: we assign each replica set a unique identity, which is the name
		// of the replica set itself.
		replicaSet := nanny.ReplicaSetForComponent(component, cfg)
		cfg.ComponentIdentity[component] = replicaSet
		return replicaSet
	}
	for _, edge := range callGraph {
		src := edge[0]
		dst := edge[1]
		addIdentity(dst)
		srcIdentity := addIdentity(src)
		allow := cfg.IdentityAllowlist[srcIdentity]
		if allow == nil {
			allow = &config.GKEConfig_Components{}
			cfg.IdentityAllowlist[srcIdentity] = allow
		}
		allow.Component = append(allow.Component, dst)
	}

	// Prepare the rollout request.
	req := &controller.RolloutRequest{
		Config:    cfg,
		NannyAddr: fmt.Sprintf("http://localhost:%d", controllerPort),
	}
	for _, region := range cfg.Regions {
		req.Locations = append(req.Locations, &controller.RolloutRequest_Location{
			Name:            region,
			DistributorAddr: fmt.Sprintf("http://localhost:%d", distributorPorts[region]),
		})
	}
	return req, http.DefaultClient, nil
}

// ensureWeaverServices ensures that Service Weaver services (i.e., controller and all
// needed distributors and managers) are running. It returns a map that assigns
// the region of every distributor to the port it's listening on.
func ensureWeaverServices(ctx context.Context, cfg *config.GKEConfig) (map[string]int, error) {
	// "weaver-gke-local deploy" launches Service Weaver applications. The
	// "weaver-gke-local" tool also has subcommands to launch a proxy, a
	// controller, and a distributor.
	ex, err := os.Executable()
	if err != nil {
		return nil, err
	}

	// Launch the proxy.
	proxy := exec.Command(ex, "proxy", "--port", fmt.Sprint(proxyPort))
	proxy.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := proxy.Start(); err != nil {
		return nil, err
	}

	// Launch the controller.
	controller := exec.Command(ex, "controller",
		"--region", "us-central1",
		"--port", fmt.Sprint(controllerPort))
	controller.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := controller.Start(); err != nil {
		return nil, err
	}

	// Launch the distributors.
	s, err := Store("us-central1")
	if err != nil {
		return nil, err
	}
	ports := map[string]int{}
	for _, region := range cfg.Regions {
		// Pick a port.
		offset, err := store.Sequence(ctx, s, portsKey, region)
		if err != nil {
			return nil, err
		}
		port := controllerPort + 1 + offset
		ports[region] = port

		// Start the distributor.
		distributor := exec.Command(ex, "distributor",
			"--region", region,
			"--port", fmt.Sprint(port))
		distributor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := distributor.Start(); err != nil {
			return nil, err
		}
	}
	return ports, nil
}
