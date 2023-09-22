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
	"os"
	"os/exec"
	"syscall"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/bin"
	"github.com/ServiceWeaver/weaver/runtime/graph"
)

const (
	// The gke-local deployer spawns one controller and one distributor/manager
	// per region. The controller and distributors all listen on different
	// ports. The store is used to assign ports to regions.
	proxyPort      = 8000           // proxy port
	controllerPort = 8001           // controller port
	portsKey       = "/nanny_ports" // region -> distributor/manager port
	projectName    = "local"        // fake name for the local GCP "project"
)

// PrepareRollout returns a new rollout request for the given application
// version. This call may mutate the passed-in config.
func PrepareRollout(ctx context.Context, cfg *config.GKEConfig) (*controller.RolloutRequest, error) {
	// Ensure the CA certificate/key files have been created on the local
	// machine. This ensures that there is no race when creating these files
	// between the tool and weaver services below (i.e., controller,
	// distributor, and manager).
	ensureCACert()

	// Ensure all Service Weaver service processes (i.e., controller,
	// distributor/manager) are running.
	distributorPorts, err := ensureWeaverServices(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Generate per-component identities and use the call graph to compute
	// the set of components each identity is allowed to invoke methods on.
	components, g, err := bin.ReadComponentGraph(cfg.Deployment.App.Binary)
	if err != nil {
		return nil, fmt.Errorf("cannot read the call graph from the application binary: %w", err)
	}
	cfg.ComponentIdentity = map[string]string{}
	cfg.IdentityAllowlist = map[string]*config.GKEConfig_Components{}
	g.PerNode(func(n graph.Node) {
		// Assign an identity to the component, which is a combination of
		// the replica set name and the deployment id.
		component := components[n]
		replicaSet := nanny.ReplicaSetForComponent(component, cfg)
		cfg.ComponentIdentity[component] = fmt.Sprintf("%s-%s", replicaSet, cfg.Deployment.Id)

		// Allow the identity to invoke methods on the target components.
		allow := cfg.IdentityAllowlist[replicaSet]
		if allow == nil {
			allow = &config.GKEConfig_Components{}
			cfg.IdentityAllowlist[replicaSet] = allow
		}
		g.PerOutEdge(n, func(e graph.Edge) {
			allow.Component = append(allow.Component, components[e.Dst])
		})
	})

	// Prepare the rollout request.
	req := &controller.RolloutRequest{
		Config: cfg,
	}
	for _, region := range cfg.Regions {
		req.Locations = append(req.Locations, &controller.RolloutRequest_Location{
			Name:            region,
			DistributorAddr: fmt.Sprintf("https://localhost:%d", distributorPorts[region]),
		})
	}

	return req, nil
}

// ensureWeaverServices ensures that Service Weaver services (i.e., controller,
// proxy, and all needed distributors and managers) are running. It returns a
// map that assigns the region of every distributor to the port it's listening
// on.
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

	// Launch the distributors and managers.
	s, err := Store("us-central1")
	if err != nil {
		return nil, err
	}
	distributorPorts := map[string]int{}
	for _, region := range cfg.Regions {
		offset, err := store.Sequence(ctx, s, portsKey, region)
		if err != nil {
			return nil, err
		}
		distributorPort := controllerPort + 1 + 2*offset
		managerPort := controllerPort + 1 + 2*offset + 1
		distributorPorts[region] = distributorPort

		// Start the distributor.
		distributor := exec.Command(ex, "distributor",
			"--region", region,
			"--port", fmt.Sprint(distributorPort),
			"--manager_port", fmt.Sprint(managerPort))
		distributor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := distributor.Start(); err != nil {
			return nil, err
		}

		// Start the manager.
		manager := exec.Command(ex, "manager",
			"--region", region,
			"--port", fmt.Sprint(managerPort),
			"--proxy_port", fmt.Sprint(proxyPort))
		manager.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := manager.Start(); err != nil {
			return nil, err
		}
	}
	return distributorPorts, nil
}
