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

package tool

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/bin"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
	"github.com/ServiceWeaver/weaver/runtime/colors"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/ServiceWeaver/weaver/runtime/tool"
	"github.com/ServiceWeaver/weaver/runtime/version"
	"github.com/google/uuid"
)

const (
	// The time interval we keep trying to deploy the application for.
	// NOTE(spetrovic): This interval is high right now because of GKE:
	// even after all nanny jobs have started, it takes a while for nanny jobs
	// to become visible to the super-nanny (due to the slow GKE Multi-Cluster-
	// Services registration process).
	// TODO(rgrandl): Once the GKE super-nanny saves the rollout request and
	// starts connecting to the nanny jobs in the background, this time
	// interval can be reduced.
	deployTimeout = 8 * time.Minute

	// Name of the base image to build the application image, if the user doesn't
	// provide a base image.
	defaultBaseImage = "ubuntu:rolling"
)

type DeploySpec struct {
	Tool   string        // e.g., weaver-gke, weaver-gke-local
	Flags  *flag.FlagSet // command line flags
	detach *bool         // detach flag value

	// Controller returns the HTTP address of the controller and an HTTP client
	// that we can use to contact the controller.
	Controller func(context.Context, *config.GKEConfig) (string, *http.Client, error)

	PrepareRollout func(context.Context, *config.GKEConfig) (*controller.RolloutRequest, error)
	Source         func(context.Context, *config.GKEConfig) (logging.Source, error)
}

// DeployCmd returns the "deploy" command.
func DeployCmd(spec *DeploySpec) *tool.Command {
	spec.detach = spec.Flags.Bool("detach", false, "Don't follow logs after deploying")
	return &tool.Command{
		Name:        "deploy",
		Flags:       spec.Flags,
		Description: "Deploy a Service Weaver app",
		Help: fmt.Sprintf(`Usage:
  %s deploy <configfile>

Flags:
  -h, --help	Print this help message.
%s`, spec.Tool, tool.FlagsHelp(spec.Flags)),
		Fn: spec.deployFn,
	}
}

func (d *DeploySpec) deployFn(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("wrong number of arguments; expecting just config file")
	}

	// Parse the application config.
	cfgFile := args[0]
	configText, err := os.ReadFile(cfgFile)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}
	appConfig, err := runtime.ParseConfig(cfgFile, string(configText), codegen.ComponentConfigValidator)
	if err != nil {
		return fmt.Errorf("error loading config file %q: %w", cfgFile, err)
	}
	cfg, err := makeGKEConfig(appConfig)
	if err != nil {
		return fmt.Errorf("config file %q: %w", cfgFile, err)
	}
	return d.doDeploy(ctx, cfg)
}

func makeGKEConfig(app *protos.AppConfig) (*config.GKEConfig, error) {
	// GKE config as found in TOML config file.
	const gkeKey = "github.com/ServiceWeaver/weaver-gke/internal/gke"
	const shortGKEKey = "gke"

	// Use an intermediate struct, so that we can add TOML tags.
	type lisOpts struct {
		IsPublic bool   `toml:"is_public"`
		Hostname string `toml:"hostname"`
	}
	type gkeConfigSchema struct {
		Regions     []string
		Image       string
		MinReplicas int32
		Listeners   map[string]lisOpts
		MTLS        bool
	}
	parsed := &gkeConfigSchema{}
	if err := runtime.ParseConfigSection(gkeKey, shortGKEKey, app.Sections, parsed); err != nil {
		return nil, err
	}

	// Validate the config.
	binListeners, err := bin.ReadListeners(app.Binary)
	if err != nil {
		return nil, fmt.Errorf("cannot read listeners from binary %s: %w", app.Binary, err)
	}
	allListeners := make(map[string]struct{})
	for _, c := range binListeners {
		for _, l := range c.Listeners {
			allListeners[l] = struct{}{}
		}
	}
	var listeners map[string]*config.GKEConfig_ListenerOptions
	if parsed.Listeners != nil {
		listeners = map[string]*config.GKEConfig_ListenerOptions{}
		for lis, opts := range parsed.Listeners {
			if _, ok := allListeners[lis]; !ok {
				return nil, fmt.Errorf("listener %s specified in the config not found in the binary", lis)
			}
			// If the listener is public, a hostname is required.
			if opts.IsPublic && opts.Hostname == "" {
				return nil, fmt.Errorf("listener %s is public, but has no hostname specified", lis)
			}
			listeners[lis] = &config.GKEConfig_ListenerOptions{IsPublic: opts.IsPublic, Hostname: opts.Hostname}
		}
	}
	if err := validateDeployRegions(parsed.Regions); err != nil {
		return nil, err
	}
	if parsed.MinReplicas == 0 {
		// Ensure that each component runs in at least 1 pod.
		parsed.MinReplicas = 1
	}

	// Make sure we set the base image if the user doesn't provide a base image.
	if parsed.Image == "" {
		parsed.Image = defaultBaseImage
	}

	depID := uuid.New()
	cfg := &config.GKEConfig{
		Image:       parsed.Image,
		Regions:     parsed.Regions,
		MinReplicas: parsed.MinReplicas,
		Listeners:   listeners,
		Deployment: &protos.Deployment{
			App: app,
			Id:  depID.String(),
		},
		Mtls: parsed.MTLS,
	}

	return cfg, nil
}

// doDeploy deploys the specified app.
//
// As part of the deployment, we will first start the nanny if it's not already
// running. Note that the nanny and app deployments are started in the same
// goroutine, to ensure that nanny is available when we deploy a new app.
// This is because in the future, the app will rely on the nanny to start the
// remote processes.
func (d *DeploySpec) doDeploy(ctx context.Context, cfg *config.GKEConfig) error {
	deployment := cfg.Deployment
	app := deployment.App

	info, err := os.Stat(app.Binary)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("want binary, found directory at path %q", app.Binary)
	}
	// Check version compatibility.
	appBinaryVersions, err := bin.ReadVersions(app.Binary)
	if err != nil {
		return fmt.Errorf("read versions: %w", err)
	}
	tool, err := os.Executable()
	if err != nil {
		return err
	}
	weaverGKEVersions, err := bin.ReadVersions(tool)
	if err != nil {
		return fmt.Errorf("read versions: %w", err)
	}
	selfVersion, _, err := gke.ToolVersion()
	if err != nil {
		return fmt.Errorf("read weaver-gke version: %w", err)
	}
	if appBinaryVersions.DeployerVersion != version.DeployerVersion {
		// Try to relativize the binary, defaulting to the absolute path if
		// there are any errors.
		binary := app.Binary
		if cwd, err := os.Getwd(); err == nil {
			if rel, err := filepath.Rel(cwd, app.Binary); err == nil {
				binary = rel
			}
		}
		return fmt.Errorf(`
ERROR: The binary you're trying to deploy (%q) was built with
github.com/ServiceWeaver/weaver module version %s (internal version %s).
However, the 'weaver-gke' binary you're using (%s) was built with weaver
module version %s (internal version %s). These versions are incompatible.

We recommend updating both the weaver module your application is built with and
updating the 'weaver-gke' command by running the following.

    go get github.com/ServiceWeaver/weaver@latest
    go install github.com/ServiceWeaver/weaver-gke/cmd/weaver-gke@latest

Then, re-build your code and re-run 'weaver-gke deploy'. If the problem
persists, please file an issue at https://github.com/ServiceWeaver/weaver/issues`,
			binary, appBinaryVersions.ModuleVersion, appBinaryVersions.DeployerVersion, selfVersion, weaverGKEVersions.ModuleVersion, version.DeployerVersion)
	}

	// TODO(mwhittaker): Check that the controller is running the same version
	// as the tool? Have the controller check the binary version as well?

	if err := d.startRollout(ctx, cfg); err != nil {
		return err
	}
	fmt.Printf("Version %q of app %q started successfully.\n", deployment.Id, deployment.App.Name)
	if !*d.detach {
		fmt.Println("Note that stopping this binary will not affect the app in any way.")
	}

	query := fmt.Sprintf(`version == %q`, logging.Shorten(deployment.Id))
	if *d.detach {
		fmt.Printf(`To watch the version's logs, run the following command:

    %s logs --follow '%s'
`, d.Tool, query)
		return nil
	}

	fmt.Println("Tailing the logs...")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func(query string) {
		<-signals
		fmt.Printf(`To continue watching the logs, run the following command:

    %s logs --follow '%s'
`, d.Tool, query)
		os.Exit(1)
	}(query)

	// We just deployed this application, so we can filter out any entries
	// older than 5 minutes. Also hide system logs.
	cutoff := time.Now().Add(-5 * time.Minute)
	query += fmt.Sprintf(" && time >= timestamp(%q)", cutoff.Format(time.RFC3339))
	query += ` && !("serviceweaver/system" in attrs)`

	source, err := d.Source(ctx, cfg)
	if err != nil {
		return err
	}
	r, err := source.Query(ctx, query, true)
	if err != nil {
		return err
	}
	pp := logging.NewPrettyPrinter(colors.Enabled())
	for {
		entry, err := r.Read(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		fmt.Println(pp.Format(entry))
	}
}

// startRollout starts the rollout of the given application version.
func (d *DeploySpec) startRollout(ctx context.Context, cfg *config.GKEConfig) error {
	req, err := d.PrepareRollout(ctx, cfg)
	if err != nil {
		return err
	}

	controllerAddr, controllerClient, err := d.Controller(ctx, cfg)
	if err != nil {
		return err
	}

	// Send the rollout request to the nanny.
	fmt.Fprintf(os.Stderr, "Deploying the application... ")
	ctx, cancel := context.WithTimeout(ctx, deployTimeout)
	defer cancel()
	for r := retry.Begin(); r.Continue(ctx); {
		err = protomsg.Call(ctx, protomsg.CallArgs{
			Client:  controllerClient,
			Addr:    controllerAddr,
			URLPath: controller.RolloutURL,
			Request: req,
		})
		if err == nil {
			fmt.Fprintln(os.Stderr, "Done")
			return nil
		}
	}
	fmt.Fprintln(os.Stderr, "Timeout")
	return fmt.Errorf("timeout trying to deploy the app; last error: %w", err)
}

// validateDeployRegions ensures that the application config has a valid set of
// unique regions to deploy the application. If the app config doesn't specify
// any regions where to deploy the app, we pick the regions.
//
// Note that at least one region should be specified.
func validateDeployRegions(regions []string) error {
	// Ensure that at least one deployment region is specified.
	if len(regions) == 0 {
		return fmt.Errorf("no regions where the app should be deployed were specified")
	}

	// Ensure that the set of regions is unique.
	unique := make(map[string]bool, len(regions))
	for _, elem := range regions {
		if unique[elem] {
			return fmt.Errorf("the set of regions should be unique; found %s "+
				"multiple times", elem)
		}
		unique[elem] = true
	}
	return nil
}
