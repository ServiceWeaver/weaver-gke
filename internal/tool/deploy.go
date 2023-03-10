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
	"syscall"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
	"github.com/ServiceWeaver/weaver/runtime/colors"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/ServiceWeaver/weaver/runtime/tool"
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
)

var (
	deployFlags = flag.NewFlagSet("deploy", flag.ContinueOnError)
	detach      = deployFlags.Bool("detach", false, "Don't follow logs after deploying")
)

type DeploySpec struct {
	Tool           string // e.g., weaver-gke, weaver-gke-local
	PrepareRollout func(context.Context, *config.GKEConfig) (*controller.RolloutRequest, *http.Client, error)
	Source         func(context.Context, *config.GKEConfig) (logging.Source, error)
}

// DeployCmd returns the "deploy" command.
func DeployCmd(spec *DeploySpec) *tool.Command {
	return &tool.Command{
		Name:        "deploy",
		Flags:       deployFlags,
		Description: "Deploy a Service Weaver app",
		Help: fmt.Sprintf(`Usage:
  %s deploy <configfile>

Flags:
  -h, --help	Print this help message.
%s`, spec.Tool, tool.FlagsHelp(deployFlags)),
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

	type gkeConfigSchema struct {
		Project        string
		Account        string
		Regions        []string
		PublicListener []struct{ Name, Hostname string } `toml:"public_listener"`
	}
	parsed := &gkeConfigSchema{}
	if err := runtime.ParseConfigSection(gkeKey, shortGKEKey, app, parsed); err != nil {
		return nil, err
	}

	depID := uuid.New()
	cfg := &config.GKEConfig{
		Project:        parsed.Project,
		Account:        parsed.Account,
		Regions:        parsed.Regions,
		PublicListener: make([]*config.GKEConfig_PublicListener, len(parsed.PublicListener)),
		Deployment: &protos.Deployment{
			App: app,
			Id:  depID.String(),
		},
	}
	for i, lis := range parsed.PublicListener {
		if lis.Hostname == "" {
			return nil, fmt.Errorf("empty hostname for public listener %q", lis.Name)
		}
		cfg.PublicListener[i] = &config.GKEConfig_PublicListener{
			Name:     lis.Name,
			Hostname: lis.Hostname,
		}
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
	if err := runtime.CheckDeployment(deployment); err != nil {
		return err
	}

	info, err := os.Stat(app.Binary)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("want binary, found directory at path %q", app.Binary)
	}

	if err := d.startRollout(ctx, cfg); err != nil {
		return err
	}
	fmt.Printf("Version %q of app %q started successfully.\n", deployment.Id, deployment.App.Name)
	if !*detach {
		fmt.Println("Note that stopping this binary will not affect the app in any way.")
	}

	query := fmt.Sprintf(`version == %q`, logging.Shorten(deployment.Id))
	if *detach {
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
	if err := pickDeployRegions(cfg); err != nil {
		return err
	}

	req, client, err := d.PrepareRollout(ctx, cfg)
	if err != nil {
		return err
	}

	// Send the rollout request to the nanny.
	fmt.Fprintf(os.Stderr, "Deploying the application... ")
	ctx, cancel := context.WithTimeout(ctx, deployTimeout)
	defer cancel()
	for r := retry.Begin(); r.Continue(ctx); {
		if err = protomsg.Call(ctx, protomsg.CallArgs{
			Client:  client,
			Addr:    req.NannyAddr,
			URLPath: controller.RolloutURL,
			Request: req,
		}); err == nil {
			fmt.Fprintln(os.Stderr, "Done")
			return nil
		}
	}
	fmt.Fprintln(os.Stderr, "Error")
	return fmt.Errorf("timeout trying to deploy the app; last error: %w", err)
}

// pickDeployRegions ensures that the application config has a valid set of
// unique regions to deploy the application. If the app config doesn't specify
// any regions where to deploy the app, we pick the regions.
//
// TODO(rgrandl): We pick "us-west1" as the default region. However, we should
// determine the set of regions to deploy the app based on various constraints
// (e.g., traffic patterns, geographical location, etc.).
func pickDeployRegions(cfg *config.GKEConfig) error {
	if len(cfg.Regions) == 0 {
		cfg.Regions = []string{"us-west1"}
		return nil
	}

	// Ensure that the set of regions is unique.
	unique := make(map[string]bool, len(cfg.Regions))
	for _, elem := range cfg.Regions {
		if unique[elem] {
			return fmt.Errorf("the set of regions should be unique; found %s "+
				"multiple times", elem)
		}
		unique[elem] = true
	}
	return nil
}
