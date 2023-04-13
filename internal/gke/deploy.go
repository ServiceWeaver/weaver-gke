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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	sync "sync"
	"text/template"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	privateca "cloud.google.com/go/security/privateca/apiv1"
	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/iam/v1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	artifactregistrypb "google.golang.org/genproto/googleapis/devtools/artifactregistry/v1beta2"
	"google.golang.org/protobuf/types/known/wrapperspb"
	appsv1 "k8s.io/api/apps/v1"
	autoscaling "k8s.io/api/autoscaling/v1"
	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	vautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	backendconfigv1 "k8s.io/ingress-gce/pkg/apis/backendconfig/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

// All of the functions in this file are invoked only from the user machine,
// i.e., by the command-line tool.

const (
	// Name of the Service Weaver repository in artifact registries.
	dockerRepoName = "serviceweaver-repo"

	// Central location in which images are built and stored.
	buildLocation = "us"

	// Name of the Service Weaver subnetwork used by regional gateways.
	subnetName = "serviceweaver"

	// Name of the SSL custom role used by varous cloud service accounts.
	sslRole = "serviceweaver_ssl"

	// Managed DNS zone for the Service Weaver's internal domain name.
	managedDNSZoneName = "serviceweaver-internal"

	// Names of the GCP IAM service accounts used by various Service Weaver actors.
	controllerIAMServiceAccount  = "serviceweaver-controller"
	distributorIAMServiceAccount = "serviceweaver-distributor"
	managerIAMServiceAccount     = "serviceweaver-manager"
	applicationIAMServiceAccount = "serviceweaver-application"

	// Names of the GKE service accounts used by various Service Weaver actors.
	controllerKubeServiceAccount  = "controller"
	distributorKubeServiceAccount = "distributor"
	managerKubeServiceAccount     = "manager"
	applicationKubeServiceAccount = "application"

	// Various settings for the Service Weaver root Certificate Authority.
	rootCAName         = "serviceweaver-root"
	rootCAPoolName     = "serviceweaver-root"
	rootCAOrganization = "serviceweaver"
	rootCALocation     = ConfigClusterRegion

	// Various settings for the Service Weaver subordinate Certificate Authority.
	subCAName         = "serviceweaver-sub"
	subCAPoolName     = "serviceweaver-sub"
	subCAOrganization = "serviceweaver"

	// Serving port for the nanny.
	nannyServingPort = 80

	// Name for the "ServiceExports" CustomResourceDefinition on GKE.
	serviceExportsResourceName = "serviceexports.net.gke.io"
)

// iamBindings stores IAM bindings for a given resource as a map from
// member to a set of roles for that member.
type iamBindings map[string]map[string]struct{}

// PrepareRollout returns a new rollout request for the given application
// version, along with the HTTP client that should be used to reach it.
// May mutate the passed-in deployment.
// REQUIRES: Called by the weaver-gke command.
func PrepareRollout(ctx context.Context, config CloudConfig, cfg *config.GKEConfig) (*controller.RolloutRequest, *http.Client, error) {
	dep := cfg.Deployment
	localBinary := dep.App.Binary // Save before finalizeConfig rewrites it

	// Finalize the config.
	if err := finalizeConfig(config, cfg); err != nil {
		return nil, nil, err
	}

	// Enable the required cloud services.
	if err := enableCloudServices(config,
		"artifactregistry.googleapis.com",
		"cloudbuild.googleapis.com",
		"cloudresourcemanager.googleapis.com",
		"container.googleapis.com",
		"dns.googleapis.com",
		"gkehub.googleapis.com",
		"gkeconnect.googleapis.com",
		"iam.googleapis.com",
		"multiclusterservicediscovery.googleapis.com",
		"multiclusteringress.googleapis.com",
		"privateca.googleapis.com",
		"trafficdirector.googleapis.com",
	); err != nil {
		return nil, nil, err
	}
	if err := enableMultiClusterServices(config); err != nil {
		return nil, nil, err
	}

	// Wait until the robot accounts corresponding to the enabled cloud services
	// have been created. This creation can sometimes lags behind and cause
	// failures for further setup steps.
	sub := func(str string) string {
		return fmt.Sprintf(str, config.ProjectNumber)
	}
	if err := waitServiceAccountsCreated(ctx, config,
		sub("serviceAccount:%s-compute@developer.gserviceaccount.com"),
		sub("serviceAccount:service-%s@gcp-sa-artifactregistry.iam.gserviceaccount.com"),
		sub("serviceAccount:%s@cloudbuild.gserviceaccount.com"),
		sub("serviceAccount:service-%s@gcp-sa-cloudbuild.iam.gserviceaccount.com"),
		sub("serviceAccount:service-%s@compute-system.iam.gserviceaccount.com"),
		sub("serviceAccount:service-%s@container-engine-robot.iam.gserviceaccount.com"),
		sub("serviceAccount:%s@cloudservices.gserviceaccount.com"),
		sub("serviceAccount:service-%s@gcp-sa-gkehub.iam.gserviceaccount.com"),
		sub("serviceAccount:service-%s@gcp-sa-mcsd.iam.gserviceaccount.com"),
	); err != nil {
		return nil, nil, err
	}

	// Setup the Service Weaver artifacts repository that will host container images.
	if err := patchRepository(ctx, config, patchOptions{}, &artifactregistrypb.Repository{
		Name: fmt.Sprintf("projects/%s/locations/%s/repositories/%s",
			config.Project, buildLocation, dockerRepoName),
		Format: artifactregistrypb.Repository_DOCKER,
	}); err != nil {
		return nil, nil, err
	}

	// In parallel, build the container image and setup the cloud project.
	childCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	var firstErr error
	stop := func(err error) {
		once.Do(func() {
			firstErr = err
			cancel()
			fmt.Fprintln(os.Stderr, "Stopping with error:", firstErr)
		})
	}

	// NOTE(spetrovic): We build a single container that contains both the
	// user's application binary, as well as the copy of the weaver-gke tool.
	// However, we apply two tags to this container: one tag for the
	// application Pods to use, and another for the Service Weaver nanny Pods
	// (i.e., controller, distributor, and manager) to use. This allows us
	// to push the latest weaver-gke binary along with the application binary
	// without issuing multiple builds.
	appImageURL := fmt.Sprintf("%s-docker.pkg.dev/%s/%s/app:tag%s",
		buildLocation, config.Project, dockerRepoName, dep.Id[:8])
	toolImageURL := fmt.Sprintf("%s-docker.pkg.dev/%s/%s/weaver-gke:latest",
		buildLocation, config.Project, dockerRepoName)

	fmt.Fprintln(os.Stderr, "Starting the application container build", appImageURL)

	// Start the build in a separate goroutine.
	buildDone := make(chan struct{})
	files := []string{localBinary}
	var goInstall []string
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		// Use the running weaver-gke tool binary.
		toolBinPath, err := os.Executable()
		if err != nil {
			return nil, nil, err
		}
		files = append(files, toolBinPath)
	} else {
		// Cross-compile the weaver-gke tool binary inside the container.
		goInstall = append(goInstall, "github.com/ServiceWeaver/weaver-gke/cmd/weaver-gke@latest")
	}
	go func() {
		defer close(buildDone)
		if err := buildImage(childCtx, buildSpec{
			Tags:      []string{toolImageURL, appImageURL},
			Files:     files,
			GoInstall: goInstall,
			Config:    config,
		}); err != nil {
			stop(err)
		}
	}()
	configCluster, externalGatewayIP, err := prepareProject(childCtx, config, cfg)
	if err != nil {
		stop(err)
	}
	<-buildDone
	if firstErr != nil {
		return nil, nil, firstErr
	}
	fmt.Fprintln(os.Stderr, "Successfully built container image", appImageURL)

	// Start ServiceWeaver services.
	if err := ensureWeaverServices(ctx, config, cfg); err != nil {
		return nil, nil, err
	}

	// Finalize the deployment.
	cfg.Container = appImageURL

	// Print helpful information about the listeners.
	const help = `Project setup complete.
-----
NOTE: The applications' public listeners will be accessible via a
Service Weaver managed L7 load-balancer running at the public IP address:
  http://{{.ExternalGatewayIP}}

This load-balancer uses hostname-based routing to route request to the
appropriate listeners. As a result, all HTTP(s) requests reaching this
load-balancer must have the correct "Host" header field populated. This can be
achieved in one of two ways:
  1. Setting the request header manually, e.g.
     - curl --header "Host: <my_hostname>" http://{{.ExternalGatewayIP}}
  2. Associating the load-balancer address "http://{{.ExternalGatewayIP}}" with
     the listener hostname in your DNS configuration, e.g.
     - Add an "A" record that maps ".foo.domain.com" to
       "http://{{.ExternalGatewayIP}}" in your DNS configuration for "domain.com".

The applications' private listeners will be accessible from inside the
project's VPC using the schema:
  - http://<listener_name>.<region>.serviceweaver.internal

, where <listener_name> is the name the listener was created with in the
application (i.e., via a call to Listener()). For these names to
be resolveable from non-Service Weaver GKE clusters, clusters must be configured
to use CloudDNS for name resolution [1].

[1]: https://cloud.google.com/kubernetes-engine/docs/how-to/cloud-dns
-----`
	helpTmpl := template.Must(template.New("help").Parse(help))
	var b bytes.Buffer
	if err := helpTmpl.Execute(&b, struct{ ExternalGatewayIP string }{
		externalGatewayIP,
	}); err != nil {
		return nil, nil, err
	}
	fmt.Fprintln(os.Stderr, b.String())

	// Build the rollout request.
	req := buildRolloutRequest(configCluster, cfg)
	return req, configCluster.apiRESTClient.Client, nil
}

// enableCloudServices() enables the required cloud services for the given
// cloud config.
func enableCloudServices(config CloudConfig, services ...string) error {
	// Get the current set of enabled services.
	out, err := runGcloud(config, "", cmdOptions{},
		"services", "list", "--format", "value(config.name)")
	if err != nil {
		return err
	}
	enabled := map[string]struct{}{}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		enabled[sc.Text()] = struct{}{}
	}
	if sc.Err() != nil {
		return sc.Err()
	}

	// See what services need to be enabled, if any.
	var toEnable []string
	for _, svc := range services {
		if _, ok := enabled[svc]; ok { // already enabled
			continue
		}
		toEnable = append(toEnable, svc)
	}
	if toEnable == nil { // all services already enabled
		return nil
	}
	args := append([]string{"services", "enable"}, toEnable...)
	_, err = runGcloud(config, "Enabling required cloud services",
		cmdOptions{}, args...)
	return err
}

// enableMultiClusterServices ensures that multi cluster services are
// enabled in the given project.
func enableMultiClusterServices(config CloudConfig) error {
	if _, err := runGcloud(config, "", cmdOptions{},
		"container", "fleet", "multi-cluster-services", "describe",
	); err == nil {
		// MCS services already enabled.
		return nil
	}

	// Enable MCS.
	_, err := runGcloud(
		config, "Enabling multi-cluster services", cmdOptions{},
		"container", "fleet", "multi-cluster-services", "enable",
	)
	return err
}

func finalizeConfig(config CloudConfig, cfg *config.GKEConfig) error {
	// Override the project and account values in the app config.
	cfg.Project = config.Project
	cfg.Account = config.Account

	if cfg.Deployment.App.RolloutNanos == 0 {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print(
			`No rollout duration specified in the config: the app version will be
rolled out in all locations right away. Are you sure you want to proceed? [Y/n] `)
		scanner.Scan()
		text := scanner.Text()
		if text == "" || text == "y" || text == "Y" {
			// Stick with immediate rollout
		} else {
			return fmt.Errorf("user bailed out")
		}
	}

	// Update the application binary path to point to a path inside the
	// container.
	cfg.Deployment.App.Binary = fmt.Sprintf("/weaver/%s", filepath.Base(cfg.Deployment.App.Binary))
	return nil
}

// prepareProject prepares the user project for deploying the given application,
// returning information about the Service Weaver configuration cluster and the
// IP address of the global external gateway.
func prepareProject(ctx context.Context, config CloudConfig, cfg *config.GKEConfig) (*ClusterInfo, string, error) {
	// Get the current IAM bindings for the project.
	bindings, err := getProjectIAMBindings(ctx, config)
	if err != nil {
		return nil, "", err
	}

	// Create a custom GCP role for minting SSL certificates.
	if err := createSSLRole(ctx, config); err != nil {
		return nil, "", err
	}

	// Ensure that the default compute service account has all the necessary
	// permissions.
	if err := ensureComputePermissions(ctx, config, bindings); err != nil {
		return nil, "", err
	}

	// Setup Service Weaver IAM service accounts.
	if err := ensureIAMServiceAccounts(ctx, config, bindings); err != nil {
		return nil, "", err
	}

	// Setup the root Certificate Authority.
	if err := ensureRootCA(ctx, config); err != nil {
		return nil, "", err
	}

	// Ensure the Service Weaver configuration cluster is setup.
	configCluster, globalGatewayIP, err := ensureConfigCluster(ctx, config, ConfigClusterName, ConfigClusterRegion, bindings)
	if err != nil {
		return nil, "", err
	}

	// Ensure that a cluster is started in each deployment region and that
	// a distributor and a manager are running in each cluster.
	for _, region := range cfg.Regions {
		cluster, gatewayIP, err := ensureApplicationCluster(ctx, config, applicationClusterName, region)
		if err != nil {
			return nil, "", err
		}
		if err := ensureInternalDNS(ctx, cluster, gatewayIP); err != nil {
			return nil, "", err
		}

	}
	return configCluster, globalGatewayIP, nil
}

func buildRolloutRequest(configCluster *ClusterInfo, cfg *config.GKEConfig) *controller.RolloutRequest {
	req := &controller.RolloutRequest{
		Config:    cfg,
		NannyAddr: controllerAddr(configCluster),
	}
	for _, region := range cfg.Regions {
		// NOTE: distributor address must be resolveable from anywhere inside
		// the project's VPC.
		distributorAddr :=
			fmt.Sprintf("http://distributor.%s.svc.%s-%s:80", namespaceName, applicationClusterName, region)
		req.Locations = append(req.Locations, &controller.RolloutRequest_Location{
			Name:            region,
			DistributorAddr: distributorAddr,
		})
	}
	return req
}

// getProjectIAMBindings returns the GCP IAM bindings for the given project
// configuration.
func getProjectIAMBindings(ctx context.Context, config CloudConfig) (iamBindings, error) {
	service, err := cloudresourcemanager.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, err
	}
	call := service.Projects.GetIamPolicy(config.Project, &cloudresourcemanager.GetIamPolicyRequest{})
	call.Context(ctx)
	policy, err := call.Do()
	if err != nil {
		return nil, err
	}
	bindings := iamBindings{}
	for _, b := range policy.Bindings {
		for _, member := range b.Members {
			roles, ok := bindings[member]
			if !ok {
				bindings[member] = map[string]struct{}{b.Role: {}}
				continue
			}
			roles[b.Role] = struct{}{}
		}
	}
	return bindings, nil
}

// grantMultiClusterServicesIAMPermissions grants IAM permissions in the project
// required by the Multi Cluster Services controller.
func grantMultiClusterServicesIAMPermissions(config CloudConfig, bindings iamBindings) error {
	member := fmt.Sprintf("serviceAccount:%s.svc.id.goog[gke-mcs/gke-mcs-importer]", config.Project)
	const role = "roles/compute.networkViewer"
	return ensureProjectIAMBinding(config, role, member, bindings)
}

// grantGatewayIAMPermissions grants IAM permissions in the project required by
// the Gateway controller.
func grantGatewayIAMPermissions(config CloudConfig, bindings iamBindings) error {
	member := fmt.Sprintf(
		"serviceAccount:service-%s@gcp-sa-multiclusteringress.iam.gserviceaccount.com", config.ProjectNumber)
	const role = "roles/container.admin"
	return ensureProjectIAMBinding(config, role, member, bindings)
}

// ensureComputePermissions ensures that the default compute service account
// has the appropriate permissions.
func ensureComputePermissions(ctx context.Context, config CloudConfig, bindings iamBindings) error {
	member := fmt.Sprintf("serviceAccount:%s-compute@developer.gserviceaccount.com", config.ProjectNumber)
	for _, role := range []string{
		"roles/artifactregistry.reader",
		"roles/logging.logWriter",
		"roles/monitoring.editor",
		"roles/cloudtrace.agent",
		fmt.Sprintf("projects/%s/roles/%s", config.Project, sslRole),
	} {
		if err := ensureProjectIAMBinding(config, role, member, bindings); err != nil {
			return err
		}
	}
	return nil
}

// ensureIAMServiceAccounts ensures that the the GCP IAM service accounts that
// will be associated with the per-cluster Kubernetes service accounts.
func ensureIAMServiceAccounts(ctx context.Context, config CloudConfig, bindings iamBindings) error {
	// Setup the service account for the controller.
	if err := ensureIAMServiceAccount(ctx, config, bindings, controllerIAMServiceAccount,
		"roles/logging.logWriter",
		"roles/monitoring.editor",
		fmt.Sprintf("projects/%s/roles/%s", config.Project, sslRole)); err != nil {
		return err
	}

	// Setup the service account for the distributor.
	if err := ensureIAMServiceAccount(ctx, config, bindings, distributorIAMServiceAccount,
		"roles/logging.logWriter",
		"roles/monitoring.editor",
	); err != nil {
		return err
	}

	// Setup the service account for the manager.
	if err := ensureIAMServiceAccount(ctx, config, bindings, managerIAMServiceAccount,
		"roles/logging.logWriter",
		"roles/monitoring.editor",
	); err != nil {
		return err
	}

	// Setup the service account for the application.
	return ensureIAMServiceAccount(ctx, config, bindings, applicationIAMServiceAccount,
		"roles/logging.logWriter",
		"roles/monitoring.editor",
		"roles/cloudtrace.agent",
	)
}

// waitServiceAccountsCreated waits until the given service accounts have been
// created.
// NOTE: this function doesn't actually create any service accounts.
func waitServiceAccountsCreated(ctx context.Context, config CloudConfig, accounts ...string) error {
	// Get the current IAM bindings for the project.
	bindings, err := getProjectIAMBindings(ctx, config)
	if err != nil {
		return err
	}

	waitAccount := func(acct string) error {
		if roles := bindings[acct]; roles != nil {
			return nil
		}

		// Wait up to a minute for the service account to be created.
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		fmt.Fprintf(os.Stderr, "Waiting for the service account %q to be created...", acct)
		for r := retry.Begin(); r.Continue(waitCtx); {
			if bindings, err = getProjectIAMBindings(ctx, config); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return err
			}
			if roles := bindings[acct]; roles != nil {
				fmt.Fprintf(os.Stderr, "Done\n")
				return nil
			}
		}
		fmt.Fprintf(os.Stderr, "Timeout\n")
		return ctx.Err()
	}

	// Wait for all accounts to be created.
	for _, acct := range accounts {
		if err := waitAccount(acct); err != nil {
			return err
		}
	}

	return nil
}

// ensureIAMServiceAccount creates a GCP IAM service account with the given name
// in the specified project, and binds the given roles to it.
func ensureIAMServiceAccount(ctx context.Context, config CloudConfig, bindings iamBindings, account string, iamRoles ...string) error {
	// Ensure the service account is created.
	if err := patchCloudServiceAccount(ctx, config, patchOptions{}, account, &iam.ServiceAccount{
		Description: "Service Weaver generated IAM service account.",
		DisplayName: account,
	}); err != nil {
		return err
	}

	// Bind the IAM roles to the service account.
	member := fmt.Sprintf("serviceAccount:%s@%s.iam.gserviceaccount.com", account, config.Project)
	for _, role := range iamRoles {
		if err := ensureProjectIAMBinding(config, role, member, bindings); err != nil {
			return err
		}
	}
	return nil
}

// ensureProjectIAMBinding ensures that the IAM binding member -> role exists
// in the IAM bindings for the given project.
func ensureProjectIAMBinding(config CloudConfig, role, member string, bindings iamBindings) error {
	roles := bindings[member]
	if roles == nil {
		roles = map[string]struct{}{}
		bindings[member] = roles
	}
	if _, ok := roles[role]; ok {
		// Role already bound to the member.
		return nil
	}

	// Add the member -> role binding.
	_, err := runGcloud(
		config,
		fmt.Sprintf("Binding role %q to member %q", role, member),
		cmdOptions{}, "projects", "add-iam-policy-binding",
		config.Project, "--member", member, "--role", role, "--condition=None")
	if err == nil {
		roles[role] = struct{}{}
	}
	return err
}

// ensureServiceAccountIAMBinding ensures that the binding member -> role
// exists in the IAM bindings for the given service account.
func ensureServiceAccountIAMBinding(ctx context.Context, config CloudConfig, account, role, member string) error {
	email := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", account, config.Project)
	bindings, err := getServiceAccountIAMBindings(ctx, config, email)
	if err != nil {
		return err
	}

	if _, ok := bindings[member][role]; ok {
		// Role already bound to the member.
		return nil
	}

	// Add the binding.
	_, err = runGcloud(
		config,
		fmt.Sprintf("Binding role %q to member %q in service account %q", role, member, account),
		cmdOptions{},
		"iam", "service-accounts", "add-iam-policy-binding", email,
		"--role", role, "--member", member, "--condition=None")
	return err
}

// getServiceAccountIAMBindings returns the GCP IAM bindings for the service
// account with the given email.
func getServiceAccountIAMBindings(ctx context.Context, config CloudConfig, email string) (iamBindings, error) {
	iamService, err := iam.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, err
	}
	service := iam.NewProjectsService(iamService).ServiceAccounts
	call := service.GetIamPolicy(path.Join("projects", config.Project, "serviceAccounts", email))
	call.Context(ctx)
	policy, err := call.Do()
	if err != nil {
		return nil, err
	}
	bindings := iamBindings{}
	for _, b := range policy.Bindings {
		for _, member := range b.Members {
			roles, ok := bindings[member]
			if !ok {
				bindings[member] = map[string]struct{}{b.Role: {}}
				continue
			}
			roles[b.Role] = struct{}{}
		}
	}
	return bindings, nil
}

// ensureRootCA ensures that a root Certificate Authority has been created.
func ensureRootCA(ctx context.Context, config CloudConfig) error {
	// Ensure the root CA pool has been created.
	if err := ensureCAPool(config, rootCAPoolName, rootCALocation, "enterprise"); err != nil {
		return err
	}

	// Setup the IAM bindings for the root CA pool.
	role := "roles/privateca.auditor"
	member := fmt.Sprintf(
		"serviceAccount:service-%s@container-engine-robot.iam.gserviceaccount.com",
		config.ProjectNumber)
	if err := ensureCAPoolIAMBinding(ctx, config, rootCAPoolName, rootCALocation, role, member); err != nil {
		return err
	}

	// Ensure the root CA has been created.
	if _, err := runGcloud(config, "", cmdOptions{}, "privateca", "roots",
		"describe", rootCAName, "--pool", rootCAPoolName,
		"--location", rootCALocation); err == nil {
		// Already exists.
		return nil
	}
	subject := fmt.Sprintf("CN=%s, O=%s", rootCAName, rootCAOrganization)
	_, err := runGcloud(config, "Creating root CA for the project",
		cmdOptions{}, "privateca", "roots", "create", rootCAName,
		"--pool", rootCAPoolName, "--location", rootCALocation,
		"--subject", subject, "--key-algorithm", "ec-p256-sha256",
		"--max-chain-length", "1", "--auto-enable")
	return err
}

// ensureSubordinateCA ensures that a subordinate Certificate Authority
// has been created in the given region.
func ensureSubordinateCA(ctx context.Context, config CloudConfig, region string) error {
	// Ensure the subordinate CA pool has been created in the region.
	if err := ensureCAPool(config, subCAPoolName, region, "devops"); err != nil {
		return err
	}

	// Setup the IAM bindings for the root CA pool.
	role := "roles/privateca.certificateManager"
	member := fmt.Sprintf(
		"serviceAccount:service-%s@container-engine-robot.iam.gserviceaccount.com",
		config.ProjectNumber)
	if err := ensureCAPoolIAMBinding(ctx, config, subCAPoolName, region, role, member); err != nil {
		return err
	}

	// Ensure the subordinate CA has been created in the region.
	if _, err := runGcloud(config, "", cmdOptions{}, "privateca",
		"subordinates", "describe", subCAName, "--pool", subCAPoolName,
		"--location", region); err == nil {
		// Already exists.
		return nil
	}
	subject := fmt.Sprintf("CN=%s, O=%s", subCAName, subCAOrganization)
	_, err := runGcloud(config, "Creating subordinate CA for the project",
		cmdOptions{}, "privateca", "subordinates", "create", subCAName,
		"--pool", subCAPoolName, "--location", region,
		"--issuer-pool", rootCAPoolName, "--issuer-location", rootCALocation,
		"--subject", subject, "--key-algorithm", "ec-p256-sha256",
		// NOTE: this profile enables the subordinate CA to issue both client
		// and server TLS certificates for mTLS.
		"--use-preset-profile", "subordinate_mtls_pathlen_0", "--auto-enable")
	return err
}

// ensureCAPool ensures that a Certificate Authority pool with the given
// specification has been created.
func ensureCAPool(config CloudConfig, name, location, tier string) error {
	if _, err := runGcloud(config, "", cmdOptions{}, "privateca", "pools",
		"describe", name, "--location", location); err == nil {
		// Pool already exists.
		return nil
	}

	// Create the pool.
	_, err := runGcloud(config, "Creating root CA pool for the project",
		cmdOptions{}, "privateca", "pools", "create", name,
		"--location", location, "--tier", tier)
	return err
}

// ensureCAPoolIAMBinding ensures that the binding member -> role exists
// in the IAM bindings in the given Certificate Authority pool.
func ensureCAPoolIAMBinding(ctx context.Context, config CloudConfig, pool, location, role, member string) error {
	bindings, err := getCAPoolIAMBindings(ctx, config, pool, location)
	if err != nil {
		return err
	}

	if _, ok := bindings[member][role]; ok {
		// Role already bound to the member.
		return nil
	}

	// Add the binding.
	_, err = runGcloud(
		config,
		fmt.Sprintf("Binding role %q to member %q in CA pool %q in %q", role, member, pool, location),
		cmdOptions{},
		"privateca", "pools", "add-iam-policy-binding", pool,
		"--location", location, "--role", role, "--member", member)
	return err
}

// getCAPoolIAMBindings returns the GCP IAM bindings for the Certificate
// Authority pool with the given name.
func getCAPoolIAMBindings(ctx context.Context, config CloudConfig, pool, location string) (iamBindings, error) {
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, err
	}
	policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: path.Join("projects", config.Project, "locations", location, "caPools", pool),
	})
	if err != nil {
		return nil, err
	}
	bindings := iamBindings{}
	for _, b := range policy.Bindings {
		for _, member := range b.Members {
			roles, ok := bindings[member]
			if !ok {
				bindings[member] = map[string]struct{}{b.Role: {}}
				continue
			}
			roles[b.Role] = struct{}{}
		}
	}
	return bindings, nil
}

var workloadCertificateConfigTmpl = template.Must(template.New("wcert").Parse(`{
"kind":"WorkloadCertificateConfig",
"apiVersion":"security.cloud.google.com/v1",
"metadata":{
  "name":"default"
},
"spec":{
	"certificateAuthorityConfig":{
		"certificateAuthorityServiceConfig":{
			"endpointURI":"//privateca.googleapis.com/projects/{{.Project}}/locations/{{.Location}}/caPools/{{.Pool}}"
		}
	},
	"keyAlgorithm":{
		"rsa":{
			"modulusSize":4096
		}
	},
	"validityDurationSeconds":86400,
	"rotationWindowPercentage":50
}}`))

// ensureWorkloadCertificateConfig ensures that the workload certificate config
// has been applied to the given cluster.
func ensureWorkloadCertificateConfig(ctx context.Context, cluster *ClusterInfo) error {
	// Create the workload certificate config.
	var b strings.Builder
	if err := workloadCertificateConfigTmpl.Execute(&b, struct {
		Pool     string
		Project  string
		Location string
	}{
		Pool:     subCAPoolName,
		Project:  cluster.CloudConfig.Project,
		Location: cluster.Region,
	}); err != nil {
		return err
	}
	return patchWorkloadCertificateConfig(ctx, cluster, patchOptions{}, b.String())
}

var trustConfigTmpl = template.Must(template.New("trust").Parse(`{
"kind":"TrustConfig",
"apiVersion":"security.cloud.google.com/v1",
"metadata":{
	"name":"default"
},
"spec":{
	"trustStores":[{
		"trustDomain":"{{.Project}}.svc.id.goog",
		"trustAnchors":[{
			"certificateAuthorityServiceURI":"//privateca.googleapis.com/projects/{{.Project}}/locations/{{.Location}}/caPools/{{.Pool}}"
		}]
	}]
}}`))

// ensureTrustConfig ensures that the trust config has been applied to the given
// cluster.
func ensureTrustConfig(ctx context.Context, cluster *ClusterInfo) error {
	// Create the trust config.
	var b strings.Builder
	if err := trustConfigTmpl.Execute(&b, struct {
		Pool     string
		Project  string
		Location string
	}{
		Pool:     rootCAPoolName,
		Project:  cluster.CloudConfig.Project,
		Location: rootCALocation,
	}); err != nil {
		return err
	}
	return patchTrustConfig(ctx, cluster, patchOptions{}, b.String())
}

// createSSLRole creates a custom GCP role that stores permissions necessary
// for minting SSL certificates.
func createSSLRole(ctx context.Context, config CloudConfig) error {
	return patchProjectCustomRole(ctx, config, patchOptions{}, sslRole, &iam.Role{
		Description: "Custom role that grants Service Weaver service SSL-minting permissions.",
		Stage:       "GA",
		IncludedPermissions: []string{
			"compute.sslCertificates.get",
			"compute.sslCertificates.create",
			"compute.sslCertificates.delete",
			"compute.sslCertificates.list",
		},
	})
}

// ensureConfigCluster sets up a Service Weaver configuration cluster, returning the
// cluster information and the IP address of the gateway that routes ingress
// traffic to all Service Weaver applications.
func ensureConfigCluster(ctx context.Context, config CloudConfig, name, region string, bindings iamBindings) (*ClusterInfo, string, error) {
	cluster, err := ensureManagedCluster(ctx, config, name, region)
	if err != nil {
		return nil, "", err
	}
	if err := ensureKubeServiceAccount(ctx, cluster, controllerKubeServiceAccount, controllerIAMServiceAccount,
		[]rbacv1.PolicyRule{
			{
				APIGroups: []string{""}, // Core APIs.
				Resources: []string{"configmaps"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"net.gke.io"}, // Networking APIs.
				Resources: []string{"serviceimports"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"gateway.networking.k8s.io"}, // Gateway APIs.
				Resources: []string{"gateways", "httproutes"},
				Verbs:     []string{"*"},
			},
		}); err != nil {
		return nil, "", err
	}

	// Ensure Service Weaver priority classes have been created in the cluster.
	if err := ensureControlPriorityClass(ctx, cluster); err != nil {
		return nil, "", err
	}

	// Ensure multi-cluster resources have been created in the given cluster.
	if err := ensureMultiClusterIngress(cluster); err != nil {
		return nil, "", err
	}
	if err := grantMultiClusterServicesIAMPermissions(config, bindings); err != nil {
		return nil, "", err
	}
	if err := grantGatewayIAMPermissions(config, bindings); err != nil {
		return nil, "", err
	}
	gatewayIP, err := ensureGlobalExternalGatewayIPAddress(ctx, config)
	if err != nil {
		return nil, "", err
	}
	// NOTE: Don't create the gateway, to avoid stomping on the gateway
	// updates performed by the nanny. Instead, rely on the nanny
	// creating the gateway the first time it attempts to update it.
	return cluster, gatewayIP, nil
}

// ensureApplicationCluster ensures that a Service Weaver managed cluster is available
// and running in the given region and is set up to host Service Weaver applications.
// It returns the cluster information and the IP address of the gateway that
// routes internal traffic to Service Weaver applications in the cluster.
func ensureApplicationCluster(ctx context.Context, config CloudConfig, name, region string) (*ClusterInfo, string, error) {
	cluster, err := ensureManagedCluster(ctx, config, name, region)
	if err != nil {
		return nil, "", err
	}

	// Setup the distributor/manager/application service accounts.
	if err := ensureKubeServiceAccount(ctx, cluster, distributorKubeServiceAccount, distributorIAMServiceAccount,
		[]rbacv1.PolicyRule{
			{
				APIGroups: []string{""}, // Core APIs.
				Resources: []string{"services", "configmaps"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"gateway.networking.k8s.io"}, // Gateway APIs.
				Resources: []string{"gateways", "httproutes"},
				Verbs:     []string{"*"},
			},
		}); err != nil {
		return nil, "", err
	}
	if err := ensureKubeServiceAccount(ctx, cluster, managerKubeServiceAccount, managerIAMServiceAccount,
		[]rbacv1.PolicyRule{
			{
				APIGroups: []string{""}, // Core APIs.
				Resources: []string{"pods", "services", "configmaps"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"apps"}, // Application APIs.
				Resources: []string{"deployments"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"batch"}, // Batch APIs.
				Resources: []string{"jobs"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"autoscaling.gke.io"}, // Autoscaling APIs.
				Resources: []string{"multidimpodautoscalers"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{"net.gke.io"}, // Networking APIs.
				Resources: []string{"serviceexports"},
				Verbs:     []string{"*"},
			},
		}); err != nil {
		return nil, "", err
	}

	// Ensure Service Weaver priority classes have been created in the cluster.
	if err := ensureControlPriorityClass(ctx, cluster); err != nil {
		return nil, "", err
	}
	if err := ensureApplicationPriorityClass(ctx, cluster); err != nil {
		return nil, "", err
	}
	if err := ensureSpareCapacityPriorityClass(ctx, cluster); err != nil {
		return nil, "", err
	}

	// Allocate a small permanent spare capacity for the cluster. This will
	// allow applications to scale up faster.
	// NOTE: A temporary spare capacity, used for quicker application version
	// startup, will be allocated separately by the managers in each cluster.
	spareCPU := multiplyQuantity(4, cpuUnit)
	if err := ensurePermanentSpareCapacity(ctx, cluster, spareCPU); err != nil {
		// Swallow the error as it isn't catastrophic.
		fmt.Fprintf(os.Stderr, "Warning: cannot ensure permanent spare "+
			"capacity for cluster %q in %q. Application scale-up "+
			"performance may suffer.\n", cluster.Name, cluster.Region)
	}

	if err := ensureKubeServiceAccount(ctx, cluster, applicationKubeServiceAccount, applicationIAMServiceAccount, []rbacv1.PolicyRule{}); err != nil {
		return nil, "", err
	}
	gatewayIP, err := ensureRegionalInternalGateway(ctx, cluster)
	if err != nil {
		return nil, "", err
	}
	if _, err := setupProxySubnet(ctx, cluster); err != nil {
		return nil, "", err
	}
	return cluster, gatewayIP, nil
}

// ensureManagedCluster ensures that a Service Weaver managed cluster is available
// and running in the given region.
func ensureManagedCluster(ctx context.Context, config CloudConfig, name, region string) (*ClusterInfo, error) {
	exists, err := hasCluster(ctx, config, name, region)
	if err != nil {
		return nil, err
	}
	if !exists {
		// Cluster doesn't exist: ensure that its fleet membership has been
		// removed. (A defunct fleet membership can linger if e.g. the user
		// deletes the cluster manually, without deleting the fleet membership
		// as well.)
		if err := unregisterFromFleet(ctx, config, name, region); err != nil {
			return nil, err
		}
	}

	// Ensure that the cluster has been created.
	if err := patchCluster(ctx, config, patchOptions{}, region, &containerpb.Cluster{
		Name:        name,
		Description: "Service Weaver managed cluster",
		// NOTE: We need cluster version 1.24 or later, which happens to be
		// satisfied by the "latest" alias on the release channel as of
		// 9/30/2022.
		ReleaseChannel: &containerpb.ReleaseChannel{
			Channel: containerpb.ReleaseChannel_REGULAR,
		},
		InitialClusterVersion: "latest",
		Autoscaling: &containerpb.ClusterAutoscaling{
			AutoscalingProfile: containerpb.ClusterAutoscaling_OPTIMIZE_UTILIZATION,
		},
		AddonsConfig: &containerpb.AddonsConfig{
			HttpLoadBalancing: &containerpb.HttpLoadBalancing{
				// NOTE: Gateway support requires this add-on.
				Disabled: false,
			},
			HorizontalPodAutoscaling: &containerpb.HorizontalPodAutoscaling{
				// NOTE: MultidimPodAutoscaler requires this add-on.
				Disabled: false,
			},
			KubernetesDashboard: &containerpb.KubernetesDashboard{
				Disabled: true,
			},
			NetworkPolicyConfig: &containerpb.NetworkPolicyConfig{
				// NOTE: MCS support requires this add-on.
				Disabled: false,
			},
			CloudRunConfig: &containerpb.CloudRunConfig{
				Disabled: true,
			},
			DnsCacheConfig: &containerpb.DnsCacheConfig{
				// NOTE: MCS support requires this add-on, when CloudDNS is used.
				Enabled: true,
			},
			ConfigConnectorConfig: &containerpb.ConfigConnectorConfig{
				Enabled: false,
			},
			GcePersistentDiskCsiDriverConfig: &containerpb.GcePersistentDiskCsiDriverConfig{
				Enabled: false,
			},
			GcpFilestoreCsiDriverConfig: &containerpb.GcpFilestoreCsiDriverConfig{
				Enabled: false,
			},
		},
		// Disabling logging and monitoring services ensures that no logging
		// or monitoring pods get scheduled on kubernetes nodes, saving
		// resources. (NOTE: Service Weaver handles logging and monitoring on behalf
		// of the application.)
		LoggingService: "none",
		// NOTE: GKE cluster autoscaler requires monitoring to be enabled.
		MonitoringService: "monitoring.googleapis.com/kubernetes",
		NodePools: []*containerpb.NodePool{
			{
				Name:             "default-pool",
				InitialNodeCount: 1,
				Config: &containerpb.NodeConfig{
					DiskSizeGb:  100,
					DiskType:    "pd-standard",
					MachineType: "e2-medium",
					// NOTE(spetrovic): Allow full access scopes, as
					// recommended by:
					// https://cloud.google.com/compute/docs/access/service-accounts
					OauthScopes: []string{
						"https://www.googleapis.com/auth/cloud-platform",
					},
					WorkloadMetadataConfig: &containerpb.WorkloadMetadataConfig{
						// Setting mode to GCE_METADATA ensures that no GKE
						// metadata pods are scheduled on kubernetes nodes,
						// saving resources.
						Mode: containerpb.WorkloadMetadataConfig_GCE_METADATA,
					},
				},
				// NOTE: consider using auto-provisioning, which creates new
				// node pools of various types to satisfy resource demands. For
				// simplicity, we stick with auto-scaling for now.
				Autoscaling: &containerpb.NodePoolAutoscaling{
					Enabled:           true,
					LocationPolicy:    containerpb.NodePoolAutoscaling_ANY,
					TotalMinNodeCount: 1,
					TotalMaxNodeCount: int32(math.MaxInt32),
				},
			},
		},
		VerticalPodAutoscaling: &containerpb.VerticalPodAutoscaling{Enabled: true},
		NetworkConfig: &containerpb.NetworkConfig{
			DnsConfig: &containerpb.DNSConfig{
				ClusterDns:       containerpb.DNSConfig_CLOUD_DNS,
				ClusterDnsScope:  containerpb.DNSConfig_VPC_SCOPE,
				ClusterDnsDomain: fmt.Sprintf("%s-%s", name, region),
			},
			GatewayApiConfig: &containerpb.GatewayAPIConfig{
				Channel: containerpb.GatewayAPIConfig_CHANNEL_STANDARD,
			},
		},
		MeshCertificates: &containerpb.MeshCertificates{
			EnableCertificates: wrapperspb.Bool(true),
		},
		// Enable workload identity on the cluster [1], which is required for
		// GKE Hub registration.
		//
		// [1]: https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable_on_cluster
		WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
			WorkloadPool: fmt.Sprintf("%s.svc.id.goog", config.Project),
		},
	}); err != nil {
		return nil, err
	}
	cluster, err := GetClusterInfo(ctx, config, name, region)
	if err != nil {
		return nil, err
	}

	// Setup the subordinate Certificate Authority in the cluster region.
	if err := ensureSubordinateCA(ctx, config, region); err != nil {
		return nil, err
	}

	// Setup the workload certificate config in the cluster.
	if err := ensureWorkloadCertificateConfig(ctx, cluster); err != nil {
		return nil, err
	}

	// Setup the trust config in the cluster.
	if err := ensureTrustConfig(ctx, cluster); err != nil {
		return nil, err
	}

	// Scale down resources used by system services.
	if err := scaleDownSystemServices(ctx, cluster); err != nil {
		return nil, err
	}

	// Add a Service Weaver namespace to the cluster.
	if err := patchNamespace(ctx, cluster, patchOptions{}, &apiv1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespaceName,
			Labels: map[string]string{
				"name": namespaceName,
			},
		},
	}); err != nil {
		return nil, err
	}

	// Add a Service Weaver backend-config resource to the cluster.
	if err := patchBackendConfig(ctx, cluster, patchOptions{}, &backendconfigv1.BackendConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backendConfigName,
			Namespace: namespaceName,
		},
		Spec: backendconfigv1.BackendConfigSpec{
			HealthCheck: &backendconfigv1.HealthCheckConfig{
				RequestPath: ptrOf("/healthz"),
			},
		},
	}); err != nil {
		return nil, err
	}

	// Register the cluster with the project fleet.
	if err := registerWithFleet(ctx, config, cluster); err != nil {
		return nil, err
	}

	// Wait for the service exports resource to become available in the cluster.
	if err := waitForServiceExportsResource(ctx, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

// hasCluster returns true iff the cluster with a given name exists in the
// given region.
func hasCluster(ctx context.Context, config CloudConfig, name, region string) (bool, error) {
	client, err := container.NewClusterManagerClient(ctx, config.ClientOptions()...)
	if err != nil {
		return false, err
	}
	defer client.Close()
	if _, err = client.GetCluster(ctx, &containerpb.GetClusterRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
			config.Project, region, name),
	}); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// scaleDownSystemServices removes redundant kubernetes system services
// from the given cluster.
func scaleDownSystemServices(ctx context.Context, cluster *ClusterInfo) error {
	const kubeSystemNamespace = "kube-system"

	// Stop kube-dns, since we use CloudDNS.
	depClient := cluster.Clientset.AppsV1().Deployments(kubeSystemNamespace)
	for _, name := range []string{"kube-dns-autoscaler", "kube-dns"} {
		dep, err := depClient.Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) { // deployment doesn't exist: ok.
			continue
		}
		if err != nil {
			return err
		}
		if dep.Spec.Replicas != nil && *dep.Spec.Replicas == 0 {
			// Nothing to do.
			continue
		}
		dep.Spec.Replicas = ptrOf(int32(0))
		if err := patchDeployment(ctx, cluster, patchOptions{}, dep); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// ensureControlPriorityClass ensures that the priority class used for
// control pods (e.g., controller, distributor, manager) has been created in the
// given cluster.
func ensureControlPriorityClass(ctx context.Context, cluster *ClusterInfo) error {
	return patchPriorityClass(ctx, cluster, patchOptions{}, &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: controlPriorityClassName,
		},
		Value:            10,
		PreemptionPolicy: ptrOf(apiv1.PreemptLowerPriority),
		GlobalDefault:    false,
		Description:      "Priority class used by Service Weaver control Pods",
	})
}

// ensureApplicationPriorityClass ensures that the priority class used for
// application pods has been created in the given cluster.
func ensureApplicationPriorityClass(ctx context.Context, cluster *ClusterInfo) error {
	return patchPriorityClass(ctx, cluster, patchOptions{}, &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: applicationPriorityClassName,
		},
		Value:            0,
		PreemptionPolicy: ptrOf(apiv1.PreemptLowerPriority),
		GlobalDefault:    false,
		Description:      "Priority class used by Service Weaver application Pods",
	})
}

// ensureSpareCapacityPriorityClass ensures that the priority class used for
// allocating spare cluster capacity has been created in the given cluster.
func ensureSpareCapacityPriorityClass(ctx context.Context, cluster *ClusterInfo) error {
	return patchPriorityClass(ctx, cluster, patchOptions{}, &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: spareCapacityPriorityClassName,
		},
		Value:            -10,
		PreemptionPolicy: ptrOf(apiv1.PreemptNever),
		GlobalDefault:    false,
		Description:      "Priority class used by Service Weaver to allocate spare Pod capacity",
	})
}

// ensurePermanentSpareCapacity ensures that a given permanent spare CPU
// capacity is available in the given cluster [1].
// [1]: https://wdenniss.com/gke-autopilot-spare-capacity
func ensurePermanentSpareCapacity(ctx context.Context, cluster *ClusterInfo, cpu resource.Quantity) error {
	name := "permanent-spare-capacity"

	// Split requested cpu resources into cpuUnitQuantity chunks, each chunk
	// corresponding to a spare-capacity pod replica.
	numReplicas := (cpu.Value() + cpuUnit.Value() - 1) / cpuUnit.Value()
	if numReplicas > math.MaxInt32 {
		return fmt.Errorf("too much cpu requested")
	}
	resList := v1.ResourceList{
		"memory": memoryUnit,
		"cpu":    cpuUnit,
	}
	return patchDeployment(ctx, cluster, patchOptions{}, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrOf(int32(numReplicas)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name},
				},
				Spec: v1.PodSpec{
					PriorityClassName:             spareCapacityPriorityClassName,
					TerminationGracePeriodSeconds: ptrOf(int64(0)),
					Containers: []v1.Container{
						{
							Name:    "alpine",
							Image:   "alpine",
							Command: []string{"sleep"},
							Args:    []string{"infinity"},
							Resources: v1.ResourceRequirements{
								Requests: resList,
								Limits:   resList,
							},
						},
					},
					ServiceAccountName: applicationKubeServiceAccount,
				},
			},
		},
	})
}

// ensureInternalDNS ensures that cloudDNS has been configured to route
// hostnames of the form "*.<region>.serviceweaver.internal" to the given internal
// regional gateway IP address.
func ensureInternalDNS(ctx context.Context, cluster *ClusterInfo, gatewayIP string) error {
	networkURL := getComputeURL(cluster.CloudConfig, computeResource{
		Region: "", // global
		Type:   "networks",
		Name:   "default",
	})

	// Ensure a Service Weaver managed DNS zone has been created.
	if err := patchDNSZone(ctx, cluster.CloudConfig, patchOptions{}, &dns.ManagedZone{
		Name: managedDNSZoneName,
		Description: fmt.Sprintf(
			"Managed zone for domain %s", distributor.InternalDNSDomain),
		DnsName:    distributor.InternalDNSDomain + ".",
		Visibility: "private",
		PrivateVisibilityConfig: &dns.ManagedZonePrivateVisibilityConfig{
			Networks: []*dns.ManagedZonePrivateVisibilityConfigNetwork{
				{
					NetworkUrl: networkURL,
				},
			},
		},
	}); err != nil {
		return err
	}

	// Add the A record to the managed DNS zone.
	dnsName := fmt.Sprintf("*.%s.%s.", cluster.Region, distributor.InternalDNSDomain)
	return patchDNSRecordSet(ctx, cluster.CloudConfig, patchOptions{}, managedDNSZoneName, &dns.ResourceRecordSet{
		Name:    dnsName,
		Type:    "A",
		Rrdatas: []string{gatewayIP},
		Ttl:     300, // seconds
	})
}

func ensureKubeServiceAccount(ctx context.Context, cluster *ClusterInfo, account, iamAccount string, policyRules []rbacv1.PolicyRule) error {
	// Allow the kubernetes service account to access the IAM service account.
	const accountRole = "roles/iam.workloadIdentityUser"
	accountMember := fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", cluster.CloudConfig.Project, namespaceName, account)
	if err := ensureServiceAccountIAMBinding(ctx, cluster.CloudConfig, iamAccount, accountRole, accountMember); err != nil {
		return err
	}

	// Create a Kubernetes service account and bind it to the GCP IAM service
	// account.
	if err := patchKubeServiceAccount(ctx, cluster, patchOptions{}, &apiv1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      account,
			Namespace: namespaceName,
			Annotations: map[string]string{
				"iam.gke.io/gcp-service-account": fmt.Sprintf("%s@%s.iam.gserviceaccount.com", iamAccount, cluster.CloudConfig.Project),
			},
		},
	}); err != nil {
		return err
	}

	// Create a Kubernetes cluster role.
	if err := patchClusterRole(ctx, cluster, patchOptions{}, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: account,
		},
		Rules: policyRules,
	}); err != nil {
		return err
	}

	// Bind the Kubernetes cluster role to the Kubernetes service account.
	return patchRoleBinding(ctx, cluster, patchOptions{}, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      account,
			Namespace: namespaceName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "User",
				Name:      fmt.Sprintf("system:serviceaccount:%s:%s", namespaceName, account),
				Namespace: namespaceName,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind: "ClusterRole",
			Name: account,
		},
	})
}

// registerWithFleet registers the given cluster with the project fleet, if it
// isn't already registered.
func registerWithFleet(ctx context.Context, config CloudConfig, cluster *ClusterInfo) error {
	mName := fmt.Sprintf("%s-%s", cluster.Name, cluster.Region)
	cName := fmt.Sprintf("%s/%s", cluster.Region, cluster.Name)

	// Check if the cluster has membership.
	if _, err := runGcloud(config, "", cmdOptions{},
		"container", "fleet", "memberships", "describe", mName,
		"--location", "global", "--format=value(state.code)"); err == nil {
		// Already a member: we're done.
		return nil
	}

	// Add the membership.
	_, err := runGcloud(config,
		fmt.Sprintf("Registering cluster %q in %q with the project fleet",
			cluster.Name, cluster.Region),
		cmdOptions{}, "container", "fleet", "memberships",
		"register", mName, "--gke-cluster", cName, "--enable-workload-identity",
	)
	return err
}

// unregisterFromFleet removes the given cluster's registration with the project
// fleet, if one exists.
func unregisterFromFleet(ctx context.Context, config CloudConfig, name, region string) error {
	mName := fmt.Sprintf("%s-%s", name, region)

	// Check if the cluster has membership.
	if _, err := runGcloud(config, "", cmdOptions{},
		"container", "fleet", "memberships", "describe", mName,
		"--location", "global"); err != nil {
		// Not a member: we're done.
		return nil
	}

	// Delete the membership.
	_, err := runGcloud(config, fmt.Sprintf(
		"Deleting project fleet membership for cluster %q in %q",
		name, region), cmdOptions{},
		"container", "fleet", "memberships", "unregister", mName,
		"--gke-cluster", fmt.Sprintf("%s/%s", region, name),
		"--location", "global", "--quiet",
	)
	return err
}

// waitForServiceExportsResource waits for the serviceexports resource
// to become available in the given cluster.  This resource is made available
// a short time interval after the cluster is registered with the GKE Hub
// for the first time (see registerWithHub).
func waitForServiceExportsResource(ctx context.Context, cluster *ClusterInfo) error {
	cli := cluster.extensionsClientset.ApiextensionsV1().CustomResourceDefinitions()
	ready := func() bool {
		_, err := cli.Get(ctx, serviceExportsResourceName, metav1.GetOptions{})
		return err == nil
	}
	if ready() {
		return nil
	}
	fmt.Fprintf(os.Stderr,
		"Waiting for serviceexports resource to become available in "+
			"cluster %q in %q\n", cluster.Name, cluster.Region)
	for r := retry.Begin(); r.Continue(ctx); {
		if ready() {
			return nil
		}
	}
	return fmt.Errorf(
		"timed out waiting for serviceexports resource to become available "+
			"in cluster %q in %q", cluster.Name, cluster.Region)
}

// ensureMultiClusterIngress ensures multi-cluster ingress is enabled
// fo the given (config) cluster.
func ensureMultiClusterIngress(cluster *ClusterInfo) error {
	fName := fmt.Sprintf("projects/%s/locations/global/memberships/%s-%s",
		cluster.CloudConfig.Project, cluster.Name, cluster.Region)

	out, err := runGcloud(cluster.CloudConfig, "", cmdOptions{},
		"container", "fleet", "ingress", "describe",
		"--format=value(spec.multiclusteringress.configMembership)")
	if err != nil { // Ingress feature disabled: enable it.
		// NOTE(spetrovic): Retry twice since it sometimes takes more
		// than two minutes for the ingress controller to start.
		for i := 0; i < 2; i++ {
			_, err = runGcloud(cluster.CloudConfig,
				fmt.Sprintf("Enabling multi-cluster ingress for cluster %q in %q",
					cluster.Name, cluster.Region), cmdOptions{},
				"container", "fleet", "ingress", "enable",
				"--config-membership", fName, "--quiet")
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	// Ingress feature enabled: see if it's for our cluster.
	out = strings.TrimSuffix(out, "\n") // remove trailing newline
	if out == fName {
		return nil
	}
	// Update ingress feature to point to our cluster.
	_, err = runGcloud(cluster.CloudConfig,
		fmt.Sprintf("Updating multi-cluster ingress for cluster %q in %q",
			cluster.Name, cluster.Region), cmdOptions{},
		"container", "fleet", "ingress", "update", "--config-membership",
		fName, "--quiet")
	return err
}

// setupProxySubnet sets up the sub-network used by L7 ILBs in the cluster's
// region, returning the sub-network IP range.
func setupProxySubnet(ctx context.Context, cluster *ClusterInfo) (string, error) {
	proxyIPRange, err := getIPRangeForRegion(ctx, cluster.CloudConfig, cluster.Region)
	if err != nil {
		return "", err
	}
	networkURL := getComputeURL(cluster.CloudConfig, computeResource{
		Region: "", // global
		Type:   "networks",
		Name:   "default",
	})
	regionURL := getComputeURL(cluster.CloudConfig, computeResource{
		Region: cluster.Region,
		Type:   "",
		Name:   "",
	})
	if err := patchSubnet(ctx, cluster.CloudConfig, patchOptions{}, cluster.Region, &computepb.Subnetwork{
		Name:        ptrOf(subnetName),
		Description: ptrOf("subnet for the Service Weaver regional Gateways"),
		Region:      &regionURL,
		Network:     &networkURL,
		IpCidrRange: &proxyIPRange,
		Purpose:     ptrOf(computepb.Subnetwork_INTERNAL_HTTPS_LOAD_BALANCER.String()),
		Role:        ptrOf(computepb.Subnetwork_ACTIVE.String()),
	}); err != nil {
		return "", err
	}
	return proxyIPRange, nil
}

// computeResource holds information about a resource in GCP compute.
type computeResource struct {
	Region string // Resource region . Assumed global if empty.
	Type   string // Resource type, e.g., subnetworks, targetHttpProxies.
	Name   string // Resource name.
}

// getComputeURL returns the full URL of a GCP resource.
func getComputeURL(config CloudConfig, res computeResource) string {
	const urlPrefix = "https://www.googleapis.com/compute/v1"
	project := fmt.Sprintf("projects/%s", config.Project)
	var loc string
	if res.Region == "" { // global
		loc = "global"
	} else { // regional
		loc = fmt.Sprintf("regions/%s", res.Region)
	}
	if res.Type == "" { // empty resource
		return fmt.Sprintf("%s/%s/%s", urlPrefix, project, loc)
	}
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s", urlPrefix, project, loc, res.Type, res.Name)
}

// ensureGlobalExternalGatewayIPAddress ensures that an ip address has been
// created for the global external gateway.
func ensureGlobalExternalGatewayIPAddress(ctx context.Context, config CloudConfig) (string, error) {
	ip, err := patchStaticIPAddress(ctx, config, patchOptions{}, "" /*global*/, &computepb.Address{
		Name:        ptrOf(gatewayIPAddressName),
		AddressType: ptrOf(computepb.Address_EXTERNAL.String()),
		Purpose:     ptrOf(computepb.Address_UNDEFINED_PURPOSE.String()),
		Description: ptrOf("Static IP address for the Service Weaver global external gateway"),
	})
	if err != nil {
		return "", fmt.Errorf("cannot create a static IP address for the global external gateway: %w", err)
	}
	return ip, nil
}

// ensureRegionalInternalGateway ensures that an internal regional Gateway
// has been created in the given cluster and returns the gateways (internal)
// IP address.
func ensureRegionalInternalGateway(ctx context.Context, cluster *ClusterInfo) (string, error) {
	// Create the IP address for the gateway.
	ip, err := patchStaticIPAddress(ctx, cluster.CloudConfig, patchOptions{}, cluster.Region, &computepb.Address{
		Name:        ptrOf(gatewayIPAddressName),
		AddressType: ptrOf(computepb.Address_INTERNAL.String()),
		Purpose:     ptrOf(computepb.Address_GCE_ENDPOINT.String()),
		Description: ptrOf("Static IP address for the Service Weaver internal regional gateway"),
	})
	if err != nil {
		return "", fmt.Errorf("cannot create a static IP address for the regional internal gateway: %w", err)
	}
	if err := patchGateway(ctx, cluster, patchOptions{}, &gatewayv1beta1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      internalGatewayName,
			Namespace: namespaceName,
		},
		Spec: gatewayv1beta1.GatewaySpec{
			GatewayClassName: "gke-l7-rilb",
			Addresses: []gatewayv1beta1.GatewayAddress{
				{
					Type:  ptrOf(gatewayv1beta1.NamedAddressType),
					Value: gatewayIPAddressName,
				},
			},
			Listeners: []gatewayv1beta1.Listener{
				{
					Name:     "http",
					Protocol: gatewayv1beta1.HTTPProtocolType,
					Port:     gatewayv1beta1.PortNumber(80),
					AllowedRoutes: &gatewayv1beta1.AllowedRoutes{
						Kinds: []gatewayv1beta1.RouteGroupKind{{Kind: "HTTPRoute"}},
					},
				},
			},
		},
	}); err != nil {
		return "", err
	}
	return ip, nil
}

// ensureWeaverServices ensures that Service Weaver services (i.e., controller and
// all needed distributors and managers) are running.
func ensureWeaverServices(ctx context.Context, config CloudConfig, cfg *config.GKEConfig) error {
	if err := ensureController(ctx, config); err != nil {
		return err
	}
	for _, region := range cfg.Regions {
		cluster, err := GetClusterInfo(ctx, config, applicationClusterName, region)
		if err != nil {
			return err
		}
		if err := ensureDistributor(ctx, cluster); err != nil {
			return err
		}
		if err := ensureManager(ctx, cluster); err != nil {
			return err
		}
	}
	return nil
}

// ensureController ensures that a controller is running in the config cluster.
func ensureController(ctx context.Context, config CloudConfig) error {
	cluster, err := GetClusterInfo(ctx, config, ConfigClusterName, ConfigClusterRegion)
	if err != nil {
		return err
	}
	const name = "controller"
	if err := ensureNannyDeployment(ctx, cluster, name, controllerKubeServiceAccount); err != nil {
		return err
	}
	if err := ensureNannyVerticalPodAutoscaler(ctx, cluster, name); err != nil {
		return err
	}
	return ensureNannyService(ctx, cluster, name, name)
}

// ensureDistributor ensures that a distributor is running in the given cluster.
func ensureDistributor(ctx context.Context, cluster *ClusterInfo) error {
	const name = "distributor"
	if err := ensureNannyDeployment(ctx, cluster, name, distributorKubeServiceAccount); err != nil {
		return err
	}
	if err := ensureNannyVerticalPodAutoscaler(ctx, cluster, name); err != nil {
		return err
	}
	return ensureNannyService(ctx, cluster, name, name)
}

// ensureManager ensures that a manager is running in the given cluster.
func ensureManager(ctx context.Context, cluster *ClusterInfo) error {
	const name = "manager"
	if err := ensureNannyDeployment(ctx, cluster, name, managerKubeServiceAccount); err != nil {
		return err
	}
	if err := ensureNannyVerticalPodAutoscaler(ctx, cluster, name); err != nil {
		return err
	}
	return ensureNannyService(ctx, cluster, name, name)
}

// ensureNannyDeployment ensures that a nanny deployment with the given name
// and service account is running in the given cluster.
func ensureNannyDeployment(ctx context.Context, cluster *ClusterInfo, name, serviceAccount string) error {
	meta := ContainerMetadata{
		Project:       cluster.CloudConfig.Project,
		ClusterName:   cluster.Name,
		ClusterRegion: cluster.Region,
		Namespace:     namespaceName,
		ContainerName: nannyContainerName,
		App:           name,
	}
	metaStr, err := proto.ToEnv(&meta)
	if err != nil {
		return err
	}
	toolImageURL := fmt.Sprintf("%s-docker.pkg.dev/%s/%s/weaver-gke:latest",
		buildLocation, cluster.CloudConfig.Project, dockerRepoName)

	return patchDeployment(ctx, cluster, patchOptions{}, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name},
				},
				Spec: apiv1.PodSpec{
					PriorityClassName: controlPriorityClassName,
					Containers: []apiv1.Container{
						{
							Name:  name,
							Image: toolImageURL,
							Args: []string{
								fmt.Sprintf("/weaver/weaver-gke %s --port=%d", name, nannyServingPort),
							},
							Resources: apiv1.ResourceRequirements{
								Requests: v1.ResourceList{
									"memory": memoryUnit,
									"cpu":    cpuUnit,
								},
							},
							// Enabling TTY and Stdin allows the user to run a
							// shell inside the container, for debugging.
							TTY:   true,
							Stdin: true,
							Env: []apiv1.EnvVar{
								{Name: containerMetadataEnvKey, Value: metaStr},
								nodeNameEnvVar,
								podNameEnvVar,
							},
						},
					},
					ServiceAccountName: serviceAccount,
				},
			},
		},
	})
}

// ensureNannyVerticalPodAutoscaler ensures that a nanny vertical pod autoscaler
// with a given name is running in the given cluster.
func ensureNannyVerticalPodAutoscaler(ctx context.Context, cluster *ClusterInfo, name string) error {
	return patchVerticalPodAutoscaler(ctx, cluster, patchOptions{}, &vautoscalingv1.VerticalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
		},
		Spec: vautoscalingv1.VerticalPodAutoscalerSpec{
			TargetRef: &autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       name,
			},
			UpdatePolicy: &vautoscalingv1.PodUpdatePolicy{
				UpdateMode:  ptrOf(vautoscalingv1.UpdateModeAuto),
				MinReplicas: ptrOf(int32(1)),
			},
		},
	})
}

// ensureNannyService ensures that a nanny service with a given name is running
// in the given cluster.
func ensureNannyService(ctx context.Context, cluster *ClusterInfo, svcName, targetName string) error {
	return patchService(ctx, cluster, patchOptions{}, &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespaceName,
		},
		Spec: apiv1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				"app": targetName,
			},
			Ports: []apiv1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.FromInt(nannyServingPort),
					Protocol:   apiv1.Protocol("TCP"),
				},
			},
		},
	})
}
