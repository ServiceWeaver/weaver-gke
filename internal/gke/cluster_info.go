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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"github.com/google/uuid"
	apiextension "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	vautoscaling "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/client/clientset/versioned"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	backendconfigv1 "k8s.io/ingress-gce/pkg/backendconfig/client/clientset/versioned"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1beta1"
)

// ClusterInfo stores information about a GKE cluster.
type ClusterInfo struct {
	// Cluster name.
	Name string

	// Cluster region (e.g., us-east1, europe-west1). This value is static for
	// the lifetime of the cluster.
	Region string

	// Cluster multi-region (e.g., us, europe, asia).  May be empty if the
	// cluster isn't part of a multi-region (e.g., northamerica-northeast1).
	MultiRegion string

	// Configuration information for a cloud
	CloudConfig CloudConfig

	// Kubernetes REST client for accessing the Kubernetes API server.
	apiRESTClient *rest.RESTClient

	// Kubernetes clientset for the core API.
	Clientset *kubernetes.Clientset

	// Kubernetes clientset for the extensions API.
	extensionsClientset *apiextension.Clientset

	// Kubernetes clientset for Gateway API.
	gatewayClientset *gatewayv1beta1.GatewayV1beta1Client

	// Kubernetes clientset for the GKE ingress API.
	ingressClientset *backendconfigv1.Clientset

	// Kubernetes clientset for the VerticalPodAutoscaler API.
	verticalAutoscalingClientset *vautoscaling.Clientset

	// Client used for APIs that don't have an accessible Go library.
	dynamicClient dynamic.Interface
}

// GetClusterInfo returns information about a GKE cluster running in a given
// cloud region.
// REQUIRES: The caller is running on the user machine.
func GetClusterInfo(ctx context.Context, config CloudConfig, cluster, region string) (*ClusterInfo, error) {
	// Fetch cluster credentials to the local machine.
	kubeFileName := filepath.Join(
		os.TempDir(), fmt.Sprintf("serviceweaver_%s_%s", cluster, uuid.New().String()))
	if _, err := runGcloud(config, "", cmdOptions{
		EnvOverrides: []string{
			"USE_GKE_GCLOUD_AUTH_PLUGIN=True",
			fmt.Sprintf("KUBECONFIG=%s", kubeFileName),
		}},
		"container", "clusters", "get-credentials", cluster, "--region", region,
	); err != nil {
		return nil, err
	}
	defer os.Remove(kubeFileName)
	kubeFile, err := os.Open(kubeFileName)
	if err != nil {
		return nil, err
	}

	// Manually add the --account and --project flags to the gcloud command in
	// the kubernetes config. These are dropped by the 'get-credentials' command
	// above, which causes the account and project from the currently active
	// gcloud config to be used.
	var contents bytes.Buffer
	scanner := bufio.NewScanner(kubeFile)
	for scanner.Scan() {
		line := scanner.Text()
		contents.Write([]byte(line))
		if strings.Contains(line, "cmd-args:") {
			contents.Write([]byte(fmt.Sprintf(" --account %s --project %s", config.Account, config.Project)))
		}
		contents.WriteRune('\n')
	}

	// Parse the config and fill the cluster info.
	kubeConfig, err := clientcmd.RESTConfigFromKubeConfig(contents.Bytes())
	if err != nil {
		return nil, fmt.Errorf("internal error: error creating kube config: %w", err)
	}
	return fillClusterInfo(ctx, cluster, region, config, kubeConfig)
}

// inClusterInfo returns information about the GKE cluster that the
// caller is running in.
// REQUIRES: The caller is running inside a GKE cluster.
func inClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	// Extract cluster information from the GKE environment.
	cluster, err := metadata.InstanceAttributeValue("cluster-name")
	if err != nil {
		return nil, err
	}
	location, err := metadata.InstanceAttributeValue("cluster-location")
	if err != nil {
		return nil, err
	}
	if isZone(location) {
		return nil, errors.New("zonal clusters not supported")
	}
	cc, err := inCloudConfig()
	if err != nil {
		return nil, err
	}
	kc, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return fillClusterInfo(ctx, cluster, location, cc, kc)
}

// isZone returns true iff the specified GCP location represents a zone.
func isZone(location string) bool {
	return strings.Count(location, "-") > 1
}

func fillClusterInfo(ctx context.Context, cluster, region string, cc CloudConfig, kc *rest.Config) (*ClusterInfo, error) {
	// Avoid Kubernetes' low default QPS limit.
	kc.QPS = math.MaxInt
	kc.Burst = math.MaxInt

	multiRegion, err := getMultiRegion(region)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create clientset for cluster %q in "+
				"region %q: %w", cluster, region, err)
	}
	extensionClientset, err := apiextension.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create extensions clientset for cluster "+
				"%q in region %q: %w", cluster, region, err)
	}
	gatewayClientset, err := gatewayv1beta1.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create Gateway clientset for cluster %q "+
				"in region %q: %w", cluster, region, err)
	}
	ingressClientset, err := backendconfigv1.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create GKE Ingress clientset for cluster %q "+
				"in region %q: %w", cluster, region, err)
	}
	verticalAutoscalingClientset, err := vautoscaling.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create VerticalPodAutoscaler clientset "+
				"for cluster %q "+
				"in region %q: %w", cluster, region, err)
	}
	dynamicClient, err := dynamic.NewForConfig(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create the Service Weaver dynamic client for "+
				"cluster %q in region %q: %w", cluster, region, err)
	}
	apiRESTClient, err := apiRESTClient(kc)
	if err != nil {
		return nil, fmt.Errorf(
			"internal error: couldn't create api REST client for cluster %q in "+
				"region %q: %w", cluster, region, err)
	}

	return &ClusterInfo{
		Name:                         cluster,
		Region:                       region,
		MultiRegion:                  multiRegion,
		CloudConfig:                  cc,
		Clientset:                    clientset,
		extensionsClientset:          extensionClientset,
		gatewayClientset:             gatewayClientset,
		ingressClientset:             ingressClientset,
		verticalAutoscalingClientset: verticalAutoscalingClientset,
		dynamicClient:                dynamicClient,
		apiRESTClient:                apiRESTClient,
	}, nil
}

// getMultiRegion returns the multi-region that corresponds to the given GCP
// region.  If the region doesn't belong to a multi-region, returns an
// empty string.
func getMultiRegion(region string) (string, error) {
	count := strings.Count(region, "-")
	firstIdx := strings.Index(region, "-")
	if count != 1 || firstIdx == -1 {
		return "", fmt.Errorf("invalid region %q", region)
	}
	mr := region[:firstIdx]
	switch mr {
	case "us", "europe", "asia":
		return mr, nil
	}
	return "", nil // empty multi-region
}

// apiRESTClient creates a client for accessing the cluster API server.
// We use the API server to proxy requests (e.g., rollout an application
// version) from outside of GKE to the services running inside the cluster:
//
//	https://kubernetes.io/docs/tasks/administer-cluster/access-cluster-services/#manually-constructing-apiserver-proxy-urls
func apiRESTClient(c *rest.Config) (*rest.RESTClient, error) {
	// Prepare the config for the rest client.
	config := *c
	config.GroupVersion = &schema.GroupVersion{
		Group:   "",
		Version: "v1",
	}
	config.APIPath = "/api"
	config.NegotiatedSerializer = serializer.NewCodecFactory(
		kruntime.NewScheme()).WithoutConversion()
	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	return rest.RESTClientFor(&config)
}
