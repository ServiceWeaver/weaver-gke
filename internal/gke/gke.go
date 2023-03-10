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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"text/template"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"golang.org/x/exp/maps"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

const (
	// Namespace for all Service Weaver jobs and resources.
	namespaceName = "serviceweaver"

	// Name of the container that hosts the application binary.
	appContainerName = "serviceweaver"

	// Container name for all nanny jobs.
	nannyContainerName = "serviceweaver"

	// Name of Service Weaver application clusters.
	applicationClusterName = "serviceweaver"

	// Name of a Service Weaver configuration cluster.
	ConfigClusterName = "serviceweaver-config"

	// Region for the Service Weaver configuration cluster.
	ConfigClusterRegion = "us-central1"

	// Name of the backend config used for configuring application listener
	// backends.
	backendConfigName = "serviceweaver"

	// Key in a Kubernetes resource's label map that corresponds to the
	// application name that the resource is associated with. Used when looking
	// up resources that belong to a particular application.
	appNameKey = "serviceweaver/app_name"

	// Key in Kubernetes resource's label map that corresponds to the
	// application's deployment version. Used when looking up resources that
	// belong to a particular application version.
	deploymentIDKey = "serviceweaver/deployment_id"

	// Key in external Service's annotations map that correspond to the listener
	// information for that service.
	serviceListenerKey = "serviceweaver/listener"

	// Name for the Service Weaver created external Gateways.
	externalGatewayName = "serviceweaver-external"

	// Name for the Service Weaver created internal Gateways.
	internalGatewayName = "serviceweaver-internal"

	// Name of the gateway static ip address. Same name is used for the
	// global external gateway and all regional internal gateways.
	gatewayIPAddressName = "serviceweaver"

	// Names of priority classes used for various Service Weaver pods.
	controlPriorityClassName       = "serviceweaver-control"
	applicationPriorityClassName   = "serviceweaver-application"
	spareCapacityPriorityClassName = "serviceweaver-spare-capacity"

	// Key in HTTPRoute's label map that corresponds to the gateway the route
	// is associated with.
	routeGatewayKey = "gateway"

	// Key in the Gateway's TLS Options map that stores the list of certificates
	// associated with the Gateway.
	gatewayCertsKey = gatewayv1beta1.AnnotationKey("networking.gke.io/pre-shared-certs")

	// Multiplier for the traffic fraction values (in the range [0, 1])
	// used for their conversion into HTTPRoute weights (in the range [0, 1000]).
	routeFractionMultiplier = 1000

	// Key in Service's annotation map that stores the name of the BackendConfig
	// associated with the service.
	backendConfigServiceAnnotationKey = "cloud.google.com/backend-config"

	// Prefix for the temporary spare-capacity jobs.
	tempSpareCapacityJobPrefix = "temp-spare-capacity"

	// configEnvKey is the environment variable under which a config.GKEConfig is
	// stored. For internal use by Service Weaver infrastructure.
	configEnvKey = "SERVICEWEAVER_INTERNAL_CONFIG"

	// colocationGroupEnvKey is the environment variable under which a
	// ColocationGroup is stored. For internal use by Service Weaver infrastructure.
	colocationGroupEnvKey = "SERVICEWEAVER_INTERNAL_COLOCATION_GROUP"

	// containerMetadataEnvKey is the environment variable under which a
	// partial ContainerMetadata is stored. The node_name and pod_name fields
	// should be read from NodeNameEnvKey and PodNameEnvKey. For internal use
	// by Service Weaver infrastructure.
	containerMetadataEnvKey = "SERVICEWEAVER_INTERNAL_CONTAINER_METADATA"

	// nodeNameEnvKey is the environment variable under which a node's name is
	// stored. For internal use by Service Weaver infrastructure.
	nodeNameEnvKey = "SERVICEWEAVER_INTERNAL_NODE_NAME"

	// podNameEnvKey is the environment variable under which a pod's name is
	// stored. For internal use by Service Weaver infrastructure.
	podNameEnvKey = "SERVICEWEAVER_INTERNAL_POD_NAME"
)

var (
	// Resource allocation units for "cpu" and "memory" resources.
	// We also happen to reserve exactly these many resources when starting each
	// application pod.
	// TODO(spetrovic): Should we allow the user to customize how many
	// resources each pod starts with?
	cpuUnit    = resource.MustParse("100m")
	memoryUnit = resource.MustParse("128Mi")

	// These environment variables are populated using the Kubernetes Downward
	// API [1].
	//
	// [1]: https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/#the-downward-api
	nodeNameEnvVar = v1.EnvVar{
		Name: nodeNameEnvKey,
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "spec.nodeName",
			},
		},
	}
	podNameEnvVar = v1.EnvVar{
		Name: podNameEnvKey,
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	}
)

func multiplyQuantity(mult int64, x resource.Quantity) resource.Quantity {
	x.Set(x.Value() * mult)
	return x
}

// podExists returns true iff a Pod with the given name exists in the Service Weaver
// namespace. It returns an error if the Pod information cannot be retrieved.
func podExists(ctx context.Context, cluster *ClusterInfo, name string) (bool, error) {
	cli := cluster.Clientset.CoreV1().Pods(namespaceName)
	_, err := cli.Get(ctx, name, metav1.GetOptions{})
	if err == nil { // confirmed exists
		return true, nil
	}
	if errors.IsNotFound(err) { // confirmed doesn't exist
		return false, nil
	}
	// Not sure.
	return true, err
}

// deploy deploys the Service Weaver colocation group in the given cluster.
func deploy(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, cfg *config.GKEConfig, group *protos.ColocationGroup) error {
	if err := ensureColocGroupDeployment(ctx, cluster, logger, cfg, group); err != nil {
		return err
	}

	if group.Name == "main" {
		// Allocate a temporary spare CPU capacity so that the application
		// version starts faster.
		//
		// NOTE(spetrovic): These spare-capacity jobs will remain on the
		// status pages (with zero replicas) until the application version is
		// cleaned up. If this becomes annoying, we could clean up the
		// spare-capacity jobs sooner, e.g., in the distributor's anneal loop,
		// though it is unclear if this is worth the complexity. Another option
		// is to start a CronJob that periodically checks and cleans up expired
		// spare-capacity jobs.
		spareCpu := multiplyQuantity(6, cpuUnit)
		spareDuration := 5 * time.Minute
		if err := ensureTemporarySpareCapacity(ctx, cluster, logger, cfg, spareCpu, spareDuration); err != nil {
			// Swallow the error as it isn't catastrophic.
			logger.Error("Cannot allocate temporary spare capacity", err)
		}
	}

	// Setup an autoscaler for the colocation group.
	return ensureColocGroupAutoscaler(ctx, cluster, logger, cfg, group)
}

// stop stops any resources (e.g., Deployments, Jobs) that belong to the
// provided application version, by setting the resource values to their
// minimum allowed value (e.g., zero replicas for a Deployment/Job).
func stop(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, app, version string) error {
	// NOTE(mwhittaker): There is a race between starting and stopping jobs for
	// an application version. It is possible that we stop all running jobs and
	// then we launch a new job. This is not ideal but it doesn't break
	// anything. Later, when we delete the version, we will kill these phantom
	// jobs for good.
	//
	// TODO(mwhittaker): We could also add a special label or change the
	// container to a dummy container. If we ever decide to scale jobs down to
	// 0, this will be useful to distinguish stopped jobs from scaled down
	// jobs.
	//
	// TODO(mwhittaker): When we start a deployment or stateful set, we should
	// abort the update if the number of replicas is zero. Right now, the
	// update can still succeed. As mentioned in the note above, this is not
	// catastrophic but isn't ideal.
	selector := labels.SelectorFromSet(labels.Set{deploymentIDKey: version})
	listOpts := metav1.ListOptions{LabelSelector: selector.String()}
	numReplicas := int32(0)

	// Delete the Services. This will cut off any traffic into the application
	// and prevent future auto-scaling.
	//
	// TODO(mwhittaker): Think about the fault tolerance and possibility for
	// races with listeners.
	listeners, err := getListeners(ctx, cluster.Clientset, app, version)
	if err != nil {
		return fmt.Errorf("get listeners for %q: %w", version, err)
	}
	for _, lis := range listeners {
		if err := deleteListenerService(ctx, cluster, app, version, lis); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("delete %v for %q: %w", lis, version, err)
		}
	}

	// Stop any deployments.
	deploymentsClient := cluster.Clientset.AppsV1().Deployments(namespaceName)
	deployments, err := deploymentsClient.List(ctx, listOpts)
	if err != nil {
		return err
	}
	for _, deployment := range deployments.Items {
		deployment.Spec.Replicas = &numReplicas
		if err := patchDeployment(ctx, cluster, patchOptions{logger: logger}, &deployment); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	// Stop any jobs.
	jobsClient := cluster.Clientset.BatchV1().Jobs(namespaceName)
	jobs, err := jobsClient.List(ctx, listOpts)
	if err != nil {
		return err
	}
	for _, job := range jobs.Items {
		job.Spec.Parallelism = &numReplicas
		if err := patchJob(ctx, cluster, patchOptions{logger: logger}, &job); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

type collectionDeleter interface {
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
}

// kill kills any resources (e.g., Deployments, Jobs) that belong to the
// provided application version.
func kill(ctx context.Context, cluster *ClusterInfo, app, version string) error {
	for _, deleter := range []collectionDeleter{
		cluster.dynamicClient.Resource(schema.GroupVersionResource{
			Group:    "autoscaling.gke.io",
			Version:  "v1beta1",
			Resource: "multidimpodautoscalers",
		}).Namespace(namespaceName),
		cluster.Clientset.AppsV1().Deployments(namespaceName),
	} {
		if err := killHelper(ctx, cluster, version, deleter); err != nil {
			return err
		}
	}

	// NOTE: Jobs have to be killed using the Delete method, as otherwise
	// the jobs entries stick around indefinitely in the SUCCESS state, which
	// is undesirable.
	name := name{tempSpareCapacityJobPrefix, app, version[:8]}.DNSLabel()
	if err := cluster.Clientset.BatchV1().Jobs(namespaceName).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func killHelper(ctx context.Context, cluster *ClusterInfo, version string, deleter collectionDeleter) error {
	opts := metav1.DeleteOptions{}
	selector := labels.SelectorFromSet(labels.Set{deploymentIDKey: version})
	listOpts := metav1.ListOptions{LabelSelector: selector.String()}
	return deleter.DeleteCollection(ctx, opts, listOpts)
}

// Store returns the Service Weaver store for the given cluster.
func Store(cluster *ClusterInfo) store.Store {
	return newKubeStore(cluster.Clientset.CoreV1().ConfigMaps(namespaceName))
}

func ensureColocGroupDeployment(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, cfg *config.GKEConfig, group *protos.ColocationGroup) error {
	dep := cfg.Deployment
	name := name{dep.App.Name, group.Name, dep.Id[:8]}.DNSLabel()
	container, err := colocGroupContainer(name, cluster, cfg, group)
	if err != nil {
		return err
	}
	return patchDeployment(ctx, cluster, patchOptions{logger: logger}, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
			Labels: map[string]string{
				appNameKey:      dep.App.Name,
				deploymentIDKey: dep.Id,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrOf(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": name,
					},
				},
				Spec: v1.PodSpec{
					PriorityClassName:  applicationPriorityClassName,
					Containers:         []v1.Container{container},
					ServiceAccountName: applicationKubeServiceAccount,
				},
			},
		},
	})
}

var autoJSONTmpl = template.Must(template.New("auto").Parse(`{
"kind":"MultidimPodAutoscaler",
"metadata":{
	"labels":{
		"serviceweaver/app_name":"{{.AppName}}",
		"serviceweaver/deployment_id":"{{.DeploymentID}}"
	},
	"name":"{{.Name}}",
	"namespace":"{{.Namespace}}"
},
"spec":{
	"constraints":{
		"container":[{
			"name":"{{.AppContainerName}}",
			"requests":{"minAllowed":{"memory":"{{.MinMemory}}"}}
		}],
		"containerControlledResources":["memory"],
		"global":{"maxReplicas":999999,"minReplicas":1}
	},
	"goals":{
		"metrics":[{
			"resource":{
				"name":"cpu",
				"target":{
					"averageUtilization":80,
					"type":"Utilization"
				}
			},
			"type":"Resource"
		}]
	},
	"policy":{"updateMode":"Auto"},
	"scaleTargetRef":{
		"apiVersion":"apps/v1",
		"kind":"{{.TargetKind}}",
		"name":"{{.TargetName}}"
	}
}}`))

// ensureColocGroupAutoscaler ensures that a multidimensional pod autoscaler is
// configured for the given colocation group.
func ensureColocGroupAutoscaler(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, cfg *config.GKEConfig, group *protos.ColocationGroup) error {
	dep := cfg.Deployment
	name := name{dep.App.Name, group.Name, dep.Id[:8]}.DNSLabel()
	var b strings.Builder
	if err := autoJSONTmpl.Execute(&b, struct {
		Name             string
		Namespace        string
		AppName          string
		DeploymentID     string
		AppContainerName string
		MinMemory        string
		TargetKind       string
		TargetName       string
	}{
		Name:             name,
		Namespace:        namespaceName,
		AppName:          dep.App.Name,
		DeploymentID:     dep.Id,
		AppContainerName: appContainerName,
		MinMemory:        memoryUnit.String(),
		TargetKind:       "Deployment",
		TargetName:       name,
	}); err != nil {
		return err
	}
	return patchMultidimPodAutoscaler(ctx, cluster, patchOptions{logger: logger}, b.String())
}

func colocGroupContainer(app string, cluster *ClusterInfo, cfg *config.GKEConfig, group *protos.ColocationGroup) (v1.Container, error) {
	cfgStr, err := proto.ToEnv(cfg)
	if err != nil {
		return v1.Container{}, err
	}
	groupStr, err := proto.ToEnv(group)
	if err != nil {
		return v1.Container{}, err
	}

	meta := ContainerMetadata{
		Project:       cluster.CloudConfig.Project,
		ClusterName:   cluster.Name,
		ClusterRegion: cluster.Region,
		Namespace:     namespaceName,
		ContainerName: appContainerName,
		App:           app,
	}
	metaStr, err := proto.ToEnv(&meta)
	if err != nil {
		return v1.Container{}, err
	}

	env := []v1.EnvVar{
		// These environment variables are read by a Service Weaver binary.
		{Name: configEnvKey, Value: cfgStr},
		{Name: colocationGroupEnvKey, Value: groupStr},

		// These environment variables are read by the babysitter binary.
		{Name: containerMetadataEnvKey, Value: metaStr},

		// These environment variables are read by the babysitter binary. They
		// are populated using the Kubernetes Downward API.
		nodeNameEnvVar,
		podNameEnvVar,
	}

	// NOTE: we don't enumerate all of the ports since some of them are
	// opened dynamically.  This is okay as the container port spec only
	// serves an informational purpose (i.e., the binary may open ports that
	// aren't declared in this port specification, and those ports remain
	// perfectly accessible).
	return v1.Container{
		Name:  appContainerName,
		Image: cfg.Container,
		Args:  []string{"/weaver/weaver-gke babysitter"},
		Env:   env,
		Resources: v1.ResourceRequirements{
			// NOTE: start with smallest allowed limits, and count on
			// autoscalers doing the rest.
			Requests: v1.ResourceList{
				"memory": memoryUnit,
				"cpu":    cpuUnit,
			},
			// NOTE: we don't specify any limits, allowing all available node
			// resources to be used, if needed. Note that in practice, we
			// attach autoscalers to all of our containers, so the extra-usage
			// should be only for a short period of time.
		},
		// Enabling TTY and Stdin allows the user to run a shell inside the
		// container, for debugging.
		TTY:   true,
		Stdin: true,
	}, nil
}

var serviceExportTmpl = template.Must(template.New("auto").Parse(`{
"metadata":{
	"name":"{{.Name}}",
	"namespace":"{{.Namespace}}",
	"annotations":{
		"{{.BackendConfigAnnotationKey}}":"{\"default\": \"{{.BackendConfigName}}\"}"
	}
},
"kind":"ServiceExport"
}`))

// ensureListenerService ensures that a service that exposes the given network
// listener is running in the given cluster.
func ensureListenerService(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, cfg *config.GKEConfig, group *protos.ColocationGroup, lis *protos.Listener, targetPort int) error {
	dep := cfg.Deployment
	lisEnc, err := jsonEncode(lis)
	if err != nil {
		return fmt.Errorf("internal error: error encoding listener %+v: %w", lis, err)
	}
	svcName := name{cluster.Region, dep.App.Name, lis.Name, dep.Id[:8]}.DNSLabel()
	targetName := name{dep.App.Name, group.Name, dep.Id[:8]}.DNSSubdomain()
	if err := patchService(ctx, cluster, patchOptions{logger: logger}, &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: namespaceName,
			Annotations: map[string]string{
				serviceListenerKey:                lisEnc,
				backendConfigServiceAnnotationKey: fmt.Sprintf(`{"default": "%s"}`, backendConfigName),
			},
			Labels: map[string]string{
				appNameKey:      dep.App.Name,
				deploymentIDKey: dep.Id,
			},
		},
		Spec: v1.ServiceSpec{
			Type: "ClusterIP",
			Selector: map[string]string{
				"app": targetName,
			},
			Ports: []v1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.FromInt(targetPort),
					Protocol:   "TCP",
				},
			},
		},
	}); err != nil {
		return nil
	}

	// Create service export.
	// NOTE: the BackendConfig attached to this resource isn't currently
	// propagated to the corresponding ServiceImport. As a temporary
	// workaround, we also attach the backend config annotation to
	// the ServiceImport in the config cluster, as soon as it is auto-created.
	var b strings.Builder
	if err := serviceExportTmpl.Execute(&b, struct {
		Name                       string
		Namespace                  string
		BackendConfigAnnotationKey string
		BackendConfigName          string
	}{
		Name:                       svcName,
		Namespace:                  namespaceName,
		BackendConfigAnnotationKey: backendConfigServiceAnnotationKey,
		BackendConfigName:          backendConfigName,
	}); err != nil {
		return err
	}
	return patchServiceExport(
		ctx, cluster, patchOptions{logger: logger}, b.String())
}

// deleteListenerService deletes the service that exposes the given network
// listener.
func deleteListenerService(ctx context.Context, cluster *ClusterInfo, app, version string, lis *protos.Listener) error {
	name := name{cluster.Region, app, lis.Name, version[:8]}.DNSLabel()
	if err := cluster.Clientset.CoreV1().Services(namespaceName).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	if err := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "net.gke.io",
		Version:  "v1",
		Resource: "serviceexports",
	}).Namespace(namespaceName).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil
}

// getListeners returns the list of all network listeners created by the
// given application deployment.
func getListeners(ctx context.Context, clientset *kubernetes.Clientset, app, version string) ([]*protos.Listener, error) {
	cli := clientset.CoreV1().Services(namespaceName)
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			appNameKey:      app,
			deploymentIDKey: version,
		}).String(),
	}
	svcs, err := cli.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	listeners := make([]*protos.Listener, 0, len(svcs.Items))
	for _, svc := range svcs.Items {
		enc, ok := svc.ObjectMeta.Annotations[serviceListenerKey]
		if !ok {
			continue
		}
		var listener *protos.Listener
		if err := jsonDecode(enc, &listener); err != nil {
			return nil, fmt.Errorf(
				"internal error: error decoding listener for service %q: %w",
				svc.Name, err)
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

// updateGlobalExternalTrafficRoutes updates the traffic routing rules for the
// global external gateway in the given (config) cluster, using the provided
// traffic assignments.
func updateGlobalExternalTrafficRoutes(ctx context.Context, logger *logging.FuncLogger, cluster *ClusterInfo, assignment *nanny.TrafficAssignment) error {
	if err := sanitizeGlobalTrafficRoutes(ctx, cluster, logger, assignment); err != nil {
		return err
	}

	if err := updateGlobalExternalGateway(ctx, cluster, logger, assignment); err != nil {
		return err
	}

	return updateTrafficRoutes(ctx, cluster, logger, trafficUpdateSpec{
		isGlobal:   true,
		assignment: assignment,
	})
}

// sanitizeGlobalTrafficRoutes ensures that all traffic routes in the given
// assignment are ready to receive traffic, removing them from the assignment
// if they are not.
func sanitizeGlobalTrafficRoutes(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, assignment *nanny.TrafficAssignment) error {
	// For each route that appears in the traffic assignments, perform the
	// following tasks:
	//   (1) Check if the corresponding ServiceImport resource has been
	//       auto-created in the config cluster. If not, the listener is
	//       removed from the traffic assignments. (This is okay because the
	//       external load-balancer anyway isn't able to forward traffic to the
	//       listener until the ServiceImport resource is created.)
	//   (2) Ensure that the Service Weaver BackendConfig is attached to the
	//       corresponding ServiceImport resource. This config contains
	//       some Service Weaver related backend customizations (e.g., perform
	//       health checks on the /healtz path).
	// Note that (1) can be performed without updating the traffic fractions,
	// since GKE traffic weights don't need to add up to a fixed value.
	backendConfigSpec := fmt.Sprintf(`{"default": "%s"}`, backendConfigName)

	// ensureAlloc returns true iff the traffic allocation is ready to
	// receive traffic, or an error if the allocation is invalid.
	// If the traffic allocation is ready, we also ensure that a Service Weaver backend
	// configuration is attached to it.
	ensureAlloc := func(alloc *nanny.TrafficAllocation) (bool, error) {
		if alloc.Location == "" {
			return false, fmt.Errorf("no location specified for traffic allocation %+v", alloc)
		}
		if len(alloc.VersionId) < 8 {
			return false, fmt.Errorf("invalid version id %q in allocation %+v", alloc.VersionId, alloc)
		}
		svcName := name{alloc.Location, alloc.AppName, alloc.Listener.Name, alloc.VersionId[:8]}.DNSLabel()

		if err := modifyServiceImport(ctx, logger, cluster, svcName, func(u *unstructured.Unstructured) bool {
			annotations := u.GetAnnotations()
			if annotations[backendConfigServiceAnnotationKey] == backendConfigSpec {
				return false // modified
			}
			annotations[backendConfigServiceAnnotationKey] = backendConfigSpec
			u.SetAnnotations(annotations)
			return true // modified
		}); err != nil {
			if errors.IsNotFound(err) { // ServiceImport doesn't exist (yet)
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	for host, hostAssignment := range assignment.HostAssignment {
		newAllocs := make([]*nanny.TrafficAllocation, 0, len(hostAssignment.Allocs))
		for _, alloc := range hostAssignment.Allocs {
			ready, err := ensureAlloc(alloc)
			if err != nil {
				return err
			}
			if !ready {
				logger.Debug("Removing traffic allocation because the backend is not ready",
					"allocation", alloc,
					"hostname", host,
					"region", cluster.Region)
				continue
			}
			newAllocs = append(newAllocs, alloc)
		}
		if len(newAllocs) == 0 {
			delete(assignment.HostAssignment, host)
			continue
		}
		hostAssignment.Allocs = newAllocs
	}
	return nil
}

// UpdateRegionalInternalTrafficRoutes updates the traffic routing rules for the
// internal regional gateway in the given cluster, using the provided traffic
// assignments.
func updateRegionalInternalTrafficRoutes(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, assignment *nanny.TrafficAssignment) error {
	return updateTrafficRoutes(ctx, cluster, logger, trafficUpdateSpec{
		isGlobal:   false,
		assignment: assignment,
	})
}

type trafficUpdateSpec struct {
	isGlobal   bool
	assignment *nanny.TrafficAssignment
}

// updateTrafficRoutes updates the traffic routing rules for the gateway,
// using the provided traffic assignments.
func updateTrafficRoutes(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, spec trafficUpdateSpec) error {
	gatewayName := internalGatewayName
	if spec.isGlobal {
		gatewayName = externalGatewayName
	}
	// We maintain a separate HTTPRoute for each unique hostname.
	// The route update process works as follows:
	//   * We collect the set O of all existing routes for the gateway.
	//   * We iterate over all unique hostnames in the traffic assignment and
	//     create a new updated set of routes N for the gateway.
	//   * We apply all of the routes in set N.
	//   * We delete all of the routes in subset O/N.

	// Get a list of existing routes.
	cli := cluster.gatewayClientset.HTTPRoutes(namespaceName)
	oldRoutes, err := cli.List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			routeGatewayKey: gatewayName,
		}).String(),
	})
	if err != nil {
		return fmt.Errorf("error getting HTTP routes: %w", err)
	}

	// Update routes that appear in new assignments.
	newRoutes := map[string]string{} // routeName -> hostname
	rootPath := "/"
	for host, hostAssignment := range spec.assignment.HostAssignment {
		routeName := name{host, gatewayName}.DNSSubdomain()
		newRoutes[routeName] = host
		backends, err := computeTrafficBackends(spec.isGlobal, cluster, hostAssignment.Allocs)
		if err != nil {
			return err
		}
		rules := []gatewayv1beta1.HTTPRouteRule{
			{
				Matches: []gatewayv1beta1.HTTPRouteMatch{
					{
						Path: &gatewayv1beta1.HTTPPathMatch{
							Value: &rootPath,
						},
					},
				},
				BackendRefs: backends,
			},
		}
		if spec.isGlobal {
			// TODO(spetrovic): Figure out the easiest way to return 404s
			// for the /healthz URL path, and then uncomment the code below.
			// healthzPath := "/healthz"
			// rules = append(rules, gatewayv1beta1.HTTPRouteRule{
			// 	// Make /healthz inaccessible for external traffic.
			// 	Matches: []gatewayv1beta1.HTTPRouteMatch{
			// 		{
			// 			Path: &gatewayv1beta1.HTTPPathMatch{
			// 				Value: &healthzPath,
			// 			},
			// 		},
			// 	},
			// })
		}
		if err := patchHTTPRoute(ctx, cluster, patchOptions{logger: logger}, &gatewayv1beta1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      routeName,
				Namespace: namespaceName,
				Labels: map[string]string{
					routeGatewayKey: gatewayName,
				},
			},
			Spec: gatewayv1beta1.HTTPRouteSpec{
				Hostnames: []gatewayv1beta1.Hostname{
					gatewayv1beta1.Hostname(host),
				},
				CommonRouteSpec: gatewayv1beta1.CommonRouteSpec{
					ParentRefs: []gatewayv1beta1.ParentReference{
						{
							Group:     ptrOf(gatewayv1beta1.Group("gateway.networking.k8s.io")),
							Kind:      ptrOf(gatewayv1beta1.Kind("Gateway")),
							Namespace: ptrOf(gatewayv1beta1.Namespace(namespaceName)),
							Name:      gatewayv1beta1.ObjectName(gatewayName),
						},
					},
				},
				Rules: rules,
			},
		}); err != nil {
			return fmt.Errorf("error updating route %s: %w", routeName, err)
		}
	}

	// Delete the old routes that aren't in the new set.
	for _, route := range oldRoutes.Items {
		routeName := route.ObjectMeta.Name
		if _, ok := newRoutes[routeName]; ok {
			continue
		}
		// Delete the old route.
		if err := cli.Delete(ctx, routeName, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("error deleting obsolete route %s: %w", routeName, err)
		}
	}

	return nil
}

// updateGlobalExternalGateway updates the global external gateway in the
// given (config) cluster, setting up the certificates corresponding to the
// hostnames in the given traffic assignment, if necessary.
func updateGlobalExternalGateway(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, assignment *nanny.TrafficAssignment) error {
	hosts := maps.Keys(assignment.HostAssignment)
	if len(hosts) == 0 {
		// Append a noop host; this host will never receive any traffic but
		// ensures that the gateway is configured with at least one certificate
		// (this is a gateway requirement).
		const noopHost = "noop.weaver.google.com"
		hosts = append(hosts, noopHost)
	}

	// Create an SSL certificate for each unique host.
	var certs []string
	for _, host := range hosts {
		cert, err := ensureSSLCertificate(ctx, cluster, logger, host)
		if err != nil {
			return err
		}
		certs = append(certs, cert)
	}

	allowedRoutes := &gatewayv1beta1.AllowedRoutes{
		Kinds: []gatewayv1beta1.RouteGroupKind{{Kind: "HTTPRoute"}},
	}
	return patchGateway(ctx, cluster, patchOptions{logger: logger}, &gatewayv1beta1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      externalGatewayName,
			Namespace: namespaceName,
		},
		Spec: gatewayv1beta1.GatewaySpec{
			GatewayClassName: "gke-l7-global-external-managed-mc",
			Addresses: []gatewayv1beta1.GatewayAddress{
				{
					Type:  ptrOf(gatewayv1beta1.NamedAddressType),
					Value: gatewayIPAddressName,
				},
			},
			Listeners: []gatewayv1beta1.Listener{
				{
					Name:          "http",
					Protocol:      gatewayv1beta1.HTTPProtocolType,
					Port:          gatewayv1beta1.PortNumber(80),
					AllowedRoutes: allowedRoutes,
				},
				{
					Name:          "https",
					Protocol:      gatewayv1beta1.HTTPSProtocolType,
					Port:          gatewayv1beta1.PortNumber(443),
					AllowedRoutes: allowedRoutes,
					TLS: &gatewayv1beta1.GatewayTLSConfig{
						Mode: ptrOf(gatewayv1beta1.TLSModeTerminate),
						Options: map[gatewayv1beta1.AnnotationKey]gatewayv1beta1.AnnotationValue{
							gatewayCertsKey: gatewayv1beta1.AnnotationValue(strings.Join(certs, ",")),
						},
					},
				},
			},
		},
	})
}

// computeTrafficBackends computes the list of traffic backends that correspond
// to the given list of traffic allocations.
func computeTrafficBackends(isGlobal bool, cluster *ClusterInfo, allocations []*nanny.TrafficAllocation) ([]gatewayv1beta1.HTTPBackendRef, error) {
	group := gatewayv1beta1.Group("")
	kind := gatewayv1beta1.Kind("Service")
	if isGlobal {
		group = "net.gke.io"
		kind = "ServiceImport"
	}
	backends := make([]gatewayv1beta1.HTTPBackendRef, len(allocations))
	for i, a := range allocations {
		loc := a.Location
		if loc == "" {
			if isGlobal {
				return nil, fmt.Errorf("no region specified for global traffic allocation %+v", a)
			} else {
				// Use the containing cluster region.
				loc = cluster.Region
			}
		}
		svcName := name{loc, a.AppName, a.Listener.Name, a.VersionId[:8]}.DNSLabel()
		weight := int32(a.TrafficFraction * routeFractionMultiplier)
		backend := gatewayv1beta1.HTTPBackendRef{
			BackendRef: gatewayv1beta1.BackendRef{
				BackendObjectReference: gatewayv1beta1.BackendObjectReference{
					Group: &group,
					Kind:  &kind,
					Name:  gatewayv1beta1.ObjectName(svcName),
					Port:  ptrOf(gatewayv1beta1.PortNumber(80)),
				},
				Weight: &weight,
			},
		}
		backends[i] = backend
	}
	return backends, nil
}

// ensureSSLCertificate ensures that a Google-managed SSL certificate for the
// given application hostname has been created.
func ensureSSLCertificate(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, host string) (string, error) {
	certName := name{"serviceweaver", host}.DNSLabel()
	if err := patchSSLCertificate(ctx, cluster.CloudConfig, patchOptions{logger: logger}, &computepb.SslCertificate{
		Name: &certName,
		Description: ptrOf(fmt.Sprintf(
			"Managed certificate for the Service Weaver exported hostname %q", host)),
		Type: ptrOf(computepb.SslCertificate_MANAGED.String()),
		Managed: &computepb.SslCertificateManagedSslCertificate{
			Domains: []string{host},
		},
	}); err != nil {
		return "", err
	}
	return certName, nil
}

// ensureTemporarySpareCapacity ensures that a given temporary spare CPU
// capacity is available to the given application version in the given cluster
// for the given duration [1].
// [1]: https://wdenniss.com/autopilot-capacity-reservation
func ensureTemporarySpareCapacity(ctx context.Context, cluster *ClusterInfo, logger *logging.FuncLogger, cfg *config.GKEConfig, cpu resource.Quantity, duration time.Duration) error {
	dep := cfg.Deployment
	name := name{tempSpareCapacityJobPrefix, dep.App.Name, dep.Id[:8]}.DNSLabel()

	// Split requested cpu resources into cpuUnit chunks, each chunk
	// corresponding to a job replica.
	numReplicas := (cpu.Value() + cpuUnit.Value() - 1) / cpuUnit.Value()
	if numReplicas > math.MaxInt32 {
		return fmt.Errorf("too much cpu requested")
	}
	resList := v1.ResourceList{
		"memory": memoryUnit,
		"cpu":    cpuUnit,
	}
	sleepDuration := fmt.Sprintf("%d", duration/time.Second)
	return patchJob(ctx, cluster, patchOptions{logger: logger}, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespaceName,
			Labels: map[string]string{
				appNameKey:      dep.App.Name,
				deploymentIDKey: dep.Id,
			},
		},
		Spec: batchv1.JobSpec{
			Parallelism:             ptrOf(int32(numReplicas)),
			BackoffLimit:            ptrOf(int32(0)), // don't retry
			TTLSecondsAfterFinished: ptrOf(int32(1)),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					PriorityClassName:             spareCapacityPriorityClassName,
					TerminationGracePeriodSeconds: ptrOf(int64(0)),
					RestartPolicy:                 v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:    "alpine",
							Image:   "alpine",
							Command: []string{"sleep"},
							Args:    []string{sleepDuration},
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

func jsonEncode(v interface{}) (string, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func jsonDecode(enc string, valPtr interface{}) error {
	return json.NewDecoder(bytes.NewReader([]byte(enc))).Decode(valPtr)
}

func ptrOf[T any](val T) *T { return &val }
