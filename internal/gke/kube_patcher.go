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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	vautoscalingv1 "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

const (
	// Key in the (Kubernetes) object's Labels map that stores the
	// unique identifier for the object.
	uidLabelKey = "uid"
)

// patchOptions holds options for various patch calls.
type patchOptions struct {
	// Logger to use for printing patching messages. If nil, messages are
	// printed to os.Stderr.
	logger *slog.Logger
}

// kubePatcher applies changes to Kubernetes resources (e.g., services,
// pods, deployments). It ensures that the resource is updated only when needed,
// which allows multiple concurrent patches to collectively update the resource
// to the same value, without triggering changes in response to value writes.
type kubePatcher struct {
	cluster *ClusterInfo // Cluster the resource is being patched to.
	desc    string       // Description of the resource being patched.
	opts    patchOptions // Patching options.

	// A function that returns the existing resource's value, if it exists.
	get func(context.Context) (metav1.Object, error)

	// A function that adds a new resource value.  Only called if
	// the resource doesn't already have a value.
	create func(context.Context) error

	// A function that determines if the given old resource value should be
	// updated.
	shouldUpdate func(old metav1.Object) (bool, error)

	// A function that updates the existing resource value.  Only called
	// if the resource already has a value.
	update func(context.Context) error
}

func (p kubePatcher) Run(ctx context.Context, obj metav1.Object) error {
	print := func(format string, args ...any) {
		if p.opts.logger != nil {
			p.opts.logger.Info(fmt.Sprintf(format, args...))
		} else {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}

	// Compute the unique identifier for the new resource value.
	uid, err := addUid(obj)
	if err != nil {
		return err
	}

	// Retry the update until the resource value has been written. (An update
	// may fail if the resource version has changed between the Get() call
	// below and the subsequent Update() call.)
	name := obj.GetName()
	for r := retry.Begin(); r.Continue(ctx); {
		// Get the current resource value, if any.
		oldObj, err := p.get(ctx)
		if err != nil {
			if !isRetriableKubeError(err) {
				return err
			}
			if !errors.IsNotFound(err) {
				print(
					"Error getting %s %q in cluster %s in region %s: %v. May retry\n",
					p.desc, name, p.cluster.Name, p.cluster.Region, err)
				continue
			}
			print("Creating %s %q in cluster %s in region %s... ",
				p.desc, name, p.cluster.Name, p.cluster.Region)
			if err := p.create(ctx); err != nil {
				if !isRetriableKubeError(err) {
					return err
				}
				print("Error %v. May retry\n", err)
				continue
			}
			print("Done\n")
			return nil
		}
		oldUid, ok := oldObj.GetLabels()[uidLabelKey]
		if ok && oldUid == uid { // No change
			return nil
		}
		if p.shouldUpdate != nil {
			if ok, err := p.shouldUpdate(oldObj); err != nil {
				print("Error determining if %s %q should be updated in cluster %s in region %s: %v. Bailing out\n", p.desc, name, p.cluster.Name, p.cluster.Region, err)
				return err
			} else if !ok { // don't update
				return nil
			}
			// Do the update.
		}
		obj.SetResourceVersion(oldObj.GetResourceVersion())
		print("Updating %s %q in cluster %s in region %s... ",
			p.desc, name, p.cluster.Name, p.cluster.Region)
		if err := p.update(ctx); err != nil {
			if !isRetriableKubeError(err) {
				return err
			}
			print("Error %v. May retry\n", err)
			continue
		}
		print("Done\n")
		return nil
	}
	return ctx.Err()
}

// isRetriableKubeError returns true iff the given Kubernetes API error
// is retriable.
func isRetriableKubeError(err error) bool {
	return !errors.IsForbidden(err) && !errors.IsInvalid(err) &&
		!errors.IsBadRequest(err) && !errors.IsMethodNotSupported(err) &&
		!errors.IsNotAcceptable(err) && !errors.IsRequestEntityTooLargeError(err) &&
		!errors.IsUnsupportedMediaType(err)

}

// addUid computes a short unique identifier for the given value and stores
// it in the provided object.
// REQUIRES: obj is serializable.
func addUid(obj metav1.Object) (string, error) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	if uid, ok := labels[uidLabelKey]; ok { // obj already has a uid attached
		return uid, nil
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	uid := uuid.NewSHA1(uuid.Nil, out).String()
	labels[uidLabelKey] = uid
	obj.SetLabels(labels)
	return uid, nil
}

// patchNamespace updates the kubernetes Namespace with the new configuration.
func patchNamespace(ctx context.Context, cluster *ClusterInfo, opts patchOptions, n *apiv1.Namespace) error {
	cli := cluster.Clientset.CoreV1().Namespaces()
	return kubePatcher{
		cluster: cluster,
		desc:    "namespace",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, n.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, n, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, n, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, n)
}

// patchDeployment updates the kubernetes Deployment with the new configuration.
// shouldUpdate, if non-nil, is called with the existing Deployment value to
// determine if it should be updated.
func patchDeployment(ctx context.Context, cluster *ClusterInfo, opts patchOptions, shouldUpdate func(*appsv1.Deployment) (bool, error), dep *appsv1.Deployment) error {
	cli := cluster.Clientset.AppsV1().Deployments(getNamespace(dep.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "deployment",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, dep.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, dep, metav1.CreateOptions{})
			return err
		},
		shouldUpdate: func(old metav1.Object) (bool, error) {
			if shouldUpdate == nil {
				return true, nil
			}
			return shouldUpdate(old.(*appsv1.Deployment))
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, dep, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, dep)
}

// patchService updates the kubernetes service with the new configuration.
func patchService(ctx context.Context, cluster *ClusterInfo, opts patchOptions, svc *apiv1.Service) error {
	cli := cluster.Clientset.CoreV1().Services(getNamespace(svc.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "service",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, svc.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, svc, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, svc, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, svc)
}

// patchJob updates the kubernetes Job with the new configuration.
func patchJob(ctx context.Context, cluster *ClusterInfo, opts patchOptions, job *batchv1.Job) error {
	cli := cluster.Clientset.BatchV1().Jobs(getNamespace(job.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "job",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, job.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, job, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, job, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, job)
}

// patchKubeServiceAccount updates the kubernetes service account with the new
// configuration.
func patchKubeServiceAccount(ctx context.Context, cluster *ClusterInfo, opts patchOptions, account *apiv1.ServiceAccount) error {
	cli := cluster.Clientset.CoreV1().ServiceAccounts(getNamespace(account.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "service account",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, account.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, account, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, account, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, account)
}

// patchRole updates the kubernetes role with the new configuration.
func patchRole(ctx context.Context, cluster *ClusterInfo, opts patchOptions, role *rbacv1.Role) error {
	cli := cluster.Clientset.RbacV1().Roles(getNamespace(role.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "role",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, role.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, role, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, role, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, role)
}

// patchRoleBinding updates the kubernetes role bindings with the new configuration.
func patchRoleBinding(ctx context.Context, cluster *ClusterInfo, opts patchOptions, bind *rbacv1.RoleBinding) error {
	cli := cluster.Clientset.RbacV1().RoleBindings(getNamespace(bind.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "role binding",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, bind.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, bind, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, bind, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, bind)
}

// patchGateway updates the kubernetes gateway with the new configuration.
func patchGateway(ctx context.Context, cluster *ClusterInfo, opts patchOptions, gateway *gatewayv1beta1.Gateway) error {
	cli := cluster.gatewayClientset.Gateways(getNamespace(gateway.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "gateway",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, gateway.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, gateway, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, gateway, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, gateway)
}

// patchHTTPRoute updates the kubernetes http route with the new configuration.
func patchHTTPRoute(ctx context.Context, cluster *ClusterInfo, opts patchOptions, route *gatewayv1beta1.HTTPRoute) error {
	cli := cluster.gatewayClientset.HTTPRoutes(getNamespace(route.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "http route",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, route.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, route, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, route, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, route)
}

// patchServiceExport updates the service export with the new configuration.
func patchServiceExport(ctx context.Context, cluster *ClusterInfo, opts patchOptions, seJSON string) error {
	var se unstructured.Unstructured
	if err := se.UnmarshalJSON([]byte(seJSON)); err != nil {
		return fmt.Errorf("internal error: cannot parse service export: %v", err)
	}
	se.SetAPIVersion("net.gke.io/v1")
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "net.gke.io",
		Version:  "v1",
		Resource: "serviceexports",
	}).Namespace(se.GetNamespace())
	return kubePatcher{
		cluster: cluster,
		desc:    "service export",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, se.GetName(), metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, &se, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, &se, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, &se)
}

// patchHealthCheckPolicy updates the health-check policy with the new configuration.
func patchHealthCheckPolicy(ctx context.Context, cluster *ClusterInfo, opts patchOptions, hcpJSON string) error {
	var hcp unstructured.Unstructured
	if err := hcp.UnmarshalJSON([]byte(hcpJSON)); err != nil {
		return fmt.Errorf("internal error: cannot parse health check policy: %v", err)
	}
	hcp.SetAPIVersion("networking.gke.io/v1")
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "networking.gke.io",
		Version:  "v1",
		Resource: "healthcheckpolicies",
	}).Namespace(hcp.GetNamespace())
	return kubePatcher{
		cluster: cluster,
		desc:    "health check policy",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, hcp.GetName(), metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, &hcp, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, &hcp, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, &hcp)
}

// patchVerticalPodAutoscaler updates the vertical pod autoscaler with the
// new configuration.
func patchVerticalPodAutoscaler(ctx context.Context, cluster *ClusterInfo, opts patchOptions, auto *vautoscalingv1.VerticalPodAutoscaler) error {
	cli := cluster.verticalAutoscalingClientset.AutoscalingV1().VerticalPodAutoscalers(getNamespace(auto.ObjectMeta))
	return kubePatcher{
		cluster: cluster,
		desc:    "vertical pod autoscaler",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, auto.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, auto, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, auto, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, auto)
}

// patchMultidimPodAutoscaler updates the multidimensional pod autoscaler with
// the new configuration.
func patchMultidimPodAutoscaler(ctx context.Context, cluster *ClusterInfo, opts patchOptions, autoJSON string) error {
	var auto unstructured.Unstructured
	if err := auto.UnmarshalJSON([]byte(autoJSON)); err != nil {
		return fmt.Errorf("internal error: cannot parse multidimensional pod autoscaler: %v", err)

	}
	auto.SetAPIVersion("autoscaling.gke.io/v1beta1")
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "autoscaling.gke.io",
		Version:  "v1beta1",
		Resource: "multidimpodautoscalers",
	}).Namespace(auto.GetNamespace())
	return kubePatcher{
		cluster: cluster,
		desc:    "multidimensional pod autoscaler",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, auto.GetName(), metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, &auto, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, &auto, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, &auto)
}

// patchPriorityClass updates the priority class resource with the new
// configuration.
func patchPriorityClass(ctx context.Context, cluster *ClusterInfo, opts patchOptions, p *schedulingv1.PriorityClass) error {
	cli := cluster.Clientset.SchedulingV1().PriorityClasses()
	return kubePatcher{
		cluster: cluster,
		desc:    "priority class",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, p.Name, metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, p, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, p, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, p)
}

// patchWorkloadCertificateConfig updates the workload certificate config
// resource with the new configuration.
func patchWorkloadCertificateConfig(ctx context.Context, cluster *ClusterInfo, opts patchOptions, configJSON string) error {
	var config unstructured.Unstructured
	if err := config.UnmarshalJSON([]byte(configJSON)); err != nil {
		return fmt.Errorf("internal error: cannot parse workload certificate config: %v", err)
	}
	config.SetAPIVersion("security.cloud.google.com/v1")
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "security.cloud.google.com",
		Version:  "v1",
		Resource: "workloadcertificateconfigs",
	}).Namespace(config.GetNamespace())
	return kubePatcher{
		cluster: cluster,
		desc:    "workload certificate config",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, config.GetName(), metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, &config, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, &config, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, &config)
}

// patchTrustConfig updates the trust config resource with the new configuration.
func patchTrustConfig(ctx context.Context, cluster *ClusterInfo, opts patchOptions, configJSON string) error {
	var config unstructured.Unstructured
	if err := config.UnmarshalJSON([]byte(configJSON)); err != nil {
		return fmt.Errorf("internal error: cannot parse trust config: %v", err)
	}
	config.SetAPIVersion("security.cloud.google.com/v1")
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "security.cloud.google.com",
		Version:  "v1",
		Resource: "trustconfigs",
	}).Namespace(config.GetNamespace())
	return kubePatcher{
		cluster: cluster,
		desc:    "trust config",
		opts:    opts,
		get: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, config.GetName(), metav1.GetOptions{})
		},
		create: func(ctx context.Context) error {
			_, err := cli.Create(ctx, &config, metav1.CreateOptions{})
			return err
		},
		update: func(ctx context.Context) error {
			_, err := cli.Update(ctx, &config, metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, &config)
}

func getNamespace(obj metav1.ObjectMeta) string {
	if obj.Namespace != "" {
		return obj.Namespace
	}
	return metav1.NamespaceDefault
}
