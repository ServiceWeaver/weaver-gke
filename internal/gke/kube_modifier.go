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

	"github.com/ServiceWeaver/weaver/runtime/retry"
	"golang.org/x/exp/slog"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// kubeModifier performs a read-modify-write operation on a Kubernetes
// resource, provided that the resource exists. If it doesn't exist, it does
// nothing and returns the notFound error.
// The RMW operation is retried until either (1) it succeeds, or (2) a
// non-retriable error is returned, or (3) the passed-in context is canceled,
// or (4) the resource is deleted.
type kubeModifier struct {
	cluster *ClusterInfo // Cluster the resource is being modified ins.
	desc    string       // Description of the resource being modified.
	logger  *slog.Logger

	// Reads the current value of the resource.
	read func(context.Context) (metav1.Object, error)

	// Modifies the provided resource value, returning true iff the resource
	// has been modified.
	modify func(metav1.Object) bool

	// Writes the new resource value.
	write func(context.Context, metav1.Object) error
}

func (m kubeModifier) Run(ctx context.Context, name string) error {
	// Retry the update until the resource value has been written. (An update
	// may fail if the resource version has changed between the Get() call
	// below and the subsequent Update() call.)
	for r := retry.Begin(); r.Continue(ctx); {
		// Read.
		obj, err := m.read(ctx)
		if err != nil {
			if !isRetriableKubeError(err) {
				return err
			}
			if errors.IsNotFound(err) {
				return err
			}
			m.logger.Error(
				"Error reading "+m.desc,
				err,
				"name", name,
				"cluster", m.cluster.Name,
				"region", m.cluster.Region)
			continue
		}

		// Modify.
		modified := m.modify(obj)
		if !modified { // nothing to do
			return nil
		}
		if _, err := addUid(obj); err != nil {
			return err
		}

		// Write.
		if err := m.write(ctx, obj); err != nil {
			if !isRetriableKubeError(err) {
				return err
			}
			m.logger.Error("Error writing "+m.desc,
				err,
				"name", name,
				"cluster", m.cluster.Name,
				"region", m.cluster.Region)
			continue
		}
		return nil
	}
	return ctx.Err()
}

// modifyServiceImport performs an atomic read-modify-write operation on
// the service import resource with the given name in the given cluster. If the
// resource doesn't exist, the call does nothing and returns notFound.
//
// The provided function fn is called to modify the existing resource value.
// This function must return true iff the value has been modified. If it
// returns false, it means that the existing value should be retained and
// the operation succeeds immediately; otherwise the modified value is
// written back to the cluster. If this write fails or if the value has been
// mutated in the meantime, the entire read-modify-write cycle is performed
// again.
func modifyServiceImport(ctx context.Context, logger *slog.Logger, cluster *ClusterInfo, name string, fn func(*unstructured.Unstructured) bool) error {
	cli := cluster.dynamicClient.Resource(schema.GroupVersionResource{
		Group:    "net.gke.io",
		Version:  "v1",
		Resource: "serviceimports",
	}).Namespace(namespaceName)
	return kubeModifier{
		cluster: cluster,
		desc:    "service import",
		logger:  logger,
		read: func(ctx context.Context) (metav1.Object, error) {
			return cli.Get(ctx, name, metav1.GetOptions{})
		},
		modify: func(obj metav1.Object) bool {
			return fn(obj.(*unstructured.Unstructured))
		},
		write: func(ctx context.Context, obj metav1.Object) error {
			_, err := cli.Update(ctx, obj.(*unstructured.Unstructured), metav1.UpdateOptions{})
			return err
		},
	}.Run(ctx, name)
}
