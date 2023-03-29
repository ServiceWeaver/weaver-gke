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
	"fmt"
	"strconv"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// These errors are fake, but they behave appropriately when passed to
// functions like errors.IsConflict, errors.AlreadyExists, and
// errors.IsNotFound. See [1] for more information.
//
// [1]: https://pkg.go.dev/k8s.io/apimachinery/pkg/api/errors
var (
	conflictError = errors.NewConflict(
		schema.GroupResource{Group: "conflict", Resource: "conflict"},
		"conflict",
		fmt.Errorf("conflict"),
	)
	alreadyExistsError = errors.NewAlreadyExists(
		schema.GroupResource{Group: "alreadyExists", Resource: "alreadyExists"},
		"alreadyExists",
	)
	notFoundError = errors.NewNotFound(
		schema.GroupResource{Group: "notFound", Resource: "notFound"},
		"notFound",
	)
	resourceExpiredError = errors.NewResourceExpired(
		"resourceExpired",
	)
)

// fakeConfigMapper is a fake implementation of the ConfigMapInterface
// interface. fakeConfigMapper is a very simple fake that implements only
// enough functionality to test KubeStore. The behavior of fakeConfigMapper was
// checked against the behavior of an actual ConfigMapInterface running against
// a real Kubernetes cluster on GKE.
//
// Note that Kubernetes provides their own fakes [1], but unfortunately, these
// fakes do not handle resource versions [2]. This makes the fakes unsuitable
// for testing KubeStore, which relies heavily on resource versions.
//
// [1]: https://pkg.go.dev/k8s.io/client-go/kubernetes/fake
// [2]: https://github.com/kubernetes/client-go/issues/539
type fakeConfigMapper struct {
	// m guards all of the fields in a FakeConfigMapper.
	m sync.Mutex

	// ConfigMaps are stored in configMaps, keyed by their names. That is,
	// configMaps[name].Name == name.
	configMaps map[string]*v1.ConfigMap

	// Every time a ConfigMap is created or updated, its version is set to
	// nextVersion, and then nextVersion is incremented.
	nextVersion int

	// Every time a FakeConfigMapper's Watch method is called, the
	// FakeConfigMapper creates a new fakeWatcher to perform the watching. When
	// the fakeWatcher is created, it is assigned a unique id equal to
	// nextWatcherId. nextWatcherId is then incremented.
	nextWatcherId int

	// Every fakeWatcher watches a single ConfigMap. watchers[name] is a map of
	// all the active watchers watching the ConfigMap named `name`, keyed by
	// their unique ids. When a fakeWatcher is stopped (by calling
	// fakeWatcher.Stop), it is removed from watchers.
	watchers map[string]map[int]*fakeWatcher
}

// Check that FakeConfigMapper implements the ConfigMapInterface interface.
var _ typedv1.ConfigMapInterface = &fakeConfigMapper{}

func newFakeConfigMapper() *fakeConfigMapper {
	return &fakeConfigMapper{
		configMaps: map[string]*v1.ConfigMap{},
		// In Kubernetes, the resource version "0" is a distinguished resource
		// version that is not actually assigned to any resource. Instead, it
		// can be passed to certain operations (e.g., Get, List, Watch) as an
		// option. We start nextVersion at 1, rather than 0 to avoid assigning
		// a resource a version of 0.
		nextVersion:   1,
		nextWatcherId: 0,
		watchers:      map[string]map[int]*fakeWatcher{},
	}
}

// validateConfigMap checks that a configMap is valid. It is loosely based on
// the ValidateConfigMap function [1]. Unfortunately, we cannot import this
// function [2] (at least not easily), so we write our own. Our validation
// function doesn't validate as much as the real validation function, but for a
// fake, it's good enough.
//
// [1]: https://github.com/kubernetes/kubernetes/blob/8b5a19147530eaac9476b0ab82980b4088bbc1b2/pkg/apis/core/validation/validation.go#L5361-L5391
// [2]: https://github.com/kubernetes/kubernetes/issues/90358#issuecomment-617859364
func validateConfigMap(configMap *v1.ConfigMap) error {
	errs := []string{}
	errs = append(errs, validation.IsDNS1123Subdomain(configMap.Name)...)

	totalSize := 0
	for key, value := range configMap.Data {
		errs = append(errs, validation.IsConfigMapKey(key)...)
		totalSize += len(value)
	}
	for key, value := range configMap.BinaryData {
		errs = append(errs, validation.IsConfigMapKey(key)...)
		totalSize += len(value)
	}

	if totalSize > v1.MaxSecretSize {
		err := fmt.Sprintf("%v byte ConfigMap exceeds limit (%v bytes)", totalSize, v1.MaxSecretSize)
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "\n"))
	}
	return nil
}

// broadcast sends watch.Events to all of the watchers that are watching
// configMap. The type of watch.Event is dictated by t.
func (f *fakeConfigMapper) broadcast(t watch.EventType, configMap *v1.ConfigMap) {
	for _, watcher := range f.watchers[configMap.Name] {
		watcher.c <- watch.Event{Type: t, Object: configMap}
	}
}

// blindWrite blindly writes a ConfigMap, either as a Create or an Update.
// blindWrite also advertises the change to the watchers using the provided
// EventType.
func (f *fakeConfigMapper) blindWrite(configMap *v1.ConfigMap, eventType watch.EventType) *v1.ConfigMap {
	copied := configMap.DeepCopy()
	copied.ResourceVersion = strconv.Itoa(f.nextVersion)
	f.nextVersion += 1
	f.configMaps[copied.Name] = copied
	f.broadcast(eventType, copied)
	return copied
}

// stopWatcher removes a watcher from f and closes its channel. If the watcher
// has already been removed, stopWatcher is a noop.
func (f *fakeConfigMapper) stopWatcher(watcher *fakeWatcher) {
	watchers, ok := f.watchers[watcher.name]
	if !ok {
		return
	}

	_, ok = watchers[watcher.id]
	if !ok {
		return
	}

	delete(watchers, watcher.id)
	if len(watchers) == 0 {
		delete(f.watchers, watcher.name)
	}
	close(watcher.c)
}

func (f *fakeConfigMapper) Create(_ context.Context, configMap *v1.ConfigMap, _ metav1.CreateOptions) (*v1.ConfigMap, error) {
	f.m.Lock()
	defer f.m.Unlock()

	if err := validateConfigMap(configMap); err != nil {
		return nil, err
	}

	if _, ok := f.configMaps[configMap.Name]; ok {
		return nil, alreadyExistsError
	}

	return f.blindWrite(configMap, watch.Added), nil
}

func (f *fakeConfigMapper) Update(_ context.Context, configMap *v1.ConfigMap, _ metav1.UpdateOptions) (*v1.ConfigMap, error) {
	f.m.Lock()
	defer f.m.Unlock()

	if err := validateConfigMap(configMap); err != nil {
		return nil, err
	}

	latest, ok := f.configMaps[configMap.Name]
	if !ok {
		return nil, notFoundError
	}

	// If the resource version is unset, then we perform a blind update.
	if configMap.ResourceVersion == "" {
		return f.blindWrite(configMap, watch.Modified), nil
	}

	// Otherwise, we perform the update only if the provided version matches
	// the current version.
	if configMap.ResourceVersion != latest.ResourceVersion {
		return nil, conflictError
	}

	return f.blindWrite(configMap, watch.Modified), nil
}

func (f *fakeConfigMapper) Get(_ context.Context, name string, opts metav1.GetOptions) (*v1.ConfigMap, error) {
	f.m.Lock()
	defer f.m.Unlock()

	// As documented in [1], opts.ResourceVersion can be
	//
	//   (1) "", in which case we get the latest version;
	//   (2) "0", in which case we get any version; or
	//   (3) version v; in which case we get a version at least as new as v.
	//
	// Our fake only implements option (1).
	//
	// [1] https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
	if opts.ResourceVersion != "" {
		return nil, fmt.Errorf("only unset resource versions supported")
	}

	configMap, ok := f.configMaps[name]
	if !ok {
		return nil, notFoundError
	}
	return configMap.DeepCopy(), nil
}

func (f *fakeConfigMapper) Delete(_ context.Context, name string, _ metav1.DeleteOptions) error {
	f.m.Lock()
	defer f.m.Unlock()

	configMap, ok := f.configMaps[name]
	if !ok {
		return notFoundError
	}

	delete(f.configMaps, name)
	f.broadcast(watch.Deleted, configMap)
	return nil
}

func (f *fakeConfigMapper) Watch(_ context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	f.m.Lock()
	defer f.m.Unlock()

	// As documented in [1], opts.ResourceVersion can be empty, "0", or a
	// specific version. We only support empty and a specified version because
	// these are the ones used by KubeStore.
	//
	// [1]: https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
	if opts.ResourceVersion == "0" {
		return nil, fmt.Errorf(`watching resourceVersion="0" unsupported`)
	}

	// When opts.FieldSelector is of the form "metadata.name=<name>", the
	// watcher only watches a single resource [1]. This is the only type of
	// watching we support because it's the only type used by KubeStore.
	//
	// [1]: https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/
	if !strings.HasPrefix(opts.FieldSelector, "metadata.name=") {
		return nil, fmt.Errorf("only metadata.name=<name> watches supported")
	}
	name := strings.TrimPrefix(opts.FieldSelector, "metadata.name=")

	// Create the watcher.
	id := f.nextWatcherId
	f.nextWatcherId += 1
	watcher := fakeWatcher{
		parent: f,
		name:   name,
		id:     id,
		// The buffer size of 100 was picked without much thought. All that's
		// important is that these channels have a bit of a buffer to them.
		// Since this is a fake, the exact size isn't too important.
		c: make(chan watch.Event, 100),
	}

	// Save the watcher.
	watchers, ok := f.watchers[name]
	if !ok {
		watchers = map[int]*fakeWatcher{}
		f.watchers[name] = watchers
	}
	watchers[id] = &watcher

	// If opts.ResourceVersion is a specified version v, then we have to yield
	// _every_ change to the ConfigMap named `name` that happened after version
	// v. However, we don't record historical updates, so if the ConfigMap is
	// missing or if its current version is not v, we don't know the updates
	// that happened after v. We instead yield a resourceExpiredError. An
	// actual Kubernetes cluster may also yield such an error, though likely
	// not as often. Also note that we write the error to the watcher rather
	// than returning an error to be consistent with Kubernetes' behavior.
	if opts.ResourceVersion != "" {
		configMap, ok := f.configMaps[name]
		if !ok || opts.ResourceVersion != configMap.ResourceVersion {
			watcher.c <- watch.Event{
				Type:   watch.Error,
				Object: &resourceExpiredError.ErrStatus,
			}
			f.stopWatcher(&watcher)
		}
		return &watcher, nil
	}

	// If opts.ResourceVersion is unset, then we have to send a "synthetic
	// Added event" if the ConfigMap currently exists [1].
	//
	// [1]: https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
	if configMap, ok := f.configMaps[name]; ok {
		watcher.c <- watch.Event{Type: watch.Added, Object: configMap}
	}
	return &watcher, nil
}

func (f *fakeConfigMapper) List(_ context.Context, opts metav1.ListOptions) (*v1.ConfigMapList, error) {
	f.m.Lock()
	defer f.m.Unlock()

	// When opts.LabelSelector is "serviceweaver/store=true", we return only the
	// ConfigMaps with the label "serviceweaver/store" set to "true" [1]. This is the
	// only type of listing we support because it's the only type used by
	// KubeStore.
	//
	// [1]: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	if opts.LabelSelector != "serviceweaver/store=true" {
		return nil, fmt.Errorf(`only "store=" listing supported`)
	}

	list := v1.ConfigMapList{}
	for _, configMap := range f.configMaps {
		value, ok := configMap.Labels["serviceweaver/store"]
		if ok && value == "true" {
			list.Items = append(list.Items, *configMap)
		}
	}
	return &list, nil
}

// These functions are needed in order for FakeConfigMapper to implement the
// ConfigMapInterface interface, but we don't implement them.
func (*fakeConfigMapper) DeleteCollection(context.Context, metav1.DeleteOptions, metav1.ListOptions) error {
	return fmt.Errorf("not implemented")
}
func (*fakeConfigMapper) Patch(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*v1.ConfigMap, error) {
	return nil, fmt.Errorf("not implemented")
}
func (*fakeConfigMapper) Apply(context.Context, *applyv1.ConfigMapApplyConfiguration, metav1.ApplyOptions) (*v1.ConfigMap, error) {
	return nil, fmt.Errorf("not implemented")
}

// fakeWatcher is a fake implementation of the watch.Interface interface.
type fakeWatcher struct {
	// fakeWatchers are created by calling FakeConfigMapper.Watch. parent is
	// the FakeConfigMapper instance that created this fakeWatcher.
	parent *fakeConfigMapper

	// Every fakeWatcher watches a single ConfigMap. This is the name of the
	// ConfigMap that this fakeWatcher is watching.
	name string

	// Every fakeWatcher is assigned a unique id.
	id int

	// c is the underlying channel over which watch.Events are sent. The events
	// are sent by the methods in parent.
	c chan watch.Event
}

// Check that fakeWatcher implements the watch.Interface interface.
var _ watch.Interface = &fakeWatcher{}

func (f *fakeWatcher) Stop() {
	f.parent.m.Lock()
	defer f.parent.m.Unlock()
	f.parent.stopWatcher(f)
}

func (f *fakeWatcher) ResultChan() <-chan watch.Event {
	return f.c
}
