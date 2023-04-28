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
	"crypto/sha256"
	"fmt"

	"github.com/ServiceWeaver/weaver-gke/internal/store"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// KubeStore is a Kubernetes ConfigMap backed implementation of a Store.
//
// # Overview
//
// Every key-value pair is stored in its own ConfigMap. Kubernetes requires
// that ConfigMap names are valid DNS subdomain names [1], so we cannot name a
// ConfigMap with the key it stores. Instead, the SHA-265 hash of a key serves
// as the name of the ConfigMap that stores it (with a "store-" prefix). For
// example, the ConfigMap that maps key "a key" to value "a value" looks
// something like this:
//
//	ConfigMap{
//	    ObjectMeta: metav1.ObjectMeta{
//	        Name: "store-" + SHA-256("a key")
//	    },
//	    BinaryData: map[string][]byte{
//	        "key":   []byte("a key"),
//	        "value": []byte("a value"),
//	    },
//	}
//
// Note that the key and value are stored in the BinaryData section of the
// ConfigMap. In theory, we could only store the value and not store the key,
// but practically we choose to store the key along with the value to make
// ConfigMaps more interpretable.
//
// # Versions
//
// KubeStore implements `store.Version`s using ConfigMap resource versions. We
// omitted the ResourceVersion field in the ConfigMap above to keep things
// simple, but in reality, Kubernetes assigns the ConfigMap a resource version.
// That looks something like this:
//
//	ConfigMap{
//	    ObjectMeta: metav1.ObjectMeta{
//	        Name: "store-" + SHA-256("a key")
//	        ResourceVersion: "111111111",
//	    },
//	    BinaryData: map[string][]byte{
//	        "key":   []byte("a key"),
//	        "value": []byte("a value"),
//	    },
//	}
//
// Here, the ConfigMap's resource version is "111111111". If we call
// KubeStore.Get(ctx, "a key", nil), we get a store.Version{Opaque:
// "111111111"}.
//
// # Listing
//
// A Kubernetes cluster will have many ConfigMaps besides those managed by a
// KubeStore. In order to differentiate a KubeStore managed ConfigMap from some
// other ConfigMap, the KubeStore also labels every key-value pair with the
// label "serviceweaver/store=true". So, the ConfigMap above looks something
// like this:
//
//	ConfigMap{
//	    ObjectMeta: metav1.ObjectMeta{
//	        Name: "store-" + SHA-256("a key")
//	        ResourceVersion: "111111111",
//	        Labels: map[string]string{"serviceweaver/store": "true"},
//	    },
//	    BinaryData: map[string][]byte{
//	        "key":   []byte("a key"),
//	        "value": []byte("a value"),
//	    },
//	}
//
// KubeStore also uses this label to implement the List method, fetching every
// ConfigMap that has the "serviceweaver/store=true" label.
//
// # Kubernetes Consistency
//
// Note that it is a little unclear what level of consistency ConfigMaps
// provide, but we think that the operations we use to implement KubeStore are
// all linearizable. [2] discusses how resource versions can be used to
// implement optimistic concurrency control and suggests that versioned writes
// are linearizable. [3] says that gets with an unset resource version return
// the "most recent" data, which we interpret to be linearizable. This is also
// backed by [4], which states:
//
// > Kubernetes Get and List requests are guaranteed to be "consistent reads"
// > if the resourceVersion parameter is not provided. Consistent reads are
// > served from etcd using a "quorum read".
//
// There is a possibility that we misunderstood Kubernetes' documentation or
// the Kubernetes implementation does not properly implement its promised
// consistency guarantees, but the implementation of KubeStore is written under
// the assumption that all of the underlying ConfigMap operations it calls are
// linearizable.
//
// [1]: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
// [2]: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
// [3]: https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
// [4]: https://github.com/kubernetes/enhancements/blob/0e4d5df19d396511fe41ed0860b0ab9b96f46a2d/keps/sig-api-machinery/2340-Consistent-reads-from-cache/README.md
type KubeStore struct {
	// Kubernetes' client-go documentation does not directly state that a
	// client is thread-safe, but discussions online [1, 2] indicate that it
	// is and that many Kubernetes libraries use clients from multiple
	// goroutines. This is why KubeStore is thread-safe.
	//
	// [1]: https://stackoverflow.com/questions/48449024/which-kubernetes-client-go-methods-are-safe-for-concurrent-calls
	// [2]: https://github.com/kubernetes/client-go/issues/36
	client typedv1.ConfigMapInterface
}

// Check that KubeStore implements the Store interface.
var _ store.Store = &KubeStore{}

// newKubeStore returns a new KubeStore that stores its data as ConfigMaps
// accesible via the provided ConfigMapInterface.
func newKubeStore(client typedv1.ConfigMapInterface) store.Store {
	return &KubeStore{client}
}

// tooBigError is the error returned when a user tries to Put a key-value pair
// that exceeds 1 MiB in total (i.e. len(key) + len(value) > 1 MiB).
type tooBigError struct {
	size int
}

// Error implements the error interface.
func (e tooBigError) Error() string {
	return fmt.Sprintf("%v byte key-value pair too big (%v byte limit)", e.size, v1.MaxSecretSize)
}

// Is returns true if the target error is a TooBigError.
func (e tooBigError) Is(target error) bool {
	_, ok := target.(tooBigError)
	return ok
}

func keyToName(key string) string {
	hash := sha256.Sum256([]byte(key))
	return fmt.Sprintf("store-%x", hash[:])
}

func extractVersion(c *v1.ConfigMap) *store.Version {
	return &store.Version{Opaque: c.ResourceVersion}
}

// handleEvents reads a stream of events over c and returns the first value
// with a version different than the provided version. If we receive an error
// over c, if we receive an unexpected message over c, if c is closed, or if
// the context is cancelled, an error is returned.
func handleEvents(ctx context.Context, c <-chan watch.Event, version *store.Version) (string, *store.Version, error) {
	for {
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()

		case event, ok := <-c:
			if !ok {
				return "", nil, fmt.Errorf("internal error: watch ended unexpectedly")
			}

			switch event.Type {
			case watch.Error:
				status, ok := event.Object.(*metav1.Status)
				if !ok {
					err := fmt.Errorf("internal error: got %T, want *metav1.Status", event.Object)
					return "", nil, err
				}
				return "", nil, &errors.StatusError{ErrStatus: *status}

			case watch.Bookmark:
				err := fmt.Errorf("internal error: unexpected Bookmark")
				return "", nil, err

			case watch.Deleted:
				if *version != store.Missing {
					return "", &store.Missing, nil
				}

			case watch.Added, watch.Modified:
				latest, ok := event.Object.(*v1.ConfigMap)
				if !ok {
					err := fmt.Errorf("internal error: got %T, want *v1.ConfigMap", event.Object)
					return "", nil, err
				}
				latestVersion := extractVersion(latest)
				if *version != *latestVersion {
					return string(latest.BinaryData["value"]), latestVersion, nil
				}

			default:
				err := fmt.Errorf("internal error: unexpected watch.EventType %v", event.Type)
				return "", nil, err
			}
		}
	}
}

func (f *KubeStore) Put(ctx context.Context, key, value string, version *store.Version) (*store.Version, error) {
	// As explained in [1], ConfigMaps cannot be too big.
	//
	// > A ConfigMap is not designed to hold large chunks of data. The data
	// > stored in a ConfigMap cannot exceed 1 MiB. If you need to store
	// > settings that are larger than this limit, you may want to consider
	// > mounting a volume or use a separate database or file service.
	//
	// In [2], we see that this 1 MiB limit, defined in v1.MaxSecretSize,
	// applies only to the values stored in the ConfigMap's Data and BinaryData
	// fields. The ConfigMap's keys and other metadata don't count against the
	// 1 MiB limit. However, we store both `key` and `value` as values in the
	// ConfigMap (keyed by "key" and "value" respectively), so both count
	// against the limit.
	//
	// [1]: https://kubernetes.io/docs/concepts/configuration/configmap/
	// [2]: https://github.com/kubernetes/kubernetes/blob/8b5a19147530eaac9476b0ab82980b4088bbc1b2/pkg/apis/core/validation/validation.go#L5361-L5391
	if size := len(key) + len(value); size > v1.MaxSecretSize {
		return nil, tooBigError{size}
	}

	configMap := v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   keyToName(key),
			Labels: map[string]string{"serviceweaver/store": "true"},
		},
		BinaryData: map[string][]byte{
			"key":   []byte(key),
			"value": []byte(value),
		},
	}

	// Case (1): the provided version is nil. We want to perform a blind write.
	// This means that we either have to call f.client.Create or
	// f.client.Update. f.client.Create only succeeds if the ConfigMap doesn't
	// yet exist, and f.client.Update only succeeds if the ConfigMap does
	// already exist, but we don't know if the ConfigMap exists or not. So, we
	// first attempt to Create the ConfigMap, and if that fails because the
	// ConfigMap already exists, then we Update it. This Update could
	// potentially fail too---for example, if another client deletes the
	// ConfigMap between when we call Create and when we call Update---but if
	// this happens, we return an error instead of retrying.
	if version == nil {
		latest, err := f.client.Create(ctx, &configMap, metav1.CreateOptions{})
		if err == nil {
			return extractVersion(latest), nil
		} else if err != nil && !errors.IsAlreadyExists(err) {
			return nil, err
		}

		// Note that configMap.ResourceVersion is unset, so this is a blind
		// update. It shouldn't fail because of a conflict.
		latest, err = f.client.Update(ctx, &configMap, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
		return extractVersion(latest), nil
	}

	// Case (2): the provided version is Missing. A Put with a Missing version
	// should succeed only if the key doesn't exist. This is exactly the
	// semantics of an f.client.Create.
	if *version == store.Missing {
		latest, err := f.client.Create(ctx, &configMap, metav1.CreateOptions{})
		if errors.IsAlreadyExists(err) {
			return nil, store.NewStale(*version, nil)
		} else if err != nil {
			return nil, err
		}
		return extractVersion(latest), nil
	}

	// Case (3): the provided version is some version v. The version v is also
	// the ConfigMap's version, so we perform a ConfigMap Update.
	configMap.ResourceVersion = version.Opaque
	latest, err := f.client.Update(ctx, &configMap, metav1.UpdateOptions{})
	if errors.IsConflict(err) {
		return nil, store.NewStale(*version, nil)
	} else if errors.IsNotFound(err) {
		return nil, store.NewStale(*version, &store.Missing)
	} else if err != nil {
		return nil, err
	}
	return extractVersion(latest), nil
}

func (f *KubeStore) Get(ctx context.Context, key string, version *store.Version) (string, *store.Version, error) {
	// Case (1): the provided version is nil. We perform a simple, non-blocking
	// get (i.e. an unversioned Get). We don't need to perform any watching.
	if version == nil {
		// As specified in [1], when the ResourceVersion field of the
		// GetOptions is unset, Kubernetes returns the most recent value.
		//
		// [1]: https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions
		latest, err := f.client.Get(ctx, keyToName(key), metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return "", &store.Missing, nil
		} else if err != nil {
			return "", nil, err
		}
		return string(latest.BinaryData["value"]), extractVersion(latest), nil
	}

	// Case (2): the provided version is Missing. We perform an unversioned
	// Watch and return the first non-missing value we receive.
	if *version == store.Missing {
		watchOptions := metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", keyToName(key)),
		}
		watcher, err := f.client.Watch(ctx, watchOptions)
		if err != nil {
			return "", nil, err
		}
		defer watcher.Stop()
		return handleEvents(ctx, watcher.ResultChan(), version)
	}

	// Case (3): the provided version is some version v. We want to perform an
	// unversioned watch, but there is a hiccup. If we issue an unversioned
	// Kubernetes watch for a value that is missing, the watch hangs until the
	// value is created. Thus, if the key is missing from the Store---and
	// therefore there is no corresponding ConfigMap stored in
	// Kubernetes---then the watch will hang.
	//
	// To solve this problem, we first initiate the unversioned watch and then
	// perform a get to check to see if the value is missing. If it is, we
	// return immediately. If it's not and if the latest version is newer than
	// the provided version, we can also return immediately. Otherwise, the
	// latest version is the provided version.
	//
	// In this case, we start monitoring the unversioned watch that we
	// previously initiated and wait to receive the first non-error event that
	// is newer than the provided version v.
	//
	// There is a very slight chance that during this watch, the Kubernetes
	// server replies with a ResourceExpired error. In this case, we again
	// issue a get, but this time, we are more confident that the returned
	// version will be newer than the provided version. If it weren't, then we
	// probably wouldn't have received a ResourceExpired error.
	watchOptions := metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", keyToName(key)),
	}
	watcher, err := f.client.Watch(ctx, watchOptions)
	if err != nil {
		return "", nil, err
	}
	defer watcher.Stop()

	latest, err := f.client.Get(ctx, keyToName(key), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return "", &store.Missing, nil
	} else if err != nil {
		return "", nil, err
	}
	latestVersion := extractVersion(latest)
	if *version != *latestVersion {
		return string(latest.BinaryData["value"]), latestVersion, nil
	}

	c := watcher.ResultChan()
	if value, version, err := handleEvents(ctx, c, version); err == nil || !errors.IsResourceExpired(err) {
		return value, version, err
	}

	latest, err = f.client.Get(ctx, keyToName(key), metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return "", &store.Missing, nil
	} else if err != nil {
		return "", nil, err
	}

	latestVersion = extractVersion(latest)
	if *version == *latestVersion {
		// This case shouldn't be possible. If we received a ResourceExpired
		// error, then the provided version should be stale. However, if it
		// does happen, we can always return Unchanged.
		return "", nil, store.Unchanged
	}
	return string(latest.BinaryData["value"]), latestVersion, nil
}

func (f *KubeStore) Delete(ctx context.Context, key string) error {
	// Note that f.client.Delete will fail if the ConfigMap doesn't exist. We
	// intentionally swallow this error.
	//
	// TODO(mwhittaker): If we're afraid that f.client.Delete does not interact
	// with Get and Update in a linearizable way, we can switch to using
	// f.client.Update here with a tombstone.
	err := f.client.Delete(ctx, keyToName(key), metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (f *KubeStore) List(ctx context.Context) ([]string, error) {
	// Note that performing a List with an unset ResourceVersion is
	// linearizable. From [1]:
	//
	// > Return data at the most recent resource version. The returned data
	// > must be consistent (i.e. served from etcd via a quorum read).
	//
	// [1]: https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
	listOptions := metav1.ListOptions{LabelSelector: "serviceweaver/store=true"}
	configMaps, err := f.client.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	keys := []string{}
	for _, configMap := range configMaps.Items {
		key, ok := configMap.BinaryData["key"]
		if !ok {
			return nil, fmt.Errorf("ConfigMap %q has no key", configMap.Name)
		}
		keys = append(keys, string(key))
	}
	return keys, nil
}

// Purge deletes everything from the store. Unlike operations like [Put] and
// [Get], Purge is not guaranteed to be linearizable.
func (f *KubeStore) Purge(ctx context.Context) error {
	deleteOptions := metav1.DeleteOptions{}
	listOptions := metav1.ListOptions{LabelSelector: "serviceweaver/store=true"}
	return f.client.DeleteCollection(ctx, deleteOptions, listOptions)
}
