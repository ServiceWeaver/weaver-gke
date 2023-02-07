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
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestErrors tests that "errors.Is*" functions, like errors.IsConflict, behave
// appropriately on the fake errors used by FakeConfigMapper. The fake errors
// are constructed using errors functions, like errors.NewConflict, but we pass
// in fake information. This test confirms that the errors behave
// appropriately, even with the fake information.
func TestErrors(t *testing.T) {
	testers := map[string]func(error) bool{
		"IsConflict":        errors.IsConflict,
		"IsAlreadyExists":   errors.IsAlreadyExists,
		"IsNotFound":        errors.IsNotFound,
		"IsResourceExpired": errors.IsResourceExpired,
		"IsInvalid":         errors.IsInvalid,
	}

	type test struct {
		err    error
		tester string
		want   bool
	}

	for _, test := range []test{
		{conflictError, "IsConflict", true},
		{conflictError, "IsAlreadyExists", false},
		{conflictError, "IsNotFound", false},
		{conflictError, "IsResourceExpired", false},
		{conflictError, "IsInvalid", false},

		{alreadyExistsError, "IsConflict", false},
		{alreadyExistsError, "IsAlreadyExists", true},
		{alreadyExistsError, "IsNotFound", false},
		{alreadyExistsError, "IsResourceExpired", false},
		{alreadyExistsError, "IsInvalid", false},

		{notFoundError, "IsConflict", false},
		{notFoundError, "IsAlreadyExists", false},
		{notFoundError, "IsNotFound", true},
		{notFoundError, "IsResourceExpired", false},
		{notFoundError, "IsInvalid", false},

		{resourceExpiredError, "IsConflict", false},
		{resourceExpiredError, "IsAlreadyExists", false},
		{resourceExpiredError, "IsNotFound", false},
		{resourceExpiredError, "IsResourceExpired", true},
		{resourceExpiredError, "IsInvalid", false},
	} {
		tester := testers[test.tester]
		if got := tester(test.err); got != test.want {
			t.Errorf("%q(%v) wanted %v but got %v", test.tester, test.err, test.want, got)
		}
	}
}

// cm makes a ConfigMap. Explicitly constructing a ConfigMap can be overly
// verbose, especially for these tests where we make a lot of ConfigMaps. cm is
// a helper function that makes the tests less verbose.
func cm(name string, data map[string]string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: data,
	}
}

// expectResourceExpired checks that a ResourceExpired error was written to c
// and that c was then closed. This happens when we try to watch a ConfigMap
// with a stale version.
func expectResourceExpired(t *testing.T, c <-chan watch.Event) {
	event := <-c
	if got, want := event.Type, watch.Error; got != want {
		t.Fatalf("unexpected watch type: got %v, want %v", got, want)
	}

	status, ok := event.Object.(*metav1.Status)
	if !ok {
		t.Fatalf("unexpected object type: got %T, want *metav1.Status", event.Object)
	}

	if err := (&errors.StatusError{ErrStatus: *status}); !errors.IsResourceExpired(err) {
		t.Fatalf("unexpected error: got %v, want ResourceExpired", err)
	}

	_, ok = <-c
	if ok {
		t.Fatal("channel should be closed")
	}
}

// TestStaleWatchMissing tests that an error is returned if we attempt to
// perform a versioned Watch (i.e. a Watch with a specified ResourceVersion) on
// a ConfigMap that is missing.
func TestStaleWatchMissing(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	// Create a ConfigMap and then delete it. This gives us a stale
	// ResourceVersion to watch.
	configMap := cm("a-config-map", map[string]string{})
	configMap, err := f.Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = f.Delete(ctx, "a-config-map", metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Watch the ConfigMap.
	watchOptions := metav1.ListOptions{
		FieldSelector:   "metadata.name=a-config-map",
		ResourceVersion: configMap.ResourceVersion,
	}
	watcher, err := f.Watch(ctx, watchOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	expectResourceExpired(t, watcher.ResultChan())
}

// TestStaleWatchMissing tests that an error is returned if we attempt to
// perform a versioned Watch on a ConfigMap that has been updated (i.e. our
// ResourceVersion is stale).
func TestStaleWatchUpdated(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	// Create a ConfigMap and then update it. This gives us a stale
	// ResourceVersion to watch.
	configMap := cm("a-config-map", map[string]string{})
	configMap, err := f.Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Watch the ConfigMap.
	watchOptions := metav1.ListOptions{
		FieldSelector:   "metadata.name=a-config-map",
		ResourceVersion: configMap.ResourceVersion,
	}
	watcher, err := f.Watch(ctx, watchOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	expectResourceExpired(t, watcher.ResultChan())
}

// pipeErrs pipes the error returned by f into errs, if the error is not nil.
func pipeErrs(errs chan error, f func() error) {
	if err := f(); err != nil {
		errs <- err
	}
}

// TestVersionedWatch tests that a versioned Watch will properly observe a
// modification, deletion, and creation.
func TestVersionedWatch(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	// Create a ConfigMap to get a resource version.
	configMap := cm("a-config-map", map[string]string{})
	configMap, err := f.Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	ready := make(chan struct{})
	wants := make(chan watch.Event, 3)

	watcher := func() error {
		defer wg.Done()
		defer close(ready)

		watchOptions := metav1.ListOptions{
			FieldSelector:   "metadata.name=a-config-map",
			ResourceVersion: configMap.ResourceVersion,
		}
		watcher, err := f.Watch(ctx, watchOptions)
		if err != nil {
			return err
		}
		defer watcher.Stop()
		ready <- struct{}{}

		c := watcher.ResultChan()
		for want := range wants {
			got := <-c
			if diff := cmp.Diff(want, got); diff != "" {
				return fmt.Errorf("unexpected event: (-want +got): %s", diff)
			}
		}

		return nil
	}

	modifier := func() error {
		defer wg.Done()
		defer close(wants)

		// Wait for the watcher to start watching. If we update the ConfigMap
		// too quickly, then the watcher may fail with a stale version.
		<-ready

		// Update.
		updated := cm("a-config-map", map[string]string{"status": "updated"})
		configMap, err := f.Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Modified, Object: configMap}

		// Delete.
		err = f.Delete(ctx, "a-config-map", metav1.DeleteOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Deleted, Object: configMap}

		// Add.
		added := cm("a-config-map", map[string]string{"status": "added"})
		configMap, err = f.Create(ctx, added, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Added, Object: configMap}

		return nil
	}

	errs := make(chan error, 2)
	go pipeErrs(errs, watcher)
	go pipeErrs(errs, modifier)
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestUnversionedWatchMissing tests that an unversioned Watch on a ConfigMap
// that is missing works as intended.
func TestUnversionedWatchMissing(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	ready := make(chan struct{})
	wants := make(chan watch.Event, 3)

	watcher := func() error {
		defer wg.Done()
		defer close(ready)

		watchOptions := metav1.ListOptions{
			FieldSelector: "metadata.name=a-config-map",
		}
		watcher, err := f.Watch(ctx, watchOptions)
		if err != nil {
			return err
		}
		defer watcher.Stop()
		ready <- struct{}{}

		c := watcher.ResultChan()
		for want := range wants {
			got := <-c
			if diff := cmp.Diff(want, got); diff != "" {
				return fmt.Errorf("unexpected event: (-want +got): %s", diff)
			}
		}

		return nil
	}

	modifier := func() error {
		defer wg.Done()
		defer close(wants)

		// Wait for the watcher to start watching. We want to test what happens
		// when the watcher watches a missing ConfigMap. If we write too
		// quickly, it may not be missing.
		<-ready

		// Add.
		added := cm("a-config-map", map[string]string{"status": "added"})
		configMap, err := f.Create(ctx, added, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Added, Object: configMap}

		// Update.
		updated := cm("a-config-map", map[string]string{"status": "updated"})
		configMap, err = f.Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Modified, Object: configMap}

		// Delete.
		err = f.Delete(ctx, "a-config-map", metav1.DeleteOptions{})
		if err != nil {
			return err
		}
		wants <- watch.Event{Type: watch.Deleted, Object: configMap}

		return nil
	}

	errs := make(chan error, 2)
	go pipeErrs(errs, watcher)
	go pipeErrs(errs, modifier)
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestSimpleStop tests that watcher.Stop() correctly closes the channel
// watcher.ResultChan().
func TestSimpleStop(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	watchOptions := metav1.ListOptions{
		FieldSelector: "metadata.name=a-config-map",
	}
	watcher, err := f.Watch(ctx, watchOptions)
	if err != nil {
		t.Fatal(err)
	}
	watcher.Stop()

	c := watcher.ResultChan()
	_, ok := <-c
	if ok {
		t.Fatal("channel was not closed properly")
	}
}

// TestDoubleStop tests that watcher.Stop() can be called multiple times.
func TestDoubleStop(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()
	watchOptions := metav1.ListOptions{
		FieldSelector: "metadata.name=a-config-map",
	}
	watcher, err := f.Watch(ctx, watchOptions)
	if err != nil {
		t.Fatal(err)
	}
	watcher.Stop()
	watcher.Stop()
}

// TestManyWatchers tests that watching works correctly even with multiple
// watchers watching and stopping watching a single ConfigMap.
//
// The test involves a number of watcher goroutines, all watching the same
// ConfigMap, and a modifier goroutine that creates, updates, and deletes the
// ConfigMap that the watchers are watching. The goroutines interact in lock
// step:
//
//   - The watchers repeatedly inform the modifier that they are ready,
//     receive an event from the modifier, and validate that the event they
//     watched matches the event they received from the modifier.
//   - The modifier waits for all of the watchers to be ready, modifies the
//     ConfigMap, and sends the watchers the event they should see.
//
// Moreover, not every watcher watches every event. Every watcher is assigned a
// number of events to skip and a number of events to watch. For example, the
// watcher with skip=2 and get=3:
//
//  1. receives two events from the modifier, but ignores them;
//  2. watches three events;
//  3. stops itself; and finally
//  4. ignores all subsequent events.
func TestManyWatchers(t *testing.T) {
	f := newFakeConfigMapper()
	ctx := context.Background()

	// The number of operations the modifier performs.
	numOps := 3

	// After every modification, the modifier sends the watchers the events
	// they should expect to observe by watching.
	wants := map[int]map[int]chan watch.Event{}
	numWatchers := 0
	for skip := 0; skip < numOps; skip++ {
		wants[skip] = map[int]chan watch.Event{}
		for get := 1; get+skip <= numOps; get++ {
			wants[skip][get] = make(chan watch.Event, numOps)
			numWatchers++
		}
	}

	// done is a WaitGroup used to  wait for all of the watchers and the
	// modifier to be done.
	var done sync.WaitGroup
	done.Add(numWatchers + 1)

	// ready is a WaitGroup that the watchers and modifier use to execute in
	// lock step.
	var ready sync.WaitGroup
	ready.Add(numWatchers)

	watcher := func(skip, get int) error {
		defer done.Done()

		var watcher watch.Interface
		var c <-chan watch.Event
		var err error
		toSkip := skip
		toGet := get

		for i := 0; i < numOps; i++ {
			// We're done skipping operations. It's time to watch.
			if toSkip == 0 && toGet == get {
				watchOptions := metav1.ListOptions{
					FieldSelector: "metadata.name=a-config-map",
				}
				watcher, err = f.Watch(ctx, watchOptions)
				if err != nil {
					return err
				}
				defer watcher.Stop()
				c = watcher.ResultChan()

				if i > 0 {
					// We ignore the synthetic add event.
					<-c
				}
			}

			// We just finished watching. It's time to stop.
			if i == skip+get {
				watcher.Stop()
			}

			// Let the modifier know we're ready.
			ready.Done()

			want := <-wants[skip][get]
			if toSkip > 0 {
				// We're still skipping. Don't do anything.
				toSkip--
			} else if toGet > 0 {
				// We're still watching. Check that we get what we want.
				got := <-c
				if diff := cmp.Diff(want, got); diff != "" {
					return fmt.Errorf("unexpected event: (-want +got): %s", diff)
				}
				toGet--
			} else {
				// We finished skipping and getting. Don't do anything.
			}
		}

		return nil
	}

	modifier := func() error {
		defer done.Done()

		var configMap *v1.ConfigMap
		var err error

		for i := 0; i < numOps; i++ {
			ready.Wait()
			ready.Add(numWatchers)

			var want watch.Event
			if i == 0 {
				// Add.
				added := cm("a-config-map", map[string]string{"status": "added"})
				configMap, err = f.Create(ctx, added, metav1.CreateOptions{})
				if err != nil {
					return err
				}
				want = watch.Event{Type: watch.Added, Object: configMap}
			} else if i == numOps-1 {
				// Delete.
				err = f.Delete(ctx, "a-config-map", metav1.DeleteOptions{})
				if err != nil {
					return err
				}
				want = watch.Event{Type: watch.Deleted, Object: configMap}
			} else {
				// Update.
				data := map[string]string{"status": fmt.Sprintf("updated %d", i)}
				updated := cm("a-config-map", data)
				configMap, err = f.Update(ctx, updated, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				want = watch.Event{Type: watch.Modified, Object: configMap}
			}

			// Notify the watchers.
			for _, m := range wants {
				for _, c := range m {
					c <- want
				}
			}
		}

		return nil
	}

	errs := make(chan error, numWatchers+1)
	for skip, m := range wants {
		for get := range m {
			skip := skip
			get := get
			go pipeErrs(errs, func() error { return watcher(skip, get) })
		}
	}
	go pipeErrs(errs, modifier)
	done.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
