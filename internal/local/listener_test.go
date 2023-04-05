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

package local

import (
	"context"
	"testing"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestRecordGetListeners(t *testing.T) {
	// Test plan: Record a number of network listeners and verify that all
	// are retrieved.
	ctx := context.Background()
	store := store.NewFakeStore()
	version := newAppVersion(
		"todo",
		uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		[]*nanny.Listener{
			{Name: "l1", Addr: "1.1.1.1:1"},
			{Name: "l1", Addr: "2.2.2.2:2"},
			{Name: "l1", Addr: "3.3.3.3:3"},
			{Name: "l2", Addr: "1.1.1.1:1"},
			{Name: "l2", Addr: "2.2.2.2:2"},
			{Name: "l2", Addr: "3.3.3.3:3"},
			{Name: "l3", Addr: "1.1.1.1:1"},
			{Name: "l3", Addr: "2.2.2.2:2"},
			{Name: "l3", Addr: "3.3.3.3:3"},
		})

	// Report the listeners.
	for _, lis := range version.listeners {
		if err := RecordListener(ctx, store, version.config, lis); err != nil {
			t.Fatalf("RecordListener(%v, %v): %v", version.config, lis, err)
		}
	}

	// Fetch the listeners.
	listeners, err := getListeners(ctx, store, version.config)
	if err != nil {
		t.Fatalf("GetListeners: %v", err)
	}
	less := func(x, y *nanny.Listener) bool { return x.Name < y.Name }
	if diff := cmp.Diff(version.listeners, listeners,
		cmpopts.SortSlices(less), protocmp.Transform()); diff != "" {
		t.Fatalf("bad listeners: (-want +got)\n%s", diff)
	}
}
