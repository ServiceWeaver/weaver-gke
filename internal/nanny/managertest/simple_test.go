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

package managertest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	weaver "github.com/ServiceWeaver/weaver"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestOneComponent(t *testing.T) {
	Run(t, func(root weaver.Instance) {
		ctx := context.Background()
		dst, err := weaver.Get[destination](root)
		if err != nil {
			t.Fatal(err)
		}

		// Get the PID of the dst component. Check whether root and dst are
		// running in the same process.
		cPid := os.Getpid()
		dstPid, _ := dst.Getpid(ctx)
		sameProcess := cPid == dstPid
		if sameProcess {
			t.Fatal("the root and the dst components should run in different processes")
		}
	})
}

func TestTwoComponents(t *testing.T) {
	Run(t, func(root weaver.Instance) {
		// Add a list of items to a component (dst) from another component
		// (src). Verify that dst updates the state accordingly.
		ctx := context.Background()
		file := filepath.Join(t.TempDir(), fmt.Sprintf("simple_%s", uuid.New().String()))
		src, err := weaver.Get[source](root)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := weaver.Get[destination](root)
		if err != nil {
			t.Fatal(err)
		}

		want := []string{"a", "b", "c", "d", "e"}
		for _, in := range want {
			if err := src.Emit(ctx, file, in); err != nil {
				t.Fatal(err)
			}
		}

		got, err := dst.GetAll(ctx, file)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("GetAll (-want +got):\n%s", diff)
		}
	})
}
