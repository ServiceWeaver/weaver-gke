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
	type main struct {
		weaver.Implements[weaver.Main]
		dst weaver.Ref[destination]
	}
	Run(t, func(root *main) {
		// Get the PID of the dst component. Check whether root and dst are
		// running in the same process.
		cPid := os.Getpid()
		dstPid, _ := root.dst.Get().Getpid(context.Background())
		sameProcess := cPid == dstPid
		if sameProcess {
			t.Fatal("the root and the dst components should run in different processes")
		}
	})
}

func TestTwoComponents(t *testing.T) {
	type main struct {
		weaver.Implements[weaver.Main]
		src weaver.Ref[source]
		dst weaver.Ref[destination]
	}
	Run(t, func(root *main) {
		// Add a list of items to a component (dst) from another component
		// (src). Verify that dst updates the state accordingly.
		ctx := context.Background()
		file := filepath.Join(t.TempDir(), fmt.Sprintf("simple_%s", uuid.New().String()))
		want := []string{"a", "b", "c", "d", "e"}
		for _, in := range want {
			if err := root.src.Get().Emit(ctx, file, in); err != nil {
				t.Fatal(err)
			}
		}

		got, err := root.dst.Get().GetAll(ctx, file)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("GetAll (-want +got):\n%s", diff)
		}
	})
}
