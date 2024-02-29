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

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/ServiceWeaver/weaver-gke/internal/local"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
)

var (
	storeFlags  = flag.NewFlagSet("store", flag.ContinueOnError)
	storeRegion = storeFlags.String("region", "none", "Simulated GKE region")
)

var storeSpec = tool.StoreSpec{
	Tool:  "weaver gke-local",
	Flags: storeFlags,
	Store: func(context.Context) (store.Store, error) {
		if *storeRegion == "none" {
			return nil, fmt.Errorf("must specify --region flag")
		}
		return local.Store(*storeRegion)
	},
}
