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
	"os"

	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver-gke/internal/tool"
)

var (
	storeFlags   = newCloudFlagSet("store", flag.ContinueOnError)
	storeRegion  = storeFlags.String("region", "", `Cloud region where the store resides.`)
	storeCluster = storeFlags.String("cluster", "", `GKE cluster where the store resides.`)
	storeSpec    = tool.StoreSpec{
		Tool:  "weaver gke",
		Flags: storeFlags.FlagSet,
		Store: func(ctx context.Context) (store.Store, error) {
			if *storeRegion == "" {
				return nil, fmt.Errorf("must specify --region flag")
			}
			if *storeCluster == "" {
				return nil, fmt.Errorf("must specify --cluster flag. Note that" +
					"the cluster should be either serviceweaver or serviceweaver-config")
			}
			config, err := storeFlags.CloudConfig()
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(os.Stderr, "Using project %s\n", config.Project)
			cluster, err := gke.GetClusterInfo(ctx, config, *storeCluster, *storeRegion)
			if err != nil {
				return nil, err
			}
			return gke.Store(cluster), nil
		},
	}
)
