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
	storeFlags   = flag.NewFlagSet("store", flag.ContinueOnError)
	storeProject = storeFlags.String("project", "",
		`GCP project where the store resides. If empty, a default cloud
project on the local machine is used.`)
	storeAccount = storeFlags.String("account", "",
		`GCP user account to use to access the store. If empty, a default cloud
	account on the local machine is used.`)
	storeRegion = storeFlags.String("region", gke.ConfigClusterRegion,
		`Cloud region where the store resides. Default value is the region of
the Service Weaver configuration cluster.`)
	storeCluster = storeFlags.String("cluster", gke.ConfigClusterName,
		`GKE cluster where the store resides. Default value is the name of the
Service Weaver configuration cluster.`)

	storeSpec = tool.StoreSpec{
		Tool:  "weaver gke",
		Flags: storeFlags,
		Store: func(ctx context.Context) (store.Store, error) {
			config, err := gke.SetupCloudConfig(*storeProject, *storeAccount)
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(os.Stderr, "Using account %s in project %s\n", config.Account, config.Project)
			cluster, err := gke.GetClusterInfo(ctx, config, *storeCluster, *storeRegion)
			if err != nil {
				return nil, err
			}
			return gke.Store(cluster), nil
		},
	}
)
