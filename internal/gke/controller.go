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
	"net/http"
)

// Controller returns the HTTP address of the controller and an HTTP client
// that can be used to contact the controller.
func Controller(ctx context.Context, config CloudConfig) (string, *http.Client, error) {
	configCluster, err := GetClusterInfo(ctx, config, ConfigClusterName, ConfigClusterRegion)
	if err != nil {
		return "", nil, err
	}
	return controllerAddr(configCluster), configCluster.apiRESTClient.Client, nil
}

// controllerAddr returns the URL that can be used to access the controller
// from outside of the cloud.
func controllerAddr(configCluster *ClusterInfo) string {
	return configCluster.apiRESTClient.Get().
		Namespace(namespaceName).
		Resource("services").
		Name("controller").
		SubResource("proxy").URL().String()
}
