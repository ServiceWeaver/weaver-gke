// Copyright 2023 Google LLC
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

package endpoints

import (
	"context"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// Interval at which babysitters report load to the manager.
const LoadReportInterval = 5 * time.Minute

// Babysitter is an interface for a babysitter.
type Babysitter interface {
	CheckHealth(context.Context, *protos.GetHealthRequest) (*protos.GetHealthReply, error)
	RunProfiling(context.Context, *protos.GetProfileRequest) (*protos.GetProfileReply, error)
}

// Distributor is an interface for a distributor.
type Distributor interface {
	Distribute(context.Context, *nanny.ApplicationDistributionRequest) error
	Cleanup(context.Context, *nanny.ApplicationCleanupRequest) error
	GetApplicationState(context.Context, *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error)
	GetPublicTrafficAssignment(context.Context) (*nanny.TrafficAssignment, error)
	GetPrivateTrafficAssignment(context.Context) (*nanny.TrafficAssignment, error)
	RunProfiling(context.Context, *nanny.GetProfileRequest) (*protos.GetProfileReply, error)
}

// Manager is an interface for a manager.
type Manager interface {
	Deploy(context.Context, *nanny.ApplicationDeploymentRequest) error
	Stop(context.Context, *nanny.ApplicationStopRequest) error
	Delete(context.Context, *nanny.ApplicationDeleteRequest) error
	GetReplicaSetState(context.Context, *nanny.GetReplicaSetStateRequest) (*nanny.ReplicaSetState, error)
	ActivateComponent(context.Context, *nanny.ActivateComponentRequest) error
	RegisterReplica(context.Context, *nanny.RegisterReplicaRequest) error
	ReportLoad(context.Context, *nanny.LoadReport) error
	GetListenerAddress(context.Context, *nanny.GetListenerAddressRequest) (*protos.GetListenerAddressReply, error)
	ExportListener(context.Context, *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error)
	GetRoutingInfo(context.Context, *nanny.GetRoutingRequest) (*nanny.GetRoutingReply, error)
	GetComponentsToStart(context.Context, *nanny.GetComponentsRequest) (*nanny.GetComponentsReply, error)
}
