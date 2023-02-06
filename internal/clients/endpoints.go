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

package clients

import (
	"context"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// BabysitterClient is a client to a babysitter.
type BabysitterClient interface {
	CheckHealth(ctx context.Context, status *HealthCheck) (*protos.HealthReport, error)
	RunProfiling(context.Context, *protos.RunProfiling) (*protos.Profile, error)
}

// DistributorClient is a client to a distributor.
type DistributorClient interface {
	Distribute(ctx context.Context, req *nanny.ApplicationDistributionRequest) error
	Cleanup(ctx context.Context, req *nanny.ApplicationCleanupRequest) error
	GetApplicationState(ctx context.Context, req *nanny.ApplicationStateAtDistributorRequest) (*nanny.ApplicationStateAtDistributor, error)
	GetPublicTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error)
	GetPrivateTrafficAssignment(ctx context.Context) (*nanny.TrafficAssignment, error)
	RunProfiling(context.Context, *protos.RunProfiling) (*protos.Profile, error)
}

// ManagerClient is a client to a manager.
//
// TODO(mwhittaker): Refactor the manager so that it also implements the Client
// interface.
type ManagerClient interface {
	Deploy(context.Context, *nanny.ApplicationDeploymentRequest) error
	Stop(context.Context, *nanny.ApplicationStopRequest) error
	Delete(context.Context, *nanny.ApplicationDeleteRequest) error
	GetProcessState(context.Context, *nanny.ProcessStateRequest) (*nanny.ProcessState, error)
	GetProcessesToStart(context.Context, *protos.GetProcessesToStartRequest) (*protos.GetProcessesToStartReply, error)
	StartComponent(context.Context, *protos.ComponentToStart) error
	StartColocationGroup(context.Context, *nanny.ColocationGroupStartRequest) error
	RegisterReplica(context.Context, *nanny.ReplicaToRegister) error
	ReportLoad(context.Context, *protos.WeaveletLoadReport) error
	ExportListener(context.Context, *nanny.ExportListenerRequest) (*protos.ExportListenerReply, error)
	GetRoutingInfo(context.Context, *protos.GetRoutingInfo) (*protos.RoutingInfo, error)
	GetComponentsToStart(context.Context, *protos.GetComponentsToStart) (*protos.ComponentsToStart, error)
}
