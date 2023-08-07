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
	"errors"
	"fmt"
	"os"
	"path"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1beta2"
	compute "cloud.google.com/go/compute/apiv1"
	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	dproto "github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
	artifactregistrypb "google.golang.org/genproto/googleapis/devtools/artifactregistry/v1beta2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// noRetryErr is an error that shouldn't be retried.
type noRetryErr struct {
	err error
}

func (e noRetryErr) Error() string { return e.err.Error() }

// isNotFound returns true iff the given error is a not-found error
// returned either by a HTTP or a gRPC cloud client.
func isNotFound(err error) bool {
	if gErr, ok := err.(*apierror.APIError); ok {
		if gErr.GRPCStatus().Code() != codes.OK {
			// Error is a GRPC error.
			return gErr.GRPCStatus().Code() == codes.NotFound
		}
		// Error is an HTTP error. Unfortunately, apierror.APIError interface
		// doesn't allow us to check the error code. Our best bet is to
		// unwrap the underlying error, which should be of type
		// *googleapi.Error.
		err = gErr.Unwrap()
	}
	if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
		return true
	}
	if gStatus, ok := status.FromError(err); ok && gStatus.Code() == codes.NotFound {
		return true
	}
	return false
}

// cloudPatcher applies changes to GCP resources (e.g., Subnet, Repository).
// It ensures that the resource is updated only when needed, which allows
// multiple concurrent patches to collectively update the resource to the same
// value, without triggering changes in response to value writes.
type cloudPatcher struct {
	desc string // Description of the resource being patched.
	opts patchOptions

	// A function that retrieves the existing resource value, if it exists,
	// and if so returns the value it should be updated to, or nil if the
	// value doesn't need to be updated.
	get func() (interface{}, error)

	// A function that adds a new resource value.  Only called if
	// the resource doesn't already have a value.
	create func() error

	// A function that updates the existing resource value.  Only called
	// if the resource already has a value.
	update func(interface{}) error
}

// Run runs the patcher.
func (p cloudPatcher) Run(ctx context.Context) error {
	print := func(format string, args ...any) {
		if p.opts.logger != nil {
			p.opts.logger.Info(fmt.Sprintf(format, args...))
		} else {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}
	var noRetry noRetryErr
	for r := retry.Begin(); r.Continue(ctx); {
		if updateVal, err := p.get(); err == nil {
			if updateVal == nil { // no update needed
				return nil
			}
			// Update the existing value.
			print("Updating %s... ", p.desc)
			if err := p.update(updateVal); err == nil {
				print("Done\n")
				return nil // success
			} else {
				print("Error: %v\n", err)
				if errors.As(err, &noRetry) {
					return noRetry.err
				}
				// retry
			}
		} else if isNotFound(err) {
			// No existing entry: create a new one.
			print("Creating %s... ", p.desc)
			if err := p.create(); err == nil {
				print("Done\n")
				return nil // success
			} else {
				print("Error: %v\n", err)
				if errors.As(err, &noRetry) {
					return noRetry.err
				}
				// retry
			}
		} else if errors.As(err, &noRetry) {
			return noRetry.err
		} else {
			print("Error getting %s: %v Will retry\n", p.desc, err)
		}
	}
	return ctx.Err()
}

// patchSubnet sets up the proxy-only subnet with the given configuration.
//
// Since the subnetwork client API doesn't allow updates to the existing
// subnetworks, and since the patching API is extremely limited, this function
// will return an error if an update is required (i.e., if the new subnetwork
// resource is different from the existing one).
func patchSubnet(ctx context.Context, config CloudConfig, opts patchOptions, region string, sub *computepb.Subnetwork) error {
	client, err := compute.NewSubnetworksRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	return cloudPatcher{
		desc: fmt.Sprintf("subnetwork %s in region %s", *sub.Name, region),
		opts: opts,
		get: func() (interface{}, error) {
			old, err := client.Get(ctx, &computepb.GetSubnetworkRequest{
				Region:     region,
				Subnetwork: *sub.Name,
				Project:    config.Project,
			})
			if err != nil {
				return nil, err
			}
			// See if the old value differs from the new in the fields that
			// matter.
			modif, err := dproto.Copy(old, sub, nil /*skip*/)
			if err != nil {
				return nil, err
			}
			if modif == nil { // no change from the existing value
				return nil, nil
			}
			return old, nil
		},
		create: func() error {
			_, err := client.Insert(ctx, &computepb.InsertSubnetworkRequest{
				Region:             region,
				SubnetworkResource: sub,
				Project:            config.Project,
			})
			return err
		},
		update: func(updateVal interface{}) error {
			// Update API doesn't exist and Patch API is too limited.  We also
			// cannot delete the subnet resource, since it may be in use (by a
			// load-balancer).  We therefore have no other recourse but to return
			// an error.
			// NOTE: we don't anticipate needing to change an existing subnet
			// resource; in the event that we do in the future, we'll have to use
			// one of the custom client APIs (e.g., ExpandIpCidrRange()).
			return noRetryErr{fmt.Errorf("subnetwork %q cannot be updated", *sub.Name)}
		},
	}.Run(ctx)
}

// patchSSLCertificate sets up the SSL certificate with the given configuration.
func patchSSLCertificate(ctx context.Context, config CloudConfig, opts patchOptions, cert *computepb.SslCertificate) error {
	client, err := compute.NewSslCertificatesRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	return cloudPatcher{
		desc: fmt.Sprintf("ssl certificate %s", *cert.Name),
		opts: opts,
		get: func() (interface{}, error) {
			old, err := client.Get(ctx, &computepb.GetSslCertificateRequest{
				SslCertificate: *cert.Name,
				Project:        config.Project,
			})
			if err != nil {
				return nil, err
			}

			// See if the old value differs from the new in the fields that
			// matter.
			modif, err := dproto.Merge(old, cert, nil)
			if err != nil {
				return nil, err
			}
			if !modif { // no change from the existing value
				return nil, nil
			}
			return old, nil
		},
		create: func() error {
			op, err := client.Insert(ctx, &computepb.InsertSslCertificateRequest{
				SslCertificateResource: cert,
				Project:                config.Project,
			})
			if err != nil {
				return err
			}
			return op.Wait(ctx)
		},
		update: func(updateVal interface{}) error {
			// Neither Update or a Patch API exists.  We also cannot delete the
			// certificate, since it may be in use (by a load-balancer).
			return noRetryErr{fmt.Errorf("ssl certificate resource %q cannot be updated", *cert.Name)}
		},
	}.Run(ctx)
}

// patchRepository updates the Repository with the new configuration.
func patchRepository(ctx context.Context, config CloudConfig, opts patchOptions, rep *artifactregistrypb.Repository) error {
	client, err := artifactregistry.NewClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	return cloudPatcher{
		desc: fmt.Sprintf("artifacts repository %s", rep.Name),
		opts: opts,
		get: func() (interface{}, error) {
			old, err := client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{
				Name: rep.Name,
			})
			if err != nil {
				return nil, err
			}

			// See if the old value differs from the new in the fields that
			// matter.
			modif, err := dproto.Copy(old, rep, nil /*skip*/)
			if err != nil {
				return nil, err
			}
			if modif == nil { // no change from the existing value
				return nil, nil
			}
			return old, nil
		},
		create: func() error {
			op, err := client.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{
				Parent:       path.Dir(path.Dir(rep.Name)),
				RepositoryId: path.Base(rep.Name),
				Repository:   rep,
			})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		},
		update: func(updateVal interface{}) error {
			val := updateVal.(*artifactregistrypb.Repository)
			updateReq := &artifactregistrypb.UpdateRepositoryRequest{
				Repository: val,
			}
			_, err = client.UpdateRepository(ctx, updateReq)
			return err
		},
	}.Run(ctx)
}

// patchCluster updates the cluster with the new configuration and waits
// for that cluster to be ready.
func patchCluster(ctx context.Context, config CloudConfig, opts patchOptions, location string, c *containerpb.Cluster) error {
	client, err := container.NewClusterManagerClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	wait := func(op *containerpb.Operation) error {
		var doneError error
		isDone := func(op *containerpb.Operation) bool {
			switch op.Status {
			case containerpb.Operation_DONE:
				return true
			case containerpb.Operation_ABORTING:
				doneError = fmt.Errorf(
					"error creating cluster %s: %s", c.Name, op.StatusMessage)
				return true
			default:
				return false
			}
		}
		if isDone(op) {
			return doneError
		}
		opName := op.Name
		for r := retry.Begin(); r.Continue(ctx); {
			if op, err := client.GetOperation(ctx, &containerpb.GetOperationRequest{
				Name: fmt.Sprintf("projects/%s/locations/%s/operations/%s",
					config.Project, location, opName),
			}); err == nil && isDone(op) {
				return doneError
			}
		}
		return fmt.Errorf("timeout waiting for operation %s on cluster %q", op.OperationType, c.Name)
	}
	return cloudPatcher{
		desc: fmt.Sprintf("cluster %s in location %s", c.Name, location),
		opts: opts,
		get: func() (interface{}, error) {
			old, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{
				Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
					config.Project, location, c.Name),
			})
			if err != nil {
				return nil, err
			}

			// Check the cluster status
			switch old.Status {
			case containerpb.Cluster_RUNNING, containerpb.Cluster_RECONCILING:
				// NOTE(spetrovic):  Cluster in reconciling state should still
				// be usable, so we don't have to wait for it to enter the
				// RUNNING state.
			case containerpb.Cluster_PROVISIONING, containerpb.Cluster_STOPPING, containerpb.Cluster_ERROR:
				// Cluster is transitioning either to a created state or a deleted
				// state: return an error which will cause a retry of get() at a
				// later time
				return nil, fmt.Errorf("cluster in transitional state %s", old.Status.String())
			case containerpb.Cluster_DEGRADED:
				// Cluster is in a degraded state, requiring user input.
				// Since we cannot delete the cluster (it may control serving
				// GKE resources like load-balancing), we print the error and
				// return the cluster, since it may still be usable.
				fmt.Fprintf(os.Stderr,
					"WARNING: cluster %s in degraded state: %s\n",
					c.Name, c.StatusMessage)
			default:
				// Cluster in unspecified state.  Since we don't know how to deal
				// with it, pray for the best and return it to the caller since
				// it may still be usable.
				fmt.Fprintf(os.Stderr,
					"WARNING: cluster %s in unspecified state: %s\n",
					c.Name, c.StatusMessage)
			}

			// See if the old value differs from the new in the fields that
			// matter.
			modif, err := dproto.Copy(old, c, []string{
				"description",             // Cannot update this field.
				"node_pools",              // Cannot update this field.
				"initial_cluster_version", // Cannot update this field.
				// NOTE: Updating this field leads to cluster repair, which
				// seems to be broken right now.
				"network_config",
				"addons_config", // Cannot update this field.
			})
			if err != nil {
				return nil, err
			}
			if modif == nil { // no change from the existing value
				return nil, nil
			}

			// Compute the cluster update value.
			var u containerpb.ClusterUpdate
			for _, mf := range modif {
				switch mf {
				// TODO(spetrovic): Add other fields as needed.
				case "workload_identity_config":
					u.DesiredWorkloadIdentityConfig = old.WorkloadIdentityConfig
				case "autoscaling":
					u.DesiredClusterAutoscaling = old.Autoscaling
				default:
					return nil, fmt.Errorf(
						"update to field %q in cluster %q is not possible",
						mf, c.Name)
				}
			}
			return &u, nil
		},
		create: func() error {
			op, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
				Parent:  fmt.Sprintf("projects/%s/locations/%s", config.Project, location),
				Cluster: c,
			})
			if err != nil {
				return err
			}
			return wait(op)
		},
		update: func(updateVal interface{}) error {
			u := updateVal.(*containerpb.ClusterUpdate)
			op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
				Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
					config.Project, location, c.Name),
				Update: u,
			})
			if err != nil {
				return err
			}
			// Wait until the cluster is updated.
			return wait(op)
		},
	}.Run(ctx)
}

// patchDNSZone update the managed DNS zone with the new configuration.
func patchDNSZone(ctx context.Context, config CloudConfig, opts patchOptions, zone *dns.ManagedZone) error {
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	service := dns.NewManagedZonesService(dnsService)
	get := func() (interface{}, error) {
		call := service.Get(config.Project, zone.Name)
		call.Context(ctx)
		old, err := call.Do()
		if err != nil {
			return nil, err
		}
		// See if the old value differs from the new in the fields that
		// matter.
		if old.DnsName == zone.DnsName { // No meaningful change
			return nil, nil
		}
		return zone, nil
	}
	create := func() error {
		call := service.Create(config.Project, zone)
		call.Context(ctx)
		// TODO(spetrovic): The return value of call.Do() is a *dns.ManagedZone
		// that will contain the Google nameservers that host the managed
		// zone. Propagate this information to the caller.
		_, err := call.Do()
		return err
	}
	update := func(_ interface{}) error {
		// Delete old zone entry and re-create it.
		call := service.Delete(config.Project, zone.Name)
		call.Context(ctx)
		if err := call.Do(); err != nil {
			return fmt.Errorf("cannot delete existing managed zone %q: %w", zone.Name, err)
		}
		return create()
	}
	return cloudPatcher{
		desc:   fmt.Sprintf("DNS managed zone %s", zone.Name),
		opts:   opts,
		get:    get,
		create: create,
		update: update,
	}.Run(ctx)
}

// patchDNSRecordSet updates the record set in the given managed zone with
// the new configuration.
func patchDNSRecordSet(ctx context.Context, config CloudConfig, opts patchOptions, dnsZone string, recordSet *dns.ResourceRecordSet) error {
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	service := dns.NewResourceRecordSetsService(dnsService)
	get := func() (interface{}, error) {
		call := service.Get(config.Project, dnsZone, recordSet.Name, recordSet.Type)
		call.Context(ctx)
		old, err := call.Do()
		if err != nil {
			return nil, err
		}

		// See if the old value differs from the new in the fields that
		// matter.
		same := func() bool {
			if old.Type != recordSet.Type {
				return false
			}
			if old.Ttl != recordSet.Ttl {
				return false
			}
			if len(old.Rrdatas) != len(recordSet.Rrdatas) {
				return false
			}
			for i, data := range old.Rrdatas {
				if data != recordSet.Rrdatas[i] {
					return false
				}
			}
			return true
		}
		if same() { // No meaningful change
			return nil, nil
		}
		return recordSet, nil
	}
	create := func() error {
		call := service.Create(config.Project, dnsZone, recordSet)
		call.Context(ctx)
		_, err := call.Do()
		return err
	}
	update := func(_ interface{}) error {
		// Delete old record set and re-create it.
		call := service.Delete(config.Project, dnsZone, recordSet.Name, recordSet.Type)
		call.Context(ctx)
		if _, err := call.Do(); err != nil {
			return fmt.Errorf("cannot delete record set %q in managed zone %q: %w", recordSet.Name, dnsZone, err)
		}
		return create()
	}
	return cloudPatcher{
		desc:   fmt.Sprintf("DNS record set %s", recordSet.Name),
		opts:   opts,
		get:    get,
		create: create,
		update: update,
	}.Run(ctx)
}

// patchCloudServiceAccount updates the service account with the new configuration.
func patchCloudServiceAccount(ctx context.Context, config CloudConfig, opts patchOptions, name string, account *iam.ServiceAccount) error {
	email := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", name, config.Project)
	fullName := path.Join("projects", config.Project, "serviceAccounts", email)
	iamService, err := iam.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	service := iam.NewProjectsService(iamService).ServiceAccounts
	projectName := path.Join("projects", config.Project)
	return cloudPatcher{
		desc: fmt.Sprintf("service account %s", name),
		opts: opts,
		get: func() (interface{}, error) {
			call := service.Get(fullName)
			call.Context(ctx)
			old, err := call.Do()
			if err != nil {
				return nil, err
			}

			// See if the old value differs from the new in the fields that
			// matter.
			same := func() bool {
				return old.Description == account.Description && old.DisplayName == account.DisplayName
			}
			if same() { // no change from the existing value
				return nil, nil
			}
			return account, nil
		},
		create: func() error {
			call := service.Create(projectName, &iam.CreateServiceAccountRequest{
				AccountId:      name,
				ServiceAccount: account,
			})
			call.Context(ctx)
			_, err := call.Do()
			return err
		},
		update: func(_ interface{}) error {
			call := service.Patch(fullName, &iam.PatchServiceAccountRequest{
				ServiceAccount: account,
				UpdateMask:     "description,display_name",
			})
			call.Context(ctx)
			_, err := call.Do()
			return err
		},
	}.Run(ctx)
}

// patchProjectCustomRole updates the custom role in the parent project with
// the given configuration.
func patchProjectCustomRole(ctx context.Context, config CloudConfig, opts patchOptions, id string, role *iam.Role) error {
	iamService, err := iam.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	service := iam.NewProjectsService(iamService).Roles
	parent := fmt.Sprintf("projects/%s", config.Project)
	name := fmt.Sprintf("%s/roles/%s", parent, id)
	return cloudPatcher{
		desc: fmt.Sprintf("custom role %s", name),
		opts: opts,
		get: func() (interface{}, error) {
			call := service.Get(name)
			call.Context(ctx)
			old, err := call.Do()
			if err != nil {
				return nil, err
			}

			// See if the old value differs from the new in the fields that
			// matter.
			same := func() bool {
				oldPermissions := map[string]struct{}{}
				for _, perm := range old.IncludedPermissions {
					oldPermissions[perm] = struct{}{}
				}
				if len(old.IncludedPermissions) != len(role.IncludedPermissions) {
					return false
				}
				for _, perm := range role.IncludedPermissions {
					if _, ok := oldPermissions[perm]; !ok {
						return false
					}
				}
				return true
			}
			if same() { // No meaningful change
				return nil, nil
			}
			return role, nil
		},
		create: func() error {
			call := service.Create(parent, &iam.CreateRoleRequest{
				RoleId: id,
				Role:   role,
			})
			call.Context(ctx)
			_, err := call.Do()
			return err
		},
		update: func(_ interface{}) error {
			call := service.Patch(name, role)
			call.Context(ctx)
			call.UpdateMask("included_permissions")
			_, err := call.Do()
			return err
		},
	}.Run(ctx)
}

// patchStaticIPAddress creates the static IP address with the given
// configuration.
func patchStaticIPAddress(ctx context.Context, config CloudConfig, opts patchOptions, region string, addr *computepb.Address) (string, error) {
	var globalClient *compute.GlobalAddressesClient
	var regionalClient *compute.AddressesClient
	var err error
	if region == "" { // global
		globalClient, err = compute.NewGlobalAddressesRESTClient(ctx, config.ClientOptions()...)
	} else { // regional
		regionalClient, err = compute.NewAddressesRESTClient(ctx, config.ClientOptions()...)
	}
	if err != nil {
		return "", err
	}
	var cur *computepb.Address

	get := func() (interface{}, error) {
		var old *computepb.Address
		var err error
		if region == "" { // global
			old, err = globalClient.Get(ctx, &computepb.GetGlobalAddressRequest{
				Address: *addr.Name,
				Project: config.Project,
			})
		} else { // regional
			old, err = regionalClient.Get(ctx, &computepb.GetAddressRequest{
				Address: *addr.Name,
				Project: config.Project,
				Region:  region,
			})
		}
		if err != nil {
			return nil, err
		}

		// No API exists to update a static IP address,	and we can never
		// delete it since it may be in use. Nothing useful would come from
		// updating the address anyway, so we pretend that the address doesn't
		// need to be updated.
		cur = old
		return nil, nil
	}
	create := func() error {
		var err error
		if region == "" { // global
			_, err = globalClient.Insert(ctx, &computepb.InsertGlobalAddressRequest{
				AddressResource: addr,
				Project:         config.Project,
			})
		} else { // regional
			_, err = regionalClient.Insert(ctx, &computepb.InsertAddressRequest{
				AddressResource: addr,
				Project:         config.Project,
				Region:          region,
			})
		}
		return err
	}
	p := cloudPatcher{
		desc:   fmt.Sprintf("static IP address %s", *addr.Name),
		opts:   opts,
		get:    get,
		create: create,
		update: nil,
	}
	if err := p.Run(ctx); err != nil {
		return "", err
	}

	// Wait until we get an allocated IP address.
	done := func() bool {
		return cur != nil && cur.Address != nil && *cur.Address != ""
	}
	for r := retry.Begin(); !done() && r.Continue(ctx); {
		_, err := get()
		if err != nil {
			continue
		}
	}
	if done() {
		return *cur.Address, nil
	}
	return "", fmt.Errorf("timeout waiting for the IP address %q to be allocated", *addr.Name)
}
