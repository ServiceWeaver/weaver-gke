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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1beta2"
	"cloud.google.com/go/artifactregistry/apiv1beta2/artifactregistrypb"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	privateca "cloud.google.com/go/security/privateca/apiv1"
	"cloud.google.com/go/security/privateca/apiv1/privatecapb"
	dproto "github.com/ServiceWeaver/weaver-gke/internal/proto"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
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
		// Check if gRPC status gives us the code.
		if gErr.GRPCStatus().Code() == codes.NotFound {
			return true
		}

		// Check if HTTP status gives us the code.
		if gErr.HTTPCode() == 404 {
			return true
		}

		// apierror.APIError interface doesn't allow us to check the error code.
		// Our best bet is to unwrap the underlying error, which should be of
		// type *googleapi.Error, and check the code below.
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
			_, err := client.Get(ctx, &computepb.GetSubnetworkRequest{
				Region:     region,
				Subnetwork: *sub.Name,
				Project:    config.Project,
			})
			if err != nil {
				return nil, err
			}
			// Update API doesn't exist and Patch API is too limited.  We also
			// cannot delete the subnet resource, since it may be in use (by a
			// load-balancer). For this reason, we pretend that there has been
			// no change to the existing value and don't bother applying the
			// update.
			return nil, nil
		},
		create: func() error {
			_, err := client.Insert(ctx, &computepb.InsertSubnetworkRequest{
				Region:             region,
				SubnetworkResource: sub,
				Project:            config.Project,
			})
			return err
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
			_, err := client.Get(ctx, &computepb.GetSslCertificateRequest{
				SslCertificate: *cert.Name,
				Project:        config.Project,
			})
			if err != nil {
				return nil, err
			}

			// Neither Update or a Patch API exists, so we cannot update
			// the value. Moreover, the delete-then-recreate mechanism doesn't
			// work for SSL certificates, as they cannot be deleted while in
			// use by the load-balancers. For this reason, we pretend that there
			// has been no change to the existing value and don't bother
			// applying the update.
			return nil, nil
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
	}.Run(ctx)
}

// patchRepository updates the Repository with the new configuration.
func patchRepository(ctx context.Context, config CloudConfig, opts patchOptions, rep *artifactregistrypb.Repository) error {
	client, err := artifactregistry.NewClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	create := func() error {
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
	}
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
			modif, err := dproto.Merge(old, rep, nil)
			if err != nil {
				return nil, err
			}
			if !modif { // no change from the existing value
				return nil, nil
			}
			return old, nil
		},
		create: create,
		update: func(_ interface{}) error {
			// The update API doesn't work for fields that matter, like
			// "format". For this reason, we delete and then re-create the
			// repository.
			op, err := client.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{
				Name: rep.Name,
			})
			if err != nil {
				return err
			}
			if err := op.Wait(ctx); err != nil {
				return err
			}
			return create()
		},
	}.Run(ctx)
}

// waitClusterOp waits for the given cluster operation to complete.
func waitClusterOp(ctx context.Context, config CloudConfig, client *container.ClusterManagerClient, location string, op *containerpb.Operation) error {
	var doneError error
	isDone := func(op *containerpb.Operation) bool {
		switch op.Status {
		case containerpb.Operation_DONE:
			return true
		case containerpb.Operation_ABORTING:
			doneError = fmt.Errorf("operation %s failed: %v", op.OperationType, op.Error)
			return true
		default:
			return false
		}
	}
	if isDone(op) {
		return doneError
	}
	for r := retry.Begin(); r.Continue(ctx); {
		if op, err := client.GetOperation(ctx, &containerpb.GetOperationRequest{
			Name: fmt.Sprintf("projects/%s/locations/%s/operations/%s",
				config.Project, location, op.Name),
		}); err == nil && isDone(op) {
			return doneError
		}
	}
	return fmt.Errorf("timeout waiting for operation %s: %w", op.OperationType, ctx.Err())
}

// patchCluster updates the cluster with the new configuration and waits
// for that cluster to be ready.
func patchCluster(ctx context.Context, config CloudConfig, opts patchOptions, location string, c *containerpb.Cluster) error {
	client, err := container.NewClusterManagerClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
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
			return waitClusterOp(ctx, config, client, location, op)
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
			return waitClusterOp(ctx, config, client, location, op)
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
	return cloudPatcher{
		desc: fmt.Sprintf("DNS managed zone %s", zone.Name),
		opts: opts,
		get: func() (interface{}, error) {
			call := service.Get(config.Project, zone.Name)
			call.Context(ctx)
			_, err := call.Do()
			if err != nil {
				return nil, err
			}

			// The Update or Patch API doesn't exist for a DNS zone, so there is
			// no point in updating. Moreover, the delete-then-recreate
			// mechanism is painful for DNS zones, as it requires deletion
			// (and successive re-creation) of DNS records. For this
			// reason, we pretend that there has been no change to the existing
			// value and don't bother applying the update.
			return nil, nil
		},
		create: func() error {
			call := service.Create(config.Project, zone)
			call.Context(ctx)
			// TODO(spetrovic): The return value of call.Do() is a *dns.ManagedZone
			// that will contain the Google nameservers that host the managed
			// zone. Propagate this information to the caller.
			_, err := call.Do()
			return err
		},
	}.Run(ctx)
}

// patchDNSRecordSet updates the record set in the given managed zone with
// the new configuration.
func patchDNSRecordSet(ctx context.Context, config CloudConfig, opts patchOptions, dnsZone string, rs *dns.ResourceRecordSet) error {
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	service := dns.NewResourceRecordSetsService(dnsService)
	create := func() error {
		call := service.Create(config.Project, dnsZone, rs)
		call.Context(ctx)
		_, err := call.Do()
		return err
	}
	return cloudPatcher{
		desc: fmt.Sprintf("DNS record set %s", rs.Name),
		opts: opts,
		get: func() (interface{}, error) {
			call := service.Get(config.Project, dnsZone, rs.Name, rs.Type)
			call.Context(ctx)
			old, err := call.Do()
			if err != nil {
				return nil, err
			}

			// See if the old value differs from the new.
			oldJSON, err := json.Marshal(old)
			if err != nil {
				return nil, err
			}
			newJSON, err := json.Marshal(rs)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(oldJSON, newJSON) {
				return nil, nil
			}
			return old, nil
		},
		create: create,
		update: func(_ interface{}) error {
			// The Update or Patch API doesn't exist for a DNS record, so we
			// have to delete and then re-create it.
			call := service.Delete(config.Project, dnsZone, rs.Name, rs.Type)
			call.Context(ctx)
			if _, err := call.Do(); err != nil {
				return err
			}
			return create()
		},
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
				if old.Description != account.Description {
					return false
				}
				if old.DisplayName != account.DisplayName {
					return false
				}
				return true
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

			if old.Deleted {
				// Role has been deleted previously - undelete it.
				call := service.Undelete(name, &iam.UndeleteRoleRequest{})
				call.Context(ctx)
				old, err = call.Do()
				if err != nil {
					return nil, err
				}
			}

			// See if the old value differs from the new in the fields that
			// matter.
			same := func() bool {
				if old.Description != role.Description {
					return false
				}
				if old.Stage != role.Stage {
					return false
				}
				if old.Title != role.Title {
					return false
				}
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
			call.UpdateMask("description,stage,title,included_permissions")
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

// patchCAPool updates the certificate authority pool with the new configuration.
func patchCAPool(ctx context.Context, config CloudConfig, opts patchOptions, pool *privatecapb.CaPool) error {
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	parent := path.Dir(path.Dir(pool.Name))
	name := path.Base(pool.Name)
	return cloudPatcher{
		desc: fmt.Sprintf("certificate authority pool %s", name),
		opts: opts,
		get: func() (interface{}, error) {
			_, err := client.GetCaPool(ctx, &privatecapb.GetCaPoolRequest{
				Name: pool.Name,
			})
			if err != nil {
				return nil, err
			}

			// The Update API for the CA pool doesn't actually apply any
			// updates, at least to the fields that matter like "tier".
			// Moreover, the delete-then-recreate mechanism doesn't work for
			// CA pools, as they cannot be deleted for 30 days after their
			// last certificate authorities have been deleted. For this reason,
			// we pretend that there has been no change to the existing value
			// and don't bother applying the update.
			return nil, nil
		},
		create: func() error {
			op, err := client.CreateCaPool(ctx, &privatecapb.CreateCaPoolRequest{
				Parent:   parent,
				CaPoolId: name,
				CaPool:   pool,
			})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		},
	}.Run(ctx)
}

// patchCA updates the certificate authority with the new configuration.
func patchCA(ctx context.Context, config CloudConfig, opts patchOptions, ca *privatecapb.CertificateAuthority) error {
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return err
	}
	defer client.Close()
	parent := path.Dir(path.Dir(ca.Name))
	name := path.Base(ca.Name)
	return cloudPatcher{
		desc: fmt.Sprintf("certificate authority %s", name),
		opts: opts,
		get: func() (interface{}, error) {
			old, err := client.GetCertificateAuthority(ctx, &privatecapb.GetCertificateAuthorityRequest{
				Name: ca.Name,
			})
			if err != nil {
				return nil, err
			}

			if old.State == privatecapb.CertificateAuthority_DELETED {
				// CA has been deleted previously: undelete it.
				op, err := client.UndeleteCertificateAuthority(ctx, &privatecapb.UndeleteCertificateAuthorityRequest{
					Name: ca.Name,
				})
				if err != nil {
					return nil, err
				}
				_, err = op.Wait(ctx)
				if err != nil {
					return nil, err
				}
			}

			// The Update API for the CA doesn't actually apply any updates,
			// at least to "config", and "lifetime" fields. Moreover, the
			// delete-then-recreate mechanism doesn't work for certificate
			// authorities, as they linger around with their old configuration
			// for 30 days. For this reason, we pretend that there has been no
			// change to the existing value and don't bother applying the
			// update.
			return nil, nil
		},
		create: func() error {
			op, err := client.CreateCertificateAuthority(ctx, &privatecapb.CreateCertificateAuthorityRequest{
				Parent:                 parent,
				CertificateAuthorityId: name,
				CertificateAuthority:   ca,
			})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		},
	}.Run(ctx)
}
