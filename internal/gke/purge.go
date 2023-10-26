// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	sync "sync"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1beta2"
	"cloud.google.com/go/artifactregistry/apiv1beta2/artifactregistrypb"
	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	gkehub "cloud.google.com/go/gkehub/apiv1beta1"
	"cloud.google.com/go/gkehub/apiv1beta1/gkehubpb"
	privateca "cloud.google.com/go/security/privateca/apiv1"
	"cloud.google.com/go/security/privateca/apiv1/privatecapb"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/iterator"
)

// Purge deletes all resources created by Service Weaver in the given GCP
// project. It returns an error if there are some resources that cannot
// be deleted.
// TODO: Update IAM permissions to not point to the deleted resources.
func Purge(ctx context.Context, config CloudConfig) error {
	var mu sync.Mutex
	var errs []error
	var waits []func() error
	join := func() {
		if len(waits) == 0 {
			return
		}
		var wg sync.WaitGroup
		wg.Add(len(waits))
		for _, wait := range waits {
			wait := wait
			go func() {
				defer wg.Done()
				if err := wait(); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		waits = waits[:0]
	}

	// Start deleting the clusters, if necessary.
	if wait, err := deleteConfigCluster(ctx, config); err != nil {
		errs = append(errs, err)
	} else if wait != nil {
		waits = append(waits, wait)
	}
	appClusters, err := getApplicationClusters(ctx, config)
	if err != nil {
		errs = append(errs, err)
	} else {
		for _, cluster := range appClusters {
			if err := deleteFleetMembership(ctx, config, cluster.Name, cluster.Location); err != nil {
				errs = append(errs, err)
			}
			if wait, err := deleteCluster(ctx, config, cluster.Name, cluster.Location); err != nil {
				errs = append(errs, err)
			} else if wait != nil {
				waits = append(waits, wait)
			}
		}
	}

	// Wait for the clusters to be deleted, as a lot of other resource deletions
	// cannot proceed while the clusters have a hold on those resources.
	join()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Delete cluster-related resources.
	if err := deleteGlobalIPAddress(ctx, config, gatewayIPAddressName); err != nil {
		errs = append(errs, err)
	}
	for _, cluster := range appClusters {
		if err := deleteRegionalIPAddress(ctx, config, gatewayIPAddressName, cluster.Location); err != nil {
			errs = append(errs, err)
		}

		if wait, err := deleteSubnet(ctx, config, subnetName, cluster.Location); err != nil {
			errs = append(errs, err)
		} else if wait != nil {
			waits = append(waits, wait)
		}

	}

	// Delete the internal DNS zone.
	if err := handleDNSZone(ctx, config, managedDNSZoneName); err != nil {
		errs = append(errs, err)
	}

	// Delete custom roles.
	for _, role := range []string{sslRole, iamPolicyRole} {
		if err := deleteCustomIAMRole(ctx, config, role); err != nil {
			errs = append(errs, err)
		}
	}

	// Delete IAM service accounts.
	for _, account := range []string{
		controllerIAMServiceAccount,
		distributorIAMServiceAccount,
		managerIAMServiceAccount,
		applicationIAMServiceAccount,
	} {
		if err := deleteIAMServiceAccount(ctx, config, account); err != nil {
			errs = append(errs, err)
		}
	}

	// Delete SSL certificates.
	certs, err := getSSLCertificates(ctx, config)
	if err != nil {
		errs = append(errs, err)
	}
	for _, cert := range certs {
		if wait, err := deleteSSLCertificate(ctx, config, cert); err != nil {
			errs = append(errs, err)
		} else {
			waits = append(waits, wait)
		}
	}

	// Start deleting the artifacts repository.
	if wait, err := deleteRepository(ctx, config); err != nil {
		errs = append(errs, err)
	} else if waits != nil {
		waits = append(waits, wait)
	}

	// Start deleting the certificate authority and the associated CA pool.
	if wait, err := handleCA(ctx, config); err != nil {
		errs = append(errs, err)
	} else if wait != nil {
		waits = append(waits, wait)
	}

	// Wait for the remaining deletions.
	join()

	return errors.Join(errs...)
}

// deleteRepository deletes the Service Weaver artifacts repository.
func deleteRepository(ctx context.Context, config CloudConfig) (func() error, error) {
	path := fmt.Sprintf("projects/%s/locations/%s/repositories/%s",
		config.Project, buildLocation, dockerRepoName)

	fmt.Fprintf(os.Stderr, "Fetching artifacts repository %q in region %s... ", dockerRepoName, buildLocation)
	client, err := artifactregistry.NewClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	_, err = client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{
		Name: path,
	})
	if isNotFound(err) {
		client.Close()
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	}
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}

	fmt.Fprintf(os.Stderr, "Starting async deletion... ")
	op, err := client.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{
		Name: path,
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		if !op.Done() {
			fmt.Fprintf(os.Stderr, "Waiting for the deletion of the artifacts repository %q in region %s\n", dockerRepoName, buildLocation)
			if err := op.Wait(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting artifacts repository %q in region %s\n", dockerRepoName, buildLocation)
				return err
			}
		}
		fmt.Fprintf(os.Stderr, "Done deleting artifacts repository %q in region %s\n", dockerRepoName, buildLocation)
		return nil
	}, nil
}

// deleteConfigCluster deletes the Service Weaver configuration cluster.
func deleteConfigCluster(ctx context.Context, config CloudConfig) (func() error, error) {
	if err := deleteFleetMembership(ctx, config, ConfigClusterName, ConfigClusterRegion); err != nil {
		return nil, err
	}
	exists, err := hasCluster(ctx, config, ConfigClusterName, ConfigClusterRegion)
	if err != nil {
		return nil, err
	}
	if !exists { // already deleted
		return nil, nil
	}
	return deleteCluster(ctx, config, ConfigClusterName, ConfigClusterRegion)
}

// getApplicationClusters returns the list of Service Weaver application clusters.
func getApplicationClusters(ctx context.Context, config CloudConfig) ([]*containerpb.Cluster, error) {
	fmt.Fprintf(os.Stderr, "Fetching Service Weaver application clusters... ")
	client, err := container.NewClusterManagerClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	defer client.Close()
	reply, err := client.ListClusters(ctx, &containerpb.ListClustersRequest{
		Parent: fmt.Sprintf("projects/%s/locations/-", config.Project),
	})
	if err != nil {
		return nil, doErr(err)
	}
	var clusters []*containerpb.Cluster
	for _, c := range reply.Clusters {
		if c.Name != applicationClusterName || !strings.Contains(c.Description, descriptionData) {
			// Not a Service Weaver cluster.
			continue
		}
		clusters = append(clusters, c)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return clusters, nil
}

// deleteClusters deletes the cluster with the given name in the given region.
func deleteCluster(ctx context.Context, config CloudConfig, cluster, region string) (func() error, error) {
	fmt.Fprintf(os.Stderr, "Starting async deletion of cluster %s in region %s... ", cluster, region)
	client, err := container.NewClusterManagerClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/clusters/%s", config.Project, region, cluster),
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		fmt.Fprintf(os.Stderr, "Waiting for the deletion of cluster %q in region %s\n", cluster, region)
		if err := waitClusterOp(ctx, config, client, region, op); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting cluster %q in region %s\n", cluster, region)
			return err
		}
		fmt.Fprintf(os.Stderr, "Done deleting cluster %q in region %s\n", cluster, region)
		return nil
	}, nil
}

// deleteFleetMembership deletes the fleet membership for the given cluster in
// the given region.
func deleteFleetMembership(ctx context.Context, config CloudConfig, cluster, region string) error {
	fmt.Fprintf(os.Stderr, "Fetching fleet membership for cluster %s in region %s... ", cluster, region)
	id := fmt.Sprintf("%s-%s", cluster, region)
	parent := path.Join("projects", config.Project, "locations", "global")
	path := path.Join(parent, "memberships", id)
	client, err := gkehub.NewGkeHubMembershipClient(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	defer client.Close()

	// Check if the membership has already been deleted.
	_, err = client.GetMembership(ctx, &gkehubpb.GetMembershipRequest{
		Name: path,
	})
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Not found\n")
		return nil
	}
	if err != nil {
		return doErr(err)
	}

	// Delete the membership.
	fmt.Fprintf(os.Stderr, "Deleting... ")
	op, err := client.DeleteMembership(ctx, &gkehubpb.DeleteMembershipRequest{
		Name:  path,
		Force: true,
	})
	if err != nil {
		return doErr(err)
	}
	if err := op.Wait(ctx); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteGlobalIPAddress deletes the global static IP address with the given name.
func deleteGlobalIPAddress(ctx context.Context, config CloudConfig, ipAddr string) error {
	fmt.Fprintf(os.Stderr, "Fetching global static IP address %q... ", ipAddr)
	client, err := compute.NewGlobalAddressesRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	defer client.Close()

	// Check if the IP address has already been deleted.
	_, err = client.Get(ctx, &computepb.GetGlobalAddressRequest{
		Address: ipAddr,
		Project: config.Project,
	})
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil
	}
	if err != nil {
		return doErr(err)
	}

	// Delete the IP address.
	fmt.Fprintf(os.Stderr, "Deleting... ")
	op, err := client.Delete(ctx, &computepb.DeleteGlobalAddressRequest{
		Address: ipAddr,
		Project: config.Project,
	})
	if err != nil {
		return doErr(err)
	}
	if err := op.Wait(ctx); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteRegionalIPAddress deletes the static IP address with the given name in
// the given region.
func deleteRegionalIPAddress(ctx context.Context, config CloudConfig, ipAddr, region string) error {
	fmt.Fprintf(os.Stderr, "Fetching static IP address %q in region %s... ", ipAddr, region)
	client, err := compute.NewAddressesRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	defer client.Close()

	// See if the IP address has already been deleted.
	_, err = client.Get(ctx, &computepb.GetAddressRequest{
		Address: ipAddr,
		Project: config.Project,
		Region:  region,
	})
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil
	}
	if err != nil {
		return doErr(err)
	}

	// Delete the IP address.
	fmt.Fprintf(os.Stderr, "Deleting... ")
	op, err := client.Delete(ctx, &computepb.DeleteAddressRequest{
		Address: ipAddr,
		Project: config.Project,
		Region:  region,
	})
	if err != nil {
		return doErr(err)
	}
	if err := op.Wait(ctx); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteCustomIAMRole deletes the custom IAM role with the given name.
func deleteCustomIAMRole(ctx context.Context, config CloudConfig, role string) error {
	path := fmt.Sprintf("projects/%s/roles/%s", config.Project, role)

	// Check if the role has already been deleted.
	fmt.Fprintf(os.Stderr, "Fetching custom role %q... ", role)
	iamService, err := iam.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	service := iam.NewProjectsService(iamService).Roles
	getCall := service.Get(path)
	getCall.Context(ctx)
	r, err := getCall.Do()
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil
	} else if err != nil {
		return doErr(err)
	} else if r.Deleted {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil
	}

	// Delete the role.
	fmt.Fprintf(os.Stderr, "Deleting... ")
	delCall := service.Delete(path)
	delCall.Context(ctx)
	if _, err := delCall.Do(); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteIAMServiceAccount deletes the IAM service account with the given name.
func deleteIAMServiceAccount(ctx context.Context, config CloudConfig, account string) error {
	email := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", account, config.Project)
	path := path.Join("projects", config.Project, "serviceAccounts", email)

	// Check if the service account has already been deleted.
	fmt.Fprintf(os.Stderr, "Feetching IAM service account %q... ", account)
	iamService, err := iam.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	service := iam.NewProjectsService(iamService).ServiceAccounts
	getCall := service.Get(path)
	getCall.Context(ctx)
	if _, err := getCall.Do(); isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil
	} else if err != nil {
		return doErr(err)
	}

	// Delete the service account.
	fmt.Fprintf(os.Stderr, "Deleting... ")
	call := service.Delete(path)
	if _, err := call.Do(); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// handleCA handles the deletion of the Service Weaver certificate authority
// and its associated certificate authority pool.
func handleCA(ctx context.Context, config CloudConfig) (func() error, error) {
	path := fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificateAuthorities/%s", config.Project, caLocation, caPoolName, caName)

	ca, err := getCA(ctx, config, path)
	if err != nil {
		return nil, err
	}
	if ca == nil {
		// CA permanently deleted: we can delete the CA pool.
		return deleteCAPool(ctx, config)
	}

	// CA not permanently deleted: we cannot delete the CA pool.
	defer fmt.Fprintf(os.Stderr,
		`NOTE: certificate authority pools can only be deleted after a 30-day
grace period. Re-run this command in 30 days, or just leave the pool active:
regardless, you *WILL NOT* incur any GCP costs from the (empty) CA pool.
`)

	switch ca.State {
	case privatecapb.CertificateAuthority_DELETED:
		// Already deleted (but not permanently): nothing to do but wait 30 days.
		return nil, nil
	case privatecapb.CertificateAuthority_ENABLED:
		// Enabled: must disable before deleting.
		if err := disableCA(ctx, config, path); err != nil {
			return nil, err
		}
		fallthrough
	default:
		return deleteCA(ctx, config, path)
	}
}

// getCA returns the Service Weaver certificate authority, if present, or
// nil otherwise.
func getCA(ctx context.Context, config CloudConfig, path string) (*privatecapb.CertificateAuthority, error) {
	fmt.Fprintf(os.Stderr, "Fetching certificate authority %q... ", caName)
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	defer client.Close()
	ca, err := client.GetCertificateAuthority(ctx, &privatecapb.GetCertificateAuthorityRequest{
		Name: path,
	})
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	}
	if err != nil {
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return ca, nil
}

// disableCA disables a certificate authority.
// REQUIRES: the certificate authority exists and is in an ENABLED state.
func disableCA(ctx context.Context, config CloudConfig, path string) error {
	fmt.Fprintf(os.Stderr, "Disabling certificate authority %q... ", caName)
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	defer client.Close()
	disableOp, err := client.DisableCertificateAuthority(ctx, &privatecapb.DisableCertificateAuthorityRequest{
		Name: path,
	})
	if err != nil {
		return doErr(err)
	}
	if _, err := disableOp.Wait(ctx); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteCA deletes the certificate authority.
// REQUIRES: the certificate authority exists and is not in a DELETED state.
func deleteCA(ctx context.Context, config CloudConfig, path string) (func() error, error) {
	fmt.Fprintf(os.Stderr, "Deleting certificate authority %q... ", caName)
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	op, err := client.DeleteCertificateAuthority(ctx, &privatecapb.DeleteCertificateAuthorityRequest{
		Name: path,
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		if !op.Done() {
			fmt.Fprintf(os.Stderr, "Waiting for the deletion of the certificate authority %q\n", caName)
			if _, err := op.Wait(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting certificate authority %q: %v\n", caName, err)
				return err
			}
		}
		fmt.Fprintf(os.Stderr, "Done deleting certificate authority %q\n", caName)
		return nil
	}, nil
}

// deleteCAPool deletes the Service Weaver certificate authority pool.
// REQUIRES: the pool doesn't contain any certificate authorities.
func deleteCAPool(ctx context.Context, config CloudConfig) (func() error, error) {
	path := fmt.Sprintf("projects/%s/locations/%s/caPools/%s", config.Project, caLocation, caPoolName)

	fmt.Fprintf(os.Stderr, "Fetching certificate authority pool %q... ", caPoolName)
	client, err := privateca.NewCertificateAuthorityClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	_, err = client.GetCaPool(ctx, &privatecapb.GetCaPoolRequest{Name: path})
	if isNotFound(err) {
		client.Close()
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	} else if err != nil {
		client.Close()
		return nil, doErr(err)
	}

	fmt.Fprintf(os.Stderr, "Done. Starting async deletion... ")
	op, err := client.DeleteCaPool(ctx, &privatecapb.DeleteCaPoolRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/caPools/%s", config.Project, caLocation, caPoolName),
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		if !op.Done() {
			fmt.Fprintf(os.Stderr, "Waiting for the deletion of the certificate authority pool %q\n", caPoolName)
			if err := op.Wait(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting certificate authority pool %q: %v\n", caPoolName, err)
				return err
			}
		}
		fmt.Fprintf(os.Stderr, "Done deleting certificate authority pool %q\n", caPoolName)
		return nil
	}, nil
}

// handleDNSZone handles the deletion of the given Service Weaver DNS zone
// and its DNS records.
func handleDNSZone(ctx context.Context, config CloudConfig, zone string) error {
	z, err := getDNSZone(ctx, config, zone)
	if err != nil {
		return err
	}
	if z == nil { // already deleted
		return nil
	}

	// Delete DNS record sets inside the DNS zone, as the zone must be empty
	// before it can be deleted.
	records, err := getDNSRecordSets(ctx, config, zone)
	if err != nil {
		return nil
	}
	if len(records) == 0 { // already deleted
		return nil
	}
	var errs []error
	for _, r := range records {
		if err := deleteDNSRecordSet(ctx, config, r, z.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errors.Join(errs...)
	}

	return deleteDNSZone(ctx, config, zone)
}

// getDNSZone returns the DNS zone with the given name or nil if the zone
// has already been deleted.
func getDNSZone(ctx context.Context, config CloudConfig, zone string) (*dns.ManagedZone, error) {
	fmt.Fprintf(os.Stderr, "Fetching DNS managed zone %q... ", zone)
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	service := dns.NewManagedZonesService(dnsService)
	getCall := service.Get(config.Project, zone)
	getCall.Context(ctx)
	z, err := getCall.Do()
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	}
	if err != nil {
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return z, nil
}

// getDNSRecordSets returns the DNS type A record sets in the DNS zone
// with the given name, or nil if all record sets have been deleted.
func getDNSRecordSets(ctx context.Context, config CloudConfig, zone string) ([]string, error) {
	fmt.Fprintf(os.Stderr, "Fetching DNS record sets for zone %q... ", zone)
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	service := dns.NewResourceRecordSetsService(dnsService)
	listCall := service.List(config.Project, zone)
	listCall.Context(ctx)
	reply, err := listCall.Do()
	if isNotFound(err) {
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	}
	if err != nil {
		return nil, doErr(err)
	}
	var records []string
	for _, r := range reply.Rrsets {
		if r.Type != "A" {
			continue
		}
		records = append(records, r.Name)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return records, nil
}

// deleteDNSRecordSet deletes the DNS type A record set with the given name
// in the given DNS zone.
func deleteDNSRecordSet(ctx context.Context, config CloudConfig, recordSet, zone string) error {
	fmt.Fprintf(os.Stderr, "Deleting DNS record set %q for zone %s... ", recordSet, zone)
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	service := dns.NewResourceRecordSetsService(dnsService)
	call := service.Delete(config.Project, zone, recordSet, "A")
	call.Context(ctx)
	_, err = call.Do()
	if err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// deleteDNSZone deletes the DNS zone with the given name.
// REQUIRES: the zone doesn't have any record sets in it.
func deleteDNSZone(ctx context.Context, config CloudConfig, zone string) error {
	fmt.Fprintf(os.Stderr, "Deleting DNS managed zone %q... ", zone)
	dnsService, err := dns.NewService(ctx, config.ClientOptions()...)
	if err != nil {
		return doErr(err)
	}
	service := dns.NewManagedZonesService(dnsService)
	call := service.Delete(config.Project, zone)
	call.Context(ctx)
	if err := call.Do(); err != nil {
		return doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return nil
}

// getSSLCertificates returns the list of all Service Weaver SSL certificates.
func getSSLCertificates(ctx context.Context, config CloudConfig) ([]string, error) {
	fmt.Fprintf(os.Stderr, "Fetching Service Weaver SSL certificates... ")
	client, err := compute.NewSslCertificatesRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	it := client.List(ctx, &computepb.ListSslCertificatesRequest{
		Project: config.Project,
	})
	var certs []string
	pager := iterator.NewPager(it, 1000 /*pageSize*/, "" /*pageToken*/)
	for {
		var page []*computepb.SslCertificate
		newPageToken, err := pager.NextPage(&page)
		if err != nil {
			return nil, doErr(err)
		}
		for _, cert := range page {
			if cert.Description == nil || cert.Name == nil ||
				!strings.Contains(*cert.Description, descriptionData) {
				// Not a Service Weaver-generated certificate.
				continue
			}
			certs = append(certs, *cert.Name)
		}
		if newPageToken == "" { // last page
			break
		}
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return certs, nil
}

// deleteSSLCertificate deletes the SSL certificate with the given name.
func deleteSSLCertificate(ctx context.Context, config CloudConfig, cert string) (func() error, error) {
	fmt.Fprintf(os.Stderr, "Starting async deletion of SSL certificate %q... ", cert)
	client, err := compute.NewSslCertificatesRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}

	op, err := client.Delete(ctx, &computepb.DeleteSslCertificateRequest{
		Project:        config.Project,
		SslCertificate: cert,
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		if !op.Done() {
			fmt.Fprintf(os.Stderr, "Waiting for the deletion of SSL certificate %q\n", cert)
			if err := op.Wait(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting SSL certificate %q: %v\n", cert, err)
				return err
			}
		}
		fmt.Fprintf(os.Stderr, "Done deleting SSL certificate %q\n", cert)
		return nil
	}, nil
}

// deleteSubnet deletes the sub-network with the given name in the given region.
func deleteSubnet(ctx context.Context, config CloudConfig, subnet, region string) (func() error, error) {
	fmt.Fprintf(os.Stderr, "Fetching subnet %q in region %s... ", subnet, region)
	client, err := compute.NewSubnetworksRESTClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, doErr(err)
	}
	_, err = client.Get(ctx, &computepb.GetSubnetworkRequest{
		Region:     region,
		Subnetwork: subnet,
		Project:    config.Project,
	})
	if isNotFound(err) {
		client.Close()
		fmt.Fprintf(os.Stderr, "Already deleted\n")
		return nil, nil
	}
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}

	fmt.Fprintf(os.Stderr, "Starting async deletion... ")
	op, err := client.Delete(ctx, &computepb.DeleteSubnetworkRequest{
		Region:     region,
		Subnetwork: subnet,
		Project:    config.Project,
	})
	if err != nil {
		client.Close()
		return nil, doErr(err)
	}
	fmt.Fprintf(os.Stderr, "Done\n")
	return func() error {
		defer client.Close()
		if !op.Done() {
			fmt.Fprintf(os.Stderr, "Waiting for the deletion of the subnet %q in region %s\n", subnet, region)
			if err := op.Wait(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "Error deleting subnet %q in region %s\n", subnet, region)
				return err
			}
		}
		fmt.Fprintf(os.Stderr, "Done deleting subnet %q in region %s\n", subnet, region)
		return nil
	}, nil
}

func doErr(err error) error {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	return err
}
