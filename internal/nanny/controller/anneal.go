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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/errlist"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ticker returns a new time.Ticker with the provided duration. If the
// provided duration is less than or equal to zero, the returned ticker
// will never fire.
func ticker(d time.Duration) *time.Ticker {
	if d > 0 {
		return time.NewTicker(d)
	}

	// Theoretically, we could sleep for 100 years between calling NewTicker
	// and calling Stop, but practically it's okay.
	t := time.NewTicker(24 * 365 * 100 * time.Hour)
	t.Stop()
	return t
}

// anneal runs an infinite loop that repeatedly attempts to move the actual
// state of the system towards the expected state of the system.
func (c *controller) anneal(ctx context.Context, fetchAssignmentsInterval time.Duration, applyAssignmentInterval time.Duration, manageAppInterval time.Duration) {
	tickerFetchAssignments := ticker(fetchAssignmentsInterval)
	tickerApplyAssignment := ticker(applyAssignmentInterval)
	tickerManageApps := ticker(manageAppInterval)
	defer tickerFetchAssignments.Stop()
	defer tickerApplyAssignment.Stop()
	defer tickerManageApps.Stop()
	for {
		select {
		case <-tickerFetchAssignments.C:
			if err := c.fetchTrafficAssignments(ctx); err != nil {
				c.logger.Error("error fetching traffic assignments", err)
			}
		case <-tickerApplyAssignment.C:
			if err := c.applyTrafficAssignment(ctx); err != nil {
				c.logger.Error("error applying traffic assignments", err)
			}
		case <-tickerManageApps.C:
			if err := c.manageState(ctx); err != nil {
				c.logger.Error("error managing applications", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// fetchTrafficAssignments retrieves the latest traffic assignment information
// from the distributors.
func (c *controller) fetchTrafficAssignments(ctx context.Context) error {
	state, version, err := c.loadState(ctx)
	if err != nil {
		return err
	}

	// Send a request to get the latest traffic assignment for public listeners
	// from each distributor.
	var errs errlist.ErrList
	for _, d := range state.Distributors {
		reply, err := c.distributor(d.Location.DistributorAddr).GetPublicTrafficAssignment(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to get assignment from distributor %s: %v", d.Location.Name, err))
			continue
		}

		// Update the latest traffic assignment received.
		d.TrafficAssignment = reply

		// Attach distributor location to the assignment.
		if d.TrafficAssignment != nil {
			for _, hostAssignment := range d.TrafficAssignment.HostAssignment {
				for _, alloc := range hostAssignment.Allocs {
					alloc.Location = d.Location.Name
				}
			}
		}
	}
	// Even if we are not able to save the latest traffic assignment for a
	// distributor d, it should be okay, because the merged assignment will
	// contain an older assignment for d which will eventually be updated.
	if err := c.saveState(ctx, state, version); err != nil {
		errs = append(errs, err)
	}
	return errs.ErrorOrNil()
}

// applyTrafficAssignment computes the latest assignment information and applies it.
func (c *controller) applyTrafficAssignment(ctx context.Context) error {
	if c.applyTraffic == nil {
		return nil
	}

	// Apply the newly computed traffic assignment.
	assignment, err := c.traffic(ctx)
	if err != nil {
		return err
	}
	return c.applyTraffic(ctx, assignment)
}

// manageState manages the application states and may trigger local and remote
// actions (e.g., send new rollout requests to distributors, clean local state).
//
// TODO(rgrandl): cleanup finished rollouts.
func (c *controller) manageState(ctx context.Context) error {
	state, _, err := c.loadState(ctx)
	if err != nil {
		return err
	}

	var errs errlist.ErrList
	for app := range state.Applications {
		if err := c.getDistributorState(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error getting distributor state: %v", err))
		}
		if err := c.advanceRollouts(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error advancing rollout: %v", err))
		}
		if err := c.maySendDistributionRequests(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error sending distribution requests: %v", err))
		}
		if err := c.maySendCleanupRequests(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error sending cleanup requests: %v", err))
		}
	}
	return errs.ErrorOrNil()
}

// getDistributorState retrieves the latest application state from all the
// distributors that manage the application's versions.
func (c *controller) getDistributorState(ctx context.Context, app string) error {
	state, version, err := c.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	// Gather all of the locations with a distributor.
	locations := map[string]bool{}
	for _, v := range state.Versions {
		for loc := range v.Distributors {
			locations[loc] = true
		}
	}

	// Retrieve the latest application state from every distributor.
	var errs errlist.ErrList
	for loc := range locations {
		addr, err := c.getDistributorAddr(ctx, loc)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to get the distributor address for location %s: %v", addr, err))
			continue
		}

		req := &nanny.ApplicationStateAtDistributorRequest{AppName: app}
		reply, err := c.distributor(addr).GetApplicationState(ctx, req)
		if err != nil {
			errs = append(errs, fmt.Errorf("error retrieving the state for app %s from distributor %s: %v", app, addr, err))
			continue
		}
		c.applyLocationUpdate(ctx, loc, state, reply)
	}
	if err := c.saveAppState(ctx, app, state, version); err != nil {
		errs = append(errs, err)
	}
	return errs.ErrorOrNil()
}

// applyLocationUpdate updates the application state with a state update
// received from a distributor in a single location.
func (c *controller) applyLocationUpdate(ctx context.Context, location string, state *AppState, update *nanny.ApplicationStateAtDistributor) {
	// Update active versions.
	for _, dv := range update.VersionState {
		v, found := state.Versions[dv.VersionId]
		if !found {
			c.logger.Debug("version not found", "version", dv.VersionId)
			continue
		}
		distributor := v.Distributors[location]
		distributor.Processes = dv.Processes

		if v.WaveIdx != distributor.WaveIdx ||
			distributor.Status == AppVersionDistributorStatus_ROLLED_OUT ||
			distributor.Status == AppVersionDistributorStatus_DELETING ||
			distributor.Status == AppVersionDistributorStatus_DELETED {
			// This version is not currently rolling out in this location.
			continue
		}

		if dv.RolloutCompleted {
			// This version has been fully rolled out in this location.
			distributor.Status = AppVersionDistributorStatus_ROLLED_OUT
		}

		// Check if every location in the current wave has fully rolled out.
		waveRolledOut := true
		for _, d := range v.Distributors {
			if d.WaveIdx == v.WaveIdx && d.Status != AppVersionDistributorStatus_ROLLED_OUT {
				// TODO(mwhittaker): Is it possible for a status to be DELETING
				// or DELETED here? We check for it above.
				waveRolledOut = false
				break
			}
		}
		if waveRolledOut {
			v.TimeWaveRolledOut = timestamppb.Now()
		}
	}

	// Update deleted versions.
	for _, id := range update.DeletedVersions {
		v, found := state.Versions[id]
		if !found {
			continue
		}
		distributor := v.Distributors[location]
		if distributor.Status != AppVersionDistributorStatus_DELETING {
			continue
		}
		distributor.Status = AppVersionDistributorStatus_DELETED

		// If every location for this version has been deleted, then we can
		// garbage collect the version from our state.
		allDeleted := true
		for _, d := range v.Distributors {
			if d.Status != AppVersionDistributorStatus_DELETED {
				allDeleted = false
				break
			}
		}
		if allDeleted {
			delete(state.Versions, v.Config.Deployment.Id)
		}
	}
}

// advanceRollouts advances every version from one wave to the next, if the
// current wave has been fully rolled out for sufficiently long.
func (c *controller) advanceRollouts(ctx context.Context, app string) error {
	state, version, err := c.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	for _, v := range state.Versions {
		if v.TimeWaveRolledOut == nil {
			// The current wave hasn't finished rolling out yet.
			continue
		}

		wave := v.RolloutStrategy.Waves[v.WaveIdx]
		if time.Since(v.TimeWaveRolledOut.AsTime()) < wave.WaitTime.AsDuration() {
			// The current wave hasn't been rolled out for long enough.
			continue
		}

		v.WaveIdx++
		v.TimeWaveStarted = nil
		v.TimeWaveRolledOut = nil

		if rolloutCompleted(v) {
			// There aren't any more waves; the rollout is done. Mark all
			// versions with lower submission ids for cleanup.
			for _, other := range state.Versions {
				if other.SubmissionId < v.SubmissionId {
					for _, d := range other.Distributors {
						if d.Status != AppVersionDistributorStatus_DELETED {
							d.Status = AppVersionDistributorStatus_DELETING
						}
					}
				}
			}
			continue
		}

		// Register all of the distributors in the next wave to start.
		v.TimeWaveStarted = timestamppb.Now()
		for loc := range v.RolloutStrategy.Waves[v.WaveIdx].TargetFunctions {
			v.Distributors[loc].Status = AppVersionDistributorStatus_STARTING
		}
	}

	return c.saveAppState(ctx, app, state, version)
}

// maySendDistributionRequests determines whether new rollout requests
// should be sent to the distributors, and send the requests (if any).
func (c *controller) maySendDistributionRequests(ctx context.Context, app string) error {
	state, version, err := c.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	requests := c.createDistributionRequests(app, state)
	var errs errlist.ErrList
	for loc, req := range requests {
		var versions []string
		for _, v := range req.Requests {
			versions = append(versions, v.Config.Deployment.Id)
		}

		c.logger.Info("Distributing", "versions", versions, "app", req.AppName, "location", loc)
		if err := c.sendDistributionRequest(ctx, loc, state, req); err != nil {
			errs = append(errs, fmt.Errorf("cannot distribute versions %v of application %q to location %v: %v", versions, req.AppName, loc, err))
			continue
		}
		c.logger.Info("Successfully distributed", "versions", versions, "app", req.AppName, "location", loc)
	}
	if err := c.saveAppState(ctx, app, state, version); err != nil {
		errs = append(errs, err)
	}
	return errs.ErrorOrNil()
}

// createDistributionRequests creates distribution requests for all distributors
// that are active in the current wave. Each entry in the returned map contains
// all application versions that haven't yet been successfully started at the
// given distributor.
func (c *controller) createDistributionRequests(app string, state *AppState) map[string]*nanny.ApplicationDistributionRequest {
	reqs := map[string]*nanny.ApplicationDistributionRequest{}
	for _, v := range state.Versions {
		// Ignore versions that are fully rolled out or are currently waiting for
		// rollout validation.
		if rolloutCompleted(v) || !waveRolloutInProgress(v) {
			continue
		}

		for loc, d := range v.Distributors {
			if v.WaveIdx != d.WaveIdx || d.Status != AppVersionDistributorStatus_STARTING {
				continue
			}
			if _, found := reqs[loc]; !found {
				reqs[loc] = &nanny.ApplicationDistributionRequest{AppName: app}
			}
			req := &nanny.VersionDistributionRequest{
				Config:       v.Config,
				TargetFn:     v.RolloutStrategy.Waves[v.WaveIdx].TargetFunctions[loc],
				SubmissionId: v.SubmissionId,
			}
			reqs[loc].Requests = append(reqs[loc].Requests, req)
		}
	}
	return reqs
}

// sendDistributionRequest sends the given distribution request to the
// distributor in the given location.
func (c *controller) sendDistributionRequest(ctx context.Context, loc string, state *AppState, req *nanny.ApplicationDistributionRequest) error {
	distrAddr, err := c.getDistributorAddr(ctx, loc)
	if err != nil {
		return err
	}

	if err := c.distributor(distrAddr).Distribute(ctx, req); err != nil {
		return err
	}

	for _, v := range req.Requests {
		state.Versions[v.Config.Deployment.Id].Distributors[loc].Status = AppVersionDistributorStatus_ROLLING_OUT
	}
	return nil
}

// maySendCleanupRequests determines whether cleanup requests should be sent
// to the distributors, and send the requests (if any).
func (c *controller) maySendCleanupRequests(ctx context.Context, app string) error {
	state, _, err := c.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	// Form the requests.
	reqs := map[string]*nanny.ApplicationCleanupRequest{}
	for _, v := range state.Versions {
		if !shouldCleanup(v) {
			continue
		}
		for loc := range v.Distributors {
			if _, found := reqs[loc]; !found {
				reqs[loc] = &nanny.ApplicationCleanupRequest{AppName: app}
			}
			reqs[loc].Versions = append(reqs[loc].Versions, v.Config.Deployment.Id)
		}
	}

	// Send the requests.
	var errs errlist.ErrList
	for loc, req := range reqs {
		addr, err := c.getDistributorAddr(ctx, loc)
		if err != nil {
			errs = append(errs, fmt.Errorf("unable to get the distributor address for loc %s: %v", loc, err))
			continue
		}

		if err := c.distributor(addr).Cleanup(ctx, req); err != nil {
			errs = append(errs, fmt.Errorf("error issuing a cleanup request for location %q: %v", loc, err))
			continue
		}
	}
	return errs.ErrorOrNil()
}

// getDistributorAddr returns the address of the distributor in the provided
// location.
func (c *controller) getDistributorAddr(ctx context.Context, location string) (string, error) {
	// TODO(mwhittaker): If distributor addresses don't change over time, then
	// we can cache them to avoid performing a bunch of store reads.
	state, _, err := c.loadState(ctx)
	if err != nil {
		return "", err
	}

	d, found := state.Distributors[location]
	if !found {
		return "", fmt.Errorf("unable to retrieve the address for "+
			"distributor at location: %s", location)
	}
	return d.Location.DistributorAddr, nil
}

// rolloutCompleted returns whether the application version has been
// completeley rolled out across all locations.
func rolloutCompleted(v *AppVersionState) bool {
	return int(v.WaveIdx) >= len(v.RolloutStrategy.Waves)
}

// waveRolloutInProgress returns true if one wave is in the middle of being
// rolled out.
func waveRolloutInProgress(v *AppVersionState) bool {
	return v.TimeWaveRolledOut == nil
}

// shouldCleanup returns whether every location in the version is either
// deleted or should be deleted.
func shouldCleanup(v *AppVersionState) bool {
	for _, d := range v.Distributors {
		if d.Status != AppVersionDistributorStatus_DELETING &&
			d.Status != AppVersionDistributorStatus_DELETED {
			return false
		}
	}
	return true
}
