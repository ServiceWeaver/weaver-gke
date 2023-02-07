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

package distributor

import (
	"context"
	"fmt"
	"sort"
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	config "github.com/ServiceWeaver/weaver-gke/internal/config"
	"github.com/ServiceWeaver/weaver-gke/internal/errlist"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver/runtime/protos"
)

// This file contains code for the distributor's annealing loop.

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
func (d *Distributor) anneal(ctx context.Context) {
	// TODO(mwhittaker): Can we reduce the number of intervals? We could
	// combine the computation and application of traffic.

	// ensurePositive returns the given delta value if it is positive, or
	// a tiny positive value otherwise.
	ensurePositive := func(delta time.Duration) time.Duration {
		if delta <= 0 {
			delta = 1 * time.Nanosecond
		}
		return delta
	}

	manageAppsTicker := ticker(d.manageAppsInterval)
	computeTrafficTicker := ticker(d.computeTrafficInterval)
	applyTrafficTicker := ticker(d.applyTrafficInterval)
	detectAppliedTrafficTicker := ticker(d.detectAppliedTrafficInterval)
	defer manageAppsTicker.Stop()
	defer computeTrafficTicker.Stop()
	defer applyTrafficTicker.Stop()
	defer detectAppliedTrafficTicker.Stop()

	for {
		select {
		case <-manageAppsTicker.C:
			if err := d.ManageAppStates(ctx); err != nil {
				d.logger.Error("managing applications", err)
				continue
			}
			// Compute the traffic assignments right away, to make sure
			// app state changes have an immediate effect.
			deadline, err := d.ComputeTrafficAssignments(ctx, time.Now())
			if err != nil {
				d.logger.Error("computing traffic assignments", err)
				continue
			}
			if delta := time.Until(deadline); delta < d.computeTrafficInterval {
				computeTrafficTicker.Reset(ensurePositive(delta))
			}
		case <-detectAppliedTrafficTicker.C:
			if err := d.detectAppliedTraffic(ctx, d.detectAppliedTrafficInterval); err != nil {
				d.logger.Error("error detecting applied traffic", err)
				continue
			}
		case <-computeTrafficTicker.C:
			computeTrafficTicker.Reset(d.computeTrafficInterval)
			deadline, err := d.ComputeTrafficAssignments(ctx, time.Now())
			if err != nil {
				d.logger.Error("computing traffic assignments", err)
			}
			if delta := time.Until(deadline); delta < d.computeTrafficInterval {
				computeTrafficTicker.Reset(ensurePositive(delta))
			}

		case <-applyTrafficTicker.C:
			if d.applyTraffic == nil {
				applyTrafficTicker.Stop()
				continue
			}
			assignment, err := d.getTrafficAssignment(ctx, false /*public*/)
			if err != nil {
				d.logger.Error("applying traffic", err)
				continue
			}
			if err := d.applyTraffic(ctx, assignment); err != nil {
				d.logger.Error("applying traffic", err)
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

// manageState manages the application states and may trigger local and remote
// actions (e.g., update local state, send a deployment request to the manager).
func (d *Distributor) ManageAppStates(ctx context.Context) error {
	state, _, err := d.loadState(ctx)
	if err != nil {
		return err
	}
	var errs errlist.ErrList
	for _, app := range state.Applications {
		if err := d.mayDeployApp(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error deploying app %v: %w", app, err))
		}
		if err := d.mayCleanupApp(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error cleaning up app %v: %w", app, err))
		}
		if err := d.getListenerState(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error getting listener state for app %v: %w", app, err))
		}
		if err := d.getProcessState(ctx, app); err != nil {
			errs = append(errs, fmt.Errorf("error getting process state for app %v: %w", app, err))
		}
	}
	return errs.ErrorOrNil()
}

// mayDeployApp determines whether any versions of a given application need to
// be deployed and if so sends the deployment requests.
func (d *Distributor) mayDeployApp(ctx context.Context, app string) error {
	state, version, err := d.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	// Collect the set of all versions that need to be deployed.
	var versions []*config.GKEConfig
	var ids []string
	for _, v := range state.Versions {
		if v.Status != AppVersionState_STARTING {
			continue
		}
		versions = append(versions, v.Config)
		ids = append(ids, v.Config.Deployment.Id)
	}
	if len(versions) <= 0 {
		return nil
	}

	// Deploy collected versions.
	d.logger.Info("Deploying", "versions", ids, "application", app)
	dur := time.Duration(len(versions)) * 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()
	req := &nanny.ApplicationDeploymentRequest{
		AppName:  app,
		Versions: versions,
	}
	if err := d.manager.Deploy(ctx, req); err != nil {
		return fmt.Errorf("cannot deploy versions %v: %w", ids, err)
	}

	// Mark the versions as deployed.
	for _, appVersion := range versions {
		for _, v := range state.Versions {
			if appVersion.Deployment.Id == v.Config.Deployment.Id {
				v.Status = AppVersionState_STARTED
			}
		}
	}

	// Save the application state. If the operation fails, the persisted state
	// will diverge from the actual state. This means that if the distributor
	// crashes, we may try to re-deploy the same set of versions. That is okay,
	// however, because the manager will treat those re-deployments as no-ops.
	if _, err := d.saveAppState(ctx, app, state, version); err != nil {
		return fmt.Errorf("cannot save application state: %w", err)
	}

	d.logger.Info("Successfully deployed", "versions", ids, "application", app)
	return nil
}

// mayCleanupApp determines whether any version of a given application needs to
// be stopped or deleted and if so sends the stop or cleanup requests.
func (d *Distributor) mayCleanupApp(ctx context.Context, app string) error {
	state, version, err := d.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	// Stop versions.
	if err := d.stop(ctx, app, state); err != nil {
		return err
	}
	version, err = d.saveAppState(ctx, app, state, version)
	if err != nil {
		return fmt.Errorf("cannot save application state: %w", err)
	}

	// Delete versions.
	if err := d.delete(ctx, app, state); err != nil {
		return err
	}
	_, err = d.saveAppState(ctx, app, state, version)
	if err != nil {
		return fmt.Errorf("cannot save application state: %w", err)
	}

	return nil
}

// stop attempts to stop any application versions (via the manager) that should
// be stopped (i.e. are in the STOPPING state).
func (d *Distributor) stop(ctx context.Context, app string, state *AppState) error {
	// Compute the set of versions that need to be stopped.
	var toStop []*AppVersionState
	var ids []string
	for _, v := range state.Versions {
		if v.Status == AppVersionState_STOPPING {
			toStop = append(toStop, v)
			ids = append(ids, v.Config.Deployment.Id)
		}
	}
	if len(toStop) == 0 {
		return nil
	}

	// Stop the versions.
	dur := time.Duration(len(toStop)) * 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	d.logger.Info("Stopping", "versions", ids, "application", app)
	req := &nanny.ApplicationStopRequest{AppName: app, Versions: ids}
	if err := d.manager.Stop(ctx, req); err != nil {
		return fmt.Errorf("cannot stop versions %v: %w", ids, err)
	}
	d.logger.Info("Successfully stopped", "versions", ids, "application", app)

	// Transition the versions from STOPPING to STOPPED.
	now := time.Now()
	for _, v := range toStop {
		v.Status = AppVersionState_STOPPED
		v.StoppedTime = timestamppb.New(now)
	}
	return nil
}

// delete attempts to delete any applications (via the manager) that should be
// deleted. An application version should be deleted if it has been stopped for
// sufficiently long.
func (d *Distributor) delete(ctx context.Context, app string, state *AppState) error {
	// TODO(mwhittaker): Make this an argument to distributor.Start. We use a
	// small delay for now to make debugging easier.
	const deleteDelay = 10 * time.Second

	// Compute the set of versions that need to be deleted.
	var toDelete []*AppVersionState
	var ids []string
	for _, v := range state.Versions {
		if v.Status == AppVersionState_STOPPED && time.Since(v.StoppedTime.AsTime()) >= deleteDelay {
			toDelete = append(toDelete, v)
			ids = append(ids, v.Config.Deployment.Id)
		}
	}
	if len(toDelete) == 0 {
		return nil
	}

	// Delete the versions.
	dur := time.Duration(len(toDelete)) * 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	d.logger.Info("Deleting", "versions", ids, "application", app)
	req := &nanny.ApplicationDeleteRequest{AppName: app, Versions: ids}
	if err := d.manager.Delete(ctx, req); err != nil {
		return fmt.Errorf("cannot stop versions %v: %w", ids, err)
	}
	d.logger.Info("Successfully deleted", "versions", ids, "application", app)

	// Delete the application versions from our state.
	for _, id := range ids {
		for i, v := range state.Versions {
			if id == v.Config.Deployment.Id {
				state.DeletedVersions = append(state.DeletedVersions, id)
				state.Versions = append(state.Versions[:i], state.Versions[i+1:]...)
				break
			}
		}
	}
	// Ensure that the remaining application versions are sorted based on the
	// submission order.
	sort.Slice(state.Versions, func(i, j int) bool {
		return state.Versions[i].Order < state.Versions[j].Order
	})
	return nil
}

// earlier returns the earlier of the two provided time.
func earlier(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}

// getListenerState retrieves the latest set of listeners for the application.
func (d *Distributor) getListenerState(ctx context.Context, app string) error {
	state, version, err := d.loadAppState(ctx, app)
	if err != nil {
		return err
	}
	var errs errlist.ErrList
	for _, v := range state.Versions {
		if v.Listeners, err = d.getListeners(ctx, v.Config); err != nil {
			errs = append(errs, err)
			continue
		}
	}
	if _, err := d.saveAppState(ctx, app, state, version); err != nil {
		errs = append(errs, err)
	}
	return errs.ErrorOrNil()
}

// getProcessState retrieve the latest process state for the application
// from the manager.
func (d *Distributor) getProcessState(ctx context.Context, app string) error {
	state, version, err := d.loadAppState(ctx, app)
	if err != nil {
		return err
	}

	var errs errlist.ErrList
	for _, version := range state.Versions {
		// Get process state for the application version.
		processes, err := d.manager.GetProcessState(ctx, &nanny.ProcessStateRequest{
			AppName:   app,
			VersionId: version.Config.Deployment.Id,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		version.Processes = processes
	}

	if _, err := d.saveAppState(ctx, app, state, version); err != nil {
		errs = append(errs, err)
	}
	return errs.ErrorOrNil()
}

// hostname returns the hostname for the given listener, and the boolean value
// indicating whether the given listener is public.
func (d *Distributor) hostname(lis *protos.Listener, cfg *config.GKEConfig) (string, bool) {
	for _, pub := range cfg.PublicListener {
		if lis.Name == pub.Name {
			return pub.Hostname, true
		}
	}
	// Private.
	return fmt.Sprintf("%s.%s.%s", lis.Name, d.region, InternalDNSDomain), false
}

// ComputeTrafficAssignments computes traffic assignments across all active
// versions of the applications and returns the earliest time at which a
// version's desired traffic fraction will change.
func (d *Distributor) ComputeTrafficAssignments(ctx context.Context, now time.Time) (time.Time, error) {
	state, version, err := d.loadState(ctx)
	if err != nil {
		return time.Time{}, err
	}

	// Step 1: Load app states, advance target functions, and save states.
	type appAndVersion struct {
		app   string
		state *AppVersionState
	}
	var versions []appAndVersion
	earliestDeadline := now.Add(24 * 365 * 100 * time.Hour)
	for _, app := range state.Applications {
		appState, version, err := d.loadAppState(ctx, app)
		if err != nil {
			return time.Time{}, fmt.Errorf("load app %q state: %w", app, err)
		}

		for _, v := range appState.Versions {
			// TODO(mwhittaker): If an app is STARTING, I think it's possible
			// for its schedule to advance. We should only advance a version
			// after it has started?
			if nanny.Ready(v.Schedule) {
				if err := nanny.Advance(v.Schedule); err != nil {
					return time.Time{}, err
				}
			}
			versions = append(versions, appAndVersion{app, v})
			earliestDeadline = earlier(earliestDeadline, now.Add(nanny.Remaining(v.Schedule)))
		}

		// TODO(mwhittaker): It's possible we advance and save a schedule but
		// fail to update the aggregated traffic assignment. We should probably
		// save the aggregated traffic assignment first, and only then write
		// back the apps.
		if _, err := d.saveAppState(ctx, app, appState, version); err != nil {
			return time.Time{}, fmt.Errorf("save app %q state: %w", app, err)
		}
	}

	// Step 2: Order versions by submission id.
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].state.Order < versions[j].state.Order
	})

	// Step 3: Collate our targets and compute a traffic assignment.
	publicTargets := []*nanny.CascadeTarget{}
	privateTargets := []*nanny.CascadeTarget{}
	for _, v := range versions {
		if v.state.Status != AppVersionState_STARTED {
			// Only route traffic to STARTED versions. If the version is
			// STARTING, it may not be ready to receive traffic yet. If the
			// version is DELETING, it shouldn't receive traffic anymore.
			continue
		}
		newTarget := func() *nanny.CascadeTarget {
			return &nanny.CascadeTarget{
				Location:        d.region,
				AppName:         v.app,
				VersionId:       v.state.Config.Deployment.Id,
				Listeners:       map[string][]*protos.Listener{},
				TrafficFraction: nanny.Fraction(v.state.Schedule),
			}
		}
		publicTarget := newTarget()
		privateTarget := newTarget()
		for _, lis := range v.state.Listeners {
			host, public := d.hostname(lis, v.state.Config)
			if public {
				publicTarget.Listeners[host] = append(publicTarget.Listeners[host], lis)
			} else {
				privateTarget.Listeners[host] = append(privateTarget.Listeners[host], lis)
			}
		}
		publicTargets = append(publicTargets, publicTarget)
		privateTargets = append(privateTargets, privateTarget)
	}

	publicTraffic, err := nanny.CascadeTraffic(publicTargets)
	if err != nil {
		return time.Time{}, err
	}
	privateTraffic, err := nanny.CascadeTraffic(privateTargets)
	if err != nil {
		return time.Time{}, err
	}
	state.PublicTrafficAssignment = publicTraffic
	state.PrivateTrafficAssignment = privateTraffic

	// Step 4: Save the new state.
	if err := d.saveState(ctx, state, version); err != nil {
		return time.Time{}, fmt.Errorf("cannot save state: %w", err)
	}
	return earliestDeadline, nil
}

func (d *Distributor) detectAppliedTraffic(ctx context.Context, cadence time.Duration) error {
	counts, err := d.getMetricCounts(ctx, "serviceweaver_http_request_count", "serviceweaver_app", "serviceweaver_version", "host")
	if err != nil {
		return err
	}
	// TODO(spetrovic): Retrieve and use the error counts.

	type appVersionCount struct {
		app, version string
		count        float64
	}
	hosts := map[string][]appVersionCount{}
	for _, mc := range counts {
		if len(mc.LabelVals) != 3 { // should never happen
			return fmt.Errorf("internal error: unexpected number of label values %v", mc.LabelVals)
		}
		// Matches the ordering of labels passed to d.getMetricCounts().
		app := mc.LabelVals[0]
		version := mc.LabelVals[1]
		host := mc.LabelVals[2]
		hosts[host] = append(hosts[host], appVersionCount{
			app:     app,
			version: version,
			count:   mc.Count,
		})
	}

	// Compute the actual traffic fraction for each application version at
	// a given host.
	type appVersionHost struct {
		app, version, host string
	}
	actualTraffic := map[appVersionHost]float32{}
	for host, counts := range hosts {
		var total float64
		for _, c := range counts {
			total += c.count
		}
		if total == 0 {
			// Zero traffic for the host: it means that the per-app-version
			// traffic is zero for all app versions.
			continue
		}
		for _, c := range counts {
			traffic := float32(c.count / total)
			if traffic == 0 {
				continue
			}
			actualTraffic[appVersionHost{c.app, c.version, host}] = traffic
		}
	}

	// For each application version, compare the expected vs. actual traffic.
	const maxDivergence = float32(0.01)
	state, _, err := d.loadState(ctx)
	if err != nil {
		return err
	}
	for _, app := range state.Applications {
		appState, version, err := d.loadAppState(ctx, app)
		if err != nil {
			return fmt.Errorf("load app %q state: %w", app, err)
		}
		for _, v := range appState.Versions {
			if v.Status != AppVersionState_STARTED {
				continue
			}
			expected := nanny.Fraction(v.Schedule)
			matches := true
			for _, lis := range v.Listeners {
				host, _ := d.hostname(lis, v.Config)
				actual := actualTraffic[appVersionHost{app, v.Config.Deployment.Id, host}]
				if actual < (expected - maxDivergence) {
					matches = false
					d.logger.Debug("traffic fraction not reached", "host", host, "app", app, "version", v.Config.Deployment.Id, "want", expected, "got", actual)
				}
			}
			if matches {
				nanny.IncrementAppliedDuration(v.Schedule, cadence)
			}
		}
		_, err = d.saveAppState(ctx, app, appState, version)
		if err != nil {
			return fmt.Errorf("cannot save application state: %w", err)
		}
	}
	return nil
}
