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

package tool

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"golang.org/x/exp/maps"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime/colors"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

var dimColor = colors.Color256(245) // a light gray

type StatusSpec struct {
	Tool  string        // e.g., weaver-gke, weaver-gke-local
	Flags *flag.FlagSet // command line flags

	// Controller returns the HTTP address of the controller and an HTTP client
	// that we can use to contact the controller.
	Controller func(context.Context) (string, *http.Client, error)
}

// StatusCmd implements the "weaver status" command. "weaver status" returns
// information about the current set of Service Weaver applications. It is similar to
// running "git status" or "kubectl get".
func StatusCmd(spec *StatusSpec) *tool.Command {
	return &tool.Command{
		Name:        "status",
		Flags:       spec.Flags,
		Description: "Get the status of all Service Weaver apps",
		Help: fmt.Sprintf(`Usage:
  %s status

Flags:
  -h, --help	Print this help message.
%s`, spec.Tool, tool.FlagsHelp(spec.Flags)),
		Fn: spec.statusFn,
	}
}

// formatId returns a pretty-printed prefix and suffix of the provided id. Both
// prefix and suffix are colored, and the prefix is underlined.
func formatId(id string) (prefix, suffix colors.Atom) {
	short := logging.Shorten(id)
	code := colors.ColorHash(id)
	prefix = colors.Atom{S: short, Color: code, Underline: true}
	suffix = colors.Atom{S: strings.TrimPrefix(id, short), Color: code}
	return prefix, suffix
}

// deploymentsStatus pretty-prints the set of deployments.
func deploymentsStatus(w io.Writer, status *controller.Status) {
	sort.Slice(status.Apps, func(i, j int) bool {
		return status.Apps[i].App < status.Apps[j].App
	})

	t := colors.NewTabularizer(w, colors.Atom{S: "Deployments", Bold: true}, colors.PrefixDim)
	defer t.Flush()
	t.Row("APP", "DEPLOYMENT", "AGE", "STATUS")
	for _, app := range status.Apps {
		sort.Slice(app.Versions, func(i, j int) bool {
			return app.Versions[i].SubmissionId < app.Versions[j].SubmissionId
		})
		for _, dep := range app.Versions {
			prefix, suffix := formatId(dep.Id)
			// TODO(mwhittaker): Pretty print age better.
			age := time.Since(dep.SubmissionTime.AsTime()).Truncate(time.Second)
			t.Row(dep.App, colors.Text{prefix, suffix}, age, dep.Status)
		}
	}
}

// componentsStatus pretty-prints the set of components.
func componentsStatus(w io.Writer, status *controller.Status) {
	sort.SliceStable(status.Apps, func(i, j int) bool {
		return status.Apps[i].App < status.Apps[j].App
	})

	t := colors.NewTabularizer(w, colors.Atom{S: "COMPONENTS", Bold: true}, colors.PrefixDim)
	defer t.Flush()
	t.Row("APP", "DEPLOYMENT", "LOCATION", "COMPONENT", "HEALTHY")
	for _, app := range status.Apps {
		sort.SliceStable(app.Versions, func(i, j int) bool {
			return app.Versions[i].SubmissionId < app.Versions[j].SubmissionId
		})
		for _, dep := range app.Versions {
			sort.SliceStable(dep.Processes, func(i, j int) bool {
				return dep.Processes[i].Name < dep.Processes[j].Name
			})
			sort.SliceStable(dep.Processes, func(i, j int) bool {
				return dep.Processes[i].Location < dep.Processes[j].Location
			})
			for _, proc := range dep.Processes {
				sort.Slice(proc.Components, func(i, j int) bool {
					return proc.Components[i] < proc.Components[j]
				})
				health := fmt.Sprintf("%d/%d", proc.HealthyReplicas, proc.TotalReplicas)
				for _, component := range proc.Components {
					prefix, _ := formatId(dep.Id)
					t.Row(dep.App, prefix, proc.Location, logging.ShortenComponent(component), health)
				}
			}
		}
	}
}

// trafficStatus pretty-prints the current traffic assignment.
func trafficStatus(w io.Writer, status *controller.Status) {
	deployments := map[string]int64{}
	for _, app := range status.Apps {
		for _, deployment := range app.Versions {
			deployments[deployment.Id] = deployment.SubmissionId
		}
	}

	t := colors.NewTabularizer(w, colors.Atom{S: "TRAFFIC", Bold: true}, colors.PrefixDim)
	defer t.Flush()
	t.Row("HOST", "VISIBILITY", "APP", "DEPLOYMENT", "LOCATION", "ADDRESS", "TRAFFIC FRACTION")
	show := func(traffic *nanny.TrafficAssignment, visibility string) {
		hosts := maps.Keys(traffic.HostAssignment)
		sort.Strings(hosts)
		for _, host := range hosts {
			assignment := traffic.HostAssignment[host]
			sort.SliceStable(assignment.Allocs, func(i, j int) bool {
				return assignment.Allocs[i].Location < assignment.Allocs[j].Location
			})
			sort.SliceStable(assignment.Allocs, func(i, j int) bool {
				ai, aj := assignment.Allocs[i], assignment.Allocs[j]
				return deployments[ai.VersionId] < deployments[aj.VersionId]
			})
			sort.SliceStable(assignment.Allocs, func(i, j int) bool {
				return assignment.Allocs[i].AppName < assignment.Allocs[j].AppName
			})
			for _, a := range assignment.Allocs {
				prefix, _ := formatId(a.VersionId)
				t.Row(host, visibility, a.AppName, prefix, a.Location, a.Listener.Addr, fmt.Sprint(a.TrafficFraction))
			}
		}
	}

	// Show public traffic.
	show(status.Traffic, "public")

	// Show private traffic. We have one traffic assignment per region. We
	// merge them together so that listeners for the same deployment are shown
	// next to one another.
	private := &nanny.TrafficAssignment{
		HostAssignment: map[string]*nanny.HostTrafficAssignment{},
	}
	for _, traffic := range status.PrivateTraffic {
		for host, assignment := range traffic.HostAssignment {
			merged, ok := private.HostAssignment[host]
			if !ok {
				merged = &nanny.HostTrafficAssignment{
					Allocs: []*nanny.TrafficAllocation{},
				}
				private.HostAssignment[host] = merged
			}
			merged.Allocs = append(merged.Allocs, assignment.Allocs...)
		}
	}
	show(private, "private")
}

// rolloutStatus pretty-prints the rollout schedule of every app.
func rolloutStatus(w io.Writer, status *controller.Status, p *controller.ProjectedTraffic) {
	// Gather the set of all columns.
	type col struct {
		version string
		loc     string
	}
	cols := map[col]bool{}
	for _, projection := range p.Projections {
		for _, alloc := range projection.Traffic.HostAssignment[p.App].Allocs {
			cols[col{version: alloc.VersionId, loc: alloc.Location}] = true
		}
	}

	// Get the submission id of every version.
	submissionIds := map[string]int{}
	for _, app := range status.Apps {
		for _, d := range app.Versions {
			submissionIds[d.Id] = int(d.SubmissionId)
		}
	}

	// Order the columns by location and submission id.
	ordered := []col{}
	for col := range cols {
		ordered = append(ordered, col)
	}
	sort.Slice(ordered, func(i, j int) bool {
		vi, vj := ordered[i], ordered[j]
		return submissionIds[vi.version] < submissionIds[vj.version]
	})
	sort.Slice(ordered, func(i, j int) bool {
		// TODO(mwhittaker): Sort locations by wave? It's tricky because
		// different versions of an app could be rolled out to locations in
		// different orders, though this should be rare.
		vi, vj := ordered[i], ordered[j]
		return vi.loc < vj.loc
	})

	// Print the locations.
	locs := make([]any, 1+len(ordered))
	locs[0] = ""
	for i, col := range ordered {
		if i > 0 && col.loc == ordered[i-1].loc && colors.Enabled() {
			locs[i+1] = colors.Atom{S: col.loc, Color: dimColor}
		} else {
			locs[i+1] = colors.Atom{S: col.loc}
		}
	}
	tab := colors.NewTabularizer(w, colors.Atom{S: fmt.Sprintf("ROLLOUT OF %s", p.App), Bold: true}, colors.FullDim)
	defer tab.Flush()
	tab.Row(locs...)

	// Print the shortened versions.
	shortened := make([]any, 1+len(ordered))
	shortened[0] = "TIME"
	for i, col := range ordered {
		short, _ := formatId(col.version)
		shortened[i+1] = short
	}
	tab.Row(shortened...)

	var start time.Time
	last := map[col]float32{}
	for _, projection := range p.Projections {
		// The first row shows the time. The following rows show the delta
		// from the first row.
		row := []any{}
		t := projection.Time.AsTime()
		if start.IsZero() {
			start = t
			row = append(row, t.Format(time.Stamp))
		} else {
			delta := t.Sub(start)
			row = append(row, fmt.Sprintf("%15s", "+"+delta.Truncate(time.Second).String()))
		}

		// Explode each traffic assignment into a row, with one traffic
		// fraction per column.
		fractions := map[col]float32{}
		for _, alloc := range projection.Traffic.HostAssignment[p.App].Allocs {
			fractions[col{alloc.VersionId, alloc.Location}] += alloc.TrafficFraction
		}
		if reflect.DeepEqual(last, fractions) {
			// The traffic assignment hasn't changed. Don't show the same
			// traffic assignment redundantly.
			continue
		}
		last = fractions
		data := make([]any, len(ordered))
		for i, v := range ordered {
			data[i] = fmt.Sprintf("%.2f", fractions[v])
		}
		row = append(row, data...)
		tab.Row(row...)
	}
}

func (s *StatusSpec) statusFn(ctx context.Context, _ []string) error {
	addr, client, err := s.Controller(ctx)
	if err != nil {
		return err
	}

	status := &controller.Status{}
	if err := protomsg.Call(ctx, protomsg.CallArgs{
		Client:  client,
		Addr:    addr,
		URLPath: controller.StatusURL,
		Request: &controller.StatusRequest{},
		Reply:   status,
	}); err != nil {
		return err
	}

	// TODO(mwhittaker): Fix traffic computations.
	deploymentsStatus(os.Stdout, status)
	componentsStatus(os.Stdout, status)
	trafficStatus(os.Stdout, status)
	for _, app := range status.Apps {
		rolloutStatus(os.Stdout, status, app.ProjectedTraffic)
	}

	return nil
}
