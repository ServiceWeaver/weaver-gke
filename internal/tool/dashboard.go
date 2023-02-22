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
	"embed"
	"flag"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/ServiceWeaver/weaver/runtime/tool"
	"github.com/pkg/browser"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	funcs = template.FuncMap{
		"age": func(t *timestamppb.Timestamp) string {
			return time.Since(t.AsTime()).Truncate(time.Second).String()
		},
		"unixmilli": func(t time.Time) int64 {
			return t.UnixMilli()
		},
		"strjoin": func(strs []string) string {
			return strings.Join(strs, ",")
		},
		"shorten":           logging.Shorten,
		"shorten_component": logging.ShortenComponent,
	}

	//go:embed dashboard/home.html
	dashboardHome         string
	dashboardHomeTemplate = template.Must(template.New("home").Funcs(funcs).Parse(dashboardHome))

	//go:embed dashboard/app.html
	dashboardApp         string
	dashboardAppTemplate = template.Must(template.New("app").Funcs(funcs).Parse(dashboardApp))

	//go:embed dashboard/app_version.html
	dashboardAppVersion         string
	dashboardAppVersionTemplate = template.Must(template.New("app_version").Funcs(funcs).Parse(dashboardAppVersion))

	// TODO(mwhittaker): Avoid having two copies of main.css.
	//go:embed assets/*
	assets embed.FS
)

// Links contains links to display on a status page. If a link is empty, the
// link is omitted.
type Links struct {
	Logs    string // link to logs
	Metrics string // link to metrics
	Traces  string // link to traces
}

// A Command is a labeled terminal command that a user can run. We show these
// commands on the dashboard so that users can copy and run them.
type Command struct {
	Label   string // e.g., cat logs
	Command string // e.g., weaver gke logs '--version=="12345678"'
}

// DashboardSpec configures the command returned by DashboardCmd.
type DashboardSpec struct {
	Tool  string        // e.g., weaver-gke, weaver-gke-local
	Flags *flag.FlagSet // command line flags

	// Controller returns the HTTP address of the controller and an HTTP client
	// that we can use to contact the controller.
	Controller func(context.Context) (string, *http.Client, error)

	Init func(ctx context.Context) error // initializes the dashboard

	AppLinks           func(ctx context.Context, app string) (Links, error)          // app links
	DeploymentLinks    func(ctx context.Context, app, version string) (Links, error) // version links
	AppCommands        func(app string) []Command                                    // app commands
	DeploymentCommands func(depId string) []Command                                  // deployment commands
}

// DashboardCmd implements the "weaver dashboard" command. "weaver dashboard"
// hosts a webpage that returns information about the current set of Service Weaver
// applications.
func DashboardCmd(spec *DashboardSpec) *tool.Command {
	return &tool.Command{
		Name:        "dashboard",
		Flags:       spec.Flags,
		Description: "Get the dashboard of all Service Weaver apps",
		Help: fmt.Sprintf(`Usage:
  %s dashboard

Flags:
  -h, --help	Print this help message.
%s`, spec.Tool, tool.FlagsHelp(spec.Flags)),
		Fn: spec.dashboardFn,
	}
}

func (s *DashboardSpec) dashboardFn(ctx context.Context, _ []string) error {
	if s.Init != nil {
		if err := s.Init(ctx); err != nil {
			return err
		}
	}
	addr, client, err := s.Controller(ctx)
	if err != nil {
		return err
	}
	srv := &server{
		spec:             s,
		controllerAddr:   addr,
		controllerClient: client,
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":0")
	if err != nil {
		return err
	}
	dashboardURL := fmt.Sprintf("http://localhost:%d", lis.Addr().(*net.TCPAddr).Port)
	go func() {
		// Wait until the server responds and then open the dashboard in
		// the browser.
		for r := retry.Begin(); r.Continue(ctx); {
			if _, err := http.Get(dashboardURL); err == nil {
				break
			}
		}
		fmt.Fprintln(os.Stderr, "Dashboard available at:", dashboardURL)
		browser.OpenURL(dashboardURL)
	}()
	http.HandleFunc("/", srv.homeHandler)
	http.HandleFunc("/favicon.ico", http.NotFound)
	http.HandleFunc("/app", srv.appHandler)
	http.HandleFunc("/version", srv.appVersionHandler)
	http.Handle("/assets/", http.FileServer(http.FS(assets)))
	return http.Serve(lis, nil /*handler*/)
}

type server struct {
	spec             *DashboardSpec
	controllerAddr   string
	controllerClient *http.Client
}

func (s *server) homeHandler(w http.ResponseWriter, r *http.Request) {
	status := &controller.Status{}
	if err := protomsg.Call(r.Context(), protomsg.CallArgs{
		Client:  s.controllerClient,
		Addr:    s.controllerAddr,
		URLPath: controller.StatusURL,
		Request: &controller.StatusRequest{}, // all apps and versions
		Reply:   status,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sortAppsAndVersions(status)
	content := struct {
		Tool   string
		Status *controller.Status
	}{
		s.spec.Tool,
		status,
	}
	if err := dashboardHomeTemplate.Execute(w, content); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) appHandler(w http.ResponseWriter, r *http.Request) {
	app, err := getParam(r, "app")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status := &controller.Status{}
	if err := protomsg.Call(r.Context(), protomsg.CallArgs{
		Client:  s.controllerClient,
		Addr:    s.controllerAddr,
		URLPath: controller.StatusURL,
		Request: &controller.StatusRequest{App: app},
		Reply:   status,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(status.Apps) == 0 {
		http.Error(w, "internal error: too few apps returned", http.StatusInternalServerError)
		return
	}
	if len(status.Apps) > 1 {
		http.Error(w, "internal error: too many apps returned", http.StatusInternalServerError)
		return
	}
	sortAppsAndVersions(status)

	links, err := s.spec.AppLinks(r.Context(), app)
	if err != nil {
		http.Error(w, "internal error: cannot get links", http.StatusInternalServerError)
		return
	}

	content := struct {
		Tool         string
		App          string
		GlobalStatus *controller.Status
		Status       *controller.AppStatus
		Links        Links
		Commands     []Command
		Traffic      chart
	}{
		s.spec.Tool,
		app,
		status,
		status.Apps[0],
		links,
		s.spec.AppCommands(app),
		trafficGraph(status.Apps[0])["global"],
	}
	if err := dashboardAppTemplate.Execute(w, content); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// A chart represents a traffic chart.
type chart struct {
	Title    string    // title of the chart
	Location string    // location of the traffic
	Min      time.Time // minimum time to show
	Max      time.Time // maximum time to show
	Lines    []line    // lines on the chart
}

// A line represents a line in a traffic chart.
type line struct {
	Label  string  //  line label
	Color  string  // color of the line
	Points []point // points on the line
}

// A point represents a point on a traffic chart.
type point struct {
	Time     time.Time // x-value
	Fraction float32   // y-value
}

// destutter removes all points that are immediately preceded by a point with
// the same traffic fraction.
func destutter(ps []point) []point {
	var destuttered []point
	for _, p := range ps {
		if len(destuttered) == 0 || p.Fraction != destuttered[len(destuttered)-1].Fraction {
			destuttered = append(destuttered, p)
		}
	}
	return destuttered
}

// trafficGraph computes a projected traffic graph for every location,
// including a "global" region.
func trafficGraph(app *controller.AppStatus) map[string]chart {
	// Gather the list of versions, ordered by submission.
	sort.Slice(app.Versions, func(i, j int) bool {
		return app.Versions[i].SubmissionId < app.Versions[j].SubmissionId
	})
	versions := make([]string, len(app.Versions))
	for i, v := range app.Versions {
		versions[i] = v.Id
	}

	// Gather the list of locations.
	seen := map[string]bool{}
	for _, p := range app.ProjectedTraffic.Projections {
		for _, a := range p.Traffic.HostAssignment[app.App].Allocs {
			seen[a.Location] = true
		}
	}
	locations := maps.Keys(seen)
	sort.Strings(locations)

	// Gather the traffic fractions, grouped by location and version.
	points := map[string]map[string][]point{"global": {}}
	for _, loc := range locations {
		points[loc] = map[string][]point{}
	}
	for _, proj := range app.ProjectedTraffic.Projections {
		globals := map[string]float32{}
		for _, loc := range locations {
			for _, v := range versions {
				p := point{proj.Time.AsTime(), 0}
				for _, a := range proj.Traffic.HostAssignment[app.App].Allocs {
					if a.Location != loc || a.VersionId != v {
						continue
					}
					p.Fraction = a.TrafficFraction
				}
				globals[v] += p.Fraction
				points[loc][v] = append(points[loc][v], p)
			}
		}
		for v, f := range globals {
			p := point{proj.Time.AsTime(), f}
			points["global"][v] = append(points["global"][v], p)
		}
	}

	// Destutter every set of points.
	for loc, vs := range points {
		for v, ps := range vs {
			points[loc][v] = destutter(ps)
		}
	}

	// Find the minimum and maximum time.
	min := time.Now().Add(100 * 24 * 365 * time.Hour)
	var max time.Time
	for _, vs := range points {
		for _, ps := range vs {
			for _, p := range ps {
				if p.Time.Before(min) {
					min = p.Time
				}
				if p.Time.After(max) {
					max = p.Time
				}
			}
		}
	}

	// Form the charts.
	charts := map[string]chart{}
	for loc, vs := range points {
		chart := chart{
			Title:    fmt.Sprintf("%s (%s) Rollout", app.App, loc),
			Location: loc,
			Min:      min,
			Max:      max,
		}
		// Color the lines.
		colors := []string{
			"#4e79a7",
			"#f28e2c",
			"#e15759",
			"#76b7b2",
			"#59a14f",
			"#edc949",
			"#af7aa1",
			"#ff9da7",
			"#9c755f",
			"#bab0ab",
		}
		for i, v := range versions {
			line := line{
				Label:  logging.Shorten(v),
				Points: vs[v],
				Color:  colors[i%len(colors)],
			}
			chart.Lines = append(chart.Lines, line)
		}
		charts[loc] = chart
	}
	return charts
}

func (s *server) appVersionHandler(w http.ResponseWriter, r *http.Request) {
	app, err := getParam(r, "app")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	version, err := getParam(r, "version")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status := &controller.Status{}
	if err := protomsg.Call(r.Context(), protomsg.CallArgs{
		Client:  s.controllerClient,
		Addr:    s.controllerAddr,
		URLPath: controller.StatusURL,
		Request: &controller.StatusRequest{App: app, Version: version},
		Reply:   status,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(status.Apps) != 1 {
		http.Error(w, "internal error: too many apps returned", http.StatusInternalServerError)
		return
	}
	if len(status.Apps[0].Versions) != 1 {
		http.Error(w, "internal error: too many versions returned", http.StatusInternalServerError)
		return
	}
	sortAppsAndVersions(status)
	links, err := s.spec.DeploymentLinks(r.Context(), app, version)
	if err != nil {
		http.Error(w, "internal error: cannot get links", http.StatusInternalServerError)
		return
	}
	content := struct {
		Tool         string
		GlobalStatus *controller.Status
		Status       *controller.AppVersionStatus
		Links        Links
		Commands     []Command
	}{
		s.spec.Tool,
		status,
		status.Apps[0].Versions[0],
		links,
		s.spec.DeploymentCommands(version),
	}
	if err := dashboardAppVersionTemplate.Execute(w, content); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getParam(r *http.Request, name string) (string, error) {
	paramValues, ok := r.URL.Query()[name]
	if !ok || len(paramValues) != 1 {
		return "", fmt.Errorf("invalid parameter %q value in URL %q", name, r.URL)
	}
	return paramValues[0], nil
}

// sortAppsAndVersions alphabetically sorts the apps in the provided status and
// sorts the versions within each app by submission id.
func sortAppsAndVersions(status *controller.Status) {
	sort.Slice(status.Apps, func(i, j int) bool {
		return status.Apps[i].App < status.Apps[j].App
	})
	for _, app := range status.Apps {
		sort.Slice(app.Versions, func(i, j int) bool {
			return app.Versions[i].SubmissionId < app.Versions[j].SubmissionId
		})
	}
}
