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
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/tool"
	"github.com/google/pprof/profile"
)

type ProfileSpec struct {
	Tool       string        // e.g., weaver-gke, weaver-gke-local
	Flags      *flag.FlagSet // command line flags
	GetProfile func(ctx context.Context, req *protos.GetProfileRequest) (*protos.GetProfileReply, error)

	// Controller returns the HTTP address of the controller and an HTTP client
	// that can be used to contact the controller.
	Controller func(context.Context) (string, *http.Client, error)

	// Values initialized from flags.
	typ         string
	cpuDuration time.Duration
	version     string
}

// ProfileCmd implements the "weaver profile" command. "weaver profile" profiles
// a running Service Weaver application.
func ProfileCmd(spec *ProfileSpec) *tool.Command {
	spec.Flags.StringVar(&spec.typ, "type", "cpu", `Profile type; "cpu" or "heap"`)
	spec.Flags.DurationVar(&spec.cpuDuration, "duration", 30*time.Second, "Duration of cpu profiles")
	spec.Flags.StringVar(&spec.version, "version", "", "Application version")
	const help = `Usage:
  {{.Tool}} profile [options] <app>

Flags:
  -h, --help	Print this help message.
{{.Flags}}

Description:
  '{{.Tool}} profile' profiles a deployed application and fetches
  its profile to the local machine. '{{.Tool}} profile' writes the
  profile to a file that can be passed to pprof.`
	var b strings.Builder
	t := template.Must(template.New(spec.Tool).Parse(help))
	content := struct{ Tool, Flags string }{spec.Tool, tool.FlagsHelp(spec.Flags)}
	if err := t.Execute(&b, content); err != nil {
		panic(err)
	}
	return &tool.Command{
		Name:        "profile",
		Flags:       spec.Flags,
		Description: "Profile a running Service Weaver application",
		Help:        b.String(),
		Fn:          spec.profileFn,
	}
}

func (p *ProfileSpec) profileFn(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: %s profile [options] <app>", p.Tool)
	}
	app := args[0]
	addr, client, err := p.Controller(ctx)
	if err != nil {
		return err
	}
	req := &nanny.GetProfileRequest{
		AppName:   app,
		VersionId: p.version,
	}
	switch p.typ {
	case "cpu":
		req.ProfileType = protos.ProfileType_CPU
		req.CpuDurationNs = int64(p.cpuDuration / time.Nanosecond)
	case "heap":
		req.ProfileType = protos.ProfileType_Heap
	default:
		return fmt.Errorf("invalid profile type %q; want %q or %q", p.typ, "cpu", "heap")
	}

	// Start the profile request.
	reply := &protos.GetProfileReply{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		err = protomsg.Call(ctx, protomsg.CallArgs{
			Client:  client,
			Addr:    addr,
			URLPath: controller.RunProfilingURL,
			Request: req,
			Reply:   reply,
		})
	}()

	// Wait for the profile to finish. If the profile is going to take a long
	// time, show a spinner to the user, so they know how long to wait.
	if p.typ == "cpu" && p.cpuDuration > time.Second {
		spinner("Profiling in progress...", p.cpuDuration, done)
	} else {
		<-done
	}

	if len(reply.Data) == 0 {
		if err == nil {
			return fmt.Errorf("empty profile data")
		}
		// NOTE: This branch should never be taken.
		return fmt.Errorf("cannot create profile: %v", err)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, "Partial profile data received: the profile may not be accurate. Error:", err)
	}
	prof, err := profile.ParseData(reply.Data)
	if err != nil {
		return fmt.Errorf("error parsing profile: %w", err)
	}

	// Save the profile in a file.
	name := fmt.Sprintf("serviceweaver_%s_%s_profile_*.pb.gz", app, p.typ)
	f, err := os.CreateTemp(os.TempDir(), name)
	if err != nil {
		return fmt.Errorf("saving profile: %w", err)
	}
	name = f.Name()
	if err := prof.Write(f); err != nil {
		f.Close()
		os.Remove(name)
		return fmt.Errorf("writing profile: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("writing profile: %w", err)
	}
	fmt.Println(name)
	return nil
}

// spinner prints a spinning progress bar to stderr:
//
//	⠏ [8s/10s] Profiling in progress...
//
// The spinner prints the provided message until done is is closed. The
// provided duration is an estimate of when done should be closed. The idea for
// this spinner was taken from [1].
//
// [1]: https://github.com/sindresorhus/cli-spinners
func spinner(msg string, duration time.Duration, done chan struct{}) {
	start := time.Now()
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	i := 0

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			fmt.Fprintln(os.Stderr)
			return

		case <-ticker.C:
			since := time.Since(start).Truncate(time.Second)
			fmt.Fprintf(os.Stderr, "%s [%s/%s] %s\r", frames[i], since, duration, msg)
			i = (i + 1) % len(frames)
		}
	}
}
