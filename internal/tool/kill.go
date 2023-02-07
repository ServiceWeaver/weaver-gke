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
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"text/template"

	"github.com/ServiceWeaver/weaver-gke/internal/nanny/controller"
	"github.com/ServiceWeaver/weaver/runtime/protomsg"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

type KillSpec struct {
	Tool  string        // e.g., weaver-gke, weaver-gke-local
	Flags *flag.FlagSet // command line flags
	force *bool         // the --force flag

	// Controller returns the HTTP address of the controller and an HTTP client
	// that we can use to contact the controller.
	Controller func(context.Context) (string, *http.Client, error)
}

// KillCmd implements the "weaver kill" command. "weaver kill" kills a Service Weaver
// application.
func KillCmd(spec *KillSpec) *tool.Command {
	spec.force = spec.Flags.Bool("force", false, "Kill without prompt")
	const help = `Usage:
  {{.Tool}} kill [--force] <app>

Flags:
  -h, --help	Print this help message.
{{.Flags}}

Description:
  "{{.Tool}} kill" immediately kills every active deployment of a Service Weaver
  application. You should likely not run "{{.Tool}} kill" on any
  applications that are running in production. "{{.Tool}} kill" should
  mostly be used to kill applications during development.`
	var b strings.Builder
	t := template.Must(template.New(spec.Tool).Parse(help))
	content := struct{ Tool, Flags string }{spec.Tool, tool.FlagsHelp(spec.Flags)}
	if err := t.Execute(&b, content); err != nil {
		panic(err)
	}

	return &tool.Command{
		Name:        "kill",
		Flags:       spec.Flags,
		Description: "Kill a Service Weaver app",
		Help:        b.String(),
		Fn:          spec.killFn,
	}
}

func (k *KillSpec) killFn(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: %s kill <app>", k.Tool)
	}
	app := args[0]

	// Confirm with the user.
	if !*k.force {
		fmt.Printf(`WARNING: You are about to kill every active deployment of the %q app.
The deployments will be killed immediately and irrevocably. Are you sure you
want to proceed?

Enter (y)es to continue: `, app)
		reader := bufio.NewReader(os.Stdin)
		text, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		text = text[:len(text)-1] // strip the trailing "\n"
		text = strings.ToLower(text)
		if !(text == "y" || text == "yes") {
			fmt.Println("")
			fmt.Println("Kill aborted.")
			return nil
		}
	}

	addr, client, err := k.Controller(ctx)
	if err != nil {
		return err
	}

	return protomsg.Call(ctx, protomsg.CallArgs{
		Client:  client,
		Addr:    addr,
		URLPath: controller.KillURL,
		Request: &controller.KillRequest{App: app},
	})
}
