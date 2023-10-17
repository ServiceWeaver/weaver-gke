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
	"strings"
	"text/template"
	"time"

	"github.com/ServiceWeaver/weaver-gke/internal/bench"
	"github.com/ServiceWeaver/weaver-gke/internal/gke"
	"github.com/ServiceWeaver/weaver-gke/internal/store"
	"github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/tool"
	"github.com/google/uuid"
)

type StoreSpec struct {
	Tool  string // e.g., weaver-gke, weaver-gke-local
	Flags *flag.FlagSet
	Store func(context.Context) (store.Store, error)
}

// StoreCmd returns the "store" command.
func StoreCmd(spec *StoreSpec) *tool.Command {
	const help = `Usage:
  {{.Tool}} store get <key>
  {{.Tool}} store watch <key>
  {{.Tool}} store delete <key>...
  {{.Tool}} store list
  {{.Tool}} store purge
  {{.Tool}} store bench

Flags:
  -h, --help	Print this help message.
{{.Flags}}

Examples:
  # Get every key in the store.
  for k in $({{.Tool}} store --type=<type> list); do
    {{.Tool}} store --type=<type> get "$k"
  done

  # Pretty print a JSON formatted value.
  {{.Tool}} store --type=<type> get <key> | python3 -m json.tool`
	var b strings.Builder
	t := template.Must(template.New(spec.Tool).Parse(help))
	content := struct{ Tool, Flags string }{spec.Tool, tool.FlagsHelp(spec.Flags)}
	if err := t.Execute(&b, content); err != nil {
		panic(err)
	}

	return &tool.Command{
		Name:        "store",
		Flags:       spec.Flags,
		Description: "Inspect or modify the contents of a store",
		Help:        b.String(),
		Fn:          spec.storeFn,
		Hidden:      true,
	}
}

func (s *StoreSpec) storeFn(ctx context.Context, args []string) error {
	store, err := s.Store(ctx)
	if err != nil {
		return err
	}

	if len(args) == 0 {
		return fmt.Errorf("missing command")
	}
	name := args[0]
	args = args[1:]

	commands := map[string]struct {
		minArgs int
		maxArgs int
		fn      func(args []string) error
	}{
		"get":    {1, 1, func(args []string) error { return get(ctx, store, args[0]) }},
		"watch":  {1, 1, func(args []string) error { return watch(ctx, store, args[0]) }},
		"list":   {0, 0, func(args []string) error { return list(ctx, store) }},
		"delete": {1, -1, func(args []string) error { return deleteKeys(ctx, store, args) }},
		"purge":  {0, 0, func(args []string) error { return purge(ctx, store) }},
		"bench":  {0, 0, func(args []string) error { return benchmark(ctx, s.Store) }},
	}

	command, ok := commands[name]
	if !ok {
		return fmt.Errorf("invalid command %q", name)
	}
	if len(args) < command.minArgs {
		return fmt.Errorf(
			"%q got %d arguments, wants at least %d arguments",
			name, len(args), command.minArgs)
	}
	if command.maxArgs != -1 && len(args) > command.maxArgs {
		return fmt.Errorf(
			"%q got %d arguments, wants at most %d arguments",
			name, len(args), command.maxArgs)
	}

	if err := command.fn(args); err != nil {
		return err
	}

	return nil
}

func get(ctx context.Context, s store.Store, key string) error {
	value, version, err := s.Get(ctx, key, nil)
	if err != nil {
		return err
	}
	if *version == store.Missing {
		fmt.Println("<nil>")
	} else {
		fmt.Println(value)
	}
	return nil
}

func deleteKeys(ctx context.Context, s store.Store, keys []string) error {
	for _, key := range keys {
		_, v, err := s.Get(ctx, key, nil)
		if err != nil {
			return err
		}
		if *v == store.Missing {
			fmt.Printf("Key %s does not exists in the store.\n", key)
			continue
		}
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
		fmt.Printf("Key %s was successfully deleted.\n", key)
	}
	return nil
}

func watch(ctx context.Context, s store.Store, key string) error {
	var version *store.Version
	for ctx.Err() == nil {
		value, newVersion, err := s.Get(ctx, key, version)
		if err != nil {
			return err
		}
		version = newVersion
		if *version == store.Missing {
			fmt.Println("<nil>")
		} else {
			fmt.Println(value)
		}
	}
	return ctx.Err()
}

func list(ctx context.Context, s store.Store) error {
	keys, err := s.List(ctx, store.ListOptions{})
	if err != nil {
		return err
	}

	for _, key := range keys {
		fmt.Println(key)
	}
	return nil
}

func purge(ctx context.Context, s store.Store) error {
	kubestore, ok := s.(*gke.KubeStore)
	if !ok {
		return fmt.Errorf("purge not implemented")
	}
	return kubestore.Purge(ctx)
}

func benchmark(ctx context.Context, makeStore func(context.Context) (store.Store, error)) error {
	// TODO(mwhittaker): Add flags for the fields in StoreConfig.
	logger := logging.StderrLogger(logging.Options{
		Component: "storebench",
		Weavelet:  uuid.New().String(),
	})
	config := bench.StoreConfig{
		NumKeys:      20,
		ValueSize:    500,
		ReadFraction: 0.5,
		Duration:     10 * time.Second,
		Logger:       logger,
		Parallelism:  4,
	}
	logger.Debug(fmt.Sprintf("NumKeys      : %d", config.NumKeys))
	logger.Debug(fmt.Sprintf("ValueSize    : %v", config.ValueSize))
	logger.Debug(fmt.Sprintf("ReadFraction : %0.2f", config.ReadFraction))
	logger.Debug(fmt.Sprintf("Duration     : %v", config.Duration))
	logger.Debug(fmt.Sprintf("Parallelism  : %v", config.Parallelism))

	s, err := makeStore(ctx)
	if err != nil {
		return err
	}
	stats, err := bench.BenchStore(ctx, s, config)
	if err != nil {
		return err
	}

	logger.Debug(fmt.Sprintf("Throughput         : %f ops/second", stats.Throughput))
	logger.Debug(fmt.Sprintf("Mean Read Latency  : %v/read", stats.MeanReadLatency))
	logger.Debug(fmt.Sprintf("Mean Write Latency : %v/write", stats.MeanWriteLatency))
	return nil
}
