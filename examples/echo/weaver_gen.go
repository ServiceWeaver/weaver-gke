// Code generated by "weaver generate". DO NOT EDIT.
//go:build !ignoreWeaverGen

package main

import (
	"context"
	"errors"
	"github.com/ServiceWeaver/weaver"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"reflect"
)

func init() {
	codegen.Register(codegen.Registration{
		Name:  "github.com/ServiceWeaver/weaver-gke/examples/echo/Echoer",
		Iface: reflect.TypeOf((*Echoer)(nil)).Elem(),
		Impl:  reflect.TypeOf(echoer{}),
		LocalStubFn: func(impl any, caller string, tracer trace.Tracer) any {
			return echoer_local_stub{impl: impl.(Echoer), tracer: tracer, echoMetrics: codegen.MethodMetricsFor(codegen.MethodLabels{Caller: caller, Component: "github.com/ServiceWeaver/weaver-gke/examples/echo/Echoer", Method: "Echo", Remote: false})}
		},
		ClientStubFn: func(stub codegen.Stub, caller string) any {
			return echoer_client_stub{stub: stub, echoMetrics: codegen.MethodMetricsFor(codegen.MethodLabels{Caller: caller, Component: "github.com/ServiceWeaver/weaver-gke/examples/echo/Echoer", Method: "Echo", Remote: true})}
		},
		ServerStubFn: func(impl any, addLoad func(uint64, float64)) codegen.Server {
			return echoer_server_stub{impl: impl.(Echoer), addLoad: addLoad}
		},
		ReflectStubFn: func(caller func(string, context.Context, []any, []any) error) any {
			return echoer_reflect_stub{caller: caller}
		},
		RefData: "",
	})
	codegen.Register(codegen.Registration{
		Name:      "github.com/ServiceWeaver/weaver/Main",
		Iface:     reflect.TypeOf((*weaver.Main)(nil)).Elem(),
		Impl:      reflect.TypeOf(server{}),
		Listeners: []string{"echo"},
		LocalStubFn: func(impl any, caller string, tracer trace.Tracer) any {
			return main_local_stub{impl: impl.(weaver.Main), tracer: tracer}
		},
		ClientStubFn: func(stub codegen.Stub, caller string) any { return main_client_stub{stub: stub} },
		ServerStubFn: func(impl any, addLoad func(uint64, float64)) codegen.Server {
			return main_server_stub{impl: impl.(weaver.Main), addLoad: addLoad}
		},
		ReflectStubFn: func(caller func(string, context.Context, []any, []any) error) any {
			return main_reflect_stub{caller: caller}
		},
		RefData: "⟦216b0043:wEaVeReDgE:github.com/ServiceWeaver/weaver/Main→github.com/ServiceWeaver/weaver-gke/examples/echo/Echoer⟧\n⟦914f1096:wEaVeRlIsTeNeRs:github.com/ServiceWeaver/weaver/Main→echo⟧\n",
	})
}

// weaver.InstanceOf checks.
var _ weaver.InstanceOf[Echoer] = (*echoer)(nil)
var _ weaver.InstanceOf[weaver.Main] = (*server)(nil)

// weaver.Router checks.
var _ weaver.Unrouted = (*echoer)(nil)
var _ weaver.Unrouted = (*server)(nil)

// Local stub implementations.

type echoer_local_stub struct {
	impl        Echoer
	tracer      trace.Tracer
	echoMetrics *codegen.MethodMetrics
}

// Check that echoer_local_stub implements the Echoer interface.
var _ Echoer = (*echoer_local_stub)(nil)

func (s echoer_local_stub) Echo(ctx context.Context, a0 string) (r0 string, err error) {
	// Update metrics.
	begin := s.echoMetrics.Begin()
	defer func() { s.echoMetrics.End(begin, err != nil, 0, 0) }()
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		// Create a child span for this method.
		ctx, span = s.tracer.Start(ctx, "main.Echoer.Echo", trace.WithSpanKind(trace.SpanKindInternal))
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

	return s.impl.Echo(ctx, a0)
}

type main_local_stub struct {
	impl   weaver.Main
	tracer trace.Tracer
}

// Check that main_local_stub implements the weaver.Main interface.
var _ weaver.Main = (*main_local_stub)(nil)

// Client stub implementations.

type echoer_client_stub struct {
	stub        codegen.Stub
	echoMetrics *codegen.MethodMetrics
}

// Check that echoer_client_stub implements the Echoer interface.
var _ Echoer = (*echoer_client_stub)(nil)

func (s echoer_client_stub) Echo(ctx context.Context, a0 string) (r0 string, err error) {
	// Update metrics.
	var requestBytes, replyBytes int
	begin := s.echoMetrics.Begin()
	defer func() { s.echoMetrics.End(begin, err != nil, requestBytes, replyBytes) }()

	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		// Create a child span for this method.
		ctx, span = s.stub.Tracer().Start(ctx, "main.Echoer.Echo", trace.WithSpanKind(trace.SpanKindClient))
	}

	defer func() {
		// Catch and return any panics detected during encoding/decoding/rpc.
		if err == nil {
			err = codegen.CatchPanics(recover())
			if err != nil {
				err = errors.Join(weaver.RemoteCallError, err)
			}
		}

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()

	}()

	// Preallocate a buffer of the right size.
	size := 0
	size += (4 + len(a0))
	enc := codegen.NewEncoder()
	enc.Reset(size)

	// Encode arguments.
	enc.String(a0)
	var shardKey uint64

	// Call the remote method.
	requestBytes = len(enc.Data())
	var results []byte
	results, err = s.stub.Run(ctx, 0, enc.Data(), shardKey)
	replyBytes = len(results)
	if err != nil {
		err = errors.Join(weaver.RemoteCallError, err)
		return
	}

	// Decode the results.
	dec := codegen.NewDecoder(results)
	r0 = dec.String()
	err = dec.Error()
	return
}

type main_client_stub struct {
	stub codegen.Stub
}

// Check that main_client_stub implements the weaver.Main interface.
var _ weaver.Main = (*main_client_stub)(nil)

// Note that "weaver generate" will always generate the error message below.
// Everything is okay. The error message is only relevant if you see it when
// you run "go build" or "go run".
var _ codegen.LatestVersion = codegen.Version[[0][20]struct{}](`

ERROR: You generated this file with 'weaver generate' v0.22.1-0.20231019162801-c2294d1ae0e8 (codegen
version v0.20.0). The generated code is incompatible with the version of the
github.com/ServiceWeaver/weaver module that you're using. The weaver module
version can be found in your go.mod file or by running the following command.

    go list -m github.com/ServiceWeaver/weaver

We recommend updating the weaver module and the 'weaver generate' command by
running the following.

    go get github.com/ServiceWeaver/weaver@latest
    go install github.com/ServiceWeaver/weaver/cmd/weaver@latest

Then, re-run 'weaver generate' and re-build your code. If the problem persists,
please file an issue at https://github.com/ServiceWeaver/weaver/issues.

`)

// Server stub implementations.

type echoer_server_stub struct {
	impl    Echoer
	addLoad func(key uint64, load float64)
}

// Check that echoer_server_stub implements the codegen.Server interface.
var _ codegen.Server = (*echoer_server_stub)(nil)

// GetStubFn implements the codegen.Server interface.
func (s echoer_server_stub) GetStubFn(method string) func(ctx context.Context, args []byte) ([]byte, error) {
	switch method {
	case "Echo":
		return s.echo
	default:
		return nil
	}
}

func (s echoer_server_stub) echo(ctx context.Context, args []byte) (res []byte, err error) {
	// Catch and return any panics detected during encoding/decoding/rpc.
	defer func() {
		if err == nil {
			err = codegen.CatchPanics(recover())
		}
	}()

	// Decode arguments.
	dec := codegen.NewDecoder(args)
	var a0 string
	a0 = dec.String()

	// TODO(rgrandl): The deferred function above will recover from panics in the
	// user code: fix this.
	// Call the local method.
	r0, appErr := s.impl.Echo(ctx, a0)

	// Encode the results.
	enc := codegen.NewEncoder()
	enc.String(r0)
	enc.Error(appErr)
	return enc.Data(), nil
}

type main_server_stub struct {
	impl    weaver.Main
	addLoad func(key uint64, load float64)
}

// Check that main_server_stub implements the codegen.Server interface.
var _ codegen.Server = (*main_server_stub)(nil)

// GetStubFn implements the codegen.Server interface.
func (s main_server_stub) GetStubFn(method string) func(ctx context.Context, args []byte) ([]byte, error) {
	switch method {
	default:
		return nil
	}
}

// Reflect stub implementations.

type echoer_reflect_stub struct {
	caller func(string, context.Context, []any, []any) error
}

// Check that echoer_reflect_stub implements the Echoer interface.
var _ Echoer = (*echoer_reflect_stub)(nil)

func (s echoer_reflect_stub) Echo(ctx context.Context, a0 string) (r0 string, err error) {
	err = s.caller("Echo", ctx, []any{a0}, []any{&r0})
	return
}

type main_reflect_stub struct {
	caller func(string, context.Context, []any, []any) error
}

// Check that main_reflect_stub implements the weaver.Main interface.
var _ weaver.Main = (*main_reflect_stub)(nil)

