// Code generated by "weaver generate". DO NOT EDIT.
//go:build !ignoreWeaverGen

package babysitter

import (
	"context"
	"errors"
	"github.com/ServiceWeaver/weaver"
	"github.com/ServiceWeaver/weaver/runtime/codegen"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"reflect"
)

func init() {
	codegen.Register(codegen.Registration{
		Name:  "github.com/ServiceWeaver/weaver-gke/internal/babysitter/funcLogger",
		Iface: reflect.TypeOf((*funcLogger)(nil)).Elem(),
		Impl:  reflect.TypeOf(loggerImpl{}),
		LocalStubFn: func(impl any, caller string, tracer trace.Tracer) any {
			return funcLogger_local_stub{impl: impl.(funcLogger), tracer: tracer, logBatchMetrics: codegen.MethodMetricsFor(codegen.MethodLabels{Caller: caller, Component: "github.com/ServiceWeaver/weaver-gke/internal/babysitter/funcLogger", Method: "LogBatch", Remote: false})}
		},
		ClientStubFn: func(stub codegen.Stub, caller string) any {
			return funcLogger_client_stub{stub: stub, logBatchMetrics: codegen.MethodMetricsFor(codegen.MethodLabels{Caller: caller, Component: "github.com/ServiceWeaver/weaver-gke/internal/babysitter/funcLogger", Method: "LogBatch", Remote: true})}
		},
		ServerStubFn: func(impl any, addLoad func(uint64, float64)) codegen.Server {
			return funcLogger_server_stub{impl: impl.(funcLogger), addLoad: addLoad}
		},
		ReflectStubFn: func(caller func(string, context.Context, []any, []any) error) any {
			return funcLogger_reflect_stub{caller: caller}
		},
		RefData: "",
	})
}

// weaver.InstanceOf checks.
var _ weaver.InstanceOf[funcLogger] = (*loggerImpl)(nil)

// weaver.Router checks.
var _ weaver.Unrouted = (*loggerImpl)(nil)

// Local stub implementations.

type funcLogger_local_stub struct {
	impl            funcLogger
	tracer          trace.Tracer
	logBatchMetrics *codegen.MethodMetrics
}

// Check that funcLogger_local_stub implements the funcLogger interface.
var _ funcLogger = (*funcLogger_local_stub)(nil)

func (s funcLogger_local_stub) LogBatch(ctx context.Context, a0 *protos.LogEntryBatch) (err error) {
	// Update metrics.
	begin := s.logBatchMetrics.Begin()
	defer func() { s.logBatchMetrics.End(begin, err != nil, 0, 0) }()
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		// Create a child span for this method.
		ctx, span = s.tracer.Start(ctx, "babysitter.funcLogger.LogBatch", trace.WithSpanKind(trace.SpanKindInternal))
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

	return s.impl.LogBatch(ctx, a0)
}

// Client stub implementations.

type funcLogger_client_stub struct {
	stub            codegen.Stub
	logBatchMetrics *codegen.MethodMetrics
}

// Check that funcLogger_client_stub implements the funcLogger interface.
var _ funcLogger = (*funcLogger_client_stub)(nil)

func (s funcLogger_client_stub) LogBatch(ctx context.Context, a0 *protos.LogEntryBatch) (err error) {
	// Update metrics.
	var requestBytes, replyBytes int
	begin := s.logBatchMetrics.Begin()
	defer func() { s.logBatchMetrics.End(begin, err != nil, requestBytes, replyBytes) }()

	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		// Create a child span for this method.
		ctx, span = s.stub.Tracer().Start(ctx, "babysitter.funcLogger.LogBatch", trace.WithSpanKind(trace.SpanKindClient))
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

	// Encode arguments.
	enc := codegen.NewEncoder()
	serviceweaver_enc_ptr_LogEntryBatch_fec9a5d4(enc, a0)
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
	err = dec.Error()
	return
}

// Note that "weaver generate" will always generate the error message below.
// Everything is okay. The error message is only relevant if you see it when
// you run "go build" or "go run".
var _ codegen.LatestVersion = codegen.Version[[0][20]struct{}](`

ERROR: You generated this file with 'weaver generate' v0.22.0 (codegen
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

type funcLogger_server_stub struct {
	impl    funcLogger
	addLoad func(key uint64, load float64)
}

// Check that funcLogger_server_stub implements the codegen.Server interface.
var _ codegen.Server = (*funcLogger_server_stub)(nil)

// GetStubFn implements the codegen.Server interface.
func (s funcLogger_server_stub) GetStubFn(method string) func(ctx context.Context, args []byte) ([]byte, error) {
	switch method {
	case "LogBatch":
		return s.logBatch
	default:
		return nil
	}
}

func (s funcLogger_server_stub) logBatch(ctx context.Context, args []byte) (res []byte, err error) {
	// Catch and return any panics detected during encoding/decoding/rpc.
	defer func() {
		if err == nil {
			err = codegen.CatchPanics(recover())
		}
	}()

	// Decode arguments.
	dec := codegen.NewDecoder(args)
	var a0 *protos.LogEntryBatch
	a0 = serviceweaver_dec_ptr_LogEntryBatch_fec9a5d4(dec)

	// TODO(rgrandl): The deferred function above will recover from panics in the
	// user code: fix this.
	// Call the local method.
	appErr := s.impl.LogBatch(ctx, a0)

	// Encode the results.
	enc := codegen.NewEncoder()
	enc.Error(appErr)
	return enc.Data(), nil
}

// Reflect stub implementations.

type funcLogger_reflect_stub struct {
	caller func(string, context.Context, []any, []any) error
}

// Check that funcLogger_reflect_stub implements the funcLogger interface.
var _ funcLogger = (*funcLogger_reflect_stub)(nil)

func (s funcLogger_reflect_stub) LogBatch(ctx context.Context, a0 *protos.LogEntryBatch) (err error) {
	err = s.caller("LogBatch", ctx, []any{a0}, []any{})
	return
}

// Encoding/decoding implementations.

func serviceweaver_enc_ptr_LogEntryBatch_fec9a5d4(enc *codegen.Encoder, arg *protos.LogEntryBatch) {
	if arg == nil {
		enc.Bool(false)
	} else {
		enc.Bool(true)
		enc.EncodeProto(arg)
	}
}

func serviceweaver_dec_ptr_LogEntryBatch_fec9a5d4(dec *codegen.Decoder) *protos.LogEntryBatch {
	if !dec.Bool() {
		return nil
	}
	var res protos.LogEntryBatch
	dec.DecodeProto(&res)
	return &res
}
