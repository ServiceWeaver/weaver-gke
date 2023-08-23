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

package gke

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	apiv2 "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/logging/logadmin"
	wlogging "github.com/ServiceWeaver/weaver/runtime/logging"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/google/cel-go/common/operators"
	"google.golang.org/api/iterator"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CloudLoggingClient is a Google Cloud Logging client. Code running on GKE can
// use a CloudLoggingClient to create loggers that log to Google Cloud Logging.
// These logs will also correctly show up in the "LOGS" tab on the GKE
// console.
type CloudLoggingClient struct {
	meta   *ContainerMetadata // metadata about the current container
	client *logging.Client    // Cloud Logging client
	logger *logging.Logger    // logs to projects/<project>/logs/serviceweaver
}

// newCloudLoggingClient returns a new CloudLoggingClient.
func newCloudLoggingClient(ctx context.Context, meta *ContainerMetadata) (*CloudLoggingClient, error) {
	client, err := logging.NewClient(ctx, meta.Project)
	if err != nil {
		return nil, err
	}
	return &CloudLoggingClient{meta, client, client.Logger("serviceweaver")}, nil
}

// Logger returns a new slog.Logger that logs to Google Cloud Logging.
func (cl *CloudLoggingClient) Logger(opts wlogging.Options) *slog.Logger {
	return slog.New(&wlogging.LogHandler{Opts: opts, Write: cl.Log})
}

// Log logs the provided entry to Google Cloud Logging.
func (cl *CloudLoggingClient) Log(entry *protos.LogEntry) {
	if entry.TimeMicros == 0 {
		entry.TimeMicros = time.Now().UnixMicro()
	}

	// Note: Keep an eye out for delays in log entries actually being
	// written to the logging service and showing up reasonably quickly. We
	// may need to periodically flush if necessary for freshness.
	cl.logger.Log(logging.Entry{
		Labels:  labelsFromEntry(entry, cl.meta),
		Payload: entry.Msg,
		Resource: &mrpb.MonitoredResource{
			Type: "k8s_container",
			Labels: map[string]string{
				"cluster_name":   cl.meta.ClusterName,
				"container_name": cl.meta.ContainerName,
				"location":       cl.meta.ClusterRegion,
				"namespace_name": cl.meta.Namespace,
				"project_id":     cl.meta.Project,
				"pod_name":       cl.meta.PodName,
			},
		},
	})
}

// Close closes the CloudLoggingClient.
func (cl *CloudLoggingClient) Close() error {
	return cl.client.Close()
}

// gcpSource is a dlogging.Source that reads log entries from Google Cloud
// Logging.
type gcpSource struct {
	config CloudConfig // Cloud configuration.
}

var _ wlogging.Source = &gcpSource{}

// LogSource returns a new dlogging.Source that reads log entries written by
// Service Weaver applications deployed on Google Cloud.
func LogSource(config CloudConfig) (*gcpSource, error) {
	return &gcpSource{config}, nil
}

// Query implements the dlogging.Source interface.
func (gs *gcpSource) Query(ctx context.Context, q wlogging.Query, follow bool) (wlogging.Reader, error) {
	if follow {
		return newGCPFollower(ctx, gs.config, q)
	}
	return newGCPCatter(ctx, gs.config, q)
}

// gcpCatter is a dlogging.Reader implementation that reads log entries using
// the Google Cloud Logging API.
type gcpCatter struct {
	ctx         context.Context
	filter      string                  // Google Cloud Logging filter
	client      *logadmin.Client        // Google Cloud Logging client
	iter        *logadmin.EntryIterator // iterator over entries
	pager       *iterator.Pager         // paged iterator made from iter
	token       string                  // page token
	page        []*logging.Entry        // current page
	noMorePages bool                    // have we fetched the final page?
	closed      bool                    // has Close() been called?
}

var _ wlogging.Reader = &gcpCatter{}

// newGCPCatter returns a new gcpCatter.
func newGCPCatter(ctx context.Context, config CloudConfig, q wlogging.Query) (*gcpCatter, error) {
	filter, err := Translate(config.Project, q)
	if err != nil {
		return nil, fmt.Errorf("error translating query %s: %v", q, err)
	}

	client, err := logadmin.NewClient(ctx, config.Project, config.ClientOptions()...)
	if err != nil {
		return nil, fmt.Errorf("error creating logadmin client: %w", err)
	}
	iter := client.Entries(ctx, logadmin.Filter(filter))
	pager := iterator.NewPager(iter, 1000, "") // Request pages of size 1000.
	catter := &gcpCatter{
		ctx:         ctx,
		filter:      filter,
		client:      client,
		iter:        iter,
		pager:       pager,
		token:       "",
		page:        []*logging.Entry{},
		noMorePages: false,
		closed:      false,
	}
	return catter, nil
}

// Read implements the dlogging.Reader interface.
func (gc *gcpCatter) Read(ctx context.Context) (*protos.LogEntry, error) {
	// TODO(mwhittaker): Respect the provided context. The Google Cloud Logging
	// API unfortunately doesn't allow you to use a different context for every
	// call to Next.
	if gc.closed {
		return nil, fmt.Errorf("closed")
	}

	if len(gc.page) == 0 && gc.noMorePages {
		return nil, io.EOF
	}

	// Refill the page. Google Cloud Logging has a quota of 60 reads per
	// minute, so we need an aggressive backoff to avoid saturating the quota
	// with retries.
	opts := retry.Options{
		BackoffMultiplier:  2,
		BackoffMinDuration: 5 * time.Second,
	}
	for r := retry.BeginWithOptions(opts); len(gc.page) == 0 && r.Continue(ctx); {
		newToken, err := gc.pager.NextPage(&gc.page)
		if (err == nil && newToken == "") || errors.Is(err, iterator.Done) {
			// There aren't any pages left, but the last call to NextPage may
			// have populated gc.page.
			gc.noMorePages = true
			if len(gc.page) == 0 {
				return nil, io.EOF
			}
			break
		} else if status.Code(err) == codes.ResourceExhausted {
			// We've hit the quota. Keep retrying with backoff. Note that once
			// gc.iter and gc.pager return an error, they will repeatedly
			// return the same error. In order to retry the operation, we have
			// to construct a new iterator and pager with our existing token.
			gc.iter = gc.client.Entries(gc.ctx, logadmin.Filter(gc.filter))
			gc.pager = iterator.NewPager(gc.iter, 1000, gc.token)
			continue
		} else if err != nil {
			return nil, err
		}
		gc.token = newToken
	}

	// Pop the first entry from the page.
	entry := gc.page[0]
	gc.page = gc.page[1:]

	// Parse the entry.
	logentry, err := entryFromLabels(entry.Labels)
	if err != nil {
		return nil, err
	}
	logentry.Msg = entry.Payload.(string)
	return logentry, nil
}

// Close implements the dlogging.Reader interface.
func (gc *gcpCatter) Close() {
	if gc.closed {
		return
	}
	gc.closed = true
	gc.client.Close()
}

// gcpFollower is a dlogging.Reader implementation that follows log entries
// using the Google Cloud Logging API.
type gcpFollower struct {
	// TODO(mwhittaker): Use Cloud Logging's TailLogEntries API. This
	// implementation polls the ListLogEntries API to watch for new log
	// entries. Using the TailLogEntries API is tricky through, as it only
	// shows new log entries. We would have to combine it with calls to
	// ListLogEntries.

	client *apiv2.Client                    // Google Cloud Logging client
	req    *loggingpb.ListLogEntriesRequest // list log entries request
	iter   *apiv2.LogEntryIterator          // iterator over returned entries
	seen   map[string]bool                  // insertIds of seen entries on last page
	closed bool                             // has Close() been called?
}

var _ wlogging.Reader = &gcpFollower{}

// newGCPFollower returns a new gcpFollower.
func newGCPFollower(ctx context.Context, config CloudConfig, q wlogging.Query) (*gcpFollower, error) {
	filter, err := Translate(config.Project, q)
	if err != nil {
		return nil, fmt.Errorf("error translating query %s: %v", q, err)
	}

	client, err := apiv2.NewClient(ctx, config.ClientOptions()...)
	if err != nil {
		return nil, fmt.Errorf("error creating apiv2 client: %w", err)
	}

	req := &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{fmt.Sprintf("projects/%s", config.Project)},
		Filter:        filter,
		PageSize:      1000,
	}
	follower := &gcpFollower{
		client: client,
		req:    req,
		iter:   client.ListLogEntries(ctx, req),
		seen:   map[string]bool{},
		closed: false,
	}
	return follower, nil
}

func (gf *gcpFollower) Read(ctx context.Context) (*protos.LogEntry, error) {
	// gf.iter iterates over pages of log entries, issuing one ListLogEntries
	// API call per page. If there are more pages to come,
	// gf.iter.PageInfo().Token is a non-empty page token that is passed to the
	// next call to ListLogEntries. If there are no more log entries to come,
	// then gf.iter.PageInfo().Token is the empty string. When we run out of
	// log entries, we poll ListLogEntries with the most recent non-empty page
	// token.

	// TODO(mwhittaker): Respect the provided context. The Google Cloud Logging
	// API unfortunately doesn't allow you to use a different context for every
	// call to Next.
	if gf.closed {
		return nil, fmt.Errorf("closed")
	}

	// Google Cloud Logging has a quota of 60 reads per minute, so we need an
	// aggressive backoff to avoid saturating the quota with retries.
	opts := retry.Options{
		BackoffMultiplier:  2,
		BackoffMinDuration: 5 * time.Second,
	}
	for r := retry.BeginWithOptions(opts); r.Continue(ctx); {
		entry, err := gf.iter.Next()
		if errors.Is(err, iterator.Done) {
			// We're out of log entries. Retry with the latest non-empty page
			// token after a short delay. We have to construct a new iterator,
			// or else the iterator will return iterator.Done immediately.
			time.Sleep(3 * time.Second)
			gf.iter = gf.client.ListLogEntries(ctx, gf.req)
			r.Reset()
			continue
		} else if status.Code(err) == codes.ResourceExhausted {
			// We've hit the quota. Keep retrying with backoff.
			gf.iter = gf.client.ListLogEntries(ctx, gf.req)
			continue
		} else if err != nil {
			return nil, err
		}

		if token := gf.iter.PageInfo().Token; token != "" {
			// We're not on the last page.
			gf.req.PageToken = token
			if len(gf.seen) > 0 {
				gf.seen = map[string]bool{}
			}
			return gf.makeEntry(entry)
		}

		if !gf.seen[entry.InsertId] {
			// We are on the last page, but we haven't seen this entry before.
			gf.seen[entry.InsertId] = true
			return gf.makeEntry(entry)
		}

		// We are on the last page, and we've seen this entry before.
		r.Reset()
	}
	panic("unreachable")
}

func (gf *gcpFollower) makeEntry(entry *loggingpb.LogEntry) (*protos.LogEntry, error) {
	logentry, err := entryFromLabels(entry.Labels)
	if err != nil {
		return nil, err
	}
	switch payload := entry.Payload.(type) {
	case *loggingpb.LogEntry_TextPayload:
		logentry.Msg = payload.TextPayload
	default:
		return nil, fmt.Errorf("unexpected payload %v", payload)
	}
	return logentry, nil
}

// Close implements the dlogging.Reader interface.
func (gf *gcpFollower) Close() {
	if gf.closed {
		return
	}
	gf.closed = true
	gf.client.Close()
}

// HasTime returns whether the provided query contains a predicate on the
// "time" field.
func HasTime(query wlogging.Query) (bool, error) {
	ast, err := wlogging.Parse(query)
	if err != nil {
		return false, fmt.Errorf("translate(%s): %w", query, err)
	}

	hasTime := false
	if err := translateExpr(io.Discard, &hasTime, ast.Expr()); err != nil {
		return false, fmt.Errorf("translate(%s): %w", query, err)
	}
	return hasTime, nil
}

// Translate translates a log query into a Google Cloud Logging query [1].
//
// [1]: https://cloud.google.com/logging/docs/view/logging-query-language
func Translate(project string, query wlogging.Query) (string, error) {
	// The translation is relatively straightforward, with the following
	// notable changes:
	//
	//     - The msg field is translated to "textPayload".
	//     - The time field is translated to "timestamp".
	//     - All other fields, like app and component, are changed to
	//       "labels.serviceweaver/app" and "labels.serviceweaver/component".
	//     - operators !, &&, ||, ==, contains, and matches are rewritten as
	//       NOT, AND, OR, =, :, and =~.
	//     - attrs["foo"] is rewritten labels."foo".
	//     - `"foo" in attrs` is rewritten `labels."foo":""`.
	//
	// Note that translate returns a heavily parenthesized Google Cloud Logging
	// query to ensure that we implement the correct precedence. These queries
	// are never shown to the user, so it's okay that they're a little ugly.
	ast, err := wlogging.Parse(query)
	if err != nil {
		return "", fmt.Errorf("translate(%s): %w", query, err)
	}

	var b strings.Builder
	hasTime := false
	if err := translateExpr(&b, &hasTime, ast.Expr()); err != nil {
		return "", fmt.Errorf("translate(%s): %w", query, err)
	}

	// If the filter doesn't contain "timestamp", then the Go library will
	// inject a filter of 'timestamp >= "1 day ago"'. We don't want to inject
	// this filter, so we make sure to inject our own filter that matches all
	// logs within the last century or so.
	if !hasTime {
		fmt.Fprint(&b, ` AND timestamp>="1900-01-01T00:00:00Z"`)
	}

	// Restrict the query to Service Weaver generated logs.
	//
	// TODO(mwhittaker): Restrict based on location.
	fmt.Fprintf(&b, ` AND resource.type="k8s_container"`)
	fmt.Fprintf(&b, ` AND resource.labels.project_id=%q`, project)
	fmt.Fprintf(&b, ` AND (resource.labels.cluster_name=%q OR resource.labels.cluster_name=%q)`, applicationClusterName, ConfigClusterName)
	fmt.Fprintf(&b, ` AND resource.labels.namespace_name=%q`, namespaceName)
	fmt.Fprintf(&b, ` AND (resource.labels.container_name=%q OR resource.labels.container_name=%q)`, appContainerName, nannyContainerName)
	fmt.Fprintf(&b, ` AND logName="projects/%s/logs/serviceweaver"`, project)

	return b.String(), nil
}

func translateExpr(w io.Writer, hasTime *bool, e *exprpb.Expr) error {
	switch e.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		// Note that CEL represents operators like || and ! as calls.
		return translateCall(w, hasTime, e.GetCallExpr())
	default:
		return fmt.Errorf("unexpected expression: %v", e)
	}
}

func translateCall(w io.Writer, hasTime *bool, e *exprpb.Expr_Call) error {
	if e.GetFunction() != operators.Index {
		// NOTE: index operator doesn't work correctly if braces are added
		// around it. For example, this is an invalid query:
		//   (labels."foo")="fooval"
		fmt.Fprint(w, "(")
		defer fmt.Fprint(w, ")")
	}

	binops := map[string]string{
		operators.LogicalAnd:    "AND",
		operators.LogicalOr:     "OR",
		operators.Equals:        "=",
		operators.NotEquals:     "!=",
		operators.Less:          "<",
		operators.LessEquals:    "<=",
		operators.Greater:       ">",
		operators.GreaterEquals: ">=",
		operators.Index:         ".",
		"contains":              ":",
		"matches":               "=~",
	}

	switch op := e.GetFunction(); op {
	// !
	case operators.LogicalNot:
		fmt.Fprint(w, "NOT ")
		return translateExpr(w, hasTime, e.Args[0])

	// &&, ||
	case operators.LogicalAnd, operators.LogicalOr:
		if err := translateExpr(w, hasTime, e.Args[0]); err != nil {
			return err
		}
		fmt.Fprintf(w, " %s ", binops[op])
		return translateExpr(w, hasTime, e.Args[1])

	// ==, !=, <, <=, >, >=
	case operators.Equals, operators.NotEquals,
		operators.Less, operators.LessEquals,
		operators.Greater, operators.GreaterEquals:
		if err := translateLHS(w, hasTime, e.Args[0]); err != nil {
			return err
		}
		fmt.Fprint(w, binops[op])
		return translateRHS(w, e.Args[1])

	// []
	case operators.Index:
		if err := translateLHS(w, hasTime, e.Args[0]); err != nil {
			return err
		}
		fmt.Fprint(w, binops[op])
		return translateRHS(w, e.Args[1])

	// contains, matches
	case "contains", "matches":
		if err := translateLHS(w, hasTime, e.Target); err != nil {
			return err
		}
		fmt.Fprint(w, binops[op])
		return translateRHS(w, e.Args[0])

	// in
	case operators.In:
		// `"foo" in attrs` becomes `labels."foo":""`.
		if err := translateLHS(w, hasTime, e.Args[1]); err != nil {
			return err
		}
		fmt.Fprint(w, ".")
		if err := translateRHS(w, e.Args[0]); err != nil {
			return err
		}
		fmt.Fprint(w, `:""`)
		return nil

	default:
		return fmt.Errorf("unexpected call: %v", e)
	}
}

func translateLHS(w io.Writer, hasTime *bool, e *exprpb.Expr) error {
	switch t := e.ExprKind.(type) {
	case *exprpb.Expr_IdentExpr:
		switch name := e.GetIdentExpr().GetName(); name {
		case "msg":
			fmt.Fprint(w, "textPayload")
		case "time":
			*hasTime = true
			fmt.Fprint(w, "timestamp")
		case "attrs":
			fmt.Fprint(w, "labels")
		default:
			fmt.Fprintf(w, `labels."serviceweaver/%s"`, name)
		}
		return nil
	case *exprpb.Expr_CallExpr:
		return translateCall(w, hasTime, t.CallExpr)
	default:
		return fmt.Errorf("unexpected lhs: %v", e)
	}
}

func translateRHS(w io.Writer, e *exprpb.Expr) error {
	switch e.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		c := e.GetConstExpr()
		switch c.ConstantKind.(type) {
		case *exprpb.Constant_BoolValue:
			fmt.Fprint(w, c.GetBoolValue())
		case *exprpb.Constant_Int64Value:
			fmt.Fprint(w, c.GetInt64Value())
		case *exprpb.Constant_Uint64Value:
			fmt.Fprint(w, c.GetUint64Value())
		case *exprpb.Constant_StringValue:
			fmt.Fprintf(w, "%q", c.GetStringValue())
		default:
			return fmt.Errorf("unexpected const rhs: %v", e)
		}
		return nil
	case *exprpb.Expr_CallExpr:
		call := e.GetCallExpr()
		if f := call.Function; f != "timestamp" {
			return fmt.Errorf("unexpected rhs: %v", e)
		}
		return translateRHS(w, call.Args[0])
	default:
		return fmt.Errorf("unexpected rhs: %v", e)
	}
}

// labelsFromEntry creates logging.Entry labels for a given log entry.
func labelsFromEntry(entry *protos.LogEntry, meta *ContainerMetadata) map[string]string {
	ls := make(map[string]string, 13+len(entry.Attrs)/2)
	for i := 0; i+1 < len(entry.Attrs); i += 2 {
		ls[entry.Attrs[i]] = entry.Attrs[i+1]
	}

	// This label ensures the log entry shows up in the "LOGS"
	// tab of the GKE section of the Google Cloud Console.
	ls["k8s-pod/app"] = meta.App

	// This label enables the "View monitoring details" button
	// on a log entry in the Google Cloud Logging Explorer.
	ls["compute.googleapis.com/resource_name"] = meta.NodeName

	// These labels are the contents of the entry. Note that
	// Google Cloud Logging expects RFC3339 timestamps [1].
	//
	// [1]: https://cloud.google.com/logging/docs/view/logging-query-language#values_conversions
	ls["serviceweaver/app"] = entry.App
	ls["serviceweaver/version"] = wlogging.Shorten(entry.Version)
	ls["serviceweaver/full_version"] = entry.Version
	ls["serviceweaver/node"] = wlogging.Shorten(entry.Node)
	ls["serviceweaver/full_node"] = entry.Node
	ls["serviceweaver/component"] = wlogging.ShortenComponent(entry.Component)
	ls["serviceweaver/full_component"] = entry.Component
	ls["serviceweaver/time"] = time.UnixMicro(entry.TimeMicros).Format(time.RFC3339)
	ls["serviceweaver/level"] = entry.Level
	ls["serviceweaver/file"] = entry.File
	ls["serviceweaver/line"] = fmt.Sprintf("%d", entry.Line)
	ls["serviceweaver/source"] = fmt.Sprintf("%s:%d", entry.File, entry.Line)

	return ls
}

// entryFromLabels re-constructs a log entry from the given labels, which
// were created using labelsFromEntry.
func entryFromLabels(labels map[string]string) (*protos.LogEntry, error) {
	// ex extracts the given label from the labels map.
	ex := func(label string) string {
		val := labels[label]
		delete(labels, label)
		return val
	}

	// Remove GKE-specific labels.
	ex("k8s-pod/app")
	ex("compute.googleapis.com/resource_name")

	// Remove derived labels.
	ex("serviceweaver/version")
	ex("serviceweaver/node")
	ex("serviceweaver/component")
	ex("serviceweaver/source")

	// Process remaining labels.
	time, err := time.Parse(time.RFC3339, ex("serviceweaver/time"))
	if err != nil {
		return nil, err
	}
	line, err := strconv.Atoi(ex("serviceweaver/line"))
	if err != nil {
		return nil, err
	}
	e := protos.LogEntry{
		App:        ex("serviceweaver/app"),
		Version:    ex("serviceweaver/full_version"),
		Component:  ex("serviceweaver/full_component"),
		Node:       ex("serviceweaver/full_node"),
		TimeMicros: time.UnixMicro(),
		Level:      ex("serviceweaver/level"),
		File:       ex("serviceweaver/file"),
		Line:       int32(line),
	}
	for attr, value := range labels {
		e.Attrs = append(e.Attrs, attr, value)
	}
	return &e, nil
}
