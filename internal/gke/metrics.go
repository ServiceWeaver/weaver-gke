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
	"fmt"
	"path"
	"strings"
	"text/template"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringv2 "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/ServiceWeaver/weaver-gke/internal/nanny/distributor"
	"github.com/ServiceWeaver/weaver/runtime/metrics"
	"github.com/ServiceWeaver/weaver/runtime/protos"
	"github.com/ServiceWeaver/weaver/runtime/retry"
	"github.com/golang/protobuf/ptypes/timestamp"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	mpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
)

// metricExporter exports metrics to Google Cloud.
type metricExporter struct {
	meta     *ContainerMetadata
	resource *mrpb.MonitoredResource
	client   *monitoring.MetricClient
	start    time.Time                             // when the MetricExporter was created
	descs    map[string]*metricpb.MetricDescriptor // seen metric descriptors
}

// newMetricExporter creates a new MetricExporter. meta should contain the
// metadata about the currently running container.
func newMetricExporter(ctx context.Context, meta *ContainerMetadata) (*metricExporter, error) {
	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return nil, err
	}
	return &metricExporter{
		meta: meta,
		resource: &mrpb.MonitoredResource{
			Type: "k8s_container",
			Labels: map[string]string{
				"cluster_name":   meta.ClusterName,
				"container_name": meta.ContainerName,
				"location":       meta.ClusterRegion,
				"namespace_name": meta.Namespace,
				"project_id":     meta.Project,
				"pod_name":       meta.PodName,
			},
		},
		client: client,
		start:  time.Now(),
		descs:  map[string]*metricpb.MetricDescriptor{},
	}, nil
}

// Export exports the provided metrics to Google Cloud.
func (m *metricExporter) Export(ctx context.Context, snaps []*metrics.MetricSnapshot) error {
	var tss []*mpb.TimeSeries
	for _, snap := range snaps {
		// Get or create the MetricDescriptor.
		if _, ok := m.descs[snap.Name]; !ok {
			desc, err := m.createMetricDescriptor(ctx, snap)
			if err != nil {
				return fmt.Errorf("MetricExporter: cannot create metric descriptor for metric %q: %w", snap.Name, err)
			}
			m.descs[snap.Name] = desc
		}

		// Create a (cumulative) TimeSeries.
		ts, err := m.createTimeSeries(snap)
		if err != nil {
			return fmt.Errorf("MetricExporter: cannot create time series for metric %q: %w", snap.Name, err)
		}
		tss = append(tss, ts)
	}
	if len(tss) == 0 {
		return nil
	}
	req := &mpb.CreateTimeSeriesRequest{
		Name:       "projects/" + m.meta.Project,
		TimeSeries: tss,
	}
	if err := m.client.CreateTimeSeries(ctx, req); err != nil {
		return fmt.Errorf("MetricExporter: cannot export metrics: %w\n", err)
	}
	return nil
}

// Close closes a MetricExporter.
func (m *metricExporter) Close() error {
	return m.client.Close()
}

func isAlreadyExistsError(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.AlreadyExists
}

// createMetricDescriptor creates a MetricDescriptor for the given metric.
func (m *metricExporter) createMetricDescriptor(ctx context.Context, snap *metrics.MetricSnapshot) (*metricpb.MetricDescriptor, error) {
	var kind metricpb.MetricDescriptor_MetricKind
	var valueType metricpb.MetricDescriptor_ValueType
	switch snap.Type {
	case protos.MetricType_COUNTER:
		kind = metricpb.MetricDescriptor_CUMULATIVE
		valueType = metricpb.MetricDescriptor_DOUBLE
	case protos.MetricType_GAUGE:
		kind = metricpb.MetricDescriptor_GAUGE
		valueType = metricpb.MetricDescriptor_DOUBLE
	case protos.MetricType_HISTOGRAM:
		kind = metricpb.MetricDescriptor_CUMULATIVE
		valueType = metricpb.MetricDescriptor_DISTRIBUTION
	}

	// Create and register a custom metric descriptor.
	desc := &metricpb.MetricDescriptor{
		Name:        snap.Name,
		Type:        path.Join("custom.googleapis.com", snap.Name),
		MetricKind:  kind,
		ValueType:   valueType,
		Description: snap.Help,
		DisplayName: snap.Name,
	}
	req := &mpb.CreateMetricDescriptorRequest{
		Name:             "projects/" + m.meta.Project,
		MetricDescriptor: desc,
	}
	_, err := m.client.CreateMetricDescriptor(ctx, req)
	if err == nil {
		return desc, nil
	}
	if !isAlreadyExistsError(err) {
		return nil, err
	}

	// Fetch the existing descriptor and make sure that its kind and value type
	// are as expected.
	getReq := &mpb.GetMetricDescriptorRequest{
		Name: path.Join("projects", m.meta.Project, "metricDescriptors", desc.Type),
	}
	existing, err := m.client.GetMetricDescriptor(ctx, getReq)
	if err != nil {
		return nil, err
	}
	if existing.MetricKind != desc.MetricKind || existing.ValueType != desc.ValueType {
		opts := prototext.MarshalOptions{Multiline: false}
		return nil, fmt.Errorf("metric descriptor type mismatch: got %s, want %s", opts.Format(existing), opts.Format(desc))
	}
	return desc, nil
}

// createTimeSeries creates a TimeSeries for the given metric.
func (m *metricExporter) createTimeSeries(snap *metrics.MetricSnapshot) (*mpb.TimeSeries, error) {
	def := &metricpb.Metric{
		Type:   "custom.googleapis.com/" + snap.Name,
		Labels: snap.Labels,
	}
	now := time.Now().Unix()
	switch snap.Type {
	case protos.MetricType_COUNTER:
		return &mpb.TimeSeries{
			Metric:   def,
			Resource: m.resource,
			Points: []*mpb.Point{
				{
					Interval: &mpb.TimeInterval{
						StartTime: &timestamp.Timestamp{
							Seconds: m.start.Unix(),
						},
						EndTime: &timestamp.Timestamp{
							Seconds: now,
						},
					},
					Value: &mpb.TypedValue{
						Value: &mpb.TypedValue_DoubleValue{
							DoubleValue: snap.Value,
						},
					},
				},
			},
		}, nil

	case protos.MetricType_GAUGE:
		return &mpb.TimeSeries{
			Metric:   def,
			Resource: m.resource,
			Points: []*mpb.Point{
				{
					Interval: &mpb.TimeInterval{
						StartTime: &timestamp.Timestamp{
							Seconds: now,
						},
						EndTime: &timestamp.Timestamp{
							Seconds: now,
						},
					},
					Value: &mpb.TypedValue{
						Value: &mpb.TypedValue_DoubleValue{
							DoubleValue: snap.Value,
						},
					},
				},
			},
		}, nil

	case protos.MetricType_HISTOGRAM:
		var totalCount int64
		var counts []int64
		for _, count := range snap.Counts {
			totalCount += int64(count)
			counts = append(counts, int64(count))
		}
		var mean float64
		if totalCount > 0 {
			mean = snap.Value / float64(totalCount)
		}
		return &mpb.TimeSeries{
			Metric:   def,
			Resource: m.resource,
			Points: []*mpb.Point{
				{
					Interval: &mpb.TimeInterval{
						StartTime: &timestamp.Timestamp{
							Seconds: m.start.Unix(),
						},
						EndTime: &timestamp.Timestamp{
							Seconds: now,
						},
					},
					Value: &mpb.TypedValue{
						Value: &mpb.TypedValue_DistributionValue{
							DistributionValue: &distribution.Distribution{
								BucketOptions: &distribution.Distribution_BucketOptions{
									Options: &distribution.Distribution_BucketOptions_ExplicitBuckets{
										ExplicitBuckets: &distribution.Distribution_BucketOptions_Explicit{
											Bounds: snap.Bounds,
										},
									},
								},
								BucketCounts: counts,
								Count:        totalCount,
								Mean:         mean,
							},
						},
					},
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown type %v for metric %q", snap.Type, snap.Name)
	}
}

// TimeSeriesData stores information about a queried time series.
type TimeSeriesData struct {
	Labels []string // Labels associated with the time series.

	// Time series points at various time intervals, from the most recent to the
	// least recent. Each time interval corresponds to the alignment period
	// specified in the query.
	//
	// Primitive data points (e.g., double, integer) consist of a single typed
	// value, while distributions consist of multiple typed values.
	Points [][]*monitoringpb.TypedValue
}

// MetricQuerent run queries against the Google Cloud Monitoring service.
//
// It can currently only be used to access metrics exported by the Service
// Weaver runtime.
type MetricQuerent struct {
	config CloudConfig
}

// NewMetricQuerent creates a new MetricQuerent with the given cloud
// configuration.
func NewMetricQuerent(config CloudConfig) *MetricQuerent {
	return &MetricQuerent{config: config}
}

// Exists returns true iff the metric with the given name exists.
func (mq *MetricQuerent) Exists(ctx context.Context, metric string) (bool, error) {
	client, err := monitoringv2.NewMetricClient(ctx, mq.config.ClientOptions()...)
	if err != nil {
		return false, err
	}
	defer client.Close()

	_, err = client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: fmt.Sprintf("projects/%s/metricDescriptors/%s", mq.config.Project, metric),
	})
	if isNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// QueryTimeSeries runs the given MQL [1] query and returns the resulting
// time series.
//
// Here is an example MQL query:
//
//	fetch k8s_container
//	| metric 'custom.googleapis.com/my_metric'
//	| filter metric.my_label == 'bar'
//	| align rate(1m)
//	| within 10m
//
// The "within" table operator in the above query ensures that the query applies
// only to the last 10 minutes of data points. If we didn't add this operator,
// the query would have resulted in an unbounded amount of data points, which
// could lead to OOMs. Therefore, make sure that all your queries have a
// limiting factor such as "within".
//
// [1]: https://cloud.google.com/monitoring/mql/reference
func (mq *MetricQuerent) QueryTimeSeries(ctx context.Context, queryMQL string) ([]TimeSeriesData, error) {
	client, err := monitoringv2.NewQueryClient(ctx, mq.config.ClientOptions()...)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var page []*monitoringpb.TimeSeriesData
	var ts []TimeSeriesData
	processPage := func() error {
		for _, data := range page {
			labels := make([]string, len(data.LabelValues))
			for i, label := range data.LabelValues {
				ls, ok := label.Value.(*monitoringpb.LabelValue_StringValue)
				if !ok {
					return fmt.Errorf("internal error: time series label value %v is not a string", label.Value)
				}
				labels[i] = ls.StringValue
			}
			var points [][]*monitoringpb.TypedValue
			for _, point := range data.PointData {
				points = append(points, point.Values)
			}
			ts = append(ts, TimeSeriesData{Labels: labels, Points: points})
		}
		return nil
	}

	const pageSize = 1000
	pageToken := ""
	req := &monitoringpb.QueryTimeSeriesRequest{
		Name:  fmt.Sprintf("projects/%s", mq.config.Project),
		Query: queryMQL,
	}
	it := client.QueryTimeSeries(ctx, req)
	pager := iterator.NewPager(it, pageSize, pageToken)
retry:
	for r := retry.Begin(); r.Continue(ctx); {
		newPageToken, err := pager.NextPage(&page)
		switch {
		case status.Code(err) == codes.ResourceExhausted:
			// We've hit the quota. Keep retrying with backoff.
			it = client.QueryTimeSeries(ctx, req)
			pager = iterator.NewPager(it, pageSize, pageToken)
			continue retry
		case err != nil:
			return nil, err
		case newPageToken == "": // Last page.
			if err := processPage(); err != nil {
				return nil, err
			}
			break retry
		default: // More pages to come.
			if err := processPage(); err != nil {
				return nil, err
			}
			pageToken = newPageToken
			r.Reset()
		}
	}
	return ts, ctx.Err()
}

var metricQueryTmpl = template.Must(template.New("query").Parse(`
fetch k8s_container
| metric '{{.Metric}}'
| filter (resource.location == '{{.Location}}')
| align delta(1m)
| within 1m
{{if .LabelsGroupBy}}| group_by [
  {{range .LabelsGroupBy}} metric.{{.}},{{end}}
]
{{end}}
`))

// getMetricCounts returns the list of aggregated counts for the Service
// Weaver metric, grouping the counts using the provided list of metric labels.
// It accounts for metrics accumulated during the past minute.
//
// REQUIRES: The metric is a Service Weaver metric of type COUNTER.
func getMetricCounts(ctx context.Context, config CloudConfig, region string, metric string, labelsGroupBy ...string) ([]distributor.MetricCount, error) {
	metric = fmt.Sprintf("custom.googleapis.com/%s", metric)
	var query strings.Builder
	if err := metricQueryTmpl.Execute(&query, struct {
		Metric, Location string
		LabelsGroupBy    []string
	}{
		Metric:        metric,
		Location:      region,
		LabelsGroupBy: labelsGroupBy,
	}); err != nil {
		return nil, err
	}

	// Check if the metric exists.
	querent := NewMetricQuerent(config)
	if ok, err := querent.Exists(ctx, metric); err != nil {
		return nil, err
	} else if !ok {
		// Metric doesn't exist: return an empty grouping.
		return nil, nil
	}

	// Query the metric for its counts.
	tss, err := querent.QueryTimeSeries(ctx, query.String())
	if err != nil {
		return nil, err
	}
	ret := make([]distributor.MetricCount, len(tss))
	for i, ts := range tss {
		if len(ts.Points) != 1 {
			// Given the query, we expect a single TimeSeries point.
			return nil, fmt.Errorf("internal error: invalid number of TimeSeries points %v", ts.Points)
		}
		if len(ts.Points[0]) != 1 {
			// Expect a single primitive data point.
			return nil, fmt.Errorf("internal error: expected a TimeSeries of primitive points, got %v", ts.Points[0])
		}
		var count float64
		switch val := ts.Points[0][0].Value.(type) {
		case *monitoringpb.TypedValue_Int64Value:
			count = float64(val.Int64Value)
		case *monitoringpb.TypedValue_DoubleValue:
			count = val.DoubleValue
		default:
			// Expect either an int or a double type.
			return nil, fmt.Errorf("internal error: expected a TimeSeries of int64 or double types, got %v", val)
		}
		ret[i] = distributor.MetricCount{
			LabelVals: ts.Labels,
			Count:     count,
		}
	}
	return ret, nil
}
