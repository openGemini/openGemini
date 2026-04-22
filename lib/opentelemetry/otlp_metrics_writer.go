/*
Copyright (c) 2021 InfluxData
This code is originally from: https://github.com/influxdata/influxdb-observability/blob/v0.5.6/otel2influx/metrics_telegraf_prometheus_v1.go and has been modified.
Original author: jacobmarble
License: MIT License

2025.07.21 Add a new implementation of metricWriter.
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
*/

package opentelemetry

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type OtlpMetricWriter struct {
	Logger common.Logger
	Writer otel2influx.InfluxWriter
}

type basicDataPoint interface {
	Timestamp() pcommon.Timestamp
	StartTimestamp() pcommon.Timestamp
	Attributes() pcommon.Map
}

func (c *OtlpMetricWriter) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	batch := c.Writer.NewBatch()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			ilMetrics := resourceMetrics.ScopeMetrics().At(j)
			if err := c.enqueueMetrics(ctx, resourceMetrics.Resource(), ilMetrics.Scope(), ilMetrics.Metrics(), batch); err != nil {
				return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err))
			}
		}
	}
	return batch.WriteBatch(ctx)
}

func (c *OtlpMetricWriter) initMetricTagsAndTimestamp(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, dataPoint basicDataPoint) (tags map[string]string, fields map[string]interface{}, ts time.Time) {
	ts = dataPoint.Timestamp().AsTime()

	tags = make(map[string]string)
	fields = make(map[string]interface{})
	if dataPoint.StartTimestamp() != 0 {
		fields[common.AttributeStartTimeUnixNano] = int64(dataPoint.StartTimestamp())
	}

	dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			c.Logger.Debug("metric attribute key is empty")
		} else {
			tags[k] = v.AsString()
		}
		return true
	})

	tags = otel2influx.ResourceToTags(c.Logger, resource, tags)
	tags = otel2influx.InstrumentationScopeToTags(instrumentationLibrary, tags)

	return
}

func (c *OtlpMetricWriter) enqueueHistogram(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, histogram pmetric.Histogram, batch otel2influx.InfluxWriterBatch) error {
	for i := 0; i < histogram.DataPoints().Len(); i++ {
		dataPoint := histogram.DataPoints().At(i)
		tags, fields, ts := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)

		fields[common.MetricHistogramCountFieldKey] = float64(dataPoint.Count())
		fields[common.MetricHistogramSumFieldKey] = dataPoint.Sum()
		if dataPoint.HasMin() {
			fields[common.MetricHistogramMinFieldKey] = dataPoint.Min()
		}
		if dataPoint.HasMax() {
			fields[common.MetricHistogramMaxFieldKey] = dataPoint.Max()
		}
		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if bucketCounts.Len() > 0 &&
			bucketCounts.Len() != explicitBounds.Len() &&
			bucketCounts.Len() != explicitBounds.Len()+1 {
			// The infinity bucket is not used in this schema,
			// so accept input if that particular bucket is missing.
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", bucketCounts.Len(), explicitBounds.Len())
		}
		for i := 0; i < bucketCounts.Len(); i++ {
			var bucketCount uint64
			for j := 0; j <= i; j++ {
				bucketCount += bucketCounts.At(j)
			}

			var boundFieldKey string
			if explicitBounds.Len() > i {
				boundFieldKey = strconv.FormatFloat(explicitBounds.At(i), 'f', -1, 64)
			} else {
				boundFieldKey = common.MetricHistogramInfFieldKey
			}
			fields[boundFieldKey] = float64(bucketCount)
		}

		if err := batch.EnqueuePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeHistogram); err != nil {
			return fmt.Errorf("failed to write point for histogram: %w", err)
		}
	}

	return nil
}

func (c *OtlpMetricWriter) enqueueSummary(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, summary pmetric.Summary, batch otel2influx.InfluxWriterBatch) error {
	for i := 0; i < summary.DataPoints().Len(); i++ {
		dataPoint := summary.DataPoints().At(i)
		tags, fields, ts := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)

		fields[common.MetricSummaryCountFieldKey] = float64(dataPoint.Count())
		fields[common.MetricSummarySumFieldKey] = dataPoint.Sum()
		for j := 0; j < dataPoint.QuantileValues().Len(); j++ {
			valueAtQuantile := dataPoint.QuantileValues().At(j)
			quantileFieldKey := strconv.FormatFloat(valueAtQuantile.Quantile(), 'f', -1, 64)
			fields[quantileFieldKey] = valueAtQuantile.Value()
		}

		if err := batch.EnqueuePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeSummary); err != nil {
			return fmt.Errorf("failed to write point for summary: %w", err)
		}
	}

	return nil
}

func (c *OtlpMetricWriter) enqueueMetrics(ctx context.Context, resource pcommon.Resource, scope pcommon.InstrumentationScope, metrics pmetric.MetricSlice, batch otel2influx.InfluxWriterBatch) error {
	metricTags := make(map[string]map[string]string)
	metricFields := make(map[string]map[string]interface{})
	metricTs := make(map[string]time.Time)
	var err error
	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		switch metric.Type() {
		case pmetric.MetricTypeGauge:
			err = c.enqueueGauges(ctx, resource, scope, metric.Name(), metric.Gauge(), metricTags, metricFields, metricTs, batch)
			if err != nil {
				return err
			}
		case pmetric.MetricTypeSum:
			err = c.enqueueSums(ctx, resource, scope, metric.Name(), metric.Sum(), metricTags, metricFields, metricTs, batch)
			if err != nil {
				return err
			}
		case pmetric.MetricTypeHistogram:
			err = c.enqueueHistogram(ctx, resource, scope, metric.Name(), metric.Histogram(), batch)
			if err != nil {
				return err
			}
		case pmetric.MetricTypeSummary:
			err = c.enqueueSummary(ctx, resource, scope, metric.Name(), metric.Summary(), batch)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown metric type %q", metric.Type())
		}
	}
	for k, tags := range metricTags {
		fields := metricFields[k]
		ts := metricTs[k]
		if err = batch.EnqueuePoint(ctx, scope.Name(), tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
			return fmt.Errorf("failed to write point for metrics: %w", err)
		}
	}
	return nil
}

func (c *OtlpMetricWriter) enqueueGauges(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, metricName string, gauge pmetric.Gauge,
	metricTags map[string]map[string]string, metricFields map[string]map[string]interface{}, metricTs map[string]time.Time, batch otel2influx.InfluxWriterBatch) error {
	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dataPoint := gauge.DataPoints().At(i)
		_, fields := c.initMetricTagsAndTs(resource, instrumentationLibrary, dataPoint, metricTags, metricFields, metricTs)

		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		case pmetric.NumberDataPointValueTypeDouble:
			fields[metricName] = dataPoint.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			fields[metricName] = dataPoint.IntValue()
		default:
			return fmt.Errorf("unsupported gauge data point type %d", dataPoint.ValueType())
		}
	}
	return nil
}

func (c *OtlpMetricWriter) enqueueSums(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, metricName string, sum pmetric.Sum,
	metricTags map[string]map[string]string, metricFields map[string]map[string]interface{}, metricTs map[string]time.Time, batch otel2influx.InfluxWriterBatch) error {
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dataPoint := sum.DataPoints().At(i)
		_, fields := c.initMetricTagsAndTs(resource, instrumentationLibrary, dataPoint, metricTags, metricFields, metricTs)
		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		case pmetric.NumberDataPointValueTypeDouble:
			fields[metricName] = dataPoint.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			fields[metricName] = dataPoint.IntValue()
		default:
			return fmt.Errorf("unsupported gauge data point type %d", dataPoint.ValueType())
		}
	}
	return nil
}

func (c *OtlpMetricWriter) initMetricTagsAndTs(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, dataPoint basicDataPoint, metricTags map[string]map[string]string, metricFields map[string]map[string]interface{}, metricTs map[string]time.Time) (map[string]string, map[string]interface{}) {
	ts := dataPoint.Timestamp().AsTime()

	tags := make(map[string]string)
	fields := make(map[string]interface{})

	dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			c.Logger.Debug("metric attribute key is empty")
		} else {
			tags[k] = v.AsString()
		}
		return true
	})

	tagsStr := sortAndJoinMap(tags)
	if metricTag, ok := metricTags[tagsStr]; ok {
		tags = metricTag
		fields = metricFields[tagsStr]
	} else {
		metricTags[tagsStr] = tags
		otel2influx.ResourceToTags(c.Logger, resource, tags)
		otel2influx.InstrumentationScopeToTags(instrumentationLibrary, tags)
		metricFields[tagsStr] = fields
		if dataPoint.StartTimestamp() != 0 {
			fields[common.AttributeStartTimeUnixNano] = int64(dataPoint.StartTimestamp())
		}
	}
	metricTs[tagsStr] = ts
	return tags, fields
}

func sortAndJoinMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(m))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%s", k, m[k]))
	}

	return strings.Join(parts, ",")
}
