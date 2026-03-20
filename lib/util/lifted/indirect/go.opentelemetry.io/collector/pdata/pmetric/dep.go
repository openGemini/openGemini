package pmetric

import (
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var NewMetrics = pmetric.NewMetrics
var NumberDataPointValueTypeEmpty = pmetric.NumberDataPointValueTypeEmpty
var NumberDataPointValueTypeDouble = pmetric.NumberDataPointValueTypeDouble
var NumberDataPointValueTypeInt = pmetric.NumberDataPointValueTypeInt

type Gauge = pmetric.Gauge
type Metrics = pmetric.Metrics
type Sum = pmetric.Sum
type Histogram = pmetric.Histogram
type Summary = pmetric.Summary
type MetricSlice = pmetric.MetricSlice

var MetricTypeGauge = pmetric.MetricTypeGauge
var MetricTypeSum = pmetric.MetricTypeSum
var MetricTypeHistogram = pmetric.MetricTypeHistogram
var MetricTypeSummary = pmetric.MetricTypeSummary
