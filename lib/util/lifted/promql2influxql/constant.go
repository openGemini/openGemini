package promql2influxql

import "time"

type FunctionType int

const (
	AGGREGATE_FN FunctionType = iota + 1
	SELECTOR_FN
	TRANSFORM_FN
	PREDICTOR_FN
)

const (
	DefaultFieldKey        string = "value"
	DefaultMetricKeyLabel  string = "__name__"
	DefaultMeasurementName        = "prom_metric_not_specified"
	TimeField              string = "time"
	LastCall               string = "last"
	ArgNameOfTimeFunc      string = "prom_time"
)

const DefaultLookBackDelta = 5 * time.Minute
