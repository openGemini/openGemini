package promql2influxql

import (
	"time"
)

type FunctionType int

const (
	AGGREGATE_FN FunctionType = iota + 1
	SELECTOR_FN
	TRANSFORM_FN
	PREDICTOR_FN
)

const (
	ArgNameOfTimeFunc          string = "prom_time"
	DefaultFieldKey            string = "value"
	DefaultMetricKeyLabel      string = "__name__"
	DefaultMeasurementName     string = "prom_metric_not_specified"
	DefaultDatabaseName        string = "prom"
	DefaultRetentionPolicyName string = "autogen"
	TimeField                  string = "time"
	LastCall                   string = "last"
	PromSuffix                 string = "_prom"
)

const DefaultLookBackDelta = 5 * time.Minute
