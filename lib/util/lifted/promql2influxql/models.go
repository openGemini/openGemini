package promql2influxql

import "time"

// DataType indicates a PromCommand is for table view or for graph view in data visualization platform like Grafana
// Basically,
//   - TABLE_DATA is for raw table data query
//   - GRAPH_DATA is for time-bounding graph data query
//   - LABEL_VALUES_DATA is for label values data query like fetching data for label selector options at the top of Grafana dashboard
type DataType int

const (
	TABLE_DATA DataType = iota
	GRAPH_DATA
	LABEL_KEYS_DATA
	LABEL_VALUES_DATA
	SERIES_DATA
	UNKNOWN_DATA
)

// PromCommand wraps a raw query expression with several related attributes
type PromCommand struct {
	Cmd             string
	Database        string
	RetentionPolicy string
	Measurement     string
	// Start and End attributes are mainly used for PromQL currently
	// as it doesn't support time-bounding query expression itself
	Start      *time.Time
	End        *time.Time
	Timezone   *time.Location
	Evaluation *time.Time
	// Step is evaluation step for PromQL.
	// As InfluxQL doesn't have the equivalent expression or concept,
	// we use it as interval parameter for InfluxQL GROUP BY time(interval)
	// if the raw query doesn't contain PromQL MatrixSelector expression.
	// If the raw query does contain PromQL parser.MatrixSelector expression,
	// its Range attribute will be used as the interval parameter.
	Step time.Duration

	// LookBackDelta indicates the maximum backtracking interval of the point searching
	// process in the PromQL calculation. The default value is 5m
	LookBackDelta time.Duration

	DataType DataType
	// ValueFieldKey indicates which field will be used.
	// As matrix value field as measurement in InfluxDB may contain multiple fields that is different from Prometheus,
	// so we may need to set ValueFieldKey.
	//
	// Default is ```value``` field.
	ValueFieldKey string
	// LabelName is only used for label values query.
	LabelName string
}

// RunResult wraps query result and possible error
type PromResult struct {
	ResultType string      `json:"resultType,omitempty"`
	Result     interface{} `json:"result,omitempty"`
}

func NewPromResult(result interface{}, resultType string) *PromResult {
	return &PromResult{Result: result, ResultType: resultType}
}
