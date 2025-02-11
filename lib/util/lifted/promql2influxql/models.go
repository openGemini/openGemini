package promql2influxql

import (
	"bytes"
	"fmt"
	"io"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/vmihailenco/msgpack/v5"
)

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
	META_DATA
	UNKNOWN_DATA
)

// PromCommand wraps a raw query expression with several related attributes
type PromCommand struct {
	Cmd             string
	Database        string
	RetentionPolicy string
	Measurement     string
	// Exact is used to indicate whether the query is based on the exact time range of metadata.
	// For exact query, chunk meta time is used for matching.
	// Otherwise, index duration is used for matching.
	Exact bool
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

func (c *PromCommand) WithStartEnd(start, end int64) *PromCommand {
	new := *c
	startTime := time.UnixMilli(start)
	endTime := time.UnixMilli(end)
	new.Start = &startTime
	new.End = &endTime
	return &new
}

type Response interface {
	IsPromResponse()
}

func (p *PromResponse) IsPromResponse()      {}
func (p *PromQueryResponse) IsPromResponse() {}

var json2 = jsoniter.ConfigCompatibleWithStandardLibrary

// Response represents a list of statement results.
type PromResponse struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type PromQueryResponse struct {
	Status    string    `json:"status"`
	Data      *PromData `json:"data,omitempty"`
	ErrorType string    `json:"errorType,omitempty"`
	Error     string    `json:"error,omitempty"`
}

type PromData struct {
	ResultType string `json:"resultType,omitempty"`
	// Types that are valid to be assigned to Result:
	//
	//	*PromDataVector
	//	*PromDataMatrix
	//	*PromDataScalar
	//	*PromDataString
	Result PromDataResult `json:"result,omitempty"`
}

var _ msgpack.Marshaler = (*PromData)(nil)

func (p *PromData) MarshalMsgpack() ([]byte, error) {
	var b bytes.Buffer
	enc := msgpack.NewEncoder(&b)

	if err := enc.EncodeMapLen(2); err != nil {
		return nil, err
	}

	if err := enc.Encode("ResultType"); err != nil {
		return nil, err
	}
	if err := enc.Encode(p.ResultType); err != nil {
		return nil, err
	}

	if err := enc.Encode("Result"); err != nil {
		return nil, err
	}
	if err := enc.Encode(p.Result); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

var _ msgpack.Unmarshaler = (*PromData)(nil)

func (p *PromData) UnmarshalMsgpack(b []byte) error {
	dec := msgpack.NewDecoder(bytes.NewReader(b))

	if _, err := dec.DecodeMapLen(); err != nil {
		return err
	}
	for {
		key, err := dec.DecodeString()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch key {
		case "ResultType":
			p.ResultType, err = dec.DecodeString()
			if err != nil {
				return err
			}
		case "Result":
			switch p.ResultType {
			case string(parser.ValueTypeMatrix):
				var dataValue PromDataMatrix
				if err := dec.Decode(&dataValue); err != nil {
					return err
				}
				p.Result = &dataValue
			case string(parser.ValueTypeVector):
				var dataValue PromDataVector
				if err := dec.Decode(&dataValue); err != nil {
					return err
				}
				p.Result = &dataValue
			case string(parser.ValueTypeScalar):
				var dataValue PromDataScalar
				if err := dec.Decode(&dataValue); err != nil {
					return err
				}
				p.Result = &dataValue
			case string(parser.ValueTypeString):
				var dataValue PromDataString
				if err := dec.Decode(&dataValue); err != nil {
					return err
				}
				p.Result = &dataValue
			default:
				return fmt.Errorf("not expected type,%v", p.ResultType)
			}

		}
	}
	return nil
}

func (p *PromData) GetMatrix() *promql.Matrix {
	if m, ok := p.Result.(*PromDataMatrix); ok {
		return m.Matrix
	}
	return nil
}

func (p *PromData) GetVector() *promql.Vector {
	if m, ok := p.Result.(*PromDataVector); ok {
		return m.Vector
	}
	return nil
}

type PromDataResult interface {
	PromDataResult()
}

type PromDataVector struct {
	Vector *promql.Vector `json:"vector,omitempty"`
}
type PromDataMatrix struct {
	Matrix *promql.Matrix `json:"matrix,omitempty"`
}
type PromDataScalar struct {
	Scalar *promql.Scalar `json:"scalar,omitempty"`
}
type PromDataString struct {
	String *promql.String `json:"string,omitempty"`
}

func (*PromDataVector) PromDataResult() {}
func (*PromDataMatrix) PromDataResult() {}
func (*PromDataScalar) PromDataResult() {}
func (*PromDataString) PromDataResult() {}

func (p PromDataVector) MarshalJSON() ([]byte, error) {
	if p.Vector == nil {
		return jsoniter.Marshal([]promql.Sample{})
	}
	return jsoniter.Marshal(p.Vector)
}
func (p PromDataMatrix) MarshalJSON() ([]byte, error) {
	if p.Matrix == nil {
		return jsoniter.Marshal([]promql.Series{})
	}
	return jsoniter.Marshal(p.Matrix)
}
func (p PromDataScalar) MarshalJSON() ([]byte, error) {
	if p.Scalar == nil {
		return jsoniter.Marshal([]interface{}{})
	}
	return jsoniter.Marshal(p.Scalar)
}
func (p PromDataString) MarshalJSON() ([]byte, error) {
	if p.String == nil {
		return jsoniter.Marshal([]interface{}{})
	}
	return jsoniter.Marshal(p.String)
}

func NewPromData(result PromDataResult, resultType string) *PromData {
	return &PromData{Result: result, ResultType: resultType}
}
