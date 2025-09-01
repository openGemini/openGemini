package promql2influxql

import (
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/tinylib/msgp/msgp"
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

type PromResult struct {
	ResultType string      `json:"resultType,omitempty"`
	Result     interface{} `json:"result,omitempty"`
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

func (z *PromData) IsEmptyData() bool {
	switch r := z.Result.(type) {
	case *PromDataMatrix:
		if r.Matrix == nil {
			return true
		}
		return r.Matrix.Len() == 0
	case *PromDataVector:
		if r.Vector == nil {
			return true
		}
		return len(*r.Vector) == 0
	default:
		return false
	}
}

// DecodeMsg implements msgp.Decodable
func (z *PromData) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "resultType":
			z.ResultType, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "ResultType")
				return
			}
		case "result":
			switch z.ResultType {
			case string(parser.ValueTypeMatrix):
				var dataValue PromDataMatrix
				if err = dataValue.DecodeMsg(dc); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeVector):
				var dataValue PromDataVector
				if err = dataValue.DecodeMsg(dc); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeScalar):
				var dataValue PromDataScalar
				if err = dataValue.DecodeMsg(dc); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeString):
				var dataValue PromDataString
				if err = dataValue.DecodeMsg(dc); err != nil {
					return
				}
				z.Result = &dataValue
			default:
				err = fmt.Errorf("not expected type,%v", z.ResultType)
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *PromData) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "resultType"
	err = en.Append(0x82, 0xaa, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x54, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.ResultType)
	if err != nil {
		err = msgp.WrapError(err, "ResultType")
		return
	}
	// write "result"
	err = en.Append(0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74)
	if err != nil {
		return
	}
	err = z.Result.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "Result")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PromData) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "resultType"
	o = append(o, 0x82, 0xaa, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x54, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.ResultType)
	// string "result"
	o = append(o, 0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74)
	o, err = z.Result.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "Result")
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PromData) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "resultType":
			z.ResultType, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ResultType")
				return
			}
		case "result":
			switch z.ResultType {
			case string(parser.ValueTypeMatrix):
				var dataValue PromDataMatrix
				if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeVector):
				var dataValue PromDataVector
				if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeScalar):
				var dataValue PromDataScalar
				if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
					return
				}
				z.Result = &dataValue
			case string(parser.ValueTypeString):
				var dataValue PromDataString
				if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
					return
				}
				z.Result = &dataValue
			default:
				err = fmt.Errorf("not expected type,%v", z.ResultType)
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *PromData) Msgsize() (s int) {
	s = 1 + 11 + msgp.StringPrefixSize + len(z.ResultType) + 7 + z.Result.Msgsize()
	return
}

// DecodeMsg implements msgp.Decodable
func (z *PromQueryResponse) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "status":
			z.Status, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Status")
				return
			}
		case "data":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					err = msgp.WrapError(err, "Data")
					return
				}
				z.Data = nil
			} else {
				if z.Data == nil {
					z.Data = new(PromData)
				}
				var zb0002 uint32
				zb0002, err = dc.ReadMapHeader()
				if err != nil {
					err = msgp.WrapError(err, "Data")
					return
				}
				for zb0002 > 0 {
					zb0002--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						err = msgp.WrapError(err, "Data")
						return
					}
					switch msgp.UnsafeString(field) {
					case "resultType":
						z.Data.ResultType, err = dc.ReadString()
						if err != nil {
							err = msgp.WrapError(err, "Data", "ResultType")
							return
						}
					case "result":
						switch z.Data.ResultType {
						case string(parser.ValueTypeMatrix):
							var dataValue PromDataMatrix
							if err = dataValue.DecodeMsg(dc); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeVector):
							var dataValue PromDataVector
							if err = dataValue.DecodeMsg(dc); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeScalar):
							var dataValue PromDataScalar
							if err = dataValue.DecodeMsg(dc); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeString):
							var dataValue PromDataString
							if err = dataValue.DecodeMsg(dc); err != nil {
								return
							}
							z.Data.Result = &dataValue
						default:
							err = fmt.Errorf("not expected type,%v", z.Data.ResultType)
						}
						err = z.Data.Result.DecodeMsg(dc)
						if err != nil {
							err = msgp.WrapError(err, "Data", "Result")
							return
						}
					default:
						err = dc.Skip()
						if err != nil {
							err = msgp.WrapError(err, "Data")
							return
						}
					}
				}
			}
		case "errorType":
			z.ErrorType, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "ErrorType")
				return
			}
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Error")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *PromQueryResponse) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "status"
	err = en.Append(0x84, 0xa6, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73)
	if err != nil {
		return
	}
	err = en.WriteString(z.Status)
	if err != nil {
		err = msgp.WrapError(err, "Status")
		return
	}
	// write "data"
	err = en.Append(0xa4, 0x64, 0x61, 0x74, 0x61)
	if err != nil {
		return
	}
	if z.Data == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		// map header, size 2
		// write "resultType"
		err = en.Append(0x82, 0xaa, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x54, 0x79, 0x70, 0x65)
		if err != nil {
			return
		}
		err = en.WriteString(z.Data.ResultType)
		if err != nil {
			err = msgp.WrapError(err, "Data", "ResultType")
			return
		}
		// write "result"
		err = en.Append(0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74)
		if err != nil {
			return
		}
		err = z.Data.Result.EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "Data", "Result")
			return
		}
	}
	// write "errorType"
	err = en.Append(0xa9, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x54, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.ErrorType)
	if err != nil {
		err = msgp.WrapError(err, "ErrorType")
		return
	}
	// write "error"
	err = en.Append(0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return
	}
	err = en.WriteString(z.Error)
	if err != nil {
		err = msgp.WrapError(err, "Error")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PromQueryResponse) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "status"
	o = append(o, 0x84, 0xa6, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73)
	o = msgp.AppendString(o, z.Status)
	// string "data"
	o = append(o, 0xa4, 0x64, 0x61, 0x74, 0x61)
	if z.Data == nil {
		o = msgp.AppendNil(o)
	} else {
		// map header, size 2
		// string "resultType"
		o = append(o, 0x82, 0xaa, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x54, 0x79, 0x70, 0x65)
		o = msgp.AppendString(o, z.Data.ResultType)
		// string "result"
		o = append(o, 0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74)
		o, err = z.Data.Result.MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "Data", "Result")
			return
		}
	}
	// string "errorType"
	o = append(o, 0xa9, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x54, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.ErrorType)
	// string "error"
	o = append(o, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	o = msgp.AppendString(o, z.Error)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PromQueryResponse) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "status":
			z.Status, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Status")
				return
			}
		case "data":
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				if err != nil {
					return
				}
				z.Data = nil
			} else {
				if z.Data == nil {
					z.Data = new(PromData)
				}
				var zb0002 uint32
				zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Data")
					return
				}
				for zb0002 > 0 {
					zb0002--
					field, bts, err = msgp.ReadMapKeyZC(bts)
					if err != nil {
						err = msgp.WrapError(err, "Data")
						return
					}
					switch msgp.UnsafeString(field) {
					case "resultType":
						z.Data.ResultType, bts, err = msgp.ReadStringBytes(bts)
						if err != nil {
							err = msgp.WrapError(err, "Data", "ResultType")
							return
						}
					case "result":
						switch z.Data.ResultType {
						case string(parser.ValueTypeMatrix):
							var dataValue PromDataMatrix
							if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeVector):
							var dataValue PromDataVector
							if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeScalar):
							var dataValue PromDataScalar
							if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
								return
							}
							z.Data.Result = &dataValue
						case string(parser.ValueTypeString):
							var dataValue PromDataString
							if bts, err = dataValue.UnmarshalMsg(bts); err != nil {
								return
							}
							z.Data.Result = &dataValue
						default:
							err = fmt.Errorf("not expected type,%v", z.Data.ResultType)
						}
					default:
						bts, err = msgp.Skip(bts)
						if err != nil {
							err = msgp.WrapError(err, "Data")
							return
						}
					}
				}
			}
		case "errorType":
			z.ErrorType, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ErrorType")
				return
			}
		case "error":
			z.Error, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Error")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *PromQueryResponse) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.Status) + 5
	if z.Data == nil {
		s += msgp.NilSize
	} else {
		s += 1 + 11 + msgp.StringPrefixSize + len(z.Data.ResultType) + 7 + z.Data.Result.Msgsize()
	}
	s += 10 + msgp.StringPrefixSize + len(z.ErrorType) + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}
