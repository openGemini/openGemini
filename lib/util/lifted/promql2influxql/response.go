//go:generate msgp
package promql2influxql

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/openGemini/openGemini/lib/util/lifted/prometheus/promql"
	"github.com/tinylib/msgp/msgp"
)

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
	msgp.Marshaler
	msgp.Encodable
	msgp.Decodable
	msgp.Unmarshaler
	msgp.Sizer

	PromDataResult()
}

type PromDataVector struct {
	Vector *promql.Vector `json:"vector,omitempty" msg:"vector"`
}
type PromDataMatrix struct {
	Matrix *promql.Matrix `json:"matrix,omitempty" msg:"matrix"`
}
type PromDataScalar struct {
	Scalar *promql.Scalar `json:"scalar,omitempty" msg:"scalar"`
}
type PromDataString struct {
	String *promql.String `json:"string,omitempty" msg:"string"`
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

type CachedResponse struct {
	Key     string   `msg:"key"`
	Extents []Extent `msg:"extents"`
}

type Extent struct {
	Start    int64              `msg:"start"`
	End      int64              `msg:"end"`
	Response *PromQueryResponse `msg:"response"`
}
