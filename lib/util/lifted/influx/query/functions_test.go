package query_test

import (
	"math"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestFunctionTypeMapper(t *testing.T) {
	m := query.MathTypeMapper{}
	s := query.StringFunctionTypeMapper{}

	if dataType, err := m.CallType("sin", []influxql.DataType{influxql.Float}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Float)
	}

	if dataType, err := m.CallType("abs", []influxql.DataType{influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Integer)
	}

	if dataType, err := s.CallType("str", []influxql.DataType{influxql.String, influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Boolean)
	}

	if dataType, err := s.CallType("strlen", []influxql.DataType{influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Integer)
	}

	if dataType, err := s.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}

	if dataType, err := s.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}
}

func TestClamp(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(6), float64(6), float64(7), float64(8), float64(9), float64(9)}
		outputs := make([]interface{}, 0, len(expects))
		min, max := int64(6), int64(9)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{nil, nil, nil, nil, nil, nil}
		outputs := make([]interface{}, 0, len(expects))
		min, max := int64(9), int64(6)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("3", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		nan := math.NaN()
		min, max := nan, int64(6)
		expects := []interface{}{nan, nan, nan, nan, nan, nan}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		for _, o := range outputs {
			if !math.IsNaN(o.(float64)) {
				t.Errorf("not euqal")
			}
		}
	})
}

func TestClampMax(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_max_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(5), float64(6), float64(7), float64(8), float64(9), float64(9)}
		outputs := make([]interface{}, 0, len(expects))
		max := int64(9)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestClampMin(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_min_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(6), float64(6), float64(7), float64(8), float64(9), float64(10)}
		outputs := make([]interface{}, 0, len(expects))
		min := int64(6)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}
