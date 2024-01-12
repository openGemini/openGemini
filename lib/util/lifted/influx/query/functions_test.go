package query_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestFunctionTypeMapper(t *testing.T) {
	m := query.FunctionTypeMapper{}

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

	if dataType, err := m.CallType("str", []influxql.DataType{influxql.String, influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Boolean)
	}

	if dataType, err := m.CallType("strlen", []influxql.DataType{influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Integer)
	}

	if dataType, err := m.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}

	if dataType, err := m.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}
}
