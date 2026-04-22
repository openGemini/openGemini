package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/functions.go and it has been modified to compatible files in influx/influxql and influx/query.
*/

import (
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// FieldMapper is a FieldMapper that wraps another FieldMapper and exposes
// the functions implemented by the query engine.
type FieldMapper struct {
	influxql.FieldMapper
}

func (m FieldMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if mapper, ok := m.FieldMapper.(influxql.CallTypeMapper); ok {
		typ, err := mapper.CallType(name, args)
		if err != nil {
			return influxql.Unknown, err
		} else if typ != influxql.Unknown {
			return typ, nil
		}
	}

	// Use the default FunctionTypeMapper for the query engine.
	typmap := FunctionTypeMapper{}
	return typmap.CallType(name, args)
}

// FunctionTypeMapper handles the type mapping for all functions implemented by the
// query engine.
type FunctionTypeMapper struct {
}

func (FunctionTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (FunctionTypeMapper) MapTypeBatch(measurement *influxql.Measurement, field map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	return nil
}

func (m FunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {

	if aggFunc := GetAggregateOperator(name); aggFunc != nil {
		return aggFunc.CallTypeFunc(name, args)
	}

	if mathFunc := GetMathFunction(name); mathFunc != nil {
		return mathFunc.CallTypeFunc(name, args)
	}

	if strFunc := GetStringFunction(name); strFunc != nil {
		return strFunc.CallTypeFunc(name, args)
	}

	if labelFunc := GetLabelFunction(name); labelFunc != nil {
		return labelFunc.CallTypeFunc(name, args)
	}

	if promTimeFunc := GetPromTimeFunction(name); promTimeFunc != nil {
		return promTimeFunc.CallTypeFunc(name, args)
	}

	// Handle functions implemented by the query engine.
	switch name {
	case "median", "integral", "stddev",
		"derivative", "non_negative_derivative",
		"moving_average",
		"exponential_moving_average",
		"double_exponential_moving_average",
		"triple_exponential_moving_average",
		"relative_strength_index",
		"triple_exponential_derivative",
		"kaufmans_efficiency_ratio",
		"kaufmans_adaptive_moving_average",
		"chande_momentum_oscillator",
		"holt_winters", "holt_winters_with_fit",
		"rate", "irate":
		return influxql.Float, nil
	case "elapsed", "absent":
		return influxql.Integer, nil
	case "percentile", "percentile_ogsketch", "percentile_approx", "histogram", "distinct", "top", "bottom",
		"difference", "non_negative_difference", "mode", "spread", "sample", "cumulative_sum":
		return args[0], nil
	case "ogsketch_percentile":
		return influxql.Float, nil
	case "ogsketch_insert", "ogsketch_merge":
		return influxql.FloatTuple, nil
	default:
		// TODO(jsternberg): Do not use default for this.
		return influxql.Unknown, nil
	}
}
