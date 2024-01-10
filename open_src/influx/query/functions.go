package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/functions.go and it has been modified to compatible files in influx/influxql and influx/query.
*/

import (
	"fmt"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
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

// CallTypeMapper returns the types for call iterator functions.
// Call iterator functions are commonly implemented within the storage engine
// so this mapper is limited to only the return values of those functions.
type CallTypeMapper struct{}

func (CallTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (CallTypeMapper) MapTypeBatch(measurement *influxql.Measurement, field map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	return nil
}

func (CallTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	// If the function is not implemented by the embedded field mapper, then
	// see if we implement the function and return the type here.
	switch name {
	case "mean":
		return influxql.Float, nil
	case "count":
		return influxql.Integer, nil
	case "min", "max", "sum", "first", "last":
		// TODO(jsternberg): Verify the input type.
		return args[0], nil
	}
	return influxql.Unknown, nil
}

// FunctionTypeMapper handles the type mapping for all functions implemented by the
// query engine.
type FunctionTypeMapper struct {
	CallTypeMapper
}

func (FunctionTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (FunctionTypeMapper) MapTypeBatch(measurement *influxql.Measurement, field map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	return nil
}

func (m FunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if typ, err := m.CallTypeMapper.CallType(name, args); typ != influxql.Unknown || err != nil {
		return typ, err
	}

	// Handle functions implemented by the query engine.
	switch name {
	case "sliding_window":
		return args[0], nil
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
	case "sin", "cos", "tan", "atan", "exp", "log", "ln", "log2", "log10", "sqrt", "acos", "asin", "atan2", "pow":
		return influxql.Float, nil
	case "abs", "round", "ceil", "floor", "row_max":
		return args[0], nil
	case "cast_int64":
		return influxql.Integer, nil
	case "cast_float64":
		return influxql.Float, nil
	case "cast_bool":
		return influxql.Boolean, nil
	case "cast_string":
		return influxql.String, nil
	case "str":
		return StrCallType(name, args)
	case "strlen":
		return StrLenCallType(name, args)
	case "substr":
		return SubStrCallType(name, args)
	default:
		// TODO(jsternberg): Do not use default for this.
		return influxql.Unknown, nil
	}
}

type StringFunctionTypeMapper struct{}

func (m StringFunctionTypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m StringFunctionTypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]*influxql.FieldNameSpace, _ *influxql.Schema) error {
	return nil
}

func (m StringFunctionTypeMapper) CallType(name string, _ []influxql.DataType) (influxql.DataType, error) {
	switch name {
	case "str":
		return influxql.Boolean, nil
	case "strlen":
		return influxql.Integer, nil
	case "substr":
		return influxql.String, nil
	default:
		return influxql.Unknown, nil
	}
}

func StrCallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1 influxql.DataType
	if len(args) != 2 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.String:
		return influxql.Boolean, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}
}

func StrLenCallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) != 1 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0 = args[0]
	switch arg0 {
	case influxql.String:
		return influxql.Integer, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func SubStrCallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1, arg2 influxql.DataType
	if len(args) < 2 || len(args) > 3 {
		return influxql.Unknown, fmt.Errorf("invalid argument number in %s(): %d", name, len(args))
	}
	arg0, arg1 = args[0], args[1]
	if len(args) == 3 {
		arg2 = args[2]
	}

	switch arg0 {
	case influxql.String:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.Integer:
		if len(args) == 2 {
			return influxql.String, nil
		}
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg0)
	}

	switch arg2 {
	case influxql.Integer:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the third argument in %s(): %s", name, arg0)
	}
}
