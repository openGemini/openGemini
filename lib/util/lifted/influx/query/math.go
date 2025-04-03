package query

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/math.go

2022.01.23 It has been modified to compatible files in influx/influxql and influx/query.
Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var (
	_ = RegistryMaterializeFunction("abs", &absFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("sin", &sinFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cos", &cosFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("tan", &tanFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("asin", &asinFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("acos", &acosFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("atan", &atanFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("atan2", &atan2Func{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("exp", &expFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("log", &logFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("ln", &lnFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("log2", &log2Func{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("log10", &log10Func{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("sqrt", &sqrtFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("pow", &powFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("floor", &floorFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("ceil", &ceilFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("round", &roundFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("round_prom", &roundPromFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("row_max", &rowMaxFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("if", &ifFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cast_int64", &castInt64Func{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cast_float64", &castFloat64Func{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cast_bool", &castBoolFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cast_string", &castStringFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("clamp_prom", &promClampFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("clamp_max_prom", &promClampMaxMinFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("clamp_min_prom", &promClampMaxMinFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("rad", &radFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("deg", &degFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("sinh", &sinhFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("cosh", &coshFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("tanh", &tanhFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("truncate", &truncateFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("asinh", &asinhFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("atanh", &atanhFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("sgn", &sgnFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
	_ = RegistryMaterializeFunction("acosh", &acoshFunc{
		BaseInfo: BaseInfo{FuncType: MATH},
	})
)

func GetMathFunction(name string) MaterializeFunc {
	materialize, ok := GetFunctionFactoryInstance().FindMaterFunc(name)
	if ok && materialize.GetFuncType() == MATH {
		return materialize
	}
	return nil
}

func compileMathFunction(expr *influxql.Call, c *compiledField, n int) error {
	if got := len(expr.Args); got != n {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, n, got)
	}
	// Compile all the argument expressions that are not just literals.
	for _, arg := range expr.Args {
		if _, ok := arg.(influxql.Literal); ok {
			continue
		}
		if err := c.compileExpr(arg); err != nil {
			return err
		}
	}
	return nil
}

type absFunc struct {
	BaseInfo
}

func processFunctionIf(expr *influxql.Call) error {
	conditionString := expr.Args[0].String()[1 : len(expr.Args[0].String())-1]
	conditionString = strings.Replace(conditionString, "\\", "", -1)
	condition, err := influxql.ParseExpr(conditionString)
	if err != nil {
		return fmt.Errorf("invalid condition, input like '\"key\" [operator] \\'string\\'' or '\"key\" [operator] digit'")
	}

	expr.Args[0] = condition
	return nil
}

func (f *absFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *absFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *absFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return math.Abs(arg0), true
	case int64:
		sign := arg0 >> 63
		return (arg0 ^ sign) - sign, true
	case uint64:
		return arg0, true
	default:
		return nil, true
	}
}

type sinFunc struct {
	BaseInfo
}

func (f *sinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *sinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *sinFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Sin(arg0), true
	}
	return nil, true
}

type cosFunc struct {
	BaseInfo
}

func (f *cosFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *cosFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *cosFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Cos(arg0), true
	}
	return nil, true
}

type tanFunc struct {
	BaseInfo
}

func (f *tanFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *tanFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *tanFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Tan(arg0), true
	}
	return nil, true
}

type asinFunc struct {
	BaseInfo
}

func (f *asinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *asinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType2(name, args)
}

func (f *asinFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Asin(arg0), true
	}
	return nil, true
}

type acosFunc struct {
	BaseInfo
}

func (f *acosFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *acosFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType2(name, args)
}

func (f *acosFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Acos(arg0), true
	}
	return nil, true
}

type atanFunc struct {
	BaseInfo
}

func (f *atanFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *atanFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *atanFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Atan(arg0), true
	}
	return nil, true
}

type atan2Func struct {
	BaseInfo
}

func (f *atan2Func) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 2)
}

func (f *atan2Func) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType3(name, args)
}

func (f *atan2Func) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, arg1, ok := asFloats(args[0], args[1]); ok {
		return math.Atan2(arg0, arg1), true
	}
	return nil, true
}

type expFunc struct {
	BaseInfo
}

func (f *expFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *expFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *expFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Exp(arg0), true
	}
	return nil, true
}

type logFunc struct {
	BaseInfo
}

func (f *logFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 2)
}

func (f *logFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *logFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, arg1, ok := asFloats(args[0], args[1]); ok {
		return math.Log(arg0) / math.Log(arg1), true
	}
	return nil, true
}

type lnFunc struct {
	BaseInfo
}

func (f *lnFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *lnFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *lnFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Log(arg0), true
	}
	return nil, true
}

type log2Func struct {
	BaseInfo
}

func (f *log2Func) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *log2Func) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *log2Func) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Log2(arg0), true
	}
	return nil, true
}

type log10Func struct {
	BaseInfo
}

func (f *log10Func) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *log10Func) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *log10Func) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Log10(arg0), true
	}
	return nil, true
}

type sqrtFunc struct {
	BaseInfo
}

func (f *sqrtFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *sqrtFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *sqrtFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Sqrt(arg0), true
	}
	return nil, true
}

type powFunc struct {
	BaseInfo
}

func (f *powFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 2)
}

func (f *powFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType3(name, args)
}

func (f *powFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, arg1, ok := asFloats(args[0], args[1]); ok {
		return math.Pow(arg0, arg1), true
	}
	return nil, true
}

type floorFunc struct {
	BaseInfo
}

func (f *floorFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *floorFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *floorFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return math.Floor(arg0), true
	case int64, uint64:
		return arg0, true
	default:
		return nil, true
	}
}

type ceilFunc struct {
	BaseInfo
}

func (f *ceilFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *ceilFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *ceilFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return math.Ceil(arg0), true
	case int64, uint64:
		return arg0, true
	default:
		return nil, true
	}
}

type roundFunc struct {
	BaseInfo
}

func (f *roundFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *roundFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *roundFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return round(arg0), true
	case int64, uint64:
		return arg0, true
	default:
		return nil, true
	}
}

type roundPromFunc struct {
	BaseInfo
}

func (f *roundPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if got := len(expr.Args); got != 1 && got != 2 {
		return fmt.Errorf("invalid number of arguments for %s, expected 1 or 2, got %d", expr.Name, got)
	}
	// Compile all the argument expressions that are not just literals.
	for _, arg := range expr.Args {
		if _, ok := arg.(influxql.Literal); ok {
			continue
		}
		if err := c.compileExpr(arg); err != nil {
			return err
		}
	}
	return nil
}

func (f *roundPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *roundPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0, ok := asFloat(args[0])
	if !ok {
		return nil, true
	}
	toNearest := float64(1)
	if len(args) == 2 {
		toNearest, _ = asFloat(args[1])
	}

	toNearestInverse := 1.0 / toNearest
	v := math.Floor(arg0*toNearestInverse+0.5) / toNearestInverse

	return v, true
}

type rowMaxFunc struct {
	BaseInfo
}

func (f *rowMaxFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 2)
}

func (f *rowMaxFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType4(name, args)
}

func (f *rowMaxFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return round(arg0), true
	case int64, uint64:
		return arg0, true
	default:
		return nil, true
	}
}

type ifFunc struct {
	BaseInfo
}

func (f *ifFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	// Syntax like if(CONDITION, THEN, ELSE), the CONDITION is an expression like '"key"=/'value/''
	// the THEN and ELSE are names of Tag or Field in ColumnStore and only support field in tsStore.
	err := compileMathFunction(expr, c, 3)
	if err != nil {
		return err
	}
	return processFunctionIf(expr)
}

func (f *ifFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return callTypeFunctionIf(name, args)
}

func (f *ifFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	conditionMet, noError := args[0].(bool)
	if noError {
		if conditionMet {
			return args[1], true
		}
		return args[2], true
	}
	return nil, false
}

type castInt64Func struct {
	BaseInfo
}

func (f *castInt64Func) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *castInt64Func) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Integer, nil
}

func (f *castInt64Func) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return int64(arg0), true
	case int64:
		return int64(arg0), true
	case uint64:
		return int64(arg0), true
	case bool:
		if arg0 == true {
			return int64(1), true
		} else {
			return int64(0), true
		}
	case string:
		result, err := strconv.Atoi(arg0)
		if err != nil {
			return nil, false
		}
		return result, true
	default:
		return nil, false
	}
}

type castFloat64Func struct {
	BaseInfo
}

func (f *castFloat64Func) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *castFloat64Func) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

func (f *castFloat64Func) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return float64(arg0), true
	case int64:
		return float64(arg0), true
	case uint64:
		return float64(arg0), true
	case bool:
		if arg0 == true {
			return float64(1), true
		} else {
			return float64(0), true
		}
	case string:
		result, err := strconv.ParseFloat(arg0, 64)
		if err != nil {
			return nil, false
		}
		return result, true
	default:
		return nil, false
	}
}

type castBoolFunc struct {
	BaseInfo
}

func (f *castBoolFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *castBoolFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Boolean, nil
}

func (f *castBoolFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	arg0 := args[0]
	if arg0 == nil {
		return false, true
	}
	if arg0, ok := asFloat(arg0); ok {
		if arg0 == 0 {
			return false, true
		} else {
			return true, true
		}
	}
	switch arg0 := arg0.(type) {
	case bool:
		return arg0, true
	case string:
		if strings.ToLower(arg0) == "0" || strings.ToLower(arg0) == "" {
			return false, true
		}
		return true, true
	default:
		return nil, false
	}
}

type castStringFunc struct {
	BaseInfo
}

func (f *castStringFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *castStringFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.String, nil
}

func (f *castStringFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	switch arg0 := args[0].(type) {
	case float64:
		return strconv.FormatFloat(arg0, 'f', -1, 64), true
	case int64:
		return strconv.FormatInt(arg0, 10), true
	case uint64:
		return strconv.FormatUint(arg0, 10), true
	case bool:
		if arg0 == false {
			return "false", true
		} else {
			return "true", true
		}
	case string:
		return arg0, true
	default:
		return nil, false
	}
}

type promClampFunc struct {
	BaseInfo
}

func (f *promClampFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if exp, got := 3, len(expr.Args); exp != got {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", name, exp, got)
	}

	arg1, err := getNumberArg(args[1], expr.Name)
	if err != nil {
		return err
	}

	arg2, err := getNumberArg(args[2], expr.Name)
	if err != nil {
		return err
	}

	if arg1 > arg2 {
		return fmt.Errorf("expected argument1 < argument2")
	}

	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *promClampFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

func (f *promClampFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if min, max, ok := asFloats(args[1], args[2]); ok {
		if math.IsNaN(min) || math.IsNaN(max) {
			return math.NaN(), true
		}
		if min > max {
			return nil, true
		}
		if val, ok := asFloat(args[0]); ok {
			return math.Max(min, math.Min(max, val)), true
		}
		return nil, true
	}
	return nil, true
}

type promClampMaxMinFunc struct {
	BaseInfo
}

func (f *promClampMaxMinFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	args, name := expr.Args, expr.Name
	if exp, got := 2, len(expr.Args); exp != got {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", name, exp, got)
	}

	err := checkNumberArg(args[1], expr.Name)
	if err != nil {
		return err
	}

	return c.compileSymbol(expr.Name, expr.Args[0])
}

func (f *promClampMaxMinFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

func (f *promClampMaxMinFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if val, arg1, ok := asFloats(args[0], args[1]); ok {
		if math.IsNaN(arg1) {
			return math.NaN(), true
		}
		if name == "clamp_max_prom" {
			return math.Min(val, arg1), true
		} else if name == "clamp_min_prom" {
			return math.Max(val, arg1), true
		}
	}
	return nil, true
}

type radFunc struct {
	BaseInfo
}

func (f *radFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *radFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *radFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return arg0 * math.Pi / 180, true
	}
	return nil, true
}

type degFunc struct {
	BaseInfo
}

func (f *degFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *degFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *degFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return arg0 * 180 / math.Pi, true
	}
	return nil, true
}

type sinhFunc struct {
	BaseInfo
}

func (f *sinhFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *sinhFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *sinhFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Sinh(arg0), true
	}
	return nil, true
}

type coshFunc struct {
	BaseInfo
}

func (f *coshFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *coshFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *coshFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Cosh(arg0), true
	}
	return nil, true
}

type tanhFunc struct {
	BaseInfo
}

func (f *tanhFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *tanhFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *tanhFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Tanh(arg0), true
	}
	return nil, true
}

type truncateFunc struct {
	BaseInfo
}

func (f *truncateFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("invalid argument number in %s(): %d", expr.Name, len(expr.Args))
	}

	return compileMathFunction(expr, c, 1)
}

func (f *truncateFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *truncateFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Trunc(arg0), true
	}
	return nil, true
}

type asinhFunc struct {
	BaseInfo
}

func (f *asinhFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *asinhFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *asinhFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Asinh(arg0), true
	}
	return nil, true
}

type atanhFunc struct {
	BaseInfo
}

func (f *atanhFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *atanhFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *atanhFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Atanh(arg0), true
	}
	return nil, true
}

type sgnFunc struct {
	BaseInfo
}

func (f *sgnFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *sgnFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *sgnFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		switch {
		case arg0 < 0:
			return float64(-1), true
		case arg0 > 0:
			return float64(1), true
		default:
			return arg0, true
		}
	}
	return nil, true
}

type acoshFunc struct {
	BaseInfo
}

func (f *acoshFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return compileMathFunction(expr, c, 1)
}

func (f *acoshFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return commonCallType1(name, args)
}

func (f *acoshFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if arg0, ok := asFloat(args[0]); ok {
		return math.Acosh(arg0), true
	}
	return nil, true
}

func checkNumberArg(arg influxql.Expr, callName string) error {
	switch arg.(type) {
	case *influxql.IntegerLiteral:
	case *influxql.NumberLiteral:
	default:
		return fmt.Errorf("expected float argument in %s()", callName)
	}
	return nil
}

func getNumberArg(arg influxql.Expr, callName string) (float64, error) {
	switch a := arg.(type) {
	case *influxql.IntegerLiteral:
		return float64(a.Val), nil
	case *influxql.NumberLiteral:
		return a.Val, nil
	default:
		return 0, fmt.Errorf("expected float argument in %s()", callName)
	}
	return 0, fmt.Errorf("expected float argument in %s()", callName)
}

func commonCallType1(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) > 0 {
		arg0 = args[0]
	}
	switch arg0 {
	case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
		return influxql.Float, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func commonCallType2(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) > 0 {
		arg0 = args[0]
	}
	switch arg0 {
	case influxql.Float, influxql.Unknown, influxql.Integer:
		return influxql.Float, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func commonCallType3(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0, arg1 influxql.DataType
	if len(args) > 0 {
		arg0 = args[0]
	}
	if len(args) > 1 {
		arg1 = args[1]
	}

	switch arg0 {
	case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
		// Pass through to verify the second argument.
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}

	switch arg1 {
	case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
		return influxql.Float, nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the second argument in %s(): %s", name, arg1)
	}
}

func commonCallType4(name string, args []influxql.DataType) (influxql.DataType, error) {
	var arg0 influxql.DataType
	if len(args) > 0 {
		arg0 = args[0]
	}
	switch arg0 {
	case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.Unknown:
		return args[0], nil
	default:
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, arg0)
	}
}

func callTypeFunctionIf(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[1] != args[2] {
		return influxql.Unknown, fmt.Errorf("the 2nd and 3rd argument must be of same type in %s()", name)
	}
	callType := args[1]
	switch callType {
	case influxql.Float, influxql.Integer, influxql.String, influxql.Boolean, influxql.Time, influxql.Duration, influxql.Unsigned:
		return callType, nil
	case influxql.Tag:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("unkonw data type")
	}
}

type MathTypeMapper struct{}

func (MathTypeMapper) MapType(measurement *influxql.Measurement, field string) influxql.DataType {
	return influxql.Unknown
}

func (MathTypeMapper) MapTypeBatch(measurement *influxql.Measurement, field map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	return nil
}

func (MathTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if mathFunc := GetMathFunction(name); mathFunc != nil {
		return mathFunc.CallTypeFunc(name, args)
	}
	return influxql.Unknown, nil
}

type MathValuer struct{}

var _ influxql.CallValuer = MathValuer{}

func (MathValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (MathValuer) SetValuer(v influxql.Valuer, index int) {

}

func (v MathValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if mathFunc := GetMathFunction(name); mathFunc != nil {
		return mathFunc.CallFunc(name, args)
	}
	return nil, false
}

func asFloat(x interface{}) (float64, bool) {
	switch arg0 := x.(type) {
	case float64:
		return arg0, true
	case int64:
		return float64(arg0), true
	case uint64:
		return float64(arg0), true
	default:
		return 0, false
	}
}

func asFloats(x, y interface{}) (float64, float64, bool) {
	arg0, ok := asFloat(x)
	if !ok {
		return 0, 0, false
	}
	arg1, ok := asFloat(y)
	if !ok {
		return 0, 0, false
	}
	return arg0, arg1, true
}

func round(x float64) float64 {
	t := math.Trunc(x)
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}
