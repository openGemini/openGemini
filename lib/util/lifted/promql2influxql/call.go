package promql2influxql

import (
	"fmt"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

func init() {
	parser.EnableExperimentalFunctions = true
}

var rangeVectorFunctions = map[string]aggregateFn{
	"sum_over_time": {
		name:         "sum_over_time",
		functionType: AGGREGATE_FN,
	},
	"avg_over_time": {
		name:         "avg_over_time",
		functionType: AGGREGATE_FN,
	},
	"max_over_time": {
		name:         "max_over_time",
		functionType: SELECTOR_FN,
	},
	"min_over_time": {
		name:         "min_over_time",
		functionType: SELECTOR_FN,
	},
	"count_over_time": {
		name:         "count_over_time",
		functionType: AGGREGATE_FN,
	},
	"stddev_over_time": {
		name:         "stddev_over_time_prom",
		functionType: AGGREGATE_FN,
	},
	"present_over_time": {
		name:         "present_over_time_prom",
		functionType: AGGREGATE_FN,
	},
	"last_over_time": {
		name:         "last_over_time_prom",
		functionType: AGGREGATE_FN,
		keepMetric:   true,
	},
	"quantile_over_time": {
		name:           "quantile_over_time_prom",
		functionType:   SELECTOR_FN,
		vectorPosition: 1,
	},
	"rate": {
		name:         "rate_prom",
		functionType: TRANSFORM_FN,
	},
	"irate": {
		name:         "irate_prom",
		functionType: TRANSFORM_FN,
	},
	"deriv": {
		name:         "deriv",
		functionType: TRANSFORM_FN,
	},
	"predict_linear": {
		name:         "predict_linear",
		functionType: TRANSFORM_FN,
	},
	"increase": {
		name:         "increase",
		functionType: TRANSFORM_FN,
	},
	"delta": {
		name:         "delta_prom",
		functionType: TRANSFORM_FN,
	},
	"idelta": {
		name:         "idelta_prom",
		functionType: TRANSFORM_FN,
	},
	"stdvar_over_time": {
		name:         "stdvar_over_time_prom",
		functionType: TRANSFORM_FN,
	},
	"holt_winters": {
		name:           "holt_winters_prom",
		functionType:   SELECTOR_FN,
		vectorPosition: 0,
	},
	"changes": {
		name:         "changes_prom",
		functionType: TRANSFORM_FN,
	},
	"resets": {
		name:         "resets_prom",
		functionType: TRANSFORM_FN,
	},
	"absent_over_time": {
		name:         "absent_over_time_prom",
		functionType: AGGREGATE_FN,
	},
	"mad_over_time": {
		name:         "mad_over_time_prom",
		functionType: SELECTOR_FN,
	},
}

var instantVectorFunctions = map[string]aggregateFn{
	"histogram_quantile": {
		name:           "histogram_quantile",
		functionType:   AGGREGATE_FN,
		vectorPosition: 1,
	},
	"scalar": {
		name:         "scalar_prom",
		functionType: AGGREGATE_FN,
	},
	"absent": {
		name:         "absent_prom",
		functionType: AGGREGATE_FN,
	},
}

var vectorMathFunctions = map[string]aggregateFn{
	"abs": {
		name:         "abs",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"ceil": {
		name:         "ceil",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"floor": {
		name:         "floor",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"exp": {
		name:         "exp",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"sqrt": {
		name:         "sqrt",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"ln": {
		name:         "ln",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"log2": {
		name:         "log2",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"log10": {
		name:         "log10",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"round": {
		name:         "round_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"acos": {
		name:         "acos",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"asin": {
		name:         "asin",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"atan": {
		name:         "atan",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"cos": {
		name:         "cos",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"sin": {
		name:         "sin",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"tan": {
		name:         "tan",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"clamp": {
		name:         "clamp_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"clamp_max": {
		name:         "clamp_max_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"clamp_min": {
		name:         "clamp_min_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"rad": {
		name:         "rad",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"deg": {
		name:         "deg",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"sinh": {
		name:         "sinh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"cosh": {
		name:         "cosh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"tanh": {
		name:         "tanh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"asinh": {
		name:         "asinh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"atanh": {
		name:         "atanh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"sgn": {
		name:         "sgn",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"acosh": {
		name:         "acosh",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
}

var vectorLabelFunctions = map[string]aggregateFn{
	"label_replace": {
		name:         "label_replace",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"label_join": {
		name:         "label_join",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
}

var vectorTimeFunctions = map[string]aggregateFn{
	"year": {
		name:         "year_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"time": {
		name:         "time_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},

	"timestamp": {
		name:         "timestamp_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},

	"month": {
		name:         "month_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"minute": {
		name:         "minute_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"hour": {
		name:         "hour_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"day_of_week": {
		name:         "day_of_week_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"day_of_month": {
		name:         "day_of_month_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"day_of_year": {
		name:         "day_of_year_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"days_in_month": {
		name:         "days_in_month_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"vector": {
		name:         "vector_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
	"pi": {
		name:         "pi_prom",
		functionType: TRANSFORM_FN,
		KeepFill:     true,
	},
}

var vectorSortFunctions = map[string]aggregateFn{
	"sort": {
		name:         "sort_prom",
		functionType: SELECTOR_FN,
	},
	"sort_desc": {
		name:         "sort_desc_prom",
		functionType: SELECTOR_FN,
	},
	"sort_by_label": {
		name:           "sort_by_label_prom",
		functionType:   SELECTOR_FN,
		vectorPosition: 0,
	},
	"sort_by_label_desc": {
		name:           "sort_by_label_desc_prom",
		functionType:   SELECTOR_FN,
		vectorPosition: 0,
	},
}

func getOriCallName(call string) string {
	if strings.HasSuffix(call, PromSuffix) {
		return call[:len(call)-len(PromSuffix)]
	}
	return call
}

type SetFieldsFunc func(selectStatement *influxql.SelectStatement, field *influxql.Field, parameter []influxql.Expr, aggFn aggregateFn)

func (t *Transpiler) transpileVectorTimeFunc(aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	if len(inArgs) == 0 {
		return t.transpileTimeFunc2CallExpr(aggFn)
	}
	return t.transpilePromFunc(aggFn, inArgs, t.setAggregateFields)
}

func (t *Transpiler) transpileVectorMathFunc(aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	return t.transpilePromFunc(aggFn, inArgs, t.setAggregateFields)
}

func (t *Transpiler) transpileVectorLabelFunc(aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	return t.transpilePromFunc(aggFn, inArgs, t.setLabelFuncFields)
}

func (t *Transpiler) transpileVectorSortFunc(aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	return t.transpileSort(aggFn, inArgs)
}

func (t *Transpiler) transpilePromFuncWithExprArgs(aggFn aggregateFn, arg influxql.Node) (influxql.Node, error) {
	callExpr := &influxql.Call{Name: aggFn.name, Args: []influxql.Expr{arg.(influxql.Expr)}}
	return callExpr, nil
}

func (t *Transpiler) transpilePromSubqueryFunc(subExpr *parser.SubqueryExpr, aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	table, parameter := t.transpileParameter(aggFn.vectorPosition, inArgs)

	node, ok := table.(influxql.Statement)
	if !ok {
		return nil, fmt.Errorf("transpilePromSubqueryFunc expr InArgs error")
	}
	switch statement := node.(type) {
	case *influxql.SelectStatement:
		subCall := influxql.PromSubCall{
			Name:      aggFn.name,
			StartTime: t.minT * int64(time.Millisecond),
			EndTime:   t.maxT * int64(time.Millisecond),
			Range:     subExpr.Range,
			InArgs:    parameter,
			Offset:    subExpr.Offset,
			SubStartT: t.subStartT * int64(time.Millisecond),
			SubEndT:   t.subEndT * int64(time.Millisecond),
			SubStep:   subExpr.Step.Nanoseconds(),
		}
		if t.lowerStepInvariant {
			subCall.LowerStepInvariant = true
			t.lowerStepInvariant = false
		}
		if t.Step != 0 {
			subCall.Interval = t.Step.Nanoseconds()
		} else {
			subCall.Interval = t.timeRange.Nanoseconds()
			if subCall.Interval == 0 {
				subCall.Interval = 1
			}
		}
		statement.PromSubCalls = append(statement.PromSubCalls, &subCall)
		if aggFn.name == "absent_over_time_prom" {
			field, _ := getSelectFieldIdx(statement)
			statement = &influxql.SelectStatement{
				Sources:     []influxql.Source{&influxql.SubQuery{Statement: statement}},
				Step:        t.Step,
				Range:       subExpr.Range,
				IsPromQuery: true,
			}
			wrappedField := &influxql.Field{
				Expr:  &influxql.VarRef{Val: field.Name()},
				Alias: DefaultFieldKey,
			}
			start, end := timestamp.Time(t.minT-t.LookBackDelta.Milliseconds()-subExpr.Range.Milliseconds()-durationMilliseconds(subExpr.Offset)), timestamp.Time(t.maxT-durationMilliseconds(subExpr.Offset))
			statement.Range = statement.Range + t.LookBackDelta
			timeCondition := GetTimeCondition(&start, &end)
			statement.Condition = CombineConditionAnd(statement.Condition, timeCondition)
			t.setAggregateFields(statement, wrappedField, parameter, instantVectorFunctions["absent"])
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
			}
		}
		return statement, nil
	default:
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}
}

func (t *Transpiler) transpilePromFunc(aggFn aggregateFn, inArgs []influxql.Node, setFieldsFunc SetFieldsFunc) (influxql.Node, error) {
	table, parameter := t.transpileParameter(aggFn.vectorPosition, inArgs)

	node, ok := table.(influxql.Statement)
	if !ok {
		return t.transpilePromFuncWithExprArgs(aggFn, table)
	}
	switch statement := node.(type) {
	case *influxql.SelectStatement:
		field, _ := getSelectFieldIdx(statement)
		switch field.Expr.(type) {
		case *influxql.Call, *influxql.BinaryExpr:
			selectStatement := &influxql.SelectStatement{
				Sources: []influxql.Source{
					&influxql.SubQuery{
						Statement: statement,
					}},
				Step:        t.Step,
				IsPromQuery: true,
			}
			wrappedField := &influxql.Field{
				Expr: &influxql.VarRef{
					Val: field.Name(),
				},
				Alias: DefaultFieldKey,
			}
			setFieldsFunc(selectStatement, wrappedField, parameter, aggFn)
			t.setTimeCondition(selectStatement, false)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(selectStatement)
				selectStatement.Fill = influxql.NoFill
			}
			if aggFn.name == "histogram_quantile" {
				selectStatement.Dimensions = statement.Dimensions
			}
			return selectStatement, nil
		default:
			setFieldsFunc(statement, field, parameter, aggFn)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
			}
			if aggFn.name == "absent_over_time_prom" {
				field, _ := getSelectFieldIdx(statement)
				absentFunc := instantVectorFunctions["absent"]
				t.setAggregateFields(statement, field, parameter, absentFunc)
			}
		}
	default:
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}
	return table, nil
}

func (t *Transpiler) transpileParameter(pos int, inArgs []influxql.Node) (influxql.Node, []influxql.Expr) {
	table := inArgs[pos]
	parameter := make([]influxql.Expr, len(inArgs)-1)
	for i := 0; i < pos; i++ {
		parameter[i] = inArgs[i].(influxql.Expr)
	}
	for i := pos + 1; i < len(inArgs); i++ {
		parameter[i-1] = inArgs[i].(influxql.Expr)
	}
	return table, parameter
}

func (t *Transpiler) setLabelFuncFields(selectStatement *influxql.SelectStatement, field *influxql.Field, parameter []influxql.Expr, aggFn aggregateFn) {
	var fields []*influxql.Field
	aggArgs := []influxql.Expr{field.Expr}
	for _, p := range parameter {
		if str, ok := p.(*influxql.StringLiteral); ok {
			aggArgs = append(aggArgs, str)
		} else {
			aggArgs = append(aggArgs, p)
		}
	}
	fields = append(fields, &influxql.Field{
		Expr:  &influxql.Call{Name: aggFn.name, Args: aggArgs},
		Alias: DefaultFieldKey,
	})
	selectStatement.Fields = fields
}

func (t *Transpiler) transpileSort(aggFn aggregateFn, inArgs []influxql.Node) (influxql.Node, error) {
	table, parameter := t.transpileParameter(aggFn.vectorPosition, inArgs)
	node, ok := table.(influxql.Statement)
	if !ok {
		return t.transpilePromFuncWithExprArgs(aggFn, table)
	}
	switch statement := node.(type) {
	case *influxql.SelectStatement:
		isAscending := !strings.HasSuffix(aggFn.name, "_desc_prom")
		var sortFields influxql.SortFields
		switch aggFn.name {
		case "sort_prom", "sort_desc_prom":
			sortFields = influxql.SortFields{&influxql.SortField{
				Name:      DefaultFieldKey,
				Ascending: isAscending,
			}}
		case "sort_by_label_prom", "sort_by_label_desc_prom":
			sortFields = make(influxql.SortFields, len(parameter))
			for i, param := range parameter {
				varRef, ok := param.(*influxql.StringLiteral)
				if !ok {
					return nil, errno.NewError(errno.UnsupportedDataType)
				}
				sortFields[i] = &influxql.SortField{
					Name:      varRef.Val,
					Ascending: isAscending,
				}
			}
		}
		statement.SortFields = sortFields
	default:
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}
	return table, nil
}

// transpileCall transpiles PromQL Call expression
func (t *Transpiler) transpileCall(a *parser.Call) (influxql.Node, error) {
	// The PromQL parser already verifies argument counts and types, so we don't have to check this here.
	args := make([]influxql.Node, len(a.Args))
	for i := range a.Args {
		unwrapParenExpr(&a.Args[i])
		a.Args[i] = unwrapStepInvariantExpr(a.Args[i])
		tArg, err := t.transpileExpr(a.Args[i])
		if err != nil {
			return nil, errno.NewError(errno.TranspileFunctionFail, err.Error())
		}
		args[i] = tArg
	}

	// {count,avg,sum,min,max,...}_over_time()
	if fn, ok := rangeVectorFunctions[a.Func.Name]; ok {
		t.dropMetric = true
		if subExpr, subOk := a.Args[fn.vectorPosition].(*parser.SubqueryExpr); subOk {
			return t.transpilePromSubqueryFunc(subExpr, fn, args)
		}
		return t.transpilePromFunc(fn, args, t.setAggregateFields)
	}

	if fn, ok := instantVectorFunctions[a.Func.Name]; ok {
		t.dropMetric = true
		return t.transpilePromFunc(fn, args, t.setAggregateFields)
	}

	if fn, ok := vectorMathFunctions[a.Func.Name]; ok {
		t.dropMetric = true
		return t.transpileVectorMathFunc(fn, args)
	}

	if fn, ok := vectorLabelFunctions[a.Func.Name]; ok {
		return t.transpileVectorLabelFunc(fn, args)
	}

	if fn, ok := vectorTimeFunctions[a.Func.Name]; ok {
		if a.Func.Name != "time" {
			t.dropMetric = true
		}
		return t.transpileVectorTimeFunc(fn, args)
	}

	if fn, ok := vectorSortFunctions[a.Func.Name]; ok {
		return t.transpileVectorSortFunc(fn, args)
	}

	return nil, errno.NewError(errno.UnsupportedPromExpr)
}

func (t *Transpiler) transpileTimeFunc2CallExpr(aggFn aggregateFn) (influxql.Expr, error) {
	callExpr := &influxql.Call{Name: aggFn.name, Args: []influxql.Expr{&influxql.VarRef{Val: ArgNameOfTimeFunc}}}
	return callExpr, nil
}
