package promql2influxql

import (
	"fmt"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

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
		name:         "stddev_over_time",
		functionType: AGGREGATE_FN,
	},
	"quantile_over_time": {
		name:           "quantile_over_time",
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
}

var instantVectorFunctions = map[string]aggregateFn{
	"histogram_quantile": {
		name:           "histogram_quantile",
		functionType:   AGGREGATE_FN,
		vectorPosition: 1,
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
		name:         "log",
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
		name:         "round",
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
		field := statement.Fields[len(statement.Fields)-1]
		switch field.Expr.(type) {
		case *influxql.Call:
			selectStatement := &influxql.SelectStatement{
				Sources: []influxql.Source{
					&influxql.SubQuery{
						Statement: statement,
					}},
				IsPromQuery: true,
			}
			wrappedField := &influxql.Field{
				Expr: &influxql.VarRef{
					Val: field.Name(),
				},
				Alias: DefaultFieldKey,
			}
			setFieldsFunc(selectStatement, wrappedField, parameter, aggFn)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(selectStatement)
				selectStatement.Fill = influxql.NoFill
			}
			return selectStatement, nil
		default:
			setFieldsFunc(statement, field, parameter, aggFn)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
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
		return t.transpileVectorMathFunc(fn, args)
	}

	if fn, ok := vectorLabelFunctions[a.Func.Name]; ok {
		return t.transpileVectorLabelFunc(fn, args)
	}

	if fn, ok := vectorTimeFunctions[a.Func.Name]; ok {
		return t.transpileVectorTimeFunc(fn, args)
	}
	return nil, errno.NewError(errno.UnsupportedPromExpr)
}

func (t *Transpiler) transpileTimeFunc2CallExpr(aggFn aggregateFn) (influxql.Expr, error) {
	callExpr := &influxql.Call{Name: aggFn.name, Args: []influxql.Expr{&influxql.VarRef{Val: ArgNameOfTimeFunc}}}
	return callExpr, nil
}
