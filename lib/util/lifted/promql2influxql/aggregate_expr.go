package promql2influxql

import (
	"sort"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

type aggregateFn struct {
	name string
	// keep the metric calculated by the upper layer as the label.
	KeepFill               bool
	keepMetric             bool
	expectIntegerParameter bool
	functionType           FunctionType
	vectorPosition         int
}

var aggregateFns = map[parser.ItemType]aggregateFn{
	parser.SUM:          {name: "sum", functionType: AGGREGATE_FN},
	parser.AVG:          {name: "mean", functionType: AGGREGATE_FN},
	parser.MAX:          {name: "max_prom", functionType: SELECTOR_FN},
	parser.MIN:          {name: "min_prom", functionType: SELECTOR_FN},
	parser.COUNT:        {name: "count_prom", functionType: AGGREGATE_FN},
	parser.STDDEV:       {name: "stddev_prom", functionType: AGGREGATE_FN},
	parser.TOPK:         {name: "top", functionType: SELECTOR_FN, expectIntegerParameter: true, keepMetric: true},
	parser.BOTTOMK:      {name: "bottom", functionType: SELECTOR_FN, expectIntegerParameter: true, keepMetric: true},
	parser.QUANTILE:     {name: "quantile_prom", functionType: SELECTOR_FN},
	parser.COUNT_VALUES: {name: "count_values_prom", functionType: AGGREGATE_FN},
	parser.STDVAR:       {name: "stdvar_prom", functionType: AGGREGATE_FN},
	parser.GROUP:        {name: "group_prom", functionType: AGGREGATE_FN},
}

// generateDimension is used to generate the dimensions of group by to Dimensions.
func (t *Transpiler) generateDimension(statement *influxql.SelectStatement, grouping ...string) {
	if len(grouping) == 0 {
		return
	}
	statement.Dimensions = make([]*influxql.Dimension, len(grouping))
	for i, group := range grouping {
		statement.Dimensions[i] = &influxql.Dimension{Expr: &influxql.VarRef{Val: group}}
	}
}

// without == true
func (t *Transpiler) setAggregateDimensionOfSubquery(dims influxql.Dimensions, statement *influxql.SelectStatement, grouping ...string) {
	if len(dims) == 0 {
		statement.Without = true
		t.generateDimension(statement, grouping...)
		return
	}
	_, dimRefs := dims.Normalize()
	if len(dimRefs) == 0 {
		var isGroupByStar bool
		for i := range dims {
			expr, ok := dims[i].Expr.(*influxql.Wildcard)
			if ok && (expr.Type == influxql.ILLEGAL || expr.Type == influxql.TAG) {
				isGroupByStar = true
				break
			}
		}
		if isGroupByStar {
			// exclude grouping from full set to generate dimension
			statement.Without = true
			t.generateDimension(statement, grouping...)
		}
		return
	}
	sort.Strings(dimRefs)
	groupMap := make(map[string]bool, len(grouping))
	for i := range grouping {
		groupMap[grouping[i]] = true
	}
	var newGrouping []string
	for i := range dimRefs {
		if !groupMap[dimRefs[i]] {
			newGrouping = append(newGrouping, dimRefs[i])
		}
	}
	// exclude grouping from sub-query dimension to generate dimension
	t.generateDimension(statement, newGrouping...)
}

// setAggregateDimension sets the group by expression of selectStatement
func (t *Transpiler) setAggregateDimension(statement *influxql.SelectStatement, without bool, grouping ...string) {
	// if without is false, dimension is generated based on grouping.
	if !without {
		t.generateDimension(statement, grouping...)
		return
	}
	// if without is true, dimension is generated based on the dimension excluding grouping for the sub-query or full set
	if len(statement.Sources) != 1 {
		panic("the number of source should be 1 for promql agg query")
	}
	switch source := statement.Sources[0].(type) {
	case *influxql.Measurement:
		// exclude grouping from full set to generate dimension
		statement.Without = true
		t.generateDimension(statement, grouping...)
	case *influxql.SubQuery:
		// subquery is without, skip setAggDims perf
		if source.Statement.Without {
			statement.Without = true
			t.generateDimension(statement, grouping...)
			return
		}
		t.setAggregateDimensionOfSubquery(source.Statement.Dimensions, statement, grouping...)
	case *influxql.BinOp:
		unionDims := make(influxql.Dimensions, 0)
		lsource := source.LSrc.(*influxql.SubQuery)
		rsource := source.RSrc.(*influxql.SubQuery)
		if lsource.Statement.Without || rsource.Statement.Without {
			statement.Without = true
			t.generateDimension(statement, grouping...)
			return
		}
		unionDims = append(unionDims, lsource.Statement.Dimensions...)
		unionDims = append(unionDims, rsource.Statement.Dimensions...)
		t.setAggregateDimensionOfSubquery(unionDims, statement, grouping...)
	}
}

// setAggregateFields sets the field of selectStatement
func (t *Transpiler) setAggregateFields(selectStatement *influxql.SelectStatement, field *influxql.Field, parameter []influxql.Expr, aggFn aggregateFn) {
	var fields []*influxql.Field
	aggArgs := []influxql.Expr{field.Expr}
	for _, p := range parameter {
		arg, ok := p.(*influxql.NumberLiteral)
		if ok {
			if aggFn.expectIntegerParameter {
				aggArgs = append(aggArgs, &influxql.IntegerLiteral{
					Val: int64(arg.Val),
				})
			} else {
				aggArgs = append(aggArgs, &influxql.NumberLiteral{
					Val: arg.Val,
				})
			}
		} else {
			aggArgs = append(aggArgs, p)
		}
	}
	fields = append(fields, &influxql.Field{
		Expr: &influxql.Call{Name: aggFn.name, Args: aggArgs}, Alias: DefaultFieldKey,
	})
	selectStatement.Fields = fields
}

// transpileAggregateExpr transpiles PromQL AggregateExpr to InfluxQL SelectStatement
func (t *Transpiler) transpileAggregateExpr(a *parser.AggregateExpr) (influxql.Node, error) {
	// Recursively transpile sub expression
	t.dropMetric = true
	expr, err := t.transpileExpr(a.Expr)
	if err != nil {
		return nil, errno.NewError(errno.TranspileAggFail, err.Error())
	}
	// Get Aggregate function parameter
	var parameter []influxql.Expr
	if a.Param != nil {
		unwrapParenExpr(&a.Param)
		a.Param = unwrapStepInvariantExpr(a.Param)
		if !yieldsFloat(a.Param) && !yieldsString(a.Param) {
			return nil, errno.NewError(errno.ErrFloatParamAggExpr)
		}
		param, err := t.transpileExpr(a.Param)
		if err != nil {
			return nil, errno.NewError(errno.TranspileAggFail, err.Error())
		}
		parameter = []influxql.Expr{param.(influxql.Expr)}
	}
	aggFn, ok := aggregateFns[a.Op]
	if !ok {
		return nil, errno.NewError(errno.UnsupportedAggType, a.Op.String())
	}

	node, ok := expr.(influxql.Statement)
	if !ok {
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}

	switch statement := node.(type) {
	case *influxql.SelectStatement:
		// Get the last field of sub expression. The last field is the matrix value.
		field := statement.Fields[len(statement.Fields)-1]
		switch field.Expr.(type) {
		case *influxql.Call, *influxql.BinaryExpr, *influxql.ParenExpr:
			if t.canPushDownAggWithFunction(a, statement, field, parameter, aggFn) {
				return statement, nil
			}
			// If the field is a Call expression, we need to wrap the sub expression as InfluxQL SubQuery
			selectStatement := &influxql.SelectStatement{
				Sources: []influxql.Source{
					&influxql.SubQuery{
						Statement: statement,
					}},
				IsPromQuery: true,
			}
			wrappedField := &influxql.Field{
				Expr: &influxql.VarRef{
					Val:   field.Name(),
					Alias: DefaultFieldKey,
				},
				Alias: DefaultFieldKey,
			}
			t.setAggregateFields(selectStatement, wrappedField, parameter, aggFn)
			t.setTimeCondition(selectStatement)
			if aggFn.keepMetric {
				selectStatement.Dimensions = statement.Dimensions
				return selectStatement, nil
			}
			t.setAggregateDimension(selectStatement, a.Without, a.Grouping...)
			if t.Step > 0 {
				t.setTimeInterval(selectStatement)
				selectStatement.Fill = influxql.NoFill
			}
			return selectStatement, nil
		case *influxql.VarRef:
			t.setAggregateFields(statement, field, parameter, aggFn)
			if !aggFn.keepMetric {
				statement.Dimensions = statement.Dimensions[:0]
				t.setAggregateDimension(statement, a.Without, a.Grouping...)
			}
			if t.Step > 0 {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
			}

			if aggFn.name == "count_values_prom" {
				return t.transpileCountValues(statement, a, aggFn, field, parameter), nil
			}
			return statement, nil

		default:
			return nil, errno.NewError(errno.UnsupportedPromExpr)
		}
	default:
		return expr, nil
	}
}

// canPushDownAggWithFunction used to optimizing the nested push down of function and aggregate operator
func (t *Transpiler) canPushDownAggWithFunction(agg *parser.AggregateExpr, statement *influxql.SelectStatement, field *influxql.Field, parameter []influxql.Expr, aggFn aggregateFn) bool {
	if aggFn.name == "mean" {
		return false
	}
	call, ok := field.Expr.(*influxql.Call)
	if !ok {
		return false
	}
	_, ok = rangeVectorFunctions[getOriCallName(call.Name)]
	if !ok {
		return false
	}
	if statement.Range <= 0 {
		return false
	}
	t.setAggregateFields(statement, field, parameter, aggFn)
	if !aggFn.keepMetric {
		statement.Dimensions = statement.Dimensions[:0]
		t.setAggregateDimension(statement, agg.Without, agg.Grouping...)
		if t.Step > 0 {
			t.setTimeInterval(statement)
			statement.Fill = influxql.NoFill
		}
	}
	return true
}

func (t *Transpiler) transpileCountValues(statement *influxql.SelectStatement, a *parser.AggregateExpr, aggFn aggregateFn, field *influxql.Field, parameter []influxql.Expr) *influxql.SelectStatement {
	selectStatement := &influxql.SelectStatement{
		Sources: []influxql.Source{
			&influxql.SubQuery{
				Statement: statement,
			}},
		IsPromQuery: true,
	}
	wrappedField := &influxql.Field{
		Expr: &influxql.VarRef{
			Val:   field.Name(),
			Alias: DefaultFieldKey,
		},
		Alias: DefaultFieldKey,
	}
	sumFn := aggregateFns[parser.SUM]
	t.setAggregateFields(selectStatement, wrappedField, nil, sumFn)
	t.setTimeCondition(selectStatement)
	selectStatement.LookBackDelta = t.LookBackDelta
	selectStatement.QueryOffset = statement.QueryOffset
	selectStatement.Step = statement.Step
	if sumFn.keepMetric {
		selectStatement.Dimensions = statement.Dimensions
		return selectStatement
	}
	grouping := getCountValuesGrouping(a)
	t.setAggregateDimension(selectStatement, a.Without, grouping...)
	if t.Step > 0 {
		t.setTimeInterval(selectStatement)
		selectStatement.Fill = influxql.NoFill
	}
	return selectStatement
}

func getCountValuesGrouping(a *parser.AggregateExpr) []string {
	grouping := make([]string, 0, len(a.Grouping))
	param, ok := a.Param.(*parser.StringLiteral)
	if !ok {
		return a.Grouping[:len(a.Grouping)]
	}
	paramName := param.Val
	if !a.Without {
		if len(a.Grouping) > 0 {
			grouping = a.Grouping[:len(a.Grouping)]
		}
		for _, name := range a.Grouping {
			if name == paramName {
				return grouping
			}
		}
		grouping = append(grouping, paramName)
	} else {
		for _, name := range a.Grouping {
			if name == paramName {
				continue
			}
			grouping = append(grouping, name)
		}
	}

	return grouping
}
