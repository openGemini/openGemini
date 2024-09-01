package promql2influxql

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// Transpiler is responsible for transpiling a single PromQL expression to InfluxQL expression.
// It will be gc-ed after its work done.
type Transpiler struct {
	PromCommand
	timeRange         time.Duration
	parenExprCount    int
	dropMetric        bool
	removeTableName   bool
	duplicateResult   bool
	minT, maxT        int64
	timeCondition     influxql.Expr
	isStepVariantExpr bool
}

func (t *Transpiler) rewriteMinMaxTime() {
	if t.Start != nil && t.End != nil {
		t.minT, t.maxT = timestamp.FromTime(*t.Start), timestamp.FromTime(*t.End)
	} else if t.Evaluation != nil {
		t.minT, t.maxT = timestamp.FromTime(*t.Evaluation), timestamp.FromTime(*t.Evaluation)
	}
}

// Transpile converts a PromQL expression with the time ranges set in the transpiler
// into an InfluxQL expression. The resulting InfluxQL expression can be executed and the result needs to
// be transformed using InfluxResultToPromQLValue() (implemented in the promql package of this repo)
// to get a result value that is fully equivalent to the result of a native PromQL
// execution.
// During the transpiling procedure, the transpiler recursively translates the PromQL AST into
// equivalent InfluxQL AST.
func (t *Transpiler) Transpile(expr parser.Expr) (influxql.Node, error) {
	if !IsMetaQuery(t.DataType) {
		s := t.newEvalStmt(expr)
		t.rewriteMinMaxTime()
		// Modify the offset of vector and matrix selectors for the @ modifier
		// w.r.t. the start time since only 1 evaluation will be done on them.
		setOffsetForAtModifier(timeMilliseconds(s.Start), s.Expr)
		expr = s.Expr
	}
	influxNode, err := t.transpile(expr)
	if err != nil {
		return nil, errno.NewError(errno.TranspileExprFail, err.Error())
	}
	return influxNode, nil
}

func (t *Transpiler) transpile(expr parser.Expr) (influxql.Node, error) {
	if t.Start != nil && expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
		return nil, errno.NewError(errno.InvalidExprType, parser.DocumentedType(expr.Type()))
	}
	return t.transpileExpr(expr)
}

// transpileExpr recursively transpile PromQL expression.
// TODO It doesn't support PromQL SubqueryExpr and StepInvariantExpr yet.
func (t *Transpiler) transpileExpr(expr parser.Expr) (influxql.Node, error) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return t.transpileParenExpr(e)
	case *parser.UnaryExpr:
		return t.transpileUnaryExpr(e)
	case *parser.NumberLiteral:
		return &influxql.NumberLiteral{Val: e.Val}, nil
	case *parser.StringLiteral:
		return &influxql.StringLiteral{Val: e.Val}, nil
	case *parser.VectorSelector:
		return t.transpileInstantVectorSelector(e)
	case *parser.MatrixSelector:
		return t.transpileRangeVectorSelector(e)
	case *parser.AggregateExpr:
		return t.transpileAggregateExpr(e)
	case *parser.BinaryExpr:
		return t.transpileBinaryExpr(e)
	case *parser.Call:
		return t.transpileCall(e)
	case *parser.StepInvariantExpr:
		return t.transpileStepInvariantExpr(e)
	case *parser.SubqueryExpr:
		return t.transpileSubqueryExpr(e)
	default:
		return nil, errno.NewError(errno.UnsupportedNodeType, expr.String())
	}
}

// setTimeInterval sets time interval and offset in InfluxQL GROUP BY clause
func (t *Transpiler) setTimeInterval(statement *influxql.SelectStatement) {
	interval := &influxql.Dimension{
		Expr: &influxql.Call{
			Name: TimeField,
			Args: []influxql.Expr{
				&influxql.DurationLiteral{Val: t.Step},
			},
		},
	}
	defer func() {
		statement.Dimensions = append(statement.Dimensions, interval)
	}()
	if t.Start == nil || t.Step == 0 {
		return
	}
	remain := t.Start.UnixNano() % t.Step.Nanoseconds()
	offset := time.Duration(remain) * time.Nanosecond
	interval.Expr.(*influxql.Call).Args = append(
		interval.Expr.(*influxql.Call).Args,
		&influxql.DurationLiteral{Val: offset},
	)
	return
}

// setTimeCondition sets time range and timezone condition in InfluxQL WHERE clause
func (t *Transpiler) setTimeCondition(node influxql.Statement) {
	switch statement := node.(type) {
	case *influxql.SelectStatement:
		statement.Condition = CombineConditionAnd(statement.Condition, t.timeCondition)
		statement.Location = t.Timezone
	case *influxql.ShowTagValuesStatement:
		statement.Condition = CombineConditionAnd(statement.Condition, t.timeCondition)
	default:
	}
}

// DropMetric determines whether the promql is an aggregate query.
func (t *Transpiler) DropMetric() bool {
	return t.dropMetric
}

func (t *Transpiler) RemoveTableName() bool {
	return t.removeTableName
}

func (t *Transpiler) DuplicateResult() bool {
	return t.duplicateResult
}

func (t *Transpiler) newEvalStmt(expr parser.Expr) *parser.EvalStmt {
	s := &parser.EvalStmt{}
	if t.PromCommand.Step == 0 {
		s.Start = *t.PromCommand.Evaluation
		s.End = *t.PromCommand.Evaluation
		s.Interval = 0
	} else {
		s.Start = *t.PromCommand.Start
		s.End = *t.PromCommand.End
		s.Interval = t.PromCommand.Step
	}
	s.Expr = t.PreprocessExpr(expr, s.Start, s.End)
	return s
}

// PreprocessExpr wraps all possible step invariant parts of the given expression with
// StepInvariantExpr. It also resolves the preprocessors.
func (t *Transpiler) PreprocessExpr(expr parser.Expr, start, end time.Time) parser.Expr {
	isStepInvariant := preprocessExprHelper(expr, start, end)
	if isStepInvariant {
		t.isStepVariantExpr = true
		return newStepInvariantExpr(expr)
	}
	return expr
}

func (t *Transpiler) findMinMaxTime(s *parser.EvalStmt) (int64, int64) {
	var minTimestamp, maxTimestamp int64 = math.MaxInt64, math.MinInt64
	// Whenever a MatrixSelector is evaluated, evalRange is set to the corresponding range.
	// The evaluation of the VectorSelector inside then evaluates the given range and unsets
	// the variable.
	var evalRange time.Duration
	parser.Inspect(s.Expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			start, end := t.getTimeRangesForSelector(s, n, path, evalRange)
			if start < minTimestamp {
				minTimestamp = start
			}
			if end > maxTimestamp {
				maxTimestamp = end
			}
			evalRange = 0

		case *parser.MatrixSelector:
			evalRange = n.Range
		}
		return nil
	})

	if maxTimestamp == math.MinInt64 {
		// This happens when there was no selector. Hence no time range to select.
		minTimestamp = 0
		maxTimestamp = 0
	}

	return minTimestamp, maxTimestamp
}

func (t *Transpiler) getTimeRangesForSelector(s *parser.EvalStmt, n *parser.VectorSelector, path []parser.Node, evalRange time.Duration) (int64, int64) {
	start, end := timestamp.FromTime(s.Start), timestamp.FromTime(s.End)
	subqOffset, subqRange, subqTs := subqueryTimes(path)

	if subqTs != nil {
		// The timestamp on the subquery overrides the eval statement time ranges.
		start = *subqTs
		end = *subqTs
	}

	if n.Timestamp != nil {
		// The timestamp on the selector overrides everything.
		start = *n.Timestamp
		end = *n.Timestamp
	} else {
		offsetMilliseconds := durationMilliseconds(subqOffset)
		start = start - offsetMilliseconds - durationMilliseconds(subqRange)
		end = end - offsetMilliseconds
	}

	if evalRange == 0 {
		start = start - durationMilliseconds(t.LookBackDelta)
	} else {
		// For all matrix queries we want to ensure that we have (end-start) + range selected
		// this way we have `range` data before the start time
		start = start - durationMilliseconds(evalRange)
	}

	offsetMilliseconds := durationMilliseconds(n.OriginalOffset)
	start = start - offsetMilliseconds
	end = end - offsetMilliseconds

	return start, end
}

func (t *Transpiler) transpileParenExpr(e *parser.ParenExpr) (influxql.Node, error) {
	t.parenExprCount++
	node, err := t.transpileExpr(e.Expr)
	if err != nil {
		return nil, err
	}
	expr, ok := node.(influxql.Expr)
	if !ok {
		return node, nil
	}
	return &influxql.ParenExpr{Expr: expr}, err
}

func (t *Transpiler) transpileStepInvariantExpr(e *parser.StepInvariantExpr) (influxql.Node, error) {
	switch ce := e.Expr.(type) {
	case *parser.StringLiteral, *parser.NumberLiteral:
		return t.transpileExpr(ce)
	}
	if t.isStepVariantExpr {
		t.maxT = t.minT // Always a single evaluation.
	}
	node, err := t.transpile(e.Expr)
	switch e.Expr.(type) {
	case *parser.MatrixSelector, *parser.SubqueryExpr:
		// We do not duplicate results for range selectors since result is a matrix
		// with their unique timestamps which does not depend on the step.
		return node, err
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	t.duplicateResult = true
	return node, err
}

func (t *Transpiler) transpileSubqueryExpr(e *parser.SubqueryExpr) (influxql.Node, error) {
	preMinT := t.minT
	preMaxT := t.maxT
	preInterval := t.Step

	offsetMills := durationMilliseconds(e.Offset)
	rangeMillis := durationMilliseconds(e.Range)
	newEndTime := t.maxT - offsetMills
	newInterval := rangeMillis
	if e.Step != 0 {
		newInterval = durationMilliseconds(e.Step)
	}
	newStartTime := newInterval * ((t.minT - offsetMills - rangeMillis) / newInterval)
	if newStartTime < (t.minT - offsetMills - rangeMillis) {
		newStartTime += newInterval
	}
	if newStartTime != t.minT {
		// Adjust the offset of selectors based on the new
		// start time of the evaluator since the calculation
		// of the offset with @ happens w.r.t. the start time.
		setOffsetForAtModifier(newStartTime, e.Expr)
	}

	t.minT, t.maxT, t.Step = newStartTime, newEndTime, time.Duration(newInterval*int64(time.Millisecond/time.Nanosecond))

	node, err := t.transpileExpr(e.Expr)
	t.minT, t.maxT, t.Step = preMinT, preMaxT, preInterval
	return node, err
}

// setOffsetForAtModifier modifies the offset of vector and matrix selector
// and subquery in the tree to accommodate the timestamp of @ modifier.
// The offset is adjusted w.r.t. the given evaluation time.
func setOffsetForAtModifier(evalTime int64, expr parser.Expr) {
	getOffset := func(ts *int64, originalOffset time.Duration, path []parser.Node) time.Duration {
		if ts == nil {
			return originalOffset
		}

		subqOffset, _, subqTs := subqueryTimes(path)
		if subqTs != nil {
			subqOffset += time.Duration(evalTime-*subqTs) * time.Millisecond
		}

		offsetForTs := time.Duration(evalTime-*ts) * time.Millisecond
		offsetDiff := offsetForTs - subqOffset
		return originalOffset + offsetDiff
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)

		case *parser.MatrixSelector:
			vs := n.VectorSelector.(*parser.VectorSelector)
			vs.Offset = getOffset(vs.Timestamp, vs.OriginalOffset, path)

		case *parser.SubqueryExpr:
			n.Offset = getOffset(n.Timestamp, n.OriginalOffset, path)
		}
		return nil
	})
}

// subqueryTimes returns the sum of offsets and ranges of all subqueries in the path.
// If the @ modifier is used, then the offset and range is w.r.t. that timestamp
// (i.e. the sum is reset when we have @ modifier).
// The returned *int64 is the closest timestamp that was seen. nil for no @ modifier.
func subqueryTimes(path []parser.Node) (time.Duration, time.Duration, *int64) {
	var (
		subqOffset, subqRange time.Duration
		ts                    int64 = math.MaxInt64
	)
	for _, node := range path {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			subqOffset += n.OriginalOffset
			subqRange += n.Range
			if n.Timestamp != nil {
				// The @ modifier on subquery invalidates all the offset and
				// range till now. Hence resetting it here.
				subqOffset = n.OriginalOffset
				subqRange = n.Range
				ts = *n.Timestamp
			}
		}
	}
	var tsp *int64
	if ts != math.MaxInt64 {
		tsp = &ts
	}
	return subqOffset, subqRange, tsp
}

// unwrapParenExpr does the AST equivalent of removing parentheses around a expression.
func unwrapParenExpr(e *parser.Expr) {
	for {
		if p, ok := (*e).(*parser.ParenExpr); ok {
			*e = p.Expr
		} else {
			break
		}
	}
}

func unwrapStepInvariantExpr(e parser.Expr) parser.Expr {
	if p, ok := e.(*parser.StepInvariantExpr); ok {
		return p.Expr
	}
	return e
}

func IsMetaQuery(dt DataType) bool {
	switch dt {
	case LABEL_KEYS_DATA, LABEL_VALUES_DATA, SERIES_DATA:
		return true
	}
	return false
}

func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}

// yieldsTable checks PromQL expression returns matrix or vector or not
func yieldsTable(expr parser.Expr) bool {
	return !yieldsFloat(expr) && !yieldsString(expr)
}

// yieldsFloat checks PromQL expression returns float or not
func yieldsFloat(expr parser.Expr) bool {
	return expr.Type() == parser.ValueTypeScalar
}

// yieldsString checks PromQL expression returns string or not
func yieldsString(expr parser.Expr) bool {
	return expr.Type() == parser.ValueTypeString
}

func yieldsVector(expr parser.Expr) bool {
	return expr.Type() == parser.ValueTypeVector
}

func makeInt64Pointer(val int64) *int64 {
	valp := new(int64)
	*valp = val
	return valp
}

func CombineConditionAnd(lhs, rhs influxql.Expr) influxql.Expr {
	if lhs == nil && rhs == nil {
		return nil
	}
	if lhs == nil {
		return rhs
	}
	if rhs == nil {
		return lhs
	}
	return &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: lhs,
		RHS: rhs,
	}
}

func CombineConditionOr(lhs, rhs influxql.Expr) influxql.Expr {
	if lhs == nil && rhs == nil {
		return nil
	}
	if lhs == nil {
		return rhs
	}
	if rhs == nil {
		return lhs
	}
	return &influxql.BinaryExpr{
		Op:  influxql.OR,
		LHS: lhs,
		RHS: rhs,
	}
}

// preprocessExprHelper wraps the child nodes of the expression
// with a StepInvariantExpr wherever it's step invariant. The returned boolean is true if the
// passed expression qualifies to be wrapped by StepInvariantExpr.
// It also resolves the preprocessors.
func preprocessExprHelper(expr parser.Expr, start, end time.Time) bool {
	switch n := expr.(type) {
	case *parser.VectorSelector:
		switch n.StartOrEnd {
		case parser.START:
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(start))
		case parser.END:
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(end))
		}
		return n.Timestamp != nil

	case *parser.AggregateExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.BinaryExpr:
		isInvariant1, isInvariant2 := preprocessExprHelper(n.LHS, start, end), preprocessExprHelper(n.RHS, start, end)
		if isInvariant1 && isInvariant2 {
			return true
		}

		if isInvariant1 {
			n.LHS = newStepInvariantExpr(n.LHS)
		}
		if isInvariant2 {
			n.RHS = newStepInvariantExpr(n.RHS)
		}

		return false

	case *parser.Call:
		_, ok := promql.AtModifierUnsafeFunctions[n.Func.Name]
		isStepInvariant := !ok
		isStepInvariantSlice := make([]bool, len(n.Args))
		for i := range n.Args {
			isStepInvariantSlice[i] = preprocessExprHelper(n.Args[i], start, end)
			isStepInvariant = isStepInvariant && isStepInvariantSlice[i]
		}

		if isStepInvariant {
			// The function and all arguments are step invariant.
			return true
		}

		for i, isi := range isStepInvariantSlice {
			if isi {
				n.Args[i] = newStepInvariantExpr(n.Args[i])
			}
		}
		return false

	case *parser.MatrixSelector:
		return preprocessExprHelper(n.VectorSelector, start, end)

	case *parser.SubqueryExpr:
		// Since we adjust offset for the @ modifier evaluation,
		// it gets tricky to adjust it for every subquery step.
		// Hence we wrap the inside of subquery irrespective of
		// @ on subquery (given it is also step invariant) so that
		// it is evaluated only once w.r.t. the start time of subquery.
		isInvariant := preprocessExprHelper(n.Expr, start, end)
		if isInvariant {
			n.Expr = newStepInvariantExpr(n.Expr)
		}
		switch n.StartOrEnd {
		case parser.START:
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(start))
		case parser.END:
			n.Timestamp = makeInt64Pointer(timestamp.FromTime(end))
		}
		return n.Timestamp != nil

	case *parser.ParenExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.UnaryExpr:
		return preprocessExprHelper(n.Expr, start, end)

	case *parser.StringLiteral, *parser.NumberLiteral:
		return true
	}
	// this can't happen
	return false
}

func newStepInvariantExpr(expr parser.Expr) parser.Expr {
	return &parser.StepInvariantExpr{Expr: expr}
}
