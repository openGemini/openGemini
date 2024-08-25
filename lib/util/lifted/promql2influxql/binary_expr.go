package promql2influxql

import (
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	POW    = "pow"
	POW_OP = 57501
)

var arithBinOps = map[parser.ItemType]influxql.Token{
	parser.ADD: influxql.ADD,
	parser.SUB: influxql.SUB,
	parser.MUL: influxql.MUL,
	parser.DIV: influxql.DIV,
	parser.MOD: influxql.MOD,
	parser.POW: POW_OP,
}

var compBinOps = map[parser.ItemType]influxql.Token{
	parser.EQLC: influxql.EQ,
	parser.NEQ:  influxql.NEQ,
	parser.GTR:  influxql.GT,
	parser.LSS:  influxql.LT,
	parser.GTE:  influxql.GTE,
	parser.LTE:  influxql.LTE,
}

type BinExprType int

const (
	LEFT_EXPR BinExprType = iota
	RIGHT_EXPR
)

// NewBinaryExpr creates a pointer to influxql.BinaryExpr
func (t *Transpiler) NewBinaryExpr(op influxql.Token, lhs, rhs influxql.Expr, returnBool bool) influxql.Expr {
	expr := &influxql.BinaryExpr{
		Op:         op,
		LHS:        lhs,
		RHS:        rhs,
		ReturnBool: returnBool,
	}
	if t.parenExprCount > 0 {
		defer func() {
			t.parenExprCount--
		}()
		return &influxql.ParenExpr{
			Expr: expr,
		}
	}
	return expr
}

// NewBinaryCallExpr creates a pointer to influxql.Call
func (t *Transpiler) NewBinaryCallExpr(opFn string, lhs, rhs influxql.Expr) influxql.Expr {
	expr := &influxql.Call{
		Name: opFn,
		Args: []influxql.Expr{
			lhs,
			rhs,
		},
	}
	if t.parenExprCount > 0 {
		defer func() {
			t.parenExprCount--
		}()
	}
	return expr
}

// transpileArithBinOps transpiles math operator PromQL BinaryExpr
func (t *Transpiler) transpileArithBinOps(b *parser.BinaryExpr, op influxql.Token, lhs, rhs influxql.Node, swap bool) (influxql.Node, error) {
	var vector influxql.Node
	var scalar influxql.Expr
	if !swap {
		vector = lhs
		scalar = rhs.(influxql.Expr)
	} else {
		vector = rhs
		scalar = lhs.(influxql.Expr)
	}
	switch node := vector.(type) {
	case influxql.Expr:
		if b.Op == parser.POW {
			return t.NewBinaryCallExpr(POW, lhs.(influxql.Expr), rhs.(influxql.Expr)), nil
		}
		return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), false), nil
	case influxql.Statement:
		switch statement := node.(type) {
		case *influxql.SelectStatement:
			field := statement.Fields[len(statement.Fields)-1]
			lastField := &influxql.Field{Alias: DefaultFieldKey}
			if !swap {
				if b.Op == parser.POW {
					lastField.Expr = t.NewBinaryCallExpr(POW, field.Expr, scalar)
				} else {
					lastField.Expr = t.NewBinaryExpr(op, field.Expr, scalar, false)
				}
			} else {
				if b.Op == parser.POW {
					lastField.Expr = t.NewBinaryCallExpr(POW, scalar, field.Expr)
				} else {
					lastField.Expr = t.NewBinaryExpr(op, scalar, field.Expr, false)
				}
			}
			statement.Fields[len(statement.Fields)-1] = lastField
		default:
			return nil, errno.NewError(errno.UnsupportedPromExpr)
		}
	}
	return vector, nil
}

// transpileCompBinOps transpiles comparison operator PromQL BinaryExpr
func (t *Transpiler) transpileCompBinOps(b *parser.BinaryExpr, op influxql.Token, lhs, rhs influxql.Node, swap bool) (influxql.Node, error) {
	var vector influxql.Node
	var scalar influxql.Expr
	if !swap {
		vector = lhs
		scalar = rhs.(influxql.Expr)
	} else {
		vector = rhs
		scalar = lhs.(influxql.Expr)
	}
	switch node := vector.(type) {
	case influxql.Expr:
		return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), b.ReturnBool), nil
	case influxql.Statement:
		switch statement := node.(type) {
		case *influxql.SelectStatement:
			// if there is a bool modifier, the result of the comparison is converted to 1 and 0.
			if b.ReturnBool {
				field := statement.Fields[len(statement.Fields)-1]
				var selectField influxql.Expr
				if !swap {
					selectField = &influxql.BinaryExpr{Op: op, LHS: field.Expr, RHS: scalar, ReturnBool: b.ReturnBool}
				} else {
					selectField = &influxql.BinaryExpr{Op: op, LHS: scalar, RHS: field.Expr, ReturnBool: b.ReturnBool}
				}
				statement.Fields[len(statement.Fields)-1] = &influxql.Field{
					Expr:  selectField,
					Alias: DefaultFieldKey,
				}
				return statement, nil
			}
			// for the comparison operation of the source-table query, add the operation to the condition.
			if t.isSrcMstQuery(statement) {
				field := &influxql.VarRef{Val: DefaultFieldKey}
				comBin := &influxql.BinaryExpr{Op: op, ReturnBool: false}
				if !swap {
					comBin.LHS, comBin.RHS = field, scalar
				} else {
					comBin.LHS, comBin.RHS = scalar, field
				}
				statement.Condition = CombineConditionAnd(statement.Condition, comBin)
				return statement, nil
			}
			// for the comparison operation of the non-source-table query, use the sub-query to implement the operation.
			var selectStatement = &influxql.SelectStatement{IsPromQuery: true}
			selectStatement.Sources = []influxql.Source{&influxql.SubQuery{Statement: statement}}
			fieldExpr := &influxql.VarRef{Val: statement.Fields[len(statement.Fields)-1].Name()}
			selectStatement.Fields = append(selectStatement.Fields, &influxql.Field{Expr: fieldExpr, Alias: DefaultFieldKey})
			if !swap {
				selectStatement.Condition = &influxql.BinaryExpr{Op: op, LHS: fieldExpr, RHS: scalar}
			} else {
				selectStatement.Condition = &influxql.BinaryExpr{Op: op, LHS: scalar, RHS: fieldExpr}
			}
			t.setTimeCondition(selectStatement)
			if len(statement.Dimensions) > 0 {
				selectStatement.Without = statement.Without
				if _, ok := statement.Dimensions[len(statement.Dimensions)-1].Expr.(*influxql.Call); ok {
					selectStatement.Dimensions = statement.Dimensions[:len(statement.Dimensions)-1]
				} else {
					selectStatement.Dimensions = statement.Dimensions
				}
			}
			return selectStatement, nil
		default:
			return nil, errno.NewError(errno.UnsupportedPromExpr)
		}
	default:
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}
}

func (t *Transpiler) isSrcMstQuery(statement *influxql.SelectStatement) bool {
	if len(statement.Sources) == 0 {
		return false
	}
	if _, ok := statement.Sources[0].(*influxql.Measurement); !ok {
		return false
	}
	if _, ok := statement.Fields[len(statement.Fields)-1].Expr.(*influxql.VarRef); !ok {
		return false
	}
	return true
}

// transpileBinaryExpr transpiles PromQL BinaryExpr.
// TODO It doesn't support expressions that both sides return matrix or vector value.
func (t *Transpiler) transpileBinaryExpr(b *parser.BinaryExpr) (influxql.Node, error) {
	lhs, err := t.transpileExpr(b.LHS)
	if err != nil {
		return nil, errno.NewError(errno.UnableLeftBinOp, err.Error())
	}
	rhs, err := t.transpileExpr(b.RHS)
	if err != nil {
		return nil, errno.NewError(errno.UnableRightBinOp, err)
	}
	if b.ReturnBool {
		t.dropMetric = true
	}
	t.removeTableName = true
	switch {
	case yieldsFloat(b.LHS) && yieldsFloat(b.RHS):
		// Handle both sides return scalar value.
		if op, ok := arithBinOps[b.Op]; ok {
			if b.Op == parser.POW {
				return t.NewBinaryCallExpr(POW, lhs.(influxql.Expr), rhs.(influxql.Expr)), nil
			}
			return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), false), nil
		}
		if op, ok := compBinOps[b.Op]; ok {
			return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), b.ReturnBool), nil
		}
	case yieldsFloat(b.LHS) && yieldsTable(b.RHS), yieldsTable(b.LHS) && yieldsFloat(b.RHS):
		// Handle one side return scalar value, the other side return matrix or vector value.
		swap := yieldsFloat(b.LHS) && yieldsVector(b.RHS)
		if op, ok := arithBinOps[b.Op]; ok {
			return t.transpileArithBinOps(b, op, lhs, rhs, swap)
		}
		if op, ok := compBinOps[b.Op]; ok {
			return t.transpileCompBinOps(b, op, lhs, rhs, swap)
		}
	case yieldsVector(b.LHS) && yieldsVector(b.RHS):
		lStmt, lok := lhs.(*influxql.SelectStatement)
		rStmt, rok := rhs.(*influxql.SelectStatement)
		if !lok && !rok {
			if op, ok := arithBinOps[b.Op]; ok {
				return t.transpileArithBinOps(b, op, lhs, rhs, false)
			}
			if op, ok := compBinOps[b.Op]; ok {
				return t.transpileCompBinOps(b, op, lhs, rhs, false)
			}
		} else if !lok || !rok {
			// todo: vector(with mst)+vector(without mst)
			return nil, errno.NewError(errno.UnsupportedExprType)
		}
		return t.transpileBinOpOfBothVector(b, influxql.Token(b.Op), lStmt, rStmt)
	default:
		return nil, errno.NewError(errno.UnsupportedBothVS, b.String())
	}
	return nil, errno.NewError(errno.InvalidSVBinOp, b.Op.String())
}

func (t *Transpiler) transpileBinOpOfBothVector(b *parser.BinaryExpr, op influxql.Token, lStmt, rStmt *influxql.SelectStatement) (influxql.Node, error) {
	lSub := &influxql.SubQuery{Statement: lStmt}
	rSub := &influxql.SubQuery{Statement: rStmt}
	binOp := &influxql.BinOp{
		LSrc:        lSub,
		RSrc:        rSub,
		OpType:      int(op),
		On:          b.VectorMatching.On,
		MatchKeys:   b.VectorMatching.MatchingLabels,
		MatchCard:   influxql.MatchCardinality(b.VectorMatching.Card),
		IncludeKeys: b.VectorMatching.Include,
		ReturnBool:  b.ReturnBool,
	}
	newStmt := &influxql.SelectStatement{
		Sources:     influxql.Sources{binOp},
		Fields:      influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: DefaultFieldKey, Alias: DefaultFieldKey}}},
		IsPromQuery: true,
		QueryOffset: lStmt.QueryOffset,
	}
	// set query time range
	t.setTimeCondition(newStmt)
	return newStmt, nil
}

func ShouldDropMetricName(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.DIV, parser.MUL, parser.POW, parser.MOD:
		return true
	default:
		return false
	}
}
