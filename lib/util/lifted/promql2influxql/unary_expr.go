package promql2influxql

import (
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/prometheus/prometheus/promql/parser"
)

// transpileUnaryExpr transpiles PromQL UnaryExpr
func (t *Transpiler) transpileUnaryExpr(ue *parser.UnaryExpr) (influxql.Node, error) {
	node, err := t.transpileExpr(ue.Expr)
	if err != nil {
		return nil, errno.NewError(errno.TranspileUnaryExprFail, err.Error())
	}

	switch ue.Op {
	case parser.ADD, parser.SUB:
		mul := 1
		if ue.Op == parser.SUB {
			mul = -1
		}
		switch n := node.(type) {
		case influxql.Expr:
			switch expr := n.(type) {
			case *influxql.NumberLiteral:
				expr.Val *= float64(mul)
			case *influxql.IntegerLiteral:
				expr.Val *= int64(mul)
			default:
				return &influxql.BinaryExpr{
					Op: influxql.MUL,
					LHS: &influxql.IntegerLiteral{
						Val: int64(mul),
					},
					RHS: expr,
				}, nil
			}
		case influxql.Statement:
			switch statement := n.(type) {
			case *influxql.SelectStatement:
				if ue.Op == parser.ADD {
					return node, nil
				}
				statement.Fields[len(statement.Fields)-1] = &influxql.Field{
					Expr: &influxql.BinaryExpr{
						Op: influxql.MUL,
						LHS: &influxql.IntegerLiteral{
							Val: int64(mul),
						},
						RHS: statement.Fields[len(statement.Fields)-1].Expr,
					},
					Alias: DefaultFieldKey,
				}
			default:

			}
		}
		return node, nil
	default:
		// PromQL fails to parse unary operators other than +/-, so this should never happen.
		return nil, errno.NewError(errno.InvalidUnaryExpr)
	}
}
