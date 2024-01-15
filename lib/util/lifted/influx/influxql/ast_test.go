package influxql

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/assert"
)

func TestRewritePercentileOGSketchStatement(t *testing.T) {
	st := &SelectStatement{}
	st.Sources = append(st.Sources, &SubQuery{})
	st.Fields = append(st.Fields, &Field{Expr: &Call{Name: "percentile_ogsketch"}})
	RewritePercentileOGSketchStatement(st)
	assert.Equal(t, "percentile_approx", st.Fields[0].Expr.(*Call).Name)

	st1 := &SelectStatement{}
	st1.Fields = append(st1.Fields, &Field{Expr: &Call{Name: "percentile_ogsketch"}})
	RewritePercentileOGSketchStatement(st1)
	assert.Equal(t, "percentile_ogsketch", st1.Fields[0].Expr.(*Call).Name)

	st2 := &SelectStatement{}
	st2.Sources = append(st2.Sources, &SubQuery{})
	st2.Fields = append(st2.Fields, &Field{Expr: &Call{Name: "max"}})
	RewritePercentileOGSketchStatement(st)
	assert.Equal(t, "max", st2.Fields[0].Expr.(*Call).Name)
}

func Test_RewriteCondForLogKeeper(t *testing.T) {
	schema := map[string]Expr{
		"content": &VarRef{Val: "content", Type: String},
		"age":     &VarRef{Val: "age", Type: Integer},
		"weight":  &VarRef{Val: "weight", Type: Unsigned},
		"height":  &VarRef{Val: "height", Type: Float},
		"alive":   &VarRef{Val: "alive", Type: Boolean},
	}
	condition := &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op: OR,
			LHS: &BinaryExpr{
				Op: OR,
				LHS: &BinaryExpr{
					Op:  MATCHPHRASE,
					LHS: &VarRef{Val: "content"},
					RHS: &StringLiteral{Val: "shanghai"},
				},
				RHS: &BinaryExpr{
					Op:  MATCHPHRASE,
					LHS: &VarRef{Val: "alive"},
					RHS: &StringLiteral{Val: "TRUE"},
				},
			},
			RHS: &BinaryExpr{
				Op: OR,
				LHS: &BinaryExpr{
					Op:  GT,
					LHS: &VarRef{Val: "height"},
					RHS: &StringLiteral{Val: "120.5"},
				},
				RHS: &BinaryExpr{
					Op:  LT,
					LHS: &VarRef{Val: "height"},
					RHS: &StringLiteral{Val: "180.9"},
				},
			},
		},
		RHS: &BinaryExpr{
			Op: AND,
			LHS: &BinaryExpr{
				Op: AND,
				LHS: &BinaryExpr{
					Op:  GTE,
					LHS: &VarRef{Val: "weight"},
					RHS: &StringLiteral{Val: "120"},
				},
				RHS: &BinaryExpr{
					Op:  LTE,
					LHS: &VarRef{Val: "weight"},
					RHS: &StringLiteral{Val: "180"},
				},
			},
			RHS: &BinaryExpr{
				Op:  GTE,
				LHS: &VarRef{Val: "age"},
				RHS: &StringLiteral{Val: "18"},
			},
		},
	}
	config.SetProductType("logkeeper")
	err := RewriteCondVarRef(condition, schema)
	if err != nil {
		t.Fatal(err)
	}
}
