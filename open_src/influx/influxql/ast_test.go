package influxql

import (
	"testing"

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
