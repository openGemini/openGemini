package query_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestFunctionTypeMapper(t *testing.T) {
	m := query.MathTypeMapper{}
	s := query.StringFunctionTypeMapper{}

	if dataType, err := m.CallType("sin", []influxql.DataType{influxql.Float}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Float)
	}

	if dataType, err := m.CallType("abs", []influxql.DataType{influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Integer)
	}

	if dataType, err := s.CallType("str", []influxql.DataType{influxql.String, influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Boolean)
	}

	if dataType, err := s.CallType("strlen", []influxql.DataType{influxql.String}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.Integer)
	}

	if dataType, err := s.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}

	if dataType, err := s.CallType("substr", []influxql.DataType{influxql.String, influxql.Integer, influxql.Integer}); err != nil {
		t.Fatalf("raise error: %s", err.Error())
	} else {
		assert.Equal(t, dataType, influxql.String)
	}
}

func TestClamp(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(6), float64(6), float64(7), float64(8), float64(9), float64(9)}
		outputs := make([]interface{}, 0, len(expects))
		min, max := int64(6), int64(9)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{nil, nil, nil, nil, nil, nil}
		outputs := make([]interface{}, 0, len(expects))
		min, max := int64(9), int64(6)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("3", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		nan := math.NaN()
		min, max := nan, int64(6)
		expects := []interface{}{nan, nan, nan, nan, nan, nan}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min, max}); ok {
				outputs = append(outputs, out)
			}
		}
		for _, o := range outputs {
			if !math.IsNaN(o.(float64)) {
				t.Errorf("not euqal")
			}
		}
	})
}

func TestClampMax(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_max_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(5), float64(6), float64(7), float64(8), float64(9), float64(9)}
		outputs := make([]interface{}, 0, len(expects))
		max := int64(9)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, max}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestClampMin(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "clamp_min_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5, 6, 7, 8, 9, 10}
		expects := []interface{}{float64(6), float64(6), float64(7), float64(8), float64(9), float64(10)}
		outputs := make([]interface{}, 0, len(expects))
		min := int64(6)
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, min}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestPromRound(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "round_prom"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{5.2, 6.4, 7.3, 8.9, 9.5, 9.7}
		expects := []interface{}{float64(5), float64(6), float64(7), float64(9), float64(10), float64(10)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{5.2, 6.4, 7.3, 8.9, 9.5, 9.7}
		expects := []interface{}{float64(5.2), float64(6.4), float64(7.4), float64(9), float64(9.6), float64(9.8)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, 0.2}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("3", func(t *testing.T) {
		inputArgs := []float64{5.2, 6.4, 7.3, 8.9, 9.5, 9.7}
		expects := []interface{}{float64(5), float64(6.5), float64(7.5), float64(9), float64(9.5), float64(9.5)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, 0.5}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("4", func(t *testing.T) {
		inputArgs := []float64{5.2, 6.4, 7.3, 8.9, 9.5, 9.7}
		expects := []interface{}{float64(6), float64(6), float64(8), float64(8), float64(10), float64(10)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg, int64(2)}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestRad(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "rad"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{180, 120, 60, 0, -60}
		expects := make([]interface{}, 0, len(inputArgs))
		for _, input := range inputArgs {
			expects = append(expects, input*math.Pi/180)
		}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{360, 720, 30, 0, 60}
		expects := make([]interface{}, 0, len(inputArgs))
		for _, input := range inputArgs {
			expects = append(expects, input*math.Pi/180)
		}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestDeg(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "deg"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{1 * math.Pi, 2 * math.Pi, 0, -1 * math.Pi, -2 * math.Pi}
		expects := []interface{}{float64(180), float64(360), float64(0), float64(-180), float64(-360)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{float64(2) / float64(3) * math.Pi, float64(1) / float64(3) * math.Pi, 0, float64(-1) / float64(3) * math.Pi, float64(-2) / float64(3) * math.Pi}
		expects := []interface{}{119.99999999999999, 59.99999999999999, float64(0), -59.99999999999999, -119.99999999999999}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestTrigFunc(t *testing.T) {
	mathValuer := query.MathValuer{}

	t.Run("sinh", func(t *testing.T) {
		inputName := "sinh"
		inputArgs := []float64{2, 1, 0, 0.5, math.Inf(+1)}
		expects := []interface{}{3.626860407847019, 1.1752011936438014, float64(0), 0.5210953054937474, math.Inf(+1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("cosh", func(t *testing.T) {
		inputName := "cosh"
		inputArgs := []float64{2, 1, 0, 0.5, math.Inf(+1)}
		expects := []interface{}{3.7621956910836314, 1.5430806348152437, float64(1), 1.1276259652063807, math.Inf(+1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("tanh", func(t *testing.T) {
		inputName := "tanh"
		inputArgs := []float64{2, 1, 0, 0.5, math.Inf(+1)}
		expects := []interface{}{0.9640275800758169, 0.7615941559557649, float64(0), 0.46211715726000974, float64(1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("asinh", func(t *testing.T) {
		inputName := "asinh"
		inputArgs := []float64{2, 1, 0, 0.5, math.Inf(+1)}
		expects := []interface{}{1.4436354751788103, 0.881373587019543, float64(0), 0.48121182505960347, math.Inf(+1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("atanh", func(t *testing.T) {
		inputName := "atanh"
		inputArgs := []float64{-0.5, 1, 0, 0.5, float64(1) / float64(3)}
		expects := []interface{}{-0.5493061443340548, math.Inf(+1), float64(0), 0.5493061443340548, 0.34657359027997264}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestSgn(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "sgn"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{180, 120, 60, 0, -60}
		expects := []interface{}{float64(1), float64(1), float64(1), float64(0), float64(-1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
	t.Run("2", func(t *testing.T) {
		inputArgs := []float64{360, 720, 30, 0, 60}
		expects := []interface{}{float64(1), float64(1), float64(1), float64(0), float64(1)}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestAcosh(t *testing.T) {
	mathValuer := query.MathValuer{}
	inputName := "acosh"
	t.Run("1", func(t *testing.T) {
		inputArgs := []float64{2, 3, 4, 5, 1}
		expects := make([]interface{}, 0, len(inputArgs))
		for _, input := range inputArgs {
			expects = append(expects, math.Acosh(input))
		}
		outputs := make([]interface{}, 0, len(expects))
		for _, arg := range inputArgs {
			if out, ok := mathValuer.Call(inputName, []interface{}{arg}); ok {
				outputs = append(outputs, out)
			}
		}
		assert.Equal(t, outputs, expects)
	})
}

func TestGetRules(t *testing.T) {

	materialize := query.GetFunctionFactoryInstance().GetMaterializeOp()
	for _, function := range materialize {
		function.GetRules("test")
	}
	aggregate := query.GetFunctionFactoryInstance().GetAggregateOp()
	for _, function := range aggregate {
		function.GetRules("test")
	}
	label := query.GetFunctionFactoryInstance().GetLabelOp()
	for _, function := range label {
		function.GetRules("test")
	}
	promTime := query.GetFunctionFactoryInstance().GetPromTimeOp()
	for _, function := range promTime {
		function.GetRules("test")
	}

	// pick one instance to test
	testFunction := query.GetStringFunction("regexp_extract")
	rules := testFunction.GetRules("test")
	assert.Equal(t, 3, len(rules))
}

func TestArgNumberCheckRule_Check(t *testing.T) {
	a := &query.ArgNumberCheckRule{
		Name: "test",
		Min:  1,
		Max:  2,
	}
	b := &query.ArgNumberCheckRule{
		Name: "test",
		Min:  2,
		Max:  2,
	}
	tests := []struct {
		checkRule *query.ArgNumberCheckRule
		expr      *influxql.Call
		err       error
	}{
		{
			checkRule: a,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.StringLiteral{},
				},
			},
			err: nil,
		},
		{
			checkRule: a,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
				},
			},
			err: nil,
		},
		{
			checkRule: a,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.StringLiteral{},
					&influxql.StringLiteral{},
				},
			},
			err: fmt.Errorf("invalid number of arguments for %s, expected %d-%d, got %d", "test", a.Min, a.Max, 3),
		},
		{
			checkRule: b,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.StringLiteral{},
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.checkRule.Check(tt.expr), tt.err)
	}
}

func TestTypeCheckRule_Check(t *testing.T) {
	a := &query.TypeCheckRule{
		Name:  "test",
		Index: 1,
		Asserts: []func(interface{}) bool{
			query.AssertStringLiteral,
		},
	}
	b := &query.TypeCheckRule{
		Name:  "test",
		Index: 1,
		Asserts: []func(interface{}) bool{
			query.AssertIntegerLiteral,
		},
	}
	c := &query.TypeCheckRule{
		Name:  "test",
		Index: 1,
		Asserts: []func(interface{}) bool{
			query.AssertNumberLiteral,
		},
	}
	tests := []struct {
		checkRule *query.TypeCheckRule
		expr      *influxql.Call
		err       error
	}{
		{
			checkRule: a,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.StringLiteral{},
				},
			},
			err: nil,
		},
		{
			checkRule: a,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.IntegerLiteral{Val: 1},
				},
			},
			err: fmt.Errorf("invalid argument type for the %s argument in %s(): %s", "2nd", "test", "1"),
		},
		{
			checkRule: b,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.StringLiteral{Val: "1"},
				},
			},
			err: fmt.Errorf("invalid argument type for the %s argument in %s(): %s", "2nd", "test", "'1'"),
		},
		{
			checkRule: b,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.IntegerLiteral{},
				},
			},
			err: nil,
		},
		{
			checkRule: c,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.NumberLiteral{},
				},
			},
			err: nil,
		},
		{
			checkRule: c,
			expr: &influxql.Call{
				Name: "test",
				Args: []influxql.Expr{
					&influxql.VarRef{},
					&influxql.IntegerLiteral{Val: 1},
				},
			},
			err: fmt.Errorf("invalid argument type for the %s argument in %s(): %s", "2nd", "test", "1"),
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.checkRule.Check(tt.expr), tt.err)
	}
}
