package query_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestCompareFunc_CompileFunc(t *testing.T) {
	funcName := "compare"
	tests := []struct {
		args     *influxql.Call
		expected error
	}{
		{
			args: &influxql.Call{
				Name: funcName,
				Args: []influxql.Expr{&influxql.StringLiteral{Val: "100"}},
			},
			expected: fmt.Errorf("invalid number of arguments for %s, expected more than one arguments, got %d", funcName, 1),
		},
		{
			args: &influxql.Call{
				Name: funcName,
				Args: []influxql.Expr{
					&influxql.StringLiteral{Val: "100"},
					&influxql.StringLiteral{Val: "100"},
				},
			},
			expected: fmt.Errorf("invalid argument type for argument in %s(): %s", funcName, "'100'"),
		},
		{
			args: &influxql.Call{
				Name: funcName,
				Args: []influxql.Expr{
					&influxql.StringLiteral{Val: "100"},
					&influxql.IntegerLiteral{Val: 100},
				},
			},
			expected: nil,
		},
		{
			args: &influxql.Call{
				Name: funcName,
				Args: []influxql.Expr{
					&influxql.StringLiteral{Val: "100"},
					&influxql.IntegerLiteral{Val: 100},
					&influxql.StringLiteral{Val: "100"},
				},
			},
			expected: fmt.Errorf("invalid argument type for argument in %s(): %s", funcName, "'100'"),
		},
		{
			args: &influxql.Call{
				Name: funcName,
				Args: []influxql.Expr{
					&influxql.StringLiteral{Val: "100"},
					&influxql.IntegerLiteral{Val: 100},
					&influxql.IntegerLiteral{Val: 100},
				},
			},
			expected: nil,
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.expected, query.GetCompareFunction(test.args.Name).CompileFunc(test.args, nil))
	}
}

func TestCompareFunc_CallTypeFunc(t *testing.T) {
	inputArgs := []string{"compare"}
	expects := []interface{}{influxql.Unknown}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		got, err := query.GetCompareFunction(arg).CallTypeFunc(arg, []influxql.DataType{})
		if err != nil {
			continue
		}
		outputs = append(outputs, got)

	}
	assert.Equal(t, expects, outputs)
}
