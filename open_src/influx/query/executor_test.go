package query_test

import (
	"fmt"
	"testing"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/coordinator"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

func NewQueryExecutor() *query.Executor {
	concurrence := 4
	return query.NewExecutor(concurrence)
}

func TestQueryExecutor_Serial(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu;SELECT mean(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = newMockStatementExecutor()
	results := e.ExecuteQuery(q, query.ExecutionOptions{}, nil, nil)
	fmt.Println(results)
}

func TestQueryExecutor_Parallel(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu;SELECT mean(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	e := NewQueryExecutor()
	e.StatementExecutor = newMockStatementExecutor()
	option := query.ExecutionOptions{
		ParallelQuery: true,
	}
	results := e.ExecuteQuery(q, option, nil, nil)
	discardOutput(results)
}

func discardOutput(results <-chan *query2.Result) {
	for range results {
		// Read all results and discard.
	}
}

func newMockStatementExecutor() *coordinator.StatementExecutor {
	//StatementExecutor
	statementExecutor := &coordinator.StatementExecutor{
		StmtExecLogger: Logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "StatementExecutor")),
	}
	return statementExecutor
}

func Test_RewriteCondForLogKeeper(t *testing.T) {
	schema := map[string]int32{
		"content": influx.Field_Type_String,
		"age":     influx.Field_Type_Int,
		"weight":  influx.Field_Type_UInt,
		"height":  influx.Field_Type_Float,
		"alive":   influx.Field_Type_Boolean,
	}
	condition := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op: influxql.OR,
			LHS: &influxql.BinaryExpr{
				Op: influxql.OR,
				LHS: &influxql.BinaryExpr{
					Op:  influxql.MATCHPHRASE,
					LHS: &influxql.VarRef{Val: "content"},
					RHS: &influxql.StringLiteral{Val: "shanghai"},
				},
				RHS: &influxql.BinaryExpr{
					Op:  influxql.MATCHPHRASE,
					LHS: &influxql.VarRef{Val: "alive"},
					RHS: &influxql.StringLiteral{Val: "TRUE"},
				},
			},
			RHS: &influxql.BinaryExpr{
				Op: influxql.OR,
				LHS: &influxql.BinaryExpr{
					Op:  influxql.GT,
					LHS: &influxql.VarRef{Val: "height"},
					RHS: &influxql.StringLiteral{Val: "120.5"},
				},
				RHS: &influxql.BinaryExpr{
					Op:  influxql.LT,
					LHS: &influxql.VarRef{Val: "height"},
					RHS: &influxql.StringLiteral{Val: "180.9"},
				},
			},
		},
		RHS: &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.BinaryExpr{
				Op: influxql.AND,
				LHS: &influxql.BinaryExpr{
					Op:  influxql.GTE,
					LHS: &influxql.VarRef{Val: "weight"},
					RHS: &influxql.StringLiteral{Val: "120"},
				},
				RHS: &influxql.BinaryExpr{
					Op:  influxql.LTE,
					LHS: &influxql.VarRef{Val: "weight"},
					RHS: &influxql.StringLiteral{Val: "180"},
				},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.GTE,
				LHS: &influxql.VarRef{Val: "age"},
				RHS: &influxql.StringLiteral{Val: "18"},
			},
		},
	}
	err := query.RewriteCondForPipeSyntax(condition, schema)
	if err != nil {
		t.Fatal(err)
	}
}
