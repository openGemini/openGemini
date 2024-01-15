package query_test

import (
	"fmt"
	"testing"

	query2 "github.com/influxdata/influxdb/query"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/coordinator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
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
