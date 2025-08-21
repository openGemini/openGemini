package query_test

import (
	"fmt"
	"testing"

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

func TestQueryExecutor_Recover(t *testing.T) {
	e := NewQueryExecutor()
	e.StatementExecutor = newMockStatementExecutor()
	results := e.ExecuteQuery(nil, query.ExecutionOptions{}, nil, nil)
	fmt.Println(results)
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

func discardOutput(results <-chan *query.Result) {
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

type testRes struct {
	query string
	isOK  bool
}

func Test_TimerangePushDown(t *testing.T) {
	cases := []testRes{
		{"SELECT count(value) FROM cpu", false},
		{"SELECT count(value) FROM cpu where time>1 and time<100", true},
		{"SELECT sum / cnt  FROM (select SUM(value) AS sum, COUNT(value) AS cnt from cpu)", false},
		{"SELECT sum / cnt  FROM (select SUM(value) AS sum, COUNT(value) AS cnt from cpu ) WHERE time>=1 AND time<100", true},
		{"SELECT sum / cnt  FROM (select SUM(value) AS sum, COUNT(value) AS cnt from cpu WHERE time>=1 AND time<100)", true},
		{"SELECT sum / cnt  FROM (select SUM(value) AS sum, COUNT(value) AS cnt from cpu WHERE time>=1 AND time<100) WHERE time>=10 AND time<20", true},
	}

	query.TimeFilterProtection = true
	for i := 0; i < len(cases); i++ {
		q, err := influxql.ParseQuery(cases[i].query)
		if err != nil {
			t.Fatal(err)
		}
		statement := q.Statements[0].(*influxql.SelectStatement)
		_, _, err = query.Compile(statement, query.CompileOptions{})
		isOK := (err == nil)
		if isOK != cases[i].isOK {
			t.Fatalf("statement-%d timerange check failed. exepect: %+v == %+v, err:%+v", i, cases[i].isOK, isOK, err)
		}
	}
}
