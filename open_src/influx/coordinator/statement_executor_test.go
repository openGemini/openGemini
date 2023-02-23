package coordinator

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestLimitStringSlice(t *testing.T) {
	originSeries := []string{"a", "b", "c", "d", "e", "f", "g"}
	series := make([]string, len(originSeries))
	copy(series, originSeries)
	res := limitStringSlice(series, 0, 0)
	assert.Equal(t, originSeries, res)
	series = make([]string, len(originSeries))
	copy(series, originSeries)
	res = limitStringSlice(series, 1, 2)
	assert.Equal(t, []string{"b", "c"}, res)
	copy(series, originSeries)
	res = limitStringSlice(series, 5, 10)
	assert.Equal(t, []string{"f", "g"}, res)
	copy(series, originSeries)
	res = limitStringSlice(series, 7, 0)
	assert.Equal(t, true, res == nil)
}

type MockMetaClient struct {
	meta.MetaClient
}

type MockShardMapper struct {
	query.ShardMapper
}

func (csm *MockShardMapper) MapShards(source influxql.Sources, _ influxql.TimeRange, _ query.SelectOptions, _ influxql.Expr) (query.ShardGroup, error) {
	mst := source[0].(*influxql.Measurement)
	if mst.RetentionPolicy != "rp" {
		return nil, errors.New("retention policy not found")
	}
	if mst.Name == "wrongMst" {
		return nil, errno.NewError(errno.ErrMeasurementNotFound)
	}
	return nil, errno.NewError(errno.NoConnectionAvailable)
}

func newMockStatementExecutor() *StatementExecutor {
	//MetaClient meta.MetaClient
	client := &MockMetaClient{}
	shardCluster := &MockShardMapper{}
	//StatementExecutor
	statementExecutor := &StatementExecutor{
		MetaClient:     client,
		ShardMapper:    shardCluster,
		StmtExecLogger: Logger.NewLogger(errno.ModuleQueryEngine).With(zap.String("query", "StatementExecutor")),
	}
	return statementExecutor
}

func newMockSelectStatement(rp, mst string) *influxql.SelectStatement {
	selectStatement := &influxql.SelectStatement{
		Fields:  make(influxql.Fields, 0, 3),
		Sources: make(influxql.Sources, 0, 3),
	}
	selectStatement.Fields = append(selectStatement.Fields,
		&influxql.Field{
			Expr:  &influxql.VarRef{Val: "field", Type: influxql.Integer},
			Alias: "",
		},
	)
	selectStatement.Sources = append(selectStatement.Sources,
		&influxql.Measurement{Database: "db", RetentionPolicy: rp, Name: mst})
	return selectStatement
}

func MockExcuteStatementErr(rp, mst string, tm time.Duration) error {
	var err error
	ch := make(chan struct{})
	defer close(ch)

	timer := time.NewTimer(tm * time.Second)
	defer timer.Stop()
	go func() {
		statementExecutor := newMockStatementExecutor()
		slectStatement := newMockSelectStatement(rp, mst)
		ctx := &query.ExecutionContext{}
		err = statementExecutor.ExecuteStatement(slectStatement, ctx)
		ch <- struct{}{}
	}()

	select {
	case <-timer.C:
		err = errors.New("timeout")
	case <-ch:
	}
	return err
}

func TestExcuteStatementErr(t *testing.T) {
	err := MockExcuteStatementErr("wrongRp", "mst", 5)
	assert.True(t, strings.Contains(err.Error(), "retention policy not found"))

	err = MockExcuteStatementErr("rp", "wrongMst", 5)
	assert.True(t, errno.Equal(err, errno.ErrMeasurementNotFound))

	err = MockExcuteStatementErr("rp", "mst", 1)
	assert.True(t, strings.Contains(err.Error(), "timeout"))

	config.SetHaEnable(true)
	err = MockExcuteStatementErr("wrongRp", "mst", 5)
	assert.True(t, strings.Contains(err.Error(), "retention policy not found"))

}
