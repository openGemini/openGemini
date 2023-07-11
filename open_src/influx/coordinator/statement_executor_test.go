package coordinator

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	Logger "github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
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

func generateMockExeInfos(idOffset, num int, killOne int, duration int64) []*netstorage.QueryExeInfo {
	res := make([]*netstorage.QueryExeInfo, 0, num)
	for i := 0; i < num; i++ {
		info := &netstorage.QueryExeInfo{
			QueryID:   uint64(i + idOffset),
			Stmt:      fmt.Sprintf("select * from mst%d", i),
			Database:  fmt.Sprintf("db%d", i),
			BeginTime: duration,
			RunState:  netstorage.Running,
		}
		if i == killOne {
			info.RunState = netstorage.Killed
		}
		res = append(res, info)
	}
	return res
}

var idOffset = 100000
var mockInfosNum = 20
var killOne = 8
var minBeginTime int64 = 10000000
var dataNodesNum = 3

func Test_combineQueryExeInfos(t *testing.T) {
	killedQid := killOne + idOffset
	res := make(map[uint64]*combinedQueryExeInfo)
	for i := 1; i <= dataNodesNum; i++ {
		infos := generateMockExeInfos(idOffset, mockInfosNum, killOne, int64(i)*minBeginTime)
		combineQueryExeInfos(res, infos, fmt.Sprintf("192,168.0.%d", i))
	}
	for _, cmbInfo := range res {
		assert.Equal(t, minBeginTime, cmbInfo.beginTime)
		if cmbInfo.qid == uint64(killedQid) {
			assert.Equal(t, 0, len(cmbInfo.runningHosts))
			assert.Equal(t, dataNodesNum, len(cmbInfo.killedHosts))
			assert.Equal(t, killedTotally, cmbInfo.totalRunState)
		} else {
			assert.Equal(t, dataNodesNum, len(cmbInfo.runningHosts))
			assert.Equal(t, 0, len(cmbInfo.killedHosts))
			assert.Equal(t, running, cmbInfo.totalRunState)
		}
	}
}

func (m *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	res := make([]meta2.DataNode, dataNodesNum)
	for i := 0; i < dataNodesNum; i++ {
		res[i] = meta2.DataNode{
			NodeInfo: meta2.NodeInfo{ID: uint64(i + 1), Host: fmt.Sprintf("192.168.1.808%d", i)},
		}
	}
	return res, nil
}

type mockNS struct {
	netstorage.NetStorage
}

func (s *mockNS) GetQueriesOnNode(nodeID uint64) ([]*netstorage.QueryExeInfo, error) {
	infos := generateMockExeInfos(idOffset, mockInfosNum, killOne, int64(minBeginTime))
	return infos, nil
}

func TestStatementExecutor_executeShowQueriesStatement(t *testing.T) {
	e := StatementExecutor{MetaClient: &MockMetaClient{}, NetStorage: &mockNS{}}
	rows, err := e.executeShowQueriesStatement()
	assert.NoError(t, err)
	// there is a one has been killed in all hosts
	assert.Equal(t, mockInfosNum-1, len(rows[0].Values))
}
