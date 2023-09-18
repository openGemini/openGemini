package coordinator

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

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

func (m *MockMetaClient) CreateContinuousQuery(database, name, query string) error {
	return nil
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
			assert.Equal(t, allKilled, cmbInfo.getCombinedRunState())
		} else {
			assert.Equal(t, dataNodesNum, len(cmbInfo.runningHosts))
			assert.Equal(t, 0, len(cmbInfo.killedHosts))
			assert.Equal(t, allRunning, cmbInfo.getCombinedRunState())
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

func (s *mockNS) KillQueryOnNode(nodeID, queryID uint64) error {
	return errors.New("err")
}

func TestStatementExecutor_executeShowQueriesStatement(t *testing.T) {
	e := StatementExecutor{MetaClient: &MockMetaClient{}, NetStorage: &mockNS{}}
	rows, err := e.executeShowQueriesStatement()
	assert.NoError(t, err)
	// there is a one has been killed in all hosts
	assert.Equal(t, mockInfosNum-1, len(rows[0].Values))
}

func Test_combinedQueryExeInfo_getCombinedRunState(t *testing.T) {
	type fields struct {
		runningHosts map[string]struct{}
		killedHosts  map[string]struct{}
	}
	tests := []struct {
		name   string
		fields fields
		want   combinedRunState
	}{
		{
			name: "AllRunning",
			fields: fields{
				runningHosts: map[string]struct{}{
					"127.0.0.1:8400": {},
					"127.0.0.1:8401": {},
					"127.0.0.1:8402": {},
				},
				killedHosts: nil,
			},
			want: allRunning,
		},
		{
			name: "AllKilled",
			fields: fields{
				runningHosts: nil,
				killedHosts: map[string]struct{}{
					"127.0.0.1:8400": {},
					"127.0.0.1:8401": {},
					"127.0.0.1:8402": {},
				},
			},
			want: allKilled,
		},
		{
			name: "PartiallyKilled",
			fields: fields{
				runningHosts: map[string]struct{}{"127.0.0.1:8400": {}},
				killedHosts:  map[string]struct{}{"127.0.0.1:8401": {}, "127.0.0.1:8402": {}},
			},
			want: partiallyKilled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &combinedQueryExeInfo{
				runningHosts: tt.fields.runningHosts,
				killedHosts:  tt.fields.killedHosts,
			}
			assert.Equal(t, tt.want, q.getCombinedRunState())
		})
	}
}

func TestStatementExecutor_executeKillQuery(t *testing.T) {
	e := StatementExecutor{MetaClient: &MockMetaClient{}, NetStorage: &mockNS{}, StmtExecLogger: Logger.NewLogger(errno.ModuleUnknown)}
	err1 := e.executeKillQuery(&influxql.KillQueryStatement{QueryID: uint64(1), Host: "127.0.0.1:8400"})
	assert.EqualError(t, err1, meta2.ErrUnsupportCommand.Error())

	err2 := e.executeKillQuery(&influxql.KillQueryStatement{QueryID: uint64(1)})
	assert.NoError(t, err2)
}

func TestStatementExecutor_executeCreateContinuousQueryStatement(t *testing.T) {
	e := StatementExecutor{MetaClient: &MockMetaClient{}, NetStorage: &mockNS{}, StmtExecLogger: Logger.NewLogger(errno.ModuleUnknown)}
	err := e.executeCreateContinuousQueryStatement(
		&influxql.CreateContinuousQueryStatement{
			Name:          "cq0",
			Database:      "db0",
			Source:        newMockSelectStatement("rp0", "mst0"),
			ResampleEvery: 10 * time.Minute,
			ResampleFor:   time.Hour,
		})
	assert.EqualError(t, err, "must be a SELECT INTO clause")

	// case: no GROUP BY time clause
	selectStatement1 := func() *influxql.SelectStatement {
		selectStatement := &influxql.SelectStatement{
			Fields:  make(influxql.Fields, 0, 3),
			Sources: make(influxql.Sources, 0, 3),
		}
		selectStatement.Target = &influxql.Target{Measurement: &influxql.Measurement{Database: "db1", Name: "mst1"}}
		selectStatement.Fields = append(selectStatement.Fields,
			&influxql.Field{
				Expr:  &influxql.VarRef{Val: "field", Type: influxql.Integer},
				Alias: "",
			},
		)
		selectStatement.Sources = append(selectStatement.Sources,
			&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "mst0"})
		return selectStatement
	}
	err = e.executeCreateContinuousQueryStatement(
		&influxql.CreateContinuousQueryStatement{
			Name:          "cq0",
			Database:      "db0",
			Source:        selectStatement1(),
			ResampleEvery: 10 * time.Minute,
			ResampleFor:   time.Hour,
		})
	assert.EqualError(t, err, "GROUP BY time duration must be greater than 0s")

	// case: time filter condition clause
	selectStatement2 := func() *influxql.SelectStatement {
		selectStatement := &influxql.SelectStatement{
			Fields:  make(influxql.Fields, 0, 3),
			Sources: make(influxql.Sources, 0, 3),
		}
		selectStatement.Target = &influxql.Target{Measurement: &influxql.Measurement{Database: "db1", Name: "mst1"}}
		selectStatement.Fields = append(selectStatement.Fields,
			&influxql.Field{
				Expr:  &influxql.VarRef{Val: "field", Type: influxql.Integer},
				Alias: "",
			},
		)
		selectStatement.Dimensions = influxql.Dimensions{
			{
				Expr: &influxql.Call{
					Name: "time",
					Args: []influxql.Expr{
						&influxql.DurationLiteral{
							Val: time.Minute,
						},
					},
				},
			},
		}
		selectStatement.SetTimeInterval(time.Minute)
		selectStatement.Condition =
			&influxql.BinaryExpr{
				LHS: &influxql.VarRef{
					Type: influxql.Unknown,
					Val:  "time",
				},
				Op: influxql.GT,
				RHS: &influxql.BinaryExpr{
					LHS: &influxql.Call{
						Name: "now",
					},
					Op: influxql.SUB,
					RHS: &influxql.DurationLiteral{
						Val: time.Hour,
					},
				},
			}
		selectStatement.Sources = append(selectStatement.Sources,
			&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: "mst0"})
		return selectStatement
	}
	stmt := &influxql.CreateContinuousQueryStatement{
		Name:          "cq0",
		Database:      "db0",
		Source:        selectStatement2(),
		ResampleEvery: 10 * time.Minute,
		ResampleFor:   time.Hour,
	}
	err = e.executeCreateContinuousQueryStatement(stmt)
	assert.NoError(t, err)
	cqQuery := stmt.String()
	assert.Equal(t, `CREATE CONTINUOUS QUERY cq0 ON db0 RESAMPLE EVERY 10m FOR 1h BEGIN SELECT "field"::integer INTO db1..mst1 FROM db0.rp0.mst0 GROUP BY time(1m) END`, cqQuery)
}
