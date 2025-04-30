package coordinator

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/coordinator/statement_executor.go

2022.01.23 The ExecuteStatement function is taken from original function, add statements cases:
AlterShardKeyStatement
ShowFieldKeysStatement
ShowFieldKeyCardinalityStatement
ShowTagKeyCardinalityStatement
ShowSeriesStatement
ShowTagValuesCardinalityStatement
PrepareSnapshotStatement
EndPrepareSnapshotStatement
GetRuntimeInfoStatement
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/influxdata/influxdb/models"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var dbStatCount int

const (
	maxRetrySelectCount = 8
	retrySelectInterval = time.Millisecond * 100

	// SHOW CONFIGS parameters
	sqlConfig    = "sql"
	loggingLevel = "logging.level"
)

var streamSupportMap = map[string]bool{"min": true, "max": true, "sum": true, "count": true}

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient meta.MetaClient

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager query.StatementExecutor

	NetStorage netstorage.Storage

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper query.ShardMapper

	// Holds monitoring data for SHOW STATS and SHOW DIAGNOSTICS.
	MetaExecutor *coordinator.MetaExecutor

	//Node *meta.Node

	// Select statement limits
	MaxSelectPointN         int
	MaxSelectSeriesN        int
	MaxSelectFieldsN        int
	MaxSelectBucketsN       int
	MaxQueryMem             int64
	MaxRowSizeLimit         int64
	QueryTimeCompareEnabled bool
	RetentionPolicyLimit    int
	MaxQueryParallel        int

	StmtExecLogger *logger.Logger

	// hostname for show configs statement
	Hostname   string
	SqlConfigs map[string]interface{}
}

type combinedRunState uint8

const (
	allRunning combinedRunState = iota
	partiallyKilled
	allKilled
)

type combinedQueryExeInfo struct {
	qid          uint64
	stmt         string
	database     string
	beginTime    int64
	runningHosts map[string]struct{}
	killedHosts  map[string]struct{}
}

func (q *combinedQueryExeInfo) updateBeginTime(newBegin int64) {
	if newBegin < q.beginTime {
		q.beginTime = newBegin
	}
}

func (q *combinedQueryExeInfo) updateHosts(newHost string, newRunState netstorage.RunStateType) {
	switch newRunState {
	case netstorage.Running:
		q.runningHosts[newHost] = struct{}{}
	case netstorage.Killed:
		q.killedHosts[newHost] = struct{}{}
	default:
		// current version never arriving
	}
}

func (q *combinedQueryExeInfo) getCombinedRunState() combinedRunState {
	if len(q.runningHosts) == 0 {
		return allKilled
	} else if len(q.killedHosts) > 0 {
		return partiallyKilled
	} else {
		return allRunning
	}
}

// getDurationString return the query running time until now, without decimal point. ie. 3.456s --> 3s
func (q *combinedQueryExeInfo) getDurationString() string {
	begin := q.beginTime
	d := time.Duration(time.Now().UnixNano() - begin)
	switch {
	case d >= time.Second:
		d = d - (d % time.Second)
	case d >= time.Millisecond:
		d = d - (d % time.Millisecond)
	case d >= time.Microsecond:
		d = d - (d % time.Microsecond)
	}
	return d.String()
}

func (q *combinedQueryExeInfo) toOutputRow(colNum int, isKilledPart bool) []interface{} {
	res := make([]interface{}, 0, colNum)

	var hostsJoined = func(hostsKV map[string]struct{}) string {
		hosts := make([]string, 0, len(hostsKV))
		for host := range hostsKV {
			hosts = append(hosts, host)
		}
		return strings.Join(hosts, ", ")
	}

	res = append(res, q.qid, q.stmt, q.database, q.getDurationString())
	if isKilledPart {
		res = append(res, "killed", hostsJoined(q.killedHosts))
	} else {
		res = append(res, "running", hostsJoined(q.runningHosts))
	}

	return res
}

type combinedInfos []*combinedQueryExeInfo

func (c combinedInfos) Len() int {
	return len(c)
}

func (c combinedInfos) Less(i, j int) bool {
	return c[i].beginTime < c[j].beginTime
}

func (c combinedInfos) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (e *StatementExecutor) Close() error {
	return e.ShardMapper.Close()
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext, seq int) error {
	e.MaxQueryParallel = int(atomic.LoadInt32(&syscontrol.QueryParallel))
	stmtString := stmt.String()

	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		begin := time.Now()
		err := e.retryExecuteSelectStatement(stmt, ctx, seq)
		dur := time.Since(begin)
		if err == nil {
			if dur.Nanoseconds() > time.Second.Nanoseconds() {
				e.StmtExecLogger.GetZapLogger().Warn("slow query",
					zap.String("stmt", stmtString),
					zap.Float64("duration", dur.Seconds()))
			}
			return nil
		}
		handler := statistics.NewHandler()
		if errno.Equal(err, errno.DatabaseNotFound, errno.ErrMeasurementNotFound) {
			e.StmtExecLogger.Error("execute select statement 400 error", zap.Any("stmt", stmtString),
				zap.Error(err), zap.Float64("duration", dur.Seconds()))
			handler.Query400ErrorStmtCount.Incr()
		} else {
			e.StmtExecLogger.Error("execute select statement 500 error", zap.Any("stmt", stmtString),
				zap.Error(err), zap.Float64("duration", dur.Seconds()))
			handler.QueryErrorStmtCount.Incr()
		}
		return err
	}

	e.StmtExecLogger.Info("start execute statement", zap.Any("stmt", stmtString))
	var rows models.Rows
	var messages []*query.Message
	var err error
	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeAlterRetentionPolicyStatement(stmt)
	case *influxql.AlterShardKeyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeAlterShardKeyStatement(stmt)
	case *influxql.CreateDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateDatabaseStatement(stmt)
	case *influxql.CreateMeasurementStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateMeasurementStatement(stmt)
	case *influxql.CreateRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateRetentionPolicyStatement(stmt)
	case *influxql.CreateSubscriptionStatement:
		err = e.executeCreateSubscriptionStatement(stmt)
	case *influxql.CreateContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateContinuousQueryStatement(stmt)
	case *influxql.ShowContinuousQueriesStatement:
		rows, err = e.executeShowContinuousQueriesStatement(stmt)
	case *influxql.DropContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropContinuousQueryStatement(stmt)
	case *influxql.CreateUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateUserStatement(stmt)
	case *influxql.DeleteSeriesStatement:
		return meta2.ErrUnsupportCommand
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.DropDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.DropMeasurementStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.DropSeriesStatement:
		return meta2.ErrUnsupportCommand
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.DropRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.DropShardStatement:
		return meta2.ErrUnsupportCommand
	case *influxql.DropSubscriptionStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropSubscriptionStatement(stmt)
	case *influxql.DropUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropUserStatement(stmt)
	case *influxql.ExplainStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.GrantStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantStatement(stmt)
	case *influxql.GrantAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantAdminStatement(stmt)
	case *influxql.RevokeStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		// TODO: transform to `github.com/influxdata/influxql` RevokeStatement
		stmt1 := originql.RevokeStatement{
			Privilege: originql.Privilege(stmt.Privilege),
			On:        stmt.On,
			User:      stmt.User,
		}
		err = e.executeRevokeStatement(&stmt1)
	case *influxql.RevokeAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeRevokeAdminStatement(stmt)
	case *influxql.ShowDatabasesStatement:
		rows, err = e.executeShowDatabasesStatement(stmt, ctx)
	case *influxql.ShowDiagnosticsStatement:
		return meta2.ErrUnsupportCommand
	case *influxql.ShowGrantsForUserStatement:
		rows, err = e.executeShowGrantsForUserStatement(stmt)
	case *influxql.ShowMeasurementKeysStatement:
		rows, err = e.executeShowMeasurementKeysStatement(stmt)
	case *influxql.ShowMeasurementsStatement:
		if stmt.Condition != nil {
			return meta2.ErrUnsupportCommand
		}
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowMeasurementsDetailStatement:
		err = e.executeShowMeasurementsDetailStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowMeasurementCardinalityStatement:
		if stmt.Condition != nil {
			return meta2.ErrUnsupportCommand
		}
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.ShowRetentionPoliciesStatement:
		rows, err = e.executeShowRetentionPoliciesStatement(stmt)
	case *influxql.ShowSeriesCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.ShowShardsStatement:
		rows, err = e.executeShowShardsStatement(stmt)
	case *influxql.ShowShardGroupsStatement:
		rows, err = e.executeShowShardGroupsStatement(stmt)
	case *influxql.ShowSubscriptionsStatement:
		rows, err = e.executeShowSubscriptionsStatement(stmt)
	case *influxql.ShowFieldKeysStatement:
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowFieldKeyCardinalityStatement:
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowTagKeysStatement:
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowTagKeyCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.ShowTagValuesStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.ShowSeriesStatement:
		_, err = e.retryExecuteStatement(stmt, ctx, seq)
		return err
	case *influxql.ShowTagValuesCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx, seq)
	case *influxql.ShowUsersStatement:
		rows, err = e.executeShowUsersStatement(stmt)
	case *influxql.SetPasswordUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeSetPasswordUserStatement(stmt)
	case *influxql.ShowQueriesStatement:
		rows, err = e.executeShowQueriesStatement()
	case *influxql.KillQueryStatement:
		err = e.executeKillQuery(stmt)
	case *influxql.PrepareSnapshotStatement:
		return meta2.ErrUnsupportCommand
		err = e.executePrepareSnapshotStatement(stmt, ctx)
	case *influxql.EndPrepareSnapshotStatement:
		return meta2.ErrUnsupportCommand
		err = e.executeEndPrepareSnapshotStatement(stmt, ctx)
	case *influxql.GetRuntimeInfoStatement:
		return meta2.ErrUnsupportCommand
		rows, err = e.executeGetRuntimeInfoStatement(stmt, ctx)
	case *influxql.CreateDownSampleStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateDownSamplingStmt(stmt)
	case *influxql.DropDownSampleStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropDownSamplingStmt(stmt)
	case *influxql.ShowDownSampleStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		rows, err = e.executeShowDownSamplingStmt(stmt)
	case *influxql.CreateStreamStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateStreamStatement(stmt, ctx)
	case *influxql.ShowStreamsStatement:
		rows, err = e.executeShowStreamsStatement(stmt)
	case *influxql.DropStreamsStatement:
		err = e.executeDropStream(stmt)
	case *influxql.ShowConfigsStatement:
		rows, err = e.executeShowConfigs(stmt)
	case *influxql.SetConfigStatement:
		err = e.executeSetConfig(stmt)
	case *influxql.ShowClusterStatement:
		rows, err = e.executeShowCluster(stmt)
	case *influxql.WithSelectStatement:
		err = e.executeWithSelectStatement(stmt)
	default:
		return query.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ctx.Send(&query.Result{
		Series:   rows,
		Messages: messages,
	}, seq, nil)
}

func (e *StatementExecutor) retryExecuteStatement(stmt influxql.Statement, ctx *query.ExecutionContext, seq int) (models.Rows, error) {
	startTime := time.Now()
	var retryNum uint32 = 0
	var err error
	var rows models.Rows
	for time.Now().Sub(startTime).Seconds() < coordinator.DMLTimeOutSecond {
		if retryNum > 0 {
			time.Sleep(coordinator.DMLRetryInternalMillisecond * time.Millisecond)
		}
		retryNum++

		switch stmt := stmt.(type) {
		case *influxql.DropDatabaseStatement:
			err = e.executeDropDatabaseStatement(stmt)
		case *influxql.DropMeasurementStatement:
			err = e.executeDropMeasurementStatement(stmt, ctx.Database)
		case *influxql.DropRetentionPolicyStatement:
			err = e.executeDropRetentionPolicyStatement(stmt)
		case *influxql.ShowTagKeysStatement:
			err = e.executeShowTagKeys(stmt, ctx, seq)
		case *influxql.ShowTagKeyCardinalityStatement:
			err = e.executeShowTagKeyCardinality(stmt, ctx, seq)
		case *influxql.ShowTagValuesStatement:
			rows, err = e.executeShowTagValues(stmt)
		case *influxql.ShowSeriesStatement:
			err = e.executeShowSeries(stmt, ctx, seq)
		case *influxql.ShowMeasurementsStatement:
			err = e.executeShowMeasurementsStatement(stmt, ctx, seq)
		case *influxql.ShowMeasurementsDetailStatement:
			err = e.executeShowMeasurementsDetailStatement(stmt, ctx, seq)
		case *influxql.ShowMeasurementCardinalityStatement:
			rows, err = e.executeShowMeasurementCardinalityStatement(stmt)
		case *influxql.ShowSeriesCardinalityStatement:
			rows, err = e.executeShowSeriesCardinality(stmt)
		case *influxql.ShowTagValuesCardinalityStatement:
			rows, err = e.executeShowTagValuesCardinality(stmt)
		case *influxql.ShowFieldKeysStatement:
			err = e.executeShowFieldKeys(stmt, ctx, seq)
		case *influxql.ShowFieldKeyCardinalityStatement:
			err = e.executeShowFieldKeyCardinality(stmt, ctx, seq)
		case *influxql.ExplainStatement:
			if stmt.Analyze {
				rows, err = e.executeExplainAnalyzeStatement(stmt, ctx)
			} else {
				rows, err = e.executeExplainStatement(stmt, ctx)
			}
		}

		if err == nil {
			return rows, err
		}

		if coordinator.IsRetriedError(err) || strings.Contains(err.Error(), "repeat mark delete") {
			e.StmtExecLogger.Warn("retry ExecuteStatement ", zap.Error(err), zap.Uint32("retryNum", retryNum), zap.Any("stmt", stmt))
			continue
		} else {
			break
		}
	}
	e.StmtExecLogger.Error("ExecuteStatement error ", zap.Error(err), zap.Any("stmt", stmt))
	return rows, err
}

func (e *StatementExecutor) executeCreateDownSamplingStmt(stmt *influxql.CreateDownSampleStatement) error {
	if !meta2.ValidName(stmt.DbName) {
		return errno.NewError(errno.InvalidName)
	}

	e.StmtExecLogger.Info("create downSample ", zap.String("db", stmt.DbName), zap.String("rp", stmt.RpName))

	rpi, err := e.MetaClient.RetentionPolicy(stmt.DbName, stmt.RpName)

	if err != nil {
		return err
	}
	if rpi == nil {
		return errno.NewError(errno.RpNotFound)
	}
	downSampleInfo, err := meta2.NewDownSamplePolicyInfo(stmt.Ops, stmt.Duration, stmt.SampleInterval, stmt.TimeInterval, stmt.WaterMark, rpi)
	if err != nil {
		return err
	}
	if rpi.HasDownSamplePolicy() {
		if rpi.DownSamplePolicyInfo.Equal(downSampleInfo, false) {
			return nil
		}
		return errno.NewError(errno.DownSamplePolicyExists)
	}

	return e.MetaClient.NewDownSamplePolicy(stmt.DbName, rpi.Name, downSampleInfo)
}

func (e *StatementExecutor) executeDropDownSamplingStmt(stmt *influxql.DropDownSampleStatement) error {
	if !meta2.ValidName(stmt.DbName) {
		return errno.NewError(errno.InvalidName)
	}

	e.StmtExecLogger.Info("drop downSample ", zap.String("db", stmt.DbName))

	rpi, err := e.MetaClient.RetentionPolicy(stmt.DbName, stmt.RpName)
	if err != nil {
		return err
	}
	if !stmt.DropAll {
		if rpi == nil {
			return errno.NewError(errno.RpNotFound)
		}
		if !rpi.HasDownSamplePolicy() {
			return errno.NewError(errno.DownSamplePolicyNotFound)
		}
	}

	return e.MetaClient.DropDownSamplePolicy(stmt.DbName, rpi.Name, stmt.DropAll)
}

func (e *StatementExecutor) executeShowDownSamplingStmt(stmt *influxql.ShowDownSampleStatement) (models.Rows, error) {
	if stmt.DbName == "" {
		return nil, coordinator.ErrDatabaseNameRequired
	}
	return e.MetaClient.ShowDownSamplePolicies(stmt.DbName)
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement) error {
	rpi, err := e.MetaClient.RetentionPolicy(stmt.Database, stmt.Name)
	if err != nil {
		return err
	}
	if rpi == nil {
		return errno.NewError(errno.RpNotFound)
	}
	if rpi.HasDownSamplePolicy() && stmt.Duration != nil && rpi.Duration != *stmt.Duration {
		return errno.NewError(errno.DownSamplePolicyExists)
	}
	oneReplication := 1
	rpu := &meta2.RetentionPolicyUpdate{
		Duration:           stmt.Duration,
		ReplicaN:           &oneReplication,
		ShardGroupDuration: stmt.ShardGroupDuration,
		HotDuration:        stmt.HotDuration,
		WarmDuration:       stmt.WarmDuration,
		IndexGroupDuration: stmt.IndexGroupDuration,
	}

	// Update the retention policy.
	return e.MetaClient.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu, stmt.Default)
}

func (e *StatementExecutor) getRetentionPolicyCount() int {
	dbs := e.MetaClient.Databases()
	var c int
	for _, db := range dbs {
		c += len(db.RetentionPolicies)
	}
	return c
}

func (e *StatementExecutor) getRpLimit() int {
	return e.RetentionPolicyLimit
}

func (e *StatementExecutor) getIndexRelation(stmt *influxql.CreateMeasurementStatement) (*influxql.IndexRelation, error) {
	indexR := influxql.NewIndexRelation()
	if len(stmt.IndexType) != len(stmt.IndexList) {
		return nil, fmt.Errorf("the IndexType does not match the number of index fields")
	}

	for i, indexTypeName := range stmt.IndexType {
		oid, err := index.GetIndexTypeByName(indexTypeName)
		if err != nil {
			return nil, err
		}
		if oid == index.Field && len(stmt.IndexList[i]) > 1 {
			return nil, fmt.Errorf("cannot create field index for multiple columns: %v", stmt.IndexList[i])
		}
		indexR.Oids = append(indexR.Oids, uint32(oid))
		indexR.IndexNames = append(indexR.IndexNames, indexTypeName)
		indexR.IndexList = append(indexR.IndexList, &influxql.IndexList{IList: stmt.IndexList[i]})
		if oid == index.TimeCluster {
			indexR.IndexOptions = append(indexR.IndexOptions, &influxql.IndexOptions{
				Options: []*influxql.IndexOption{
					{TimeClusterDuration: stmt.TimeClusterDuration},
				},
			})
		} else if oid == index.Text {
			indexR.IndexOptions = append(indexR.IndexOptions, &influxql.IndexOptions{
				Options: []*influxql.IndexOption{
					{Tokens: tokenizer.CONTENT_SPLITTER},
				},
			})
		} else {
			indexR.IndexOptions = append(indexR.IndexOptions, &influxql.IndexOptions{})
		}
	}
	return indexR, nil
}

func (e *StatementExecutor) executeCreateMeasurementStatement(stmt *influxql.CreateMeasurementStatement) error {
	if !meta2.ValidMeasurementName(stmt.Name) {
		return meta2.ErrInvalidName
	}

	if err := meta2.ValidShardKey(stmt.ShardKey); err != nil {
		return err
	}
	e.StmtExecLogger.Info("create measurement ", zap.String("name", stmt.Name))
	colStoreInfo := meta2.NewColStoreInfo(stmt.PrimaryKey, stmt.SortKey, stmt.Property, stmt.TimeClusterDuration, stmt.CompactType)
	schemaInfo := meta2.NewSchemaInfo(stmt.Tags, stmt.Fields)
	ski := &meta2.ShardKeyInfo{ShardKey: stmt.ShardKey, Type: stmt.Type}
	indexR, err := e.getIndexRelation(stmt)
	if err != nil {
		return err
	}
	engineType, ok := config.String2EngineType[stmt.EngineType]
	if stmt.EngineType != "" && !ok {
		return errors.New("ENGINETYPE \"" + stmt.EngineType + "\" IS NOT SUPPORTED!")
	}
	_, err = e.MetaClient.CreateMeasurement(stmt.Database, stmt.RetentionPolicy, stmt.Name, ski, int32(stmt.NumOfShards), indexR, engineType, colStoreInfo, schemaInfo, nil)
	return err
}

func (e *StatementExecutor) executeAlterShardKeyStatement(stmt *influxql.AlterShardKeyStatement) error {
	if err := meta2.ValidShardKey(stmt.ShardKey); err != nil {
		return err
	}
	ski := &meta2.ShardKeyInfo{ShardKey: stmt.ShardKey, Type: stmt.Type}
	return e.MetaClient.AlterShardKey(stmt.Database, stmt.RetentionPolicy, stmt.Name, ski)
}

func (e *StatementExecutor) executeCreateDatabaseStatement(stmt *influxql.CreateDatabaseStatement) error {
	if !meta2.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta2.ErrInvalidName
	}

	e.StmtExecLogger.Info("create database ", zap.String("db", stmt.Name))
	rpLimit := e.getRpLimit()
	if e.getRetentionPolicyCount() >= rpLimit {
		e.StmtExecLogger.Error("exceeds the rp limit", zap.String("db", stmt.Name))
		return errors.New("THE TOTAL NUMBER OF RPs EXCEEDS THE LIMIT")
	}

	if !stmt.RetentionPolicyCreate {
		_, err := e.MetaClient.CreateDatabase(stmt.Name, stmt.DatabaseAttr.EnableTagArray, stmt.DatabaseAttr.Replicas, nil)
		e.StmtExecLogger.Info("create database finish", zap.String("db", stmt.Name), zap.Error(err))
		return err
	}
	// If we're doing, for example, CREATE DATABASE "db" WITH DURATION 1d then
	// the name will not yet be set. We only need to validate non-empty
	// retention policy names, such as in the statement:
	// 	CREATE DATABASE "db" WITH DURATION 1d NAME "xyz"
	if stmt.RetentionPolicyName != "" && !meta2.ValidName(stmt.RetentionPolicyName) {
		e.StmtExecLogger.Info("create database error ErrInvalidName", zap.String("db", stmt.Name))
		return meta2.ErrInvalidName
	}

	if err := meta2.ValidShardKey(stmt.ShardKey); err != nil {
		return err
	}

	spec := meta2.RetentionPolicySpec{
		Name:               stmt.RetentionPolicyName,
		Duration:           stmt.RetentionPolicyDuration,
		ReplicaN:           stmt.RetentionPolicyReplication,
		ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
		HotDuration:        &stmt.RetentionPolicyHotDuration,
		WarmDuration:       &stmt.RetentionPolicyWarmDuration,
		IndexGroupDuration: stmt.RetentionPolicyIndexGroupDuration,
	}
	ski := &meta2.ShardKeyInfo{ShardKey: stmt.ShardKey}
	_, err := e.MetaClient.CreateDatabaseWithRetentionPolicy(stmt.Name, &spec, ski,
		stmt.DatabaseAttr.EnableTagArray, stmt.DatabaseAttr.Replicas)
	e.StmtExecLogger.Info("create database finish with RP", zap.String("db", stmt.Name), zap.Error(err))
	return err
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement) error {
	if !meta2.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateRetentionPolicy`
		// but can't go there until 1.1 is used everywhere
		return meta2.ErrInvalidName
	}

	rpLimit := e.getRpLimit()
	if e.getRetentionPolicyCount() >= rpLimit {
		e.StmtExecLogger.Error("exceeds the rp limit", zap.String("db", stmt.Name))
		return errors.New("THE TOTAL NUMBER OF RPs EXCEEDS THE LIMIT")
	}

	e.StmtExecLogger.Info("RetentionPolicySpec", zap.String("name", stmt.Name),
		zap.String("Duration", stmt.Duration.String()),
		zap.String("WarmDuration", stmt.WarmDuration.String()),
		zap.String("ShardGroupDuration", stmt.ShardGroupDuration.String()))

	spec := meta2.RetentionPolicySpec{
		Name:               stmt.Name,
		Duration:           &stmt.Duration,
		ReplicaN:           &stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
		HotDuration:        &stmt.HotDuration,
		WarmDuration:       &stmt.WarmDuration,
		IndexGroupDuration: stmt.IndexGroupDuration,
	}

	// Create new retention policy.
	_, err := e.MetaClient.CreateRetentionPolicy(stmt.Database, &spec, stmt.Default)
	return err
}

func isValidContinuousQueryStatement(query string) error {
	p := influxql.NewParser(strings.NewReader(query))
	defer p.Release()

	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	qr, err := YyParser.GetQuery()
	if err != nil {
		return err
	}
	if len(qr.Statements) == 0 {
		return errors.New("no valid continuous query statement")
	}
	stmt := qr.Statements[0]

	// check if the statement is a valid continuous query.
	q, ok := stmt.(*influxql.CreateContinuousQueryStatement)
	if !ok || q.Source.Target == nil || q.Source.Target.Measurement == nil {
		return errors.New("must be a SELECT INTO clause")
	}

	// check group by time
	interval, err := q.Source.GroupByInterval()
	if err != nil {
		return err
	} else if interval == 0 {
		return fmt.Errorf("GROUP BY time duration must be greater than 0s")
	}

	// check interval and ResampleFor/ResampleEvery
	if q.ResampleFor != 0 {
		if q.ResampleEvery != 0 && q.ResampleEvery > interval {
			interval = q.ResampleEvery
		}
		if interval > q.ResampleFor {
			return fmt.Errorf("FOR duration must be >= GROUP BY time duration: must be a minimum of %s, got %s", interval.String(), q.ResampleFor.String())
		}
	}
	return nil
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(stmt *influxql.CreateContinuousQueryStatement) error {
	// remote the time filter condition
	valuer := influxql.NowValuer{Now: time.Now()}
	cond, _, err := influxql.ConditionExpr(stmt.Source.Condition, &valuer)
	if err != nil {
		return err
	}
	stmt.Source.Condition = cond

	cqQuery := stmt.String()
	if err = isValidContinuousQueryStatement(cqQuery); err != nil {
		return err
	}
	return e.MetaClient.CreateContinuousQuery(stmt.Database, stmt.Name, cqQuery)
}

// executeDropContinuousQueryStatement drops a continuous query from the cluster.
func (e *StatementExecutor) executeDropContinuousQueryStatement(stmt *influxql.DropContinuousQueryStatement) error {
	e.StmtExecLogger.Info("delete continuous query start", zap.String("cq name", stmt.Name), zap.String("database", stmt.Database))
	if err := e.MetaClient.DropContinuousQuery(stmt.Name, stmt.Database); err != nil {
		e.StmtExecLogger.Error("delete continuous query error", zap.String("cq name", stmt.Name), zap.String("database", stmt.Database), zap.Error(err))
		return err
	}
	return nil
}

func (e *StatementExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
	if !config.GetSubscriptionEnable() {
		return errors.New("subscription is not enabled")
	}
	return e.MetaClient.CreateSubscription(q.Database, q.RetentionPolicy, q.Name, q.Mode, q.Destinations)
}

func (e *StatementExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement) error {
	_, err := e.MetaClient.CreateUser(q.Name, q.Password, q.Admin, q.Rwuser)
	return err
}

// executeDropDatabaseStatement drops a database from the cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *StatementExecutor) executeDropDatabaseStatement(stmt *influxql.DropDatabaseStatement) error {

	//here we should mark database as deleted. after all store.data deleted success then delete the meta.data
	//beacuse, we must forbidden create same name DB when the DB is being deleted

	e.StmtExecLogger.Info("mark delete database start ", zap.String("db", stmt.Name))
	if err := e.MetaClient.MarkDatabaseDelete(stmt.Name); err != nil {
		e.StmtExecLogger.Error("Delete database MarkDatabaseDelete error ", zap.String("db", stmt.Name), zap.Error(err))
		if strings.HasPrefix(err.Error(), "database not found") {
			return nil
		}
		return err
	}

	return nil
}

func (e *StatementExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) error {
	if _, err := e.MetaClient.Database(database); err != nil {
		return err
	}

	return e.MetaClient.MarkMeasurementDelete(database, stmt.RpName, stmt.Name)
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(stmt *influxql.DropRetentionPolicyStatement) error {
	e.StmtExecLogger.Info("start delete rp ", zap.String("db", stmt.Database), zap.String("rp", stmt.Name))
	dbi, _ := e.MetaClient.Database(stmt.Database)
	if dbi == nil {
		return nil
	}

	if dbi.RetentionPolicy(stmt.Name) == nil {
		return nil
	}

	if err := e.MetaClient.MarkRetentionPolicyDelete(stmt.Database, stmt.Name); err != nil {
		e.StmtExecLogger.Error("Delete rp MarkRetentionPolicyDelete error ", zap.String("db", stmt.Database), zap.String("rp", stmt.Name), zap.Error(err))
		return err
	}

	e.StmtExecLogger.Info("suc delete rp ", zap.String("db", stmt.Database), zap.String("rp", stmt.Name))

	return nil
}

func (e *StatementExecutor) executeDropSubscriptionStatement(q *influxql.DropSubscriptionStatement) error {
	if !config.GetSubscriptionEnable() {
		return errors.New("subscription is not enabled")
	}
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *StatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *StatementExecutor) executeExplainStatement(q *influxql.ExplainStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	return nil, meta2.ErrUnsupportCommand
}

func (e *StatementExecutor) executeExplainAnalyzeStatement(q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	stmt := q.Statement
	trace, span := tracing.NewTrace("SELECT")
	stmt.OmitTime = true
	ctx := tracing.NewContextWithTrace(ectx.Context, trace)
	ctx = tracing.NewContextWithSpan(ctx, span)
	span.AppendNameValue("statement", q.String())
	span.Finish()

	proxy := newRowChanProxy()
	pipSpan := span.StartSpan("create_pipeline_executor").StartPP()
	pipelineExecutor, err := e.createPipelineExecutor(ctx, stmt, ectx.ExecutionOptions, proxy.rc)
	pipSpan.Finish()

	if err != nil {
		proxy.close()
		return nil, err
	}
	if pipelineExecutor == nil {
		proxy.close()
		return models.Rows{}, nil
	}

	ec := make(chan error, 1)
	go func() {
		ec <- pipelineExecutor.ExecuteExecutor(ctx)
		close(ec)
		proxy.close()
	}()

	emSpan := span.StartSpan("emit").StartPP()
	rowCount := 0

	err = func() error {
		for {
			select {
			case rowsChan, ok := <-proxy.rc:
				if !ok {
					return nil
				}
				for _, row := range rowsChan.Rows {
					rowCount += len(row.Values)
				}
			case <-ctx.Done():
				pipelineExecutor.Abort()
				go proxy.wait()
				return ctx.Err()
			}
		}
	}()
	if err != nil {
		return nil, err
	}

	if err := <-ec; err != nil {
		e.StmtExecLogger.Error("pipeline execute failed", zap.Error(err))
		return nil, err
	}
	emSpan.AppendNameValue("row_count", rowCount)
	emSpan.Finish()

	row := &models.Row{
		Columns: []string{"EXPLAIN ANALYZE"},
	}
	for _, s := range strings.Split(trace.String(), "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}

	return models.Rows{row}, nil
}

func (e *StatementExecutor) executeGrantStatement(stmt *influxql.GrantStatement) error {
	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, originql.Privilege(stmt.Privilege))
}

func (e *StatementExecutor) executeGrantAdminStatement(stmt *influxql.GrantAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, true)
}

func (e *StatementExecutor) executeRevokeStatement(stmt *originql.RevokeStatement) error {
	priv := originql.NoPrivileges

	// Revoking all privileges means there's no need to look at existing user privileges.
	if stmt.Privilege != originql.AllPrivileges {
		p, err := e.MetaClient.UserPrivilege(stmt.User, stmt.On)
		if err != nil {
			return err
		}
		// Bit clear (AND NOT) the user's privilege with the revoked privilege.
		priv = *p &^ stmt.Privilege
	}

	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, priv)
}

func (e *StatementExecutor) executeRevokeAdminStatement(stmt *influxql.RevokeAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, false)
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement) error {
	return e.MetaClient.UpdateUser(q.Name, q.Password)
}

func (e *StatementExecutor) retryExecuteSelectStatement(stmt *influxql.SelectStatement, ctx *query.ExecutionContext, seq int) error {
	var err error

	for i := 0; i < maxRetrySelectCount; i++ {
		err = e.executeSelectStatement(stmt, ctx, seq)
		if err == nil || !errno.IsRetryErrorForPtView(err) {
			break
		}
		time.Sleep(retrySelectInterval * (1 << i))
	}
	return err
}

func (e *StatementExecutor) retryCreatePipelineExecutor(ctx context.Context, stmt *influxql.SelectStatement, opt query.ExecutionOptions, rowsChan chan query.RowsChan) (*executor.PipelineExecutor, error) {
	startTime := time.Now()
	var retryNum uint32 = 0
	for {
		pipelineExecutor, err := e.createPipelineExecutor(ctx, stmt, opt, rowsChan)
		if err == nil {
			return pipelineExecutor, err
		}

		if coordinator.IsRetriedError(err) || strings.Contains(err.Error(), "max message size") {
			if retryNum%20 == 0 {
				e.StmtExecLogger.Warn("retry retryCreatePipelineExecutor ", zap.Error(err), zap.Uint32("retryNum", retryNum), zap.Any("stmt", stmt))
			}
			if time.Now().Sub(startTime).Seconds() < coordinator.DMLTimeOutSecond {
				time.Sleep(coordinator.DMLRetryInternalMillisecond * time.Millisecond)
				retryNum++
				continue
			} else {
				return nil, err
			}
		} else {
			if strings.Contains(err.Error(), "declare empty collection") {
				return nil, nil
			} else {
				e.StmtExecLogger.Error("retry retryCreatePipelineExecutor err ", zap.Error(err), zap.Any("stmt", stmt))
			}
			return nil, err
		}
	}
}

func (e *StatementExecutor) executeSelectStatement(stmt *influxql.SelectStatement, ctx *query.ExecutionContext, seq int) error {
	start := time.Now()
	proxy := newRowChanProxy()
	// omit Time field for stmt
	stmt.OmitTime = true
	pipelineExecutor, err := e.retryCreatePipelineExecutor(ctx, stmt, ctx.ExecutionOptions, proxy.rc)
	if err == influxql.ErrDeclareEmptyCollection {
		// skip empty collection err and return empty result set
		err = nil
		pipelineExecutor = nil
	}
	if err != nil {
		proxy.close()
		return err
	}
	if pipelineExecutor == nil {
		proxy.close()
		return ctx.Send(&query.Result{
			Series: make([]*models.Row, 0),
		}, seq, nil)
	}

	end := time.Now()
	emitted := false
	closed := false

	ec := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	var ctxWithWriter context.Context
	go func() {
		defer wg.Done()
		ctxWithWriter = context.WithValue(context.Background(), executor.WRITER_CONTEXT, ctx.PointsWriter)
		var queryIndexState int32 = 0
		ctxWithWriter = context.WithValue(ctxWithWriter, index.QueryIndexState, &queryIndexState)
		ec <- pipelineExecutor.ExecuteExecutor(ctxWithWriter)
		close(ec)
		proxy.close()
	}()

	defer func() {
		qStat, _ := ctx.Value(query.QueryDurationKey).(*statistics.SQLSlowQueryStatistics)
		if qStat != nil {
			qStat.AddDuration("SqlIteratorDuration", end.Sub(start).Nanoseconds())
			qStat.AddDuration("EmitDuration", time.Now().Sub(end).Nanoseconds())
		}
	}()

	var rowsChan query.RowsChan
	var ok bool
	var init bool
	var byteSizePerRow int
	var byteSizePerSeries int
	var totalRowSize int
	for {
		select {
		case rowsChan, ok = <-proxy.rc:
			if !ok {
				closed = true
				break
			}
			result := &query.Result{
				Series:  rowsChan.Rows,
				Partial: rowsChan.Partial,
			}

			// Send results or exit if closing.
			if err := ctx.Send(result, seq, ctxWithWriter); err != nil {
				var abort, crash bool
				if sysconfig.GetInterruptQuery() && strings.Contains(err.Error(), query.ErrQueryTimeoutLimitExceeded.Error()) {
					pipelineExecutor.Crash()
					crash = true
				} else {
					pipelineExecutor.Abort()
					abort = true
				}
				e.StmtExecLogger.Error("send result rows failed", zap.Error(err), zap.Bool("crash", crash), zap.Bool("abort", abort))
				return err
			}
			emitted = true
			if e.MaxRowSizeLimit > 0 {
				if !init {
					if len(result.Series) > 0 && len(result.Series[0].Values) > 0 {
						byteSizePerSeries, byteSizePerRow = getPerSeriesAndRowSize(result.Series[0])
						init = true
					}
				}
				for i := range result.Series {
					totalRowSize += byteSizePerSeries
					totalRowSize += len(result.Series[i].Values) * byteSizePerRow
				}
				if totalRowSize > int(e.MaxRowSizeLimit) {
					e.StmtExecLogger.Warn("the queried data volume exceeds the maximum memory threshold.",
						zap.Float64("QueryMemory(Gb):", float64(totalRowSize)/config.GB),
						zap.Float64("MemoryThreshold(GB):", float64(e.MaxRowSizeLimit)/config.GB),
						zap.String("stmt", stmt.String()))
					pipelineExecutor.Abort()
					// Always emit at least one result.
					return ctx.Send(&query.Result{
						Series: make([]*models.Row, 0),
					}, seq, ctxWithWriter)
				}
			}
		case <-ctx.Done():
			var abort, crash bool
			if err := ctx.Err(); sysconfig.GetInterruptQuery() && err != nil && strings.Contains(err.Error(), query.ErrQueryTimeoutLimitExceeded.Error()) {
				pipelineExecutor.Crash()
				crash = true
			} else {
				pipelineExecutor.Abort()
				abort = true
			}
			e.StmtExecLogger.Info("aborted by user", zap.String("stmt", stmt.String()), zap.Bool("crash", crash), zap.Bool("abort", abort))
			go proxy.wait()
			return ctx.Err()
		}
		if closed {
			break
		}
	}

	wg.Wait()
	if err := <-ec; err != nil {
		e.StmtExecLogger.Error("PipelineExecutor execute failed", zap.Error(err))
		return err
	}

	// Always emit at least one result.
	if !emitted {
		return ctx.Send(&query.Result{
			Series: make([]*models.Row, 0),
		}, seq, ctxWithWriter)
	}
	return nil
}

func (e *StatementExecutor) GetOptions(opt query.ExecutionOptions, rowsChan chan query.RowsChan) query.SelectOptions {
	return query.SelectOptions{
		NodeID:                  opt.NodeID,
		MaxSeriesN:              e.MaxSelectSeriesN,
		MaxFieldsN:              e.MaxSelectFieldsN,
		MaxPointN:               e.MaxSelectPointN,
		MaxBucketsN:             e.MaxSelectBucketsN,
		Authorizer:              opt.Authorizer,
		MaxQueryMem:             e.MaxQueryMem,
		MaxQueryParallel:        e.MaxQueryParallel,
		QueryTimeCompareEnabled: e.QueryTimeCompareEnabled,
		Chunked:                 opt.Chunked,
		ChunkedSize:             opt.ChunkSize,
		QueryLimitEn:            opt.QueryLimitEn,
		RowsChan:                rowsChan,
		ChunkSize:               opt.InnerChunkSize,
		AbortChan:               opt.AbortCh,
		QueryID:                 opt.QueryID,
		IncQuery:                opt.IncQuery,
		IterID:                  opt.IterID,
	}
}

func (e *StatementExecutor) createPipelineExecutor(ctx context.Context, stmt *influxql.SelectStatement, opt query.ExecutionOptions, rowsChan chan query.RowsChan) (pipelineExecutor *executor.PipelineExecutor, err error) {
	sopt := e.GetOptions(opt, rowsChan)

	defer func() {
		if e := recover(); e != nil {
			internalErr, ok := e.(*errno.Error)
			if ok && errno.Equal(internalErr, errno.DtypeNotSupport) {
				panic(internalErr)
			}

			stackInfo := fmt.Errorf("runtime panic: %v\n %s", e, string(debug.Stack())).Error()
			logger.NewLogger(errno.ModuleQueryEngine).Error(stackInfo, zap.Any("query_id", ctx.Value(query.QueryIDKey).([]uint64)),
				zap.String("query", "pipeline executor"), zap.String("query stmt", stmt.String()))
		}
	}()

	// Create a pipelineExecutor from a selection.
	p, e_tmp := executor.Select(ctx, stmt, e.ShardMapper, sopt)
	if e_tmp != nil || p == nil {
		return nil, e_tmp
	}
	pipelineExecutor, err = p.(*executor.PipelineExecutor), e_tmp
	pipelineExecutor.Query = stmt.String()
	return pipelineExecutor, err
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	dis := e.MetaClient.Databases()
	a := ctx.ExecutionOptions.Authorizer

	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	if q.ShowDetail {
		row.Columns = append(row.Columns, "ReplicaN")
		row.Columns = append(row.Columns, "Tag Attribute")
	}

	var tagAttr string
	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		if a.AuthorizeDatabase(originql.ReadPrivilege, di.Name) || a.AuthorizeDatabase(originql.WritePrivilege, di.Name) {
			if !q.ShowDetail {
				row.Values = append(row.Values, []interface{}{di.Name})
			} else {
				if di.EnableTagArray {
					tagAttr = "array"
				} else {
					tagAttr = "default"
				}
				row.Values = append(row.Values, []interface{}{di.Name, strconv.Itoa(di.ReplicaN), tagAttr})
			}
		}
	}
	sort.Slice(row.Values, func(i, j int) bool {
		return row.Values[i][0].(string) < row.Values[j][0].(string)
	})
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowMeasurementKeysStatement(stmt *influxql.ShowMeasurementKeysStatement) (models.Rows, error) {
	db, err := e.MetaClient.Database(stmt.Database)
	if err != nil {
		return nil, err
	}
	if stmt.Rp == "" {
		stmt.Rp = db.DefaultRetentionPolicy
	}
	rp, ok := db.RetentionPolicies[stmt.Rp]
	if !ok {
		return nil, errors.New("rp not found")
	}
	mstVersion, ok := rp.MstVersions[stmt.Measurement]
	if !ok {
		return nil, errors.New("measurement not found")
	}
	mst := rp.Measurements[mstVersion.NameWithVersion]

	switch stmt.Name {
	case "PRIMARYKEY":
		if mst.EngineType != config.COLUMNSTORE {
			return nil, errors.New("only support for COLUMNSTORE engine")
		}
		return []*models.Row{getPrimaryKey(mst)}, nil
	case "SORTKEY":
		if mst.EngineType != config.COLUMNSTORE {
			return nil, errors.New("only support for COLUMNSTORE engine")
		}
		return []*models.Row{getSortKey(mst)}, nil
	case "PROPERTY":
		if mst.EngineType != config.COLUMNSTORE {
			return nil, errors.New("only support for COLUMNSTORE engine")
		}
		return []*models.Row{getProperty(mst)}, nil
	case "COMPACT":
		if mst.EngineType != config.COLUMNSTORE {
			return nil, errors.New("only support for COLUMNSTORE engine")
		}
		return []*models.Row{getCompactionType(mst)}, nil
	case "SHARDKEY":
		return []*models.Row{getShardKey(mst)}, nil
	case "ENGINETYPE":
		return []*models.Row{getEngineType(mst)}, nil
	case "INDEXES":
		return []*models.Row{getIndex(mst)}, nil
	case "SCHEMA":
		var rows []*models.Row
		rows = append(rows, getShardKey(mst), getEngineType(mst), getIndex(mst))
		if mst.EngineType == config.COLUMNSTORE {
			rows = append(rows, getPrimaryKey(mst), getSortKey(mst), getCompactionType(mst))
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("%s is not support for this command", stmt.Name)
	}
}

func getIndex(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"INDEXES"}}
	res := make([][]interface{}, len(mst.IndexRelation.Oids))
	for i, id := range mst.IndexRelation.Oids {
		indexName, _ := index.GetIndexNameByType(index.IndexType(id))
		var indexList string
		for _, col := range mst.IndexRelation.IndexList[i].IList {
			indexList += col + ","
		}
		if len(indexList) > 0 {
			indexList = indexList[:len(indexList)-1]
		}
		if id == uint32(index.TimeCluster) {
			indexList = mst.ColStoreInfo.TimeClusterDuration.String()
		}
		res[i] = []interface{}{strings.ToUpper(indexName) + "(" + indexList + ")"}
	}
	row.Values = append(row.Values, res...)
	return row
}

func getEngineType(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"ENGINETYPE"}}
	row.Values = [][]interface{}{{config.EngineType2String[mst.EngineType]}}
	return row
}

func getCompactionType(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"COMPACTION_TYPE"}}
	row.Values = [][]interface{}{{config.CompactionType2Str(mst.ColStoreInfo.CompactionType)}}
	return row
}

func getShardKey(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"SHARD_KEY", "TYPE", "SHARD_GROUP"}}
	res := make([][]interface{}, len(mst.ShardKeys))
	for i := range res {
		res[i] = make([]interface{}, 3)
	}
	for i := range mst.ShardKeys {
		res[i][0] = mst.ShardKeys[i].ShardKey
		res[i][1] = mst.ShardKeys[i].Type
		res[i][2] = mst.ShardKeys[i].ShardGroup
	}
	row.Values = res
	return row
}

func getProperty(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"PROPERTY_KEY", "PROPERTY_VALUE"}}
	keys := make([]interface{}, 0, len(mst.ColStoreInfo.PropertyKey))
	values := make([]interface{}, 0, len(mst.ColStoreInfo.PropertyValue))
	for i := range mst.ColStoreInfo.PropertyKey {
		keys = append(keys, mst.ColStoreInfo.PrimaryKey[i])
		values = append(values, mst.ColStoreInfo.PropertyValue[i])
	}
	row.Values = [][]interface{}{{keys, values}}
	return row
}

func getSortKey(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"SORT_KEY"}}
	res := make([]interface{}, 0, len(mst.ColStoreInfo.SortKey))
	for i := range mst.ColStoreInfo.SortKey {
		res = append(res, mst.ColStoreInfo.SortKey[i])
	}
	row.Values = [][]interface{}{{res}}
	return row
}

func getPrimaryKey(mst *meta2.MeasurementInfo) *models.Row {
	row := &models.Row{Columns: []string{"PRIMARY_KEY"}}
	res := make([]interface{}, 0, len(mst.ColStoreInfo.PrimaryKey))
	for i := range mst.ColStoreInfo.PrimaryKey {
		res = append(res, mst.ColStoreInfo.PrimaryKey[i])
	}
	row.Values = [][]interface{}{{res}}
	return row
}

func (e *StatementExecutor) executeShowGrantsForUserStatement(q *influxql.ShowGrantsForUserStatement) (models.Rows, error) {
	priv, err := e.MetaClient.UserPrivileges(q.Name)
	if err != nil {
		return nil, err
	}

	row := &models.Row{Columns: []string{"database", "privilege"}}
	for d, p := range priv {
		row.Values = append(row.Values, []interface{}{d, p.String()})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowMeasurementsStatement(q *influxql.ShowMeasurementsStatement, ctx *query.ExecutionContext, seq int) error {
	if q.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}
	var mms influxql.Measurements
	if q.Source != nil {
		mms = influxql.Measurements{q.Source.(*influxql.Measurement)}
	}

	measurements, err := e.MetaClient.Measurements(q.Database, mms)
	if err != nil {
		return err
	}

	if q.Offset > 0 {
		if q.Offset >= len(measurements) {
			measurements = nil
		} else {
			measurements = measurements[q.Offset:]
		}

	}

	if q.Limit > 0 {
		if q.Limit < len(measurements) {
			measurements = measurements[:q.Limit]
		}
	}

	if len(measurements) == 0 {
		return ctx.Send(&query.Result{}, seq, nil)
	}

	values := make([][]interface{}, len(measurements))
	for i := range measurements {
		values[i] = []interface{}{measurements[i]}
	}

	return ctx.Send(&query.Result{
		Series: []*models.Row{{
			Name:    "measurements",
			Columns: []string{"name"},
			Values:  values,
		}},
	}, seq, nil)
}

func (e *StatementExecutor) executeShowMeasurementsDetailStatement(stmt *influxql.ShowMeasurementsDetailStatement, ctx *query.ExecutionContext, seq int) error {
	if stmt.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}
	var mms influxql.Measurements
	if stmt.Source != nil {
		mms = influxql.Measurements{stmt.Source.(*influxql.Measurement)}
	}

	measurements, err := e.MetaClient.MatchMeasurements(stmt.Database, mms)
	if err != nil {
		return err
	}

	var blank2Nil = func(s string) string {
		// not return a empty string, return "<nil>" instead
		if s == "" {
			return "<nil>"
		}
		return s
	}

	// sliceFirstColumn gets all shard keys and indexes from the returned models.Row.Values array, then returns a sorted slice.
	// sk: true if the values are shard keys.
	var sliceFirstColumn = func(values [][]interface{}, sk bool) (ret []string) {
		ret = make([]string, 0, len(values))
		if sk {
			for i := range values {
				if values[i] == nil {
					continue
				}
				if currValue, ok := values[i][0].([]string); ok {
					ret = append(ret, currValue...)
				}
			}
		} else {
			for i := range values {
				if values[i] == nil {
					continue
				}
				if currValue, ok := values[i][0].(string); ok {
					ret = append(ret, currValue)
				}
			}
		}
		sort.Strings(ret)
		return
	}
	var emitted = false
	for key, m := range measurements {
		originName := m.OriginName()
		row := &models.Row{Name: originName, Columns: []string{"Detail"}}
		// the values has 9 rows (retention policy, shardKeys, fieldKeys etc.) at most.
		values := make([][]interface{}, 0, 9)
		// key: rpName.mstName
		policyStr := "RETENTION POLICY: " + strings.Split(key, ".")[0]
		values = append(values, []interface{}{policyStr})

		indexStr := strings.Join(sliceFirstColumn(getIndex(m).Values, false), ", ")
		indexStr = "INDEX: " + blank2Nil(indexStr)
		values = append(values, []interface{}{indexStr})

		shardKeyStr := strings.Join(sliceFirstColumn(getShardKey(m).Values, true), ", ")
		shardKeyStr = "SHARD KEY: " + blank2Nil(shardKeyStr)
		values = append(values, []interface{}{shardKeyStr})

		engineTypeStr := config.EngineType2String[m.EngineType]
		engineTypeStr = "ENGINE TYPE: " + engineTypeStr
		values = append(values, []interface{}{engineTypeStr})

		if m.EngineType == config.COLUMNSTORE {
			primaryKeyStr := strings.Join(m.ColStoreInfo.PrimaryKey, ", ")
			primaryKeyStr = "PRIMARY KEY: " + blank2Nil(primaryKeyStr)
			values = append(values, []interface{}{primaryKeyStr})

			sortKeyStr := strings.Join(m.ColStoreInfo.SortKey, ", ")
			sortKeyStr = "SORT KEY: " + blank2Nil(sortKeyStr)
			values = append(values, []interface{}{sortKeyStr})

			compactionTypeStr := config.CompactionType2Str(m.ColStoreInfo.CompactionType)
			compactionTypeStr = "COMPACTION_TYPE: " + blank2Nil(compactionTypeStr)
			values = append(values, []interface{}{compactionTypeStr})
		}

		tagKeysMap := make(map[string]map[string]struct{}, 1)
		tagKeysMap[m.Name] = make(map[string]struct{}, m.TagKeysTotal())
		m.MatchTagKeys(nil, tagKeysMap)
		tagStrs := make([]string, 0, m.TagKeysTotal())
		for k := range tagKeysMap[m.Name] {
			tagStrs = append(tagStrs, k)
		}
		sort.Strings(tagStrs)
		var tagStr string
		// cut if over-length
		tagMaxLen := 10
		if len(tagStrs) > tagMaxLen {
			tagStr = strings.Join(tagStrs[:tagMaxLen], ", ")
			tagStr += "..."
		} else {
			tagStr = strings.Join(tagStrs, ", ")
		}
		tagStr = "TAG KEYS: " + blank2Nil(tagStr)
		values = append(values, []interface{}{tagStr})

		fieldKeysMap := make(map[string]map[string]int32, 1)
		fieldKeysMap[originName] = make(map[string]int32)
		m.FieldKeys(fieldKeysMap)
		fieldStrs := make([]string, 0, len(fieldKeysMap[originName]))
		for field, fieldType := range fieldKeysMap[originName] {
			fieldStrs = append(fieldStrs, fmt.Sprintf("%s(%s)", field, influx.FieldTypeString(fieldType)))
		}
		sort.Strings(fieldStrs)
		var fieldStr string
		// cut if over-length
		fieldMaxLen := 10
		if len(fieldStrs) > fieldMaxLen {
			fieldStr = strings.Join(fieldStrs[:fieldMaxLen], ", ")
			fieldStr += "..."
		} else {
			fieldStr = strings.Join(fieldStrs, ", ")
		}
		fieldStr = "FIELD KEYS: " + blank2Nil(fieldStr)
		values = append(values, []interface{}{fieldStr})

		row.Values = values
		if err := ctx.Send(&query.Result{Series: []*models.Row{row}}, seq, nil); err != nil {
			return err
		}
		emitted = true
	}
	if !emitted {
		return ctx.Send(&query.Result{}, seq, nil)
	}
	return nil
}

func (e *StatementExecutor) executeShowMeasurementCardinalityStatement(stmt *influxql.ShowMeasurementCardinalityStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, coordinator.ErrDatabaseNameRequired
	}

	var mms influxql.Measurements
	if stmt.Sources != nil {
		mms = stmt.Sources.Measurements()
	}

	measurements, err := e.MetaClient.MatchMeasurements(stmt.Database, mms)
	if err != nil {
		return nil, err
	}
	return []*models.Row{{
		Columns: []string{"count"},
		Values:  [][]interface{}{{len(measurements)}},
	}}, nil
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement) (models.Rows, error) {
	if q.Database == "" {
		return nil, coordinator.ErrDatabaseNameRequired
	}

	return e.MetaClient.ShowRetentionPolicies(q.Database)
}

func (e *StatementExecutor) executeShowContinuousQueriesStatement(q *influxql.ShowContinuousQueriesStatement) (models.Rows, error) {
	return e.MetaClient.ShowContinuousQueries()
}

func (e *StatementExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement) (models.Rows, error) {
	if stmt.GetMstInfo() == nil {
		return e.MetaClient.ShowShards("", "", ""), nil
	}
	return e.MetaClient.ShowShards(stmt.GetDBName(), stmt.GetRPName(), stmt.GetMstName()), nil
}

func (e *StatementExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement) (models.Rows, error) {
	return e.MetaClient.ShowShardGroups(), nil
}

func (e *StatementExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement) (models.Rows, error) {
	if !config.GetSubscriptionEnable() {
		return nil, errors.New("subscription is not enabled")
	}
	return e.MetaClient.ShowSubscriptions(), nil
}

func (e *StatementExecutor) FieldKeys(database string, measurements influxql.Measurements) (netstorage.TableColumnKeys, error) {
	fieldKeysMap, err := e.MetaClient.FieldKeys(database, measurements)
	if err != nil {
		return nil, err
	}

	var fieldKeys netstorage.TableColumnKeys
	for mstName := range fieldKeysMap {
		fk := &netstorage.ColumnKeys{Name: mstName}
		for k := range fieldKeysMap[mstName] {
			fk.Keys = append(fk.Keys, meta.FieldKey{Field: k, FieldType: fieldKeysMap[mstName][k]})
		}
		sort.Sort(meta.FieldKeys(fk.Keys))
		fieldKeys = append(fieldKeys, *fk)
	}

	sort.Stable(fieldKeys)
	return fieldKeys, nil
}

func (e *StatementExecutor) executeShowFieldKeys(q *influxql.ShowFieldKeysStatement, ctx *query.ExecutionContext, seq int) error {
	if q.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}

	fieldKeys, err := e.FieldKeys(q.Database, q.Sources.Measurements())
	if err != nil {
		return err
	}
	emitted := false
	for i := range fieldKeys {
		if len(fieldKeys[i].Keys) == 0 {
			continue
		}

		row := &models.Row{
			Name:    fieldKeys[i].Name,
			Columns: []string{"fieldKey", "fieldType"},
			Values:  make([][]interface{}, len(fieldKeys[i].Keys)),
		}
		for j, key := range fieldKeys[i].Keys {
			row.Values[j] = []interface{}{key.Field, influx.FieldTypeString(key.FieldType)}
		}

		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}, seq, nil); err != nil {
			return err
		}
		emitted = true

	}
	if !emitted {
		return ctx.Send(&query.Result{}, seq, nil)
	}
	return nil
}

func (e *StatementExecutor) executeShowFieldKeyCardinality(q *influxql.ShowFieldKeyCardinalityStatement, ctx *query.ExecutionContext, seq int) error {
	if q.Condition != nil {
		return meta2.ErrUnsupportCommand
	}
	if q.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}

	fieldKeys, err := e.FieldKeys(q.Database, q.Sources.Measurements())
	if err != nil {
		return err
	}
	emitted := false
	for i := range fieldKeys {
		if len(fieldKeys[i].Keys) == 0 {
			continue
		}
		row := &models.Row{
			Name:    fieldKeys[i].Name,
			Columns: []string{"count"},
			Values:  [][]interface{}{{len(fieldKeys[i].Keys)}},
		}
		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}, seq, nil); err != nil {
			return err
		}
		emitted = true
	}
	if !emitted {
		return ctx.Send(&query.Result{}, seq, nil)
	}
	return nil
}

func (e *StatementExecutor) TagKeys(database string, measurements influxql.Measurements, cond influxql.Expr) (netstorage.TableTagKeys, error) {
	tagKeysMap, err := e.MetaClient.QueryTagKeys(database, measurements, cond)
	if err != nil {
		return nil, err
	}
	var tagKeys netstorage.TableTagKeys
	for nameWithVer := range tagKeysMap {
		mstName := influx.GetOriginMstName(nameWithVer)
		tk := &netstorage.TagKeys{Name: mstName}
		for k := range tagKeysMap[nameWithVer] {
			tk.Keys = append(tk.Keys, k)
		}
		sort.Strings(tk.Keys)
		tagKeys = append(tagKeys, *tk)
	}
	sort.Stable(tagKeys)
	return tagKeys, nil
}

func (e *StatementExecutor) executeShowTagKeys(q *influxql.ShowTagKeysStatement, ctx *query.ExecutionContext, seq int) error {
	var tagKeys netstorage.TableTagKeys
	var err error
	if influxql.OnlyHaveTimeCond(q.Condition) {
		tagKeys, err = e.TagKeys(q.Database, q.Sources.Measurements(), q.Condition)
	} else {
		exec := coordinator.NewShowTagKeysExecutor(e.StmtExecLogger, e.MetaClient, e.MetaExecutor, e.NetStorage)
		tagKeys, err = exec.Execute(q)
	}

	if err != nil {
		return err
	}
	emitted := false
	for _, m := range tagKeys {
		keys := m.Keys

		if q.Offset > 0 {
			if q.Offset >= len(keys) {
				keys = nil
			} else {
				keys = keys[q.Offset:]
			}
		}
		if q.Limit > 0 && q.Limit < len(keys) {
			keys = keys[:q.Limit]
		}

		if len(keys) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Name,
			Columns: []string{"tagKey"},
			Values:  make([][]interface{}, len(keys)),
		}
		for i, key := range keys {
			row.Values[i] = []interface{}{key}
		}

		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}, seq, nil); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{}, seq, nil)
	}
	return nil

}

func (e *StatementExecutor) executeShowTagKeyCardinality(q *influxql.ShowTagKeyCardinalityStatement, ctx *query.ExecutionContext, seq int) error {
	if q.Condition != nil {
		return meta2.ErrUnsupportCommand
	}

	if q.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}

	tagKeys, err := e.TagKeys(q.Database, q.Sources.Measurements(), q.Condition)
	if err != nil {
		return err
	}
	emitted := false
	for i := range tagKeys {
		if len(tagKeys[i].Keys) == 0 {
			continue
		}
		row := &models.Row{
			Name:    tagKeys[i].Name,
			Columns: []string{"count"},
			Values:  [][]interface{}{{len(tagKeys[i].Keys)}},
		}
		if err := ctx.Send(&query.Result{
			Series: []*models.Row{row},
		}, seq, nil); err != nil {
			return err
		}
		emitted = true
	}
	if !emitted {
		return ctx.Send(&query.Result{}, seq, nil)
	}
	return nil
}

func (e *StatementExecutor) executeShowTagValues(stmt *influxql.ShowTagValuesStatement) (models.Rows, error) {
	exec := coordinator.NewShowTagValuesExecutor(e.StmtExecLogger, e.MetaClient, e.MetaExecutor, e.NetStorage)
	return exec.Execute(stmt)
}

func (e *StatementExecutor) executeShowTagValuesCardinality(stmt *influxql.ShowTagValuesCardinalityStatement) (models.Rows, error) {
	exec := coordinator.NewShowTagValuesExecutor(e.StmtExecLogger, e.MetaClient, e.MetaExecutor, e.NetStorage)

	newStmt := &influxql.ShowTagValuesStatement{
		Database:        stmt.Database,
		Sources:         stmt.Sources,
		Op:              stmt.Op,
		TagKeyExpr:      stmt.TagKeyExpr,
		TagKeyCondition: stmt.TagKeyCondition,
		Condition:       stmt.Condition,
		SortFields:      nil,
		Limit:           0,
		Offset:          0,
	}

	exec.Cardinality(stmt.Dimensions)
	return exec.Execute(newStmt)
}

func (e *StatementExecutor) executeShowSeries(q *influxql.ShowSeriesStatement, ctx *query.ExecutionContext, seq int) error {
	mis, err := e.MetaClient.MatchMeasurements(q.Database, q.Sources.Measurements())
	if err != nil {
		return err
	}
	names := make([]string, 0, len(mis))
	for _, m := range mis {
		names = append(names, m.Name)
	}

	var series []string
	lock := new(sync.Mutex)

	exact := influxql.IsExactStatisticQueryForDDL(q)
	err = e.MetaExecutor.EachDBNodes(q.Database, func(nodeID uint64, pts []uint32) error {
		arr, err := e.NetStorage.ShowSeries(nodeID, q.Database, pts, names, q.Condition, exact)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			series = series[:0] // if execute command failed reset res
		} else {
			series = append(series, arr...)
		}
		return err
	})
	if err != nil {
		e.StmtExecLogger.Error("failed to show series", zap.Error(err))
		return err
	}

	sort.Strings(series)
	series = limitStringSlice(series, q.Offset, q.Limit)

	if len(series) == 0 {
		return nil
	}
	row := &models.Row{
		Name:    "",
		Columns: []string{"key"},
		Values:  make([][]interface{}, 0, len(series)),
	}

	for _, item := range series {
		row.Values = append(row.Values, []interface{}{item})
	}

	return ctx.Send(&query.Result{
		Series: []*models.Row{row},
	}, seq, nil)
}

func (e *StatementExecutor) executeShowSeriesCardinality(stmt *influxql.ShowSeriesCardinalityStatement) (models.Rows, error) {
	stime := time.Now()
	mis, err := e.MetaClient.MatchMeasurements(stmt.Database, stmt.Sources.Measurements())
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(mis))
	for _, m := range mis {
		names = append(names, m.Name)
	}
	e.StmtExecLogger.Info("match measurement cost", zap.Duration("duration", time.Since(stime)))
	if !stmt.Exact {
		if stmt.Condition != nil || len(stmt.Sources) > 0 {
			return e.showSeriesCardinalityWithCondition(stmt, names)
		} else {
			return e.showSeriesCardinality(stmt, names)
		}
	}

	return e.showSeriesExactCardinality(stmt, names)
}

func (e *StatementExecutor) showSeriesCardinality(stmt *influxql.ShowSeriesCardinalityStatement, names []string) ([]*models.Row, error) {
	stime := time.Now()
	var ret meta2.CardinalityInfos
	lock := new(sync.Mutex)
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32) error {
		mstCardinality, err := e.NetStorage.SeriesCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			ret = ret[:0]
			return err
		}
		for i := range mstCardinality {
			ret = append(ret, mstCardinality[i].CardinalityInfos...)
		}

		return err
	})
	if err != nil {
		e.StmtExecLogger.Error("failed to show series cardinality", zap.Error(err))
		return nil, err
	}

	e.StmtExecLogger.Info("store show series cardinality", zap.Duration("cost", time.Since(stime)))
	ret.SortAndMerge()
	rows := make([]*models.Row, 0, len(ret))
	for i := range ret {
		if ret[i].TimeRange.StartTime.IsZero() {
			continue
		}
		rows = append(rows, &models.Row{
			Columns: []string{"startTime", "endTime", "count"},
			Values: [][]interface{}{{ret[i].TimeRange.StartTime.UTC().Format(time.RFC3339),
				ret[i].TimeRange.EndTime.UTC().Format(time.RFC3339),
				ret[i].Cardinality}},
		})
	}
	e.StmtExecLogger.Info("total showSeries cost", zap.Duration("duration", time.Since(stime)))
	return rows, nil
}

func (e *StatementExecutor) showSeriesCardinalityWithCondition(stmt *influxql.ShowSeriesCardinalityStatement, names []string) ([]*models.Row, error) {
	stime := time.Now()
	ret := make(map[string]meta2.CardinalityInfos)
	lock := new(sync.Mutex)
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32) error {
		mstCardinality, err := e.NetStorage.SeriesCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			ret = make(map[string]meta2.CardinalityInfos)
			return err
		}
		for i := range mstCardinality {
			if _, ok := ret[mstCardinality[i].Name]; !ok {
				ret[mstCardinality[i].Name] = mstCardinality[i].CardinalityInfos
				continue
			}
			ret[mstCardinality[i].Name] = append(ret[mstCardinality[i].Name], mstCardinality[i].CardinalityInfos...)
		}
		return nil
	})
	if err != nil {
		e.StmtExecLogger.Error("fail to show series cardinality with condition", zap.Error(err))
		return nil, err
	}
	e.StmtExecLogger.Info("store show series cardinality with condition", zap.Duration("cost", time.Since(stime)))
	rows := make([]*models.Row, 0, len(ret))
	for mst, cardinalityInfos := range ret {
		cardinalityInfos.SortAndMerge()
		for i := range cardinalityInfos {
			if cardinalityInfos[i].TimeRange.StartTime.IsZero() {
				continue
			}
			rows = append(rows, &models.Row{
				Name:    mst,
				Columns: []string{"startTime", "endTime", "count"},
				Values: [][]interface{}{{cardinalityInfos[i].TimeRange.StartTime.UTC().Format(time.RFC3339),
					cardinalityInfos[i].TimeRange.EndTime.UTC().Format(time.RFC3339),
					cardinalityInfos[i].Cardinality}},
			})
		}
	}
	e.StmtExecLogger.Info("total showSeries with condition cost", zap.Duration("duration", time.Since(stime)))

	return rows, nil
}

func (e *StatementExecutor) showSeriesExactCardinality(stmt *influxql.ShowSeriesCardinalityStatement, names []string) ([]*models.Row, error) {
	stime := time.Now()
	ret := make(map[string]uint64)
	lock := new(sync.Mutex)
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32) error {
		tmp, err := e.NetStorage.SeriesExactCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			ret = make(map[string]uint64)
			return err
		}
		for name, n := range tmp {
			if _, ok := ret[name]; !ok {
				ret[name] = n
				continue
			}
			ret[name] += n
		}
		return nil
	})
	if err != nil {
		e.StmtExecLogger.Error("failed to show series exact cardinality", zap.Error(err))
		return nil, err
	}
	e.StmtExecLogger.Info("total show series exact cardinality cost", zap.Duration("duration", time.Since(stime)))
	rows := make([]*models.Row, 0, len(ret))
	for name, n := range ret {
		rows = append(rows, &models.Row{
			Name:    name,
			Columns: []string{"count"},
			Values:  [][]interface{}{{n}},
		})
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement) (models.Rows, error) {
	row := &models.Row{Columns: []string{"user", "admin", "rwuser"}}
	for _, ui := range e.MetaClient.Users() {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin, ui.Rwuser})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowQueriesStatement() (models.Rows, error) {
	nodes, err := e.MetaClient.DataNodes()
	if err != nil {
		return nil, err
	}

	resMap := make(map[uint64]*combinedQueryExeInfo)
	infosOnAllStore := make([][]*netstorage.QueryExeInfo, len(nodes))

	// Concurrent access to all store nodes.
	wg := sync.WaitGroup{}
	var mu sync.Mutex
	for i, node := range nodes {
		wg.Add(1)
		go func(index int, nodeID uint64) {
			defer wg.Done()
			infos := e.getQueryExeInfoOnNode(nodeID)
			mu.Lock()
			defer mu.Unlock()
			infosOnAllStore[index] = infos
		}(i, node.ID)
	}
	wg.Wait()

	// Combine all results from all store nodes into resMap.
	for i, infos := range infosOnAllStore {
		combineQueryExeInfos(resMap, infos, nodes[i].Host)
	}

	// Sort the res by duration to beautify the output.
	sortedResult := make(combinedInfos, 0, len(resMap))
	for _, val := range resMap {
		sortedResult = append(sortedResult, val)
	}
	sort.Sort(sortedResult)

	row := models.Row{Columns: []string{"qid", "query", "database", "duration", "status", "host"}}
	values := make([][]interface{}, 0, len(resMap))

	// Generate output row for every query
	for _, cmbInfo := range sortedResult {
		switch cmbInfo.getCombinedRunState() {
		case allKilled:
			continue
		case partiallyKilled:
			// If this query was killed on a part of store nodes, split hosts to 2 part of "killed" and "running"
			values = append(values, cmbInfo.toOutputRow(len(row.Columns), true))
		case allRunning:
		}
		values = append(values, cmbInfo.toOutputRow(len(row.Columns), false))
	}
	row.Values = values
	return models.Rows{&row}, nil
}

func (e *StatementExecutor) getQueryExeInfoOnNode(nodeID uint64) []*netstorage.QueryExeInfo {
	exeInfos, err := e.NetStorage.GetQueriesOnNode(nodeID)
	if err != nil {
		return make([]*netstorage.QueryExeInfo, 0)
	}
	return exeInfos
}

// combineQueryExeInfos combines queryExeInfo from different store nodes by QueryID.
func combineQueryExeInfos(dstMap map[uint64]*combinedQueryExeInfo, exeInfosOnStore []*netstorage.QueryExeInfo, host string) {
	for _, info := range exeInfosOnStore {
		// If a query in dstMap, update its killed,host and duration
		if cmbInfo, ok := dstMap[info.QueryID]; ok {
			if cmbInfo.stmt == info.Stmt {
				cmbInfo.updateBeginTime(info.BeginTime)
				cmbInfo.updateHosts(host, info.RunState)
				continue
			}

			// If a query whose qid is 1 has been sent to the store and is being queried,
			// the SQL node restarts, and the new query qid starts from 1.
			// In this case, the old query whose qid is 1 needs to be filtered out.
			if info.BeginTime <= cmbInfo.beginTime {
				continue
			}
		}
		// Create a new cmbInfo
		newCmbInfo := &combinedQueryExeInfo{
			qid:          info.QueryID,
			stmt:         info.Stmt,
			database:     info.Database,
			beginTime:    info.BeginTime,
			runningHosts: make(map[string]struct{}),
			killedHosts:  make(map[string]struct{}),
		}
		newCmbInfo.updateHosts(host, info.RunState)
		dstMap[info.QueryID] = newCmbInfo
	}
}

func (e *StatementExecutor) executeKillQuery(stmt *influxql.KillQueryStatement) error {
	if stmt.Host != "" {
		return meta2.ErrUnsupportCommand
	}
	nodes, err := e.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	notFoundCount := 0

	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(dataNode meta2.DataNode) {
			defer wg.Done()
			if err = e.NetStorage.KillQueryOnNode(dataNode.ID, stmt.QueryID); err != nil {
				var wrapErr *errno.Error
				if errors.As(err, &wrapErr) && errno.Equal(wrapErr, errno.ErrQueryNotFound) {
					notFoundCount++
					return
				}
			}
		}(n)
	}
	wg.Wait()

	if notFoundCount == len(nodes) {
		return errno.NewError(errno.ErrQueryNotFound, stmt.QueryID)
	}
	return nil
}

func (e *StatementExecutor) Statistics(buffer []byte) ([]byte, error) {
	// Statistics() period is 10
	// do db stats period 1 minute
	if dbStatCount%30 != 0 {
		buffer, _ = statistics.CollectDatabaseStatistics(buffer)
		dbStatCount++
		if dbStatCount == 30 {
			dbStatCount = 0
		}
		return buffer, nil
	}
	databases := e.MetaClient.Databases()
	var numHistorySeries uint64
	var numRecentSeries uint64

	for _, db := range databases {
		mis, err := e.MetaClient.MatchMeasurements(db.Name, nil)
		if err != nil {
			return nil, err
		}
		stmt := &influxql.ShowSeriesCardinalityStatement{
			Database: db.Name,
			Exact:    false,
		}
		rows, err := e.executeShowSeriesCardinality(stmt)
		if err != nil {
			return nil, err
		}

		if len(rows) > 1 {
			if len(rows[len(rows)-2].Columns) == 3 && rows[len(rows)-2].Columns[2] == "count" && rows[len(rows)-1].Columns[2] == "count" {
				numHistorySeries = rows[len(rows)-2].Values[0][2].(uint64)
				numRecentSeries = rows[len(rows)-1].Values[0][2].(uint64)
			}
		} else if len(rows) == 1 {
			numHistorySeries = 0
			if len(rows[len(rows)-1].Columns) == 3 && rows[len(rows)-1].Columns[2] == "count" {
				numRecentSeries = rows[len(rows)-1].Values[0][2].(uint64)
			}
		} else {
			numHistorySeries = 0
			numRecentSeries = 0
		}

		statistics.DatabaseStat.Mu.Lock()
		statistics.DatabaseStat.SetMeasurementsNum(db.Name, int64(len(mis)))
		statistics.DatabaseStat.SetSeriesNum(db.Name, int64(numRecentSeries), int64(numHistorySeries))
		statistics.DatabaseStat.Mu.Unlock()
	}

	buffer, _ = statistics.CollectDatabaseStatistics(buffer)
	dbStatCount++

	return buffer, nil
}

// NormalizeStatement adds a default database and policy to the measurements in statement.
// Parameter defaultRetentionPolicy can be "".
func (e *StatementExecutor) NormalizeStatement(stmt influxql.Statement, defaultDatabase, defaultRetentionPolicy string) (err error) {
	influxql.WalkFunc(stmt, func(node influxql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *influxql.ShowRetentionPoliciesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementsStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.DropMeasurementStatement:
			if node.RpName == "" {
				node.RpName = defaultRetentionPolicy
			}
		case *influxql.ShowMeasurementsDetailStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowFieldKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowFieldKeyCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagKeyCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagValuesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagValuesCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowSeriesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowSeriesCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.CreateMeasurementStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.AlterShardKeyStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.CreateDownSampleStatement:
			if node.DbName == "" {
				node.DbName = defaultDatabase
			}
		case *influxql.DropDownSampleStatement:
			if node.DbName == "" {
				node.DbName = defaultDatabase
			}
		case *influxql.ShowDownSampleStatement:
			if node.DbName == "" {
				node.DbName = defaultDatabase
			}
		case *influxql.CreateStreamStatement:
			err = e.normalizeMeasurement(node.Target.Measurement, defaultDatabase, defaultRetentionPolicy)
			if err != nil {
				return
			}
			err = e.NormalizeStatement(node.Query, defaultDatabase, defaultRetentionPolicy)
		case *influxql.Measurement:
			switch stmt.(type) {
			case *influxql.DropSeriesStatement, *influxql.DeleteSeriesStatement:
				// DB and RP not supported by these statements so don't rewrite into invalid
				// statements
			default:
				err = e.normalizeMeasurement(node, defaultDatabase, defaultRetentionPolicy)
			}
		case *influxql.ShowMeasurementKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		}
	})
	return
}

func (e *StatementExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase, defaultRetentionPolicy string) error {
	// Targets (measurements in an INTO clause) can have blank names, which means it will be
	// the same as the measurement name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.SystemIterator == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return coordinator.ErrDatabaseNameRequired
	}

	// Find database.
	di, err := e.MetaClient.Database(m.Database)
	if err != nil {
		return err
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if defaultRetentionPolicy != "" {
			m.RetentionPolicy = defaultRetentionPolicy
		} else if di.DefaultRetentionPolicy != "" {
			m.RetentionPolicy = di.DefaultRetentionPolicy
		} else {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
	}
	return nil
}

func (e *StatementExecutor) executePrepareSnapshotStatement(q *influxql.PrepareSnapshotStatement, ctx *query.ExecutionContext) error {
	return meta2.ErrUnsupportCommand
}

func (e *StatementExecutor) executeEndPrepareSnapshotStatement(q *influxql.EndPrepareSnapshotStatement, ctx *query.ExecutionContext) error {
	return meta2.ErrUnsupportCommand
}

func (e *StatementExecutor) executeGetRuntimeInfoStatement(q *influxql.GetRuntimeInfoStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	return nil, meta2.ErrUnsupportCommand
}

func (e *StatementExecutor) executeCreateStreamStatement(stmt *influxql.CreateStreamStatement, ctx *query.ExecutionContext) error {
	selectStmt, ok := stmt.Query.(*influxql.SelectStatement)
	if !ok {
		return errors.New("create stream query must be select statement")
	}
	selectStmt.IsCreateStream = true
	var isSelectAll bool // is select *
	if len(selectStmt.Fields) == 1 {
		field, ok := selectStmt.Fields[0].Expr.(*influxql.Wildcard)
		// select *, not select *::tag and not select *::field
		if ok && field.Type != influxql.TAG && field.Type != influxql.FIELD {
			isSelectAll = true
		}
	}

	mstInfo := stmt.Target.Measurement
	proxy := newRowChanProxy()
	opt := e.GetOptions(ctx.ExecutionOptions, proxy.rc)
	s, er := query.Prepare(selectStmt, e.ShardMapper, opt)
	if er != nil {
		return er
	}
	selectStmt = s.Statement()

	// Check sources. Only one source can be queried, and the source must be *influxql.Measurement.
	srcMst, err := stmt.CheckSource(selectStmt.Sources)
	if err != nil {
		return err
	}

	// validate create stream statement
	srcInfo, err := e.MetaClient.Measurement(srcMst.Database, srcMst.RetentionPolicy, srcMst.Name)
	if err != nil {
		return err
	}
	var srcTagCount int
	srcInfo.Schema.RangeTypCall(func(s string, i int32) {
		if i == influx.Field_Type_Tag {
			srcTagCount++
		}
	})
	if err = stmt.Check(selectStmt, streamSupportMap, srcTagCount); err != nil {
		return err
	}
	if mstInfo.Name == srcMst.Name {
		return errors.New("the source measurement cannot be the same as the destination measurement")
	}

	var info *meta2.StreamInfo
	if interval, err := selectStmt.GroupByInterval(); err == nil && interval == 0 && len(selectStmt.Dimensions) == 0 {
		// no group by and select all tags, like: select * from mst where field >= 10
		info = meta2.NewStreamInfoNoCall(stmt, selectStmt, isSelectAll)
	} else {
		info = meta2.NewStreamInfo(stmt.Name, stmt.Delay, srcMst, &meta2.StreamMeasurementInfo{
			Name:            stmt.Target.Measurement.Name,
			Database:        stmt.Target.Measurement.Database,
			RetentionPolicy: stmt.Target.Measurement.RetentionPolicy,
		}, selectStmt)
	}

	err = e.MetaClient.CreateStreamMeasurement(info, srcMst, mstInfo, selectStmt)
	if err != nil {
		return err
	}
	return e.MetaClient.CreateStreamPolicy(info)
}

func (e *StatementExecutor) executeShowStreamsStatement(stmt *influxql.ShowStreamsStatement) (models.Rows, error) {
	var showAll bool
	if stmt.Database == "" {
		showAll = true
	}
	return e.MetaClient.ShowStreams(stmt.Database, showAll)
}

func (e *StatementExecutor) executeDropStream(stmt *influxql.DropStreamsStatement) error {
	return e.MetaClient.DropStream(stmt.Name)
}

func (e *StatementExecutor) executeShowConfigs(stmt *influxql.ShowConfigsStatement) (models.Rows, error) {
	row := &models.Row{Columns: []string{"component", "instance", "name", "value"}}
	e.SqlConfigs[loggingLevel] = logger.Alevel

	keys := sortConfigs(e.SqlConfigs)

	for _, key := range keys {
		row.Values = append(row.Values, []interface{}{sqlConfig, e.Hostname, key, e.SqlConfigs[key]})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeSetConfig(stmt *influxql.SetConfigStatement) error {
	e.StmtExecLogger.Info("change config by ddl", zap.String("component", stmt.Component), zap.String("key", stmt.Key), zap.Any("value", stmt.Value))
	switch stmt.Component {
	case sqlConfig:
		switch stmt.Key {
		case loggingLevel:
			if levelString, ok := stmt.Value.(string); ok {
				return logger.SetLevel(levelString)
			}
			return fmt.Errorf("illegal type of logging level input")
		default:
		}
	default:
	}
	return fmt.Errorf("unsupported config command")
}

func sortConfigs(configs map[string]interface{}) []string {
	keys := make([]string, 0, len(configs))
	for key := range configs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (e *StatementExecutor) executeShowCluster(stmt *influxql.ShowClusterStatement) (models.Rows, error) {
	if stmt.NodeID != 0 || stmt.NodeType != "" {
		return e.executeShowClusterWithCondition(stmt)
	}
	return e.MetaClient.ShowCluster("", 0)
}

func (e *StatementExecutor) executeShowClusterWithCondition(stmt *influxql.ShowClusterStatement) (models.Rows, error) {
	ID := uint64(stmt.NodeID)
	if stmt.NodeType != "data" && stmt.NodeType != "meta" && stmt.NodeType != "" {
		e.StmtExecLogger.Error("fail to show cluster with condition")
		return nil, errno.NewError(errno.InValidNodeType, stmt.NodeType)
	}
	return e.MetaClient.ShowClusterWithCondition(stmt.NodeType, ID)
}

func (e StatementExecutor) executeWithSelectStatement(stmt *influxql.WithSelectStatement) error {
	return nil
}

type ByteStringSlice [][]byte

func (s ByteStringSlice) Len() int {
	return len(s)
}

func (s ByteStringSlice) Swap(i, j int) {
	ii := string(s[i])
	jj := string(s[j])

	s[i], s[j] = []byte(jj), []byte(ii)
}

func (s ByteStringSlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

type TagKeysSlice []netstorage.TagKeys

func (a TagKeysSlice) Len() int           { return len(a) }
func (a TagKeysSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TagKeysSlice) Less(i, j int) bool { return a[i].Name < a[j].Name }

func MergeMeasurementsNames(otherNodeNamesMap map[uint64]*netstorage.ExecuteStatementMessage) (error, [][]byte) {
	retString := make(map[string]bool, len(otherNodeNamesMap))
	clusterNames := make([][]byte, 0, len(otherNodeNamesMap))
	for _, msg := range otherNodeNamesMap {
		var names [][]byte
		if len(msg.Result) == 0 {
			continue
		}
		err := json.Unmarshal(msg.Result, &names)
		if err != nil {
			return fmt.Errorf("Unmarshal %s json bytes failed: %s\n", msg.StatementType, err), nil
		}

		if len(names) > 0 {
			clusterNames = append(clusterNames, names...)
		}
	}

	for _, name := range clusterNames {
		retString[string(name)] = true
	}

	var uniqueStrings ByteStringSlice
	for k, _ := range retString {
		uniqueStrings = append(uniqueStrings, []byte(k))
	}

	sort.Stable(uniqueStrings)
	return nil, uniqueStrings
}

func MergeTagKeys(otherNodeTagKeysMap *map[uint64][]netstorage.TagKeys) (error, []netstorage.TagKeys) {

	uniqueMap := make(map[string]set.Set[string])

	for _, nodeTagKeys := range *otherNodeTagKeysMap {
		for _, tagKey := range nodeTagKeys {
			s := set.NewSet[string]()
			for _, v := range tagKey.Keys {
				s.Add(v)
			}
			_, ok := uniqueMap[tagKey.Name]
			if ok {
				uniqueMap[tagKey.Name] = uniqueMap[tagKey.Name].Union(s)
			} else {
				uniqueMap[tagKey.Name] = s
			}
		}
	}

	var clusterTagKeys TagKeysSlice
	for k, v := range uniqueMap {
		kSlice := v.ToSlice()
		newSlice := make([]string, len(kSlice))
		for i, data := range kSlice {
			newSlice[i] = data
		}
		sort.Strings(newSlice)
		tk := netstorage.TagKeys{Name: k, Keys: newSlice}
		clusterTagKeys = append(clusterTagKeys, tk)
	}

	sort.Stable(clusterTagKeys)
	return nil, clusterTagKeys
}

type KeyValues []netstorage.TagSet

func (a KeyValues) Len() int { return len(a) }

// Swap implements sort.Interface.
func (a KeyValues) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface. Keys are compared before values.
func (a KeyValues) Less(i, j int) bool {
	ki, kj := a[i].Key, a[j].Key
	if ki == kj {
		return a[i].Value < a[j].Value
	}
	return ki < kj
}

func MergeTagValues(otherNodeTagKeysMap *map[uint64][]netstorage.TableTagSets) (error, []netstorage.TableTagSets) {
	uniqueMap := make(map[string]set.Set[netstorage.TagSet])
	for _, nodeTagValues := range *otherNodeTagKeysMap {
		for _, tagValues := range nodeTagValues {
			s := set.NewSet[netstorage.TagSet]()
			for _, v := range tagValues.Values {
				s.Add(v)
			}
			_, ok := uniqueMap[tagValues.Name]
			if ok {
				uniqueMap[tagValues.Name] = uniqueMap[tagValues.Name].Union(s)
			} else {
				uniqueMap[tagValues.Name] = s
			}
		}
	}

	var clusterTagValues coordinator.TagValuesSlice
	for k, v := range uniqueMap {
		vSlice := v.ToSlice()
		newSlice := make(netstorage.TagSets, len(vSlice))
		for i, data := range vSlice {
			newSlice[i] = data
		}
		sort.Stable(newSlice)
		tk := netstorage.TableTagSets{Name: k, Values: newSlice}
		clusterTagValues = append(clusterTagValues, tk)
	}

	sort.Stable(clusterTagValues)
	return nil, clusterTagValues
}

func GetStatementMessageType(OtherNodesMsg map[uint64]*netstorage.ExecuteStatementMessage) string {
	for _, nodeMsg := range OtherNodesMsg {
		return nodeMsg.StatementType
	}

	return ""
}

func MergeAllNodeMessage(OtherNodesMsg map[uint64]*netstorage.ExecuteStatementMessage) (error, interface{}) {
	stmtType := GetStatementMessageType(OtherNodesMsg)
	switch stmtType {
	case netstorage.ShowMeasurementsStatement:
		return MergeMeasurementsNames(OtherNodesMsg)
	case netstorage.ShowTagKeysStatement:
		clusterTagKeysMap := make(map[uint64][]netstorage.TagKeys)
		for i, nodeMsg := range OtherNodesMsg {
			var tagKeys []netstorage.TagKeys
			err := json.Unmarshal(nodeMsg.Result, &tagKeys)
			if err != nil {
				return err, nil
			}
			clusterTagKeysMap[i] = tagKeys
		}
		return MergeTagKeys(&clusterTagKeysMap)
	case netstorage.ShowTagValuesStatement:
		clusterTagValuesMap := make(map[uint64][]netstorage.TableTagSets)
		for i, nodeMsg := range OtherNodesMsg {
			var tagValues []netstorage.TableTagSets
			err := json.Unmarshal(nodeMsg.Result, &tagValues)
			if err != nil {
				return err, nil
			}
			clusterTagValuesMap[i] = tagValues
		}
		return MergeTagValues(&clusterTagValuesMap)
	case netstorage.ShowSeriesCardinalityStatement:
		return CalcCardinality(OtherNodesMsg)
	case netstorage.ShowMeasurementCardinalityStatement:
		return CalcCardinality(OtherNodesMsg)
	default:
		return fmt.Errorf("ExecuteStatement type[%s] not surpport", stmtType), nil
	}
}

func CalcCardinality(OtherNodesMsg map[uint64]*netstorage.ExecuteStatementMessage) (error, int64) {
	var nl int64
	var clusterCardinality int64
	clusterCardinality = 0
	for _, msg := range OtherNodesMsg {
		var n int64
		err := json.Unmarshal(msg.Result, &n)
		if err != nil {
			return err, 0
		}
		clusterCardinality += n
	}
	return nil, clusterCardinality + nl
}

func MergeAllNodeFiltered(OtherNodesMsg map[uint64]*netstorage.ExecuteStatementMessage) (error, interface{}) {
	// for reuse the message merge flow
	other := OtherNodesMsg
	for _, n := range other {
		n.Result = n.Filtered
	}

	stmtType := GetStatementMessageType(other)
	switch stmtType {
	case netstorage.ShowMeasurementsStatement:
		return MergeMeasurementsNames(other)
	case netstorage.ShowTagKeysStatement:
		clusterTagKeysMap := make(map[uint64][]netstorage.TagKeys)
		for i, nodeMsg := range other {
			var tagKeys []netstorage.TagKeys
			err := json.Unmarshal(nodeMsg.Result, &tagKeys)
			if err != nil {
				return err, nil
			}
			clusterTagKeysMap[i] = tagKeys
		}
		return MergeTagKeys(&clusterTagKeysMap)
	default:
		return fmt.Errorf("ExecuteStatement type[%s] not surpport", stmtType), nil
	}
}

func RemoveFiltered(result [][]byte, filetered [][]byte) [][]byte {
	if len(filetered) == 0 {
		return result
	}

	s := set.NewSet[string]()
	for _, v := range result {
		s.Add(string(v))
	}

	for _, fv := range filetered {
		if s.Contains(string(fv)) {
			s.Remove(string(fv))
		}
	}

	var last ByteStringSlice
	sl := s.ToSlice()
	for _, l := range sl {
		last = append(last, []byte(l))
	}

	sort.Sort(last)

	return last
}

func limitStringSlice(s []string, offset, limit int) []string {
	l := len(s)
	if offset >= l {
		return nil
	}

	end := offset + limit
	if limit == 0 || end >= l {
		end = l
	}
	return s[offset:end]
}

type rowChanProxy struct {
	rc       chan query.RowsChan
	finished chan struct{}
}

func newRowChanProxy() *rowChanProxy {
	p := &rowChanProxy{
		rc:       make(chan query.RowsChan),
		finished: make(chan struct{}),
	}
	return p
}

func (p *rowChanProxy) close() {
	close(p.finished)
	close(p.rc)
}

// If the client is aborted, cannot be closed "RowsChan".
// We need to wait until the execution of "pipelineExecutor" is complete
func (p *rowChanProxy) wait() {
	for {
		select {
		case <-p.finished:
			return
		case <-p.rc:
		}
	}
}

func getPerSeriesAndRowSize(s *models.Row) (int, int) {
	var byteSizePerSeries, byteSizePerRow int
	byteSizePerSeries += len(s.Name)
	for k, v := range s.Tags {
		byteSizePerSeries += len(k) + len(v)
	}
	for i := range s.Columns {
		byteSizePerSeries += len(s.Columns[i])
	}
	for i := range s.Values[0] {
		switch v := s.Values[0][i].(type) {
		case float64, int64, uint64:
			byteSizePerRow += 8
		case bool:
			byteSizePerRow += 1
		case string:
			byteSizePerRow += len(v)
		case time.Time:
			byteSizePerRow += 16
		default:
		}
	}
	return byteSizePerSeries, byteSizePerRow
}
