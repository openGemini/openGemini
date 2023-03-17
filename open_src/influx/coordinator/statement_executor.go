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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tracing"
	set "github.com/openGemini/openGemini/open_src/github.com/deckarep/golang-set"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	query2 "github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

var dbStatCount int

const (
	maxRetrySelectCount = 8
	retrySelectInterval = time.Millisecond * 100
)

var streamSupportMap = map[string]bool{"min": true, "max": true, "sum": true, "count": true}

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient meta.MetaClient

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager query2.StatementExecutor

	NetStorage netstorage.Storage

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper query2.ShardMapper

	// Holds monitoring data for SHOW STATS and SHOW DIAGNOSTICS.
	MetaExecutor *coordinator.MetaExecutor

	//Node *meta.Node

	// Select statement limits
	MaxSelectPointN         int
	MaxSelectSeriesN        int
	MaxSelectFieldsN        int
	MaxSelectBucketsN       int
	MaxQueryMem             int64
	QueryTimeCompareEnabled bool
	RetentionPolicyLimit    int
	MaxQueryParallel        int

	StmtExecLogger *logger.Logger
}

func (e *StatementExecutor) Close() error {
	return e.ShardMapper.Close()
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx *query2.ExecutionContext) error {
	e.MaxQueryParallel = int(atomic.LoadInt32(&syscontrol.QueryParallel))
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		err := e.retryExecuteSelectStatement(stmt, ctx)
		if err == nil {
			return nil
		} else if errno.Equal(err, errno.DatabaseNotFound) ||
			errno.Equal(err, errno.ErrMeasurementNotFound) {
			e.StmtExecLogger.Error("execute select statement 400 error", zap.Any("stmt", stmt), zap.Error(err))
			atomic.AddInt64(&statistics.HandlerStat.Query400ErrorStmtCount, 1)
		} else {
			e.StmtExecLogger.Error("execute select statement 500 error", zap.Any("stmt", stmt), zap.Error(err))
			atomic.AddInt64(&statistics.HandlerStat.QueryErrorStmtCount, 1)
		}
		return err
	}

	e.StmtExecLogger.Info("start execute statement", zap.Any("stmt", stmt))
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
	case *influxql.CreateUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateUserStatement(stmt)
	case *influxql.DeleteSeriesStatement:
		return meta2.ErrUnsupportCommand
		_, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.DropDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.DropMeasurementStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.DropSeriesStatement:
		return meta2.ErrUnsupportCommand
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.DropRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		_, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.DropShardStatement:
		return meta2.ErrUnsupportCommand
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropShardStatement(stmt, ctx)
	case *influxql.DropSubscriptionStatement:
		return meta2.ErrUnsupportCommand
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
		rows, err = e.retryExecuteStatement(stmt, ctx)
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
	case *influxql.ShowMeasurementsStatement:
		if stmt.Condition != nil {
			return meta2.ErrUnsupportCommand
		}
		_, err = e.retryExecuteStatement(stmt, ctx)
		return err
	case *influxql.ShowMeasurementCardinalityStatement:
		if stmt.Condition != nil {
			return meta2.ErrUnsupportCommand
		}
		rows, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.ShowRetentionPoliciesStatement:
		rows, err = e.executeShowRetentionPoliciesStatement(stmt)
	case *influxql.ShowSeriesCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.ShowShardsStatement:
		rows, err = e.executeShowShardsStatement(stmt)
	case *influxql.ShowShardGroupsStatement:
		rows, err = e.executeShowShardGroupsStatement(stmt)
	case *influxql.ShowSubscriptionsStatement:
		return meta2.ErrUnsupportCommand
		rows, err = e.executeShowSubscriptionsStatement(stmt)
	case *influxql.ShowFieldKeysStatement:
		_, err = e.retryExecuteStatement(stmt, ctx)
		return err
	case *influxql.ShowFieldKeyCardinalityStatement:
		_, err = e.retryExecuteStatement(stmt, ctx)
		return err
	case *influxql.ShowTagKeysStatement:
		_, err = e.retryExecuteStatement(stmt, ctx)
		return err
	case *influxql.ShowTagKeyCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.ShowTagValuesStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.ShowSeriesStatement:
		_, err = e.retryExecuteStatement(stmt, ctx)
		return err
	case *influxql.ShowTagValuesCardinalityStatement:
		rows, err = e.retryExecuteStatement(stmt, ctx)
	case *influxql.ShowUsersStatement:
		rows, err = e.executeShowUsersStatement(stmt)
	case *influxql.SetPasswordUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeSetPasswordUserStatement(stmt)
	case *influxql.ShowQueriesStatement, *influxql.KillQueryStatement:
		return meta2.ErrUnsupportCommand
		// Send query related statements to the task manager.
		return e.TaskManager.ExecuteStatement(stmt, ctx)
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
	default:
		return query2.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ctx.Send(&query.Result{
		Series:   rows,
		Messages: messages,
	})
}

func (e *StatementExecutor) retryExecuteStatement(stmt influxql.Statement, ctx *query2.ExecutionContext) (models.Rows, error) {
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
			err = e.executeShowTagKeys(stmt, ctx)
		case *influxql.ShowTagKeyCardinalityStatement:
			err = e.executeShowTagKeyCardinality(stmt, ctx)
		case *influxql.ShowTagValuesStatement:
			rows, err = e.executeShowTagValues(stmt)
		case *influxql.ShowSeriesStatement:
			err = e.executeShowSeries(stmt, ctx)
		case *influxql.ShowMeasurementsStatement:
			err = e.executeShowMeasurementsStatement(stmt, ctx)
		case *influxql.ShowMeasurementCardinalityStatement:
			rows, err = e.executeShowMeasurementCardinalityStatement(stmt)
		case *influxql.ShowSeriesCardinalityStatement:
			rows, err = e.executeShowSeriesCardinality(stmt)
		case *influxql.ShowTagValuesCardinalityStatement:
			rows, err = e.executeShowTagValuesCardinality(stmt)
		case *influxql.ShowFieldKeysStatement:
			err = e.executeShowFieldKeys(stmt, ctx)
		case *influxql.ShowFieldKeyCardinalityStatement:
			err = e.executeShowFieldKeyCardinality(stmt, ctx)
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

func (e *StatementExecutor) executeCreateMeasurementStatement(stmt *influxql.CreateMeasurementStatement) error {
	if !meta2.ValidName(stmt.Name) {
		return meta2.ErrInvalidName
	}

	if err := meta2.ValidShardKey(stmt.ShardKey); err != nil {
		return err
	}

	e.StmtExecLogger.Info("create measurement ", zap.String("name", stmt.Name))
	if _, err := e.MetaClient.Measurement(stmt.Database, stmt.RetentionPolicy, stmt.Name); err != meta2.ErrMeasurementNotFound {
		return fmt.Errorf("measurement exist : %s/%s/%s", stmt.Database, stmt.RetentionPolicy, stmt.Name)
	}

	schema := make(map[string]int32)
	for _, tag := range stmt.Tags {
		if _, ok := schema[tag]; !ok {
			schema[tag] = 0
		} else {
			return fmt.Errorf("field or tag conflict %s", tag)
		}
	}

	for field, _ := range stmt.Fields {
		if _, ok := schema[field]; !ok {
			schema[field] = 0
		} else {
			return fmt.Errorf("field or tag conflict %s", field)
		}
	}

	ski := &meta2.ShardKeyInfo{ShardKey: stmt.ShardKey, Type: stmt.Type}
	indexR := &meta2.IndexRelation{}

	if len(stmt.IndexInfo) > 0 {
		var textIndexs []*meta2.IndexInfor
		var fieldIndexs []*meta2.IndexInfor
		for _, IndexInfo := range stmt.IndexInfo {
			if IndexInfo.IndexType == "keyword" {
				IndexInfo.IndexType = "field"
			}

			oid, err := tsi.GetIndexIdByName(IndexInfo.IndexType)
			if err != nil {
				return err
			}
			if _, ok := stmt.Fields[IndexInfo.FieldName]; !ok {
				return fmt.Errorf("%s index only create on field, but not field: %s", IndexInfo.IndexType, IndexInfo.FieldName)
			}

			index := &meta2.IndexInfor{
				FieldName:  IndexInfo.FieldName,
				Tokens:     IndexInfo.Tokens,
				Tokenizers: IndexInfo.Tokenizers,
				IndexName:  IndexInfo.IndexName,
			}

			if oid == uint32(tsi.Field) {
				fieldIndexs = append(fieldIndexs, index)
			} else if oid == uint32(tsi.Text) {
				if val, _ := stmt.Fields[IndexInfo.FieldName]; val == influx.Field_Type_String {
					textIndexs = append(textIndexs, index)
					continue
				} else {
					return fmt.Errorf("text index only create on string, but not: %s", IndexInfo.FieldName)
				}
			}
		}
		if len(fieldIndexs) > 1 {
			return fmt.Errorf("cannot create field index for multiple columns: %v", fieldIndexs)
		}
		if len(textIndexs) > 0 {
			indexR.Oids = append(indexR.Oids, uint32(tsi.Text))
			indexR.IndexList = append(indexR.IndexList, &meta2.IndexList{
				IList: textIndexs,
			})
		}
		if len(fieldIndexs) > 0 {
			indexR.Oids = append(indexR.Oids, uint32(tsi.Field))
			indexR.IndexList = append(indexR.IndexList, &meta2.IndexList{
				IList: fieldIndexs,
			})
		}
	}

	if _, err := e.MetaClient.CreateMeasurement(stmt.Database, stmt.RetentionPolicy, stmt.Name, ski, indexR); err != nil {
		return err
	}

	if len(stmt.Tags)+len(stmt.Fields) > 0 {
		fieldToCreate := make([]*proto2.FieldSchema, 0, len(stmt.Tags)+len(stmt.Fields))
		for _, tag := range stmt.Tags {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(tag),
				FieldType: proto.Int32(influx.Field_Type_Tag),
			})
		}
		for k, v := range stmt.Fields {
			fieldToCreate = append(fieldToCreate, &proto2.FieldSchema{
				FieldName: proto.String(k),
				FieldType: proto.Int32(v),
			})
		}
		if err := e.MetaClient.UpdateSchema(stmt.Database, stmt.RetentionPolicy, stmt.Name, fieldToCreate); err != nil {
			return err
		}
	}

	return nil
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
		_, err := e.MetaClient.CreateDatabase(stmt.Name)
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

	oneReplication := 1
	spec := meta2.RetentionPolicySpec{
		Name:               stmt.RetentionPolicyName,
		Duration:           stmt.RetentionPolicyDuration,
		ReplicaN:           &oneReplication,
		ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
		HotDuration:        &stmt.RetentionPolicyHotDuration,
		WarmDuration:       &stmt.RetentionPolicyWarmDuration,
		IndexGroupDuration: stmt.RetentionPolicyIndexGroupDuration,
	}
	ski := &meta2.ShardKeyInfo{ShardKey: stmt.ShardKey}
	_, err := e.MetaClient.CreateDatabaseWithRetentionPolicy(stmt.Name, &spec, ski)
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

	oneReplication := 1
	spec := meta2.RetentionPolicySpec{
		Name:               stmt.Name,
		Duration:           &stmt.Duration,
		ReplicaN:           &oneReplication,
		ShardGroupDuration: stmt.ShardGroupDuration,
		HotDuration:        &stmt.HotDuration,
		WarmDuration:       &stmt.WarmDuration,
		IndexGroupDuration: stmt.IndexGroupDuration,
	}

	// Create new retention policy.
	_, err := e.MetaClient.CreateRetentionPolicy(stmt.Database, &spec, stmt.Default)
	return err
}

func (e *StatementExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
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

	return e.MetaClient.MarkMeasurementDelete(database, stmt.Name)
}

func (e *StatementExecutor) executeDropShardStatement(stmt *influxql.DropShardStatement, ctx *query2.ExecutionContext) error {
	db, rp, sg := e.MetaClient.ShardOwner(stmt.ID)
	if len(db) == 0 || len(rp) == 0 || sg == nil {
		return nil
	}

	var ptIds []uint32
	for _, shard := range sg.Shards {
		if shard.ID == stmt.ID {
			ptIds = shard.Owners
			break
		}
	}

	nodeIds := make([]uint64, 0, 4)
	pts, err := e.MetaClient.DBPtView(db)
	if err != nil {
		return err
	}

	for _, pt := range pts {
		nodeIds = append(nodeIds, pt.Owner.NodeID)
	}

	if len(nodeIds) == 0 {
		return nil
	}

	var eg errgroup.Group
	for _, nid := range nodeIds {
		id := nid
		eg.Go(func() error {
			return e.NetStorage.DropShard(id, db, rp, ptIds, stmt.ID)
		})
	}

	if err = eg.Wait(); err != nil {
		return err
	}

	// Remove the shard reference from the Meta Store.
	return e.MetaClient.DropShard(stmt.ID)
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
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *StatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *StatementExecutor) executeExplainStatement(q *influxql.ExplainStatement, ctx *query2.ExecutionContext) (models.Rows, error) {
	panic("impl me")
}

func (e *StatementExecutor) executeExplainAnalyzeStatement(q *influxql.ExplainStatement, ectx *query2.ExecutionContext) (models.Rows, error) {
	stmt := q.Statement
	trace, span := tracing.NewTrace("SELECT")
	stmt.OmitTime = true
	ctx := tracing.NewContextWithTrace(ectx.Context, trace)
	ctx = tracing.NewContextWithSpan(ctx, span)
	span.AppendNameValue("statement", q.String())
	span.Finish()

	proxy := newRowChanProxy()
	pipSpan := span.StartSpan("create_pipeline_executor").StartPP()
	ectx.ExecutionOptions.RowsChan = proxy.rc
	pipelineExecutor, err := e.createPipelineExecutor(ctx, stmt, ectx.ExecutionOptions)
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
			case rowsChan, ok := <-ectx.ExecutionOptions.RowsChan:
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

func (e *StatementExecutor) retryExecuteSelectStatement(stmt *influxql.SelectStatement, ctx *query2.ExecutionContext) error {
	var err error

	for i := 0; i < maxRetrySelectCount; i++ {
		err = e.executeSelectStatement(stmt, ctx)
		if err == nil || !coordinator.IsRetryErrorForPtView(err) {
			break
		}
		time.Sleep(retrySelectInterval * (1 << i))
	}
	return err
}

func (e *StatementExecutor) retryCreatePipelineExecutor(ctx context.Context, stmt *influxql.SelectStatement, opt query2.ExecutionOptions) (*executor.PipelineExecutor, error) {
	startTime := time.Now()
	var retryNum uint32 = 0
	for {
		pipelineExecutor, err := e.createPipelineExecutor(ctx, stmt, opt)
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
			e.StmtExecLogger.Error("retry retryCreatePipelineExecutor err ", zap.Error(err), zap.Any("stmt", stmt))
			return nil, err
		}
	}
}

func (e *StatementExecutor) executeSelectStatement(stmt *influxql.SelectStatement, ctx *query2.ExecutionContext) error {
	start := time.Now()
	proxy := newRowChanProxy()
	ctx.ExecutionOptions.RowsChan = proxy.rc
	// omit Time field for stmt
	stmt.OmitTime = true
	pipelineExecutor, err := e.retryCreatePipelineExecutor(ctx, stmt, ctx.ExecutionOptions)
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
		})
	}

	end := time.Now()
	emitted := false
	closed := false

	ec := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctxWithWriter := context.WithValue(context.Background(), executor.WRITER_CONTEXT, ctx.PointsWriter)
		ec <- pipelineExecutor.ExecuteExecutor(ctxWithWriter)
		close(ec)
		proxy.close()
	}()

	defer func() {
		qStat, _ := ctx.Value(query2.QueryDurationKey).(*statistics.SQLSlowQueryStatistics)
		if qStat != nil {
			qStat.AddDuration("SqlIteratorDuration", end.Sub(start).Nanoseconds())
			qStat.AddDuration("EmitDuration", time.Now().Sub(end).Nanoseconds())
		}
	}()

	var rowsChan query2.RowsChan
	var ok bool
	for {
		select {
		case rowsChan, ok = <-ctx.ExecutionOptions.RowsChan:
			if !ok {
				closed = true
				break
			}
			result := &query.Result{
				Series:  rowsChan.Rows,
				Partial: rowsChan.Partial,
			}
			// Send results or exit if closing.
			if err := ctx.Send(result); err != nil {
				pipelineExecutor.Abort()
				e.StmtExecLogger.Error("send result rows failed", zap.Error(err))
				return err
			}
			emitted = true
		case <-ctx.Done():
			e.StmtExecLogger.Info("aborted by user", zap.String("stmt", stmt.String()))
			pipelineExecutor.Abort()
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
		})
	}
	return nil
}

func (e *StatementExecutor) GetOptions(opt query2.ExecutionOptions) query2.SelectOptions {
	return query2.SelectOptions{
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
		RowsChan:                opt.RowsChan,
		ChunkSize:               opt.InnerChunkSize,
		Traceid:                 opt.Traceid,
		AbortChan:               opt.AbortCh,
	}
}

func (e *StatementExecutor) createPipelineExecutor(ctx context.Context, stmt *influxql.SelectStatement, opt query2.ExecutionOptions) (pipelineExecutor *executor.PipelineExecutor, err error) {
	sopt := e.GetOptions(opt)

	defer func() {
		if e := recover(); e != nil {
			internalErr, ok := e.(*errno.Error)
			if ok && errno.Equal(internalErr, errno.DtypeNotSupport) {
				panic(internalErr)
			}

			stackInfo := fmt.Errorf("runtime panic: %v\n %s", e, string(debug.Stack())).Error()
			logger.NewLogger(errno.ModuleQueryEngine).Error(stackInfo, zap.Uint64("trace_id", opt.Traceid),
				zap.String("query", "pipeline executor"))
		}
	}()

	// Create a pipelineExecutor from a selection.
	p, e_tmp := executor.Select(ctx, stmt, e.ShardMapper, sopt)
	if e_tmp != nil || p == nil {
		return nil, e_tmp
	}
	pipelineExecutor, err = p.(*executor.PipelineExecutor), e_tmp

	return pipelineExecutor, err
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, ctx *query2.ExecutionContext) (models.Rows, error) {
	dis := e.MetaClient.Databases()
	a := ctx.ExecutionOptions.Authorizer

	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		if a.AuthorizeDatabase(originql.ReadPrivilege, di.Name) || a.AuthorizeDatabase(originql.WritePrivilege, di.Name) {
			row.Values = append(row.Values, []interface{}{di.Name})
		}
	}
	sort.Slice(row.Values, func(i, j int) bool {
		return strings.Compare(row.Values[i][0].(string), row.Values[j][0].(string)) < 0
	})
	return []*models.Row{row}, nil
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

func (e *StatementExecutor) executeShowMeasurementsStatement(q *influxql.ShowMeasurementsStatement, ctx *query2.ExecutionContext) error {
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
	if len(measurements) == 0 {
		return ctx.Send(&query.Result{})
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
	})
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

func (e *StatementExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement) (models.Rows, error) {
	return e.MetaClient.ShowShards(), nil
}

func (e *StatementExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement) (models.Rows, error) {
	return e.MetaClient.ShowShardGroups(), nil
}

func (e *StatementExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement) (models.Rows, error) {
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

func (e *StatementExecutor) executeShowFieldKeys(q *influxql.ShowFieldKeysStatement, ctx *query2.ExecutionContext) error {
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
		}); err != nil {
			return err
		}
		emitted = true

	}
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil
}

func (e *StatementExecutor) executeShowFieldKeyCardinality(q *influxql.ShowFieldKeyCardinalityStatement, ctx *query2.ExecutionContext) error {
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
		}); err != nil {
			return err
		}
		emitted = true
	}
	if !emitted {
		return ctx.Send(&query.Result{})
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

func (e *StatementExecutor) executeShowTagKeys(q *influxql.ShowTagKeysStatement, ctx *query2.ExecutionContext) error {
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
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{})
	}
	return nil

}

func (e *StatementExecutor) executeShowTagKeyCardinality(q *influxql.ShowTagKeyCardinalityStatement, ctx *query2.ExecutionContext) error {
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
		}); err != nil {
			return err
		}
		emitted = true
	}
	if !emitted {
		return ctx.Send(&query.Result{})
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

func (e *StatementExecutor) executeShowSeries(q *influxql.ShowSeriesStatement, ctx *query2.ExecutionContext) error {
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

	err = e.MetaExecutor.EachDBNodes(q.Database, func(nodeID uint64, pts []uint32, hasErr *bool) error {
		if *hasErr {
			return nil
		}
		arr, err := e.NetStorage.ShowSeries(nodeID, q.Database, pts, names, q.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil && config.GetHaEnable() {
			*hasErr = true
			series = series[:0] // if execute command failed reset res
		}
		if !*hasErr {
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
	})
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
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32, hasErr *bool) error {
		if *hasErr {
			return nil
		}
		mstCardinality, err := e.NetStorage.SeriesCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil && config.GetHaEnable() {
			*hasErr = true
			ret = ret[:0]
		}
		if *hasErr {
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
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32, hasErr *bool) error {
		if *hasErr {
			return nil
		}
		mstCardinality, err := e.NetStorage.SeriesCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil && config.GetHaEnable() {
			*hasErr = true
			ret = make(map[string]meta2.CardinalityInfos)
		}
		if *hasErr {
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
	err := e.MetaExecutor.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32, hasErr *bool) error {
		if *hasErr {
			return nil
		}
		tmp, err := e.NetStorage.SeriesExactCardinality(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil && config.GetHaEnable() {
			*hasErr = true
			ret = make(map[string]uint64)
		}
		if *hasErr {
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

func (e *StatementExecutor) executePrepareSnapshotStatement(q *influxql.PrepareSnapshotStatement, ctx *query2.ExecutionContext) error {
	panic("impl me")
}

func (e *StatementExecutor) executeEndPrepareSnapshotStatement(q *influxql.EndPrepareSnapshotStatement, ctx *query2.ExecutionContext) error {
	panic("impl me")
}

func (e *StatementExecutor) executeGetRuntimeInfoStatement(q *influxql.GetRuntimeInfoStatement, ctx *query2.ExecutionContext) (models.Rows, error) {
	panic("impl me")
}

func (e *StatementExecutor) executeCreateStreamStatement(stmt *influxql.CreateStreamStatement, ctx *query2.ExecutionContext) error {
	selectStmt, ok := stmt.Query.(*influxql.SelectStatement)
	if !ok {
		return errors.New("create stream query must be select statement")
	}
	mstInfo := stmt.Target.Measurement
	opt := e.GetOptions(ctx.ExecutionOptions)
	s, er := query2.Prepare(selectStmt, e.ShardMapper, opt)
	if er != nil {
		return er
	}
	selectStmt = s.Statement()
	if err := stmt.Check(selectStmt, streamSupportMap); err != nil {
		return err
	}
	_, err := e.MetaClient.Measurement(mstInfo.Database, mstInfo.RetentionPolicy, mstInfo.Name)
	if err != nil {
		if err == meta2.ErrMeasurementNotFound {
			srcMst := selectStmt.Sources[0].(*influxql.Measurement)
			srcInfo, _ := e.MetaClient.Measurement(srcMst.Database, srcMst.RetentionPolicy, srcMst.Name)
			/*			if len(srcInfo.IndexRelations) > 0 {
							_, err = e.MetaClient.CreateMeasurement(mstInfo.Database, mstInfo.RetentionPolicy, mstInfo.Name, &srcInfo.ShardKeys[0], &srcInfo.IndexRelations[0])
						} else {
							_, err = e.MetaClient.CreateMeasurement(mstInfo.Database, mstInfo.RetentionPolicy, mstInfo.Name, &srcInfo.ShardKeys[0], nil)
						}*/
			_, err = e.MetaClient.CreateMeasurement(mstInfo.Database, mstInfo.RetentionPolicy, mstInfo.Name, &srcInfo.ShardKeys[0], nil)

			if err != nil {
				return err
			}
			if err := e.MetaClient.UpdateStreamMstSchema(mstInfo.Database, mstInfo.RetentionPolicy, mstInfo.Name, selectStmt); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	info := meta2.NewStreamInfo(stmt, selectStmt)
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

	uniqueMap := make(map[string]set.Set)

	for _, nodeTagKeys := range *otherNodeTagKeysMap {
		for _, tagKey := range nodeTagKeys {
			s := set.NewSet()
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
			newSlice[i] = data.(string)
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
	uniqueMap := make(map[string]set.Set)
	for _, nodeTagValues := range *otherNodeTagKeysMap {
		for _, tagValues := range nodeTagValues {
			s := set.NewSet()
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
			newSlice[i] = data.(netstorage.TagSet)
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

	s := set.NewSet()
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
		last = append(last, []byte(l.(string)))
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
	rc       chan query2.RowsChan
	finished chan struct{}
}

func newRowChanProxy() *rowChanProxy {
	p := &rowChanProxy{
		rc:       make(chan query2.RowsChan),
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
