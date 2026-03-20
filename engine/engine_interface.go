// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"context"
	"sort"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/raftlog"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type NewEngineFun func(dataPath, walPath string, options EngineOptions, ctx *metaclient.LoadCtx) (Engine, error)

var engines = make(map[string]NewEngineFun)

func RegisterNewEngineFun(name string, fn NewEngineFun) {
	if _, ok := engines[name]; ok {
		return
	}
	engines[name] = fn
}

func RegisteredEngines() []string {
	a := make([]string, 0, len(engines))
	for k := range engines {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func GetNewEngineFunction(entType string) NewEngineFun {
	fn, ok := engines[entType]
	if ok {
		return fn
	}
	return nil
}

type Engine interface {
	Open(durationInfos map[uint64]*meta.ShardDurationInfo, dbBriefInfos map[string]*meta.DatabaseBriefInfo, client metaclient.MetaClient) error
	Close() error
	ForceFlush()

	DeleteShard(db string, ptId uint32, shardID uint64) error
	DeleteMstInShard(db string, ptId uint32, shardID uint64, mst string) error
	DeleteIndex(db string, pt uint32, indexID uint64) error
	DeleteMstInIndex(db string, ptId uint32, indexID uint64, msts []string, onlyUseDiskThreshold uint64) error
	ClearIndexCache(db string, pt uint32, indexID uint64) error
	ExpiredShards(nilShardMap *map[uint64]*meta.ShardDurationInfo) []*meta.ShardIdentifier
	ExpiredShardsForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.ShardIdentifier
	ExpiredIndexes(nilIndexMap *map[uint64]*meta.IndexDurationInfo) []*meta.IndexIdentifier
	ExpiredIndexesForMst(db, rp string, mst *meta.MeasurementTTLTnfo) []*meta.IndexIdentifier
	ExpiredCacheIndexes() []*meta.IndexIdentifier
	FetchShardsNeedChangeStore() ([]*meta.ShardIdentifier, []*meta.ShardIdentifier)
	FetchIndexesNeedChangeStore() (indexesToCold []*meta.IndexIdentifier)
	ChangeShardTierToWarm(db string, ptId uint32, shardID uint64) error

	CreateShard(db, rp string, ptId uint32, shardID uint64, timeRangeInfo *meta.ShardTimeRangeInfo, mstInfo *meta.MeasurementInfo) error
	WriteRows(db, rp string, ptId uint32, shardID uint64, points []influx.Row, binaryRows []byte, snp *raftlog.SnapShotter) error
	WriteRec(db, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error
	WriteBlobs(db string, ptId uint32, shardID uint64, group *shelf.BlobGroup) error
	WriteToRaft(db, rp string, ptId uint32, tail []byte) error
	CreateDBPT(db string, pt uint32, enableTagArray bool)

	GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int

	GetShardSplitPoints(db string, ptId uint32, shardID uint64, idxes []int64) ([]string, error)

	DeleteDatabase(db string, ptId uint32) error

	DropRetentionPolicy(db string, rp string, ptId uint32) error

	DropMeasurement(db string, rp string, name string, shardIds []uint64) error

	TagKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]string, error)

	SeriesKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]string, error)
	SeriesCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]meta.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error)

	TagValues(db string, ptId []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (influxql.TablesTagSets, error)
	TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error)
	DropSeries() error
	MarkDropSeries(db string, ptID uint32, mstName []byte, expr influxql.Expr, t tsi.TimeRange) error

	DbPTRef(db string, ptId uint32, read bool) error
	DbPTUnref(db string, ptId uint32, read bool)
	CreateLogicalPlan(ctx context.Context, db string, ptId uint32, shardID []uint64, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error)
	ScanWithSparseIndex(ctx context.Context, db string, ptId uint32, shardIDs []uint64, schema hybridqp.Catalog) (executor.ShardsFragments, error)
	GetIndexInfo(db string, ptId uint32, shardIDs uint64, schema *executor.QuerySchema) (*executor.AttachedIndexInfo, error)
	RowCount(db string, ptId uint32, shardIDs []uint64, schema hybridqp.Catalog, isOnlyPKFilter bool) (int64, error)
	GetPreAgg(db string, ptId uint32, shardIDs []uint64, schema hybridqp.Catalog, queryAggExprs []string) (*record.Record, error)

	LogicalPlanCost(db string, ptId uint32, sources influxql.Sources, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error)

	UpdateShardDurationInfo(info *meta.ShardDurationInfo, nilShardMap *map[uint64]*meta.ShardDurationInfo) error
	UpdateIndexDurationInfo(info *meta.IndexDurationInfo, nilIndexMap *map[uint64]*meta.IndexDurationInfo) error

	PreOffload(opId uint64, db string, ptId uint32) error
	RollbackPreOffload(opId uint64, db string, ptId uint32) error
	PreAssign(opId uint64, db string, ptId uint32, durationInfos map[uint64]*meta.ShardDurationInfo, dbBriefInfo *meta.DatabaseBriefInfo, client metaclient.MetaClient) error
	Offload(opId uint64, db string, ptId uint32) error
	Assign(opId uint64, nodeId uint64, db string, ptId uint32, ver uint64, durationInfos map[uint64]*meta.ShardDurationInfo, dbBriefInfo *meta.DatabaseBriefInfo, client metaclient.MetaClient, storage StorageService) error

	SysCtrl(req *msgservice.SysCtrlRequest) (map[string]string, error)
	Statistics(buffer []byte) ([]byte, error)
	StatisticsOps() []opsStat.OpsStatistic

	GetShardDownSamplePolicyInfos(meta MetaDownSample) ([]*meta.ShardDownSamplePolicyInfo, error)
	GetDownSamplePolicy(key string) *meta.StoreDownSamplePolicy
	StartDownSampleTask(info *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger, meta MetaDownSample) error
	UpdateDownSampleInfo(policies *meta.DownSamplePoliciesInfoWithDbRp)
	UpdateShardDownSampleInfo(infos *meta.ShardDownSampleUpdateInfos)
	CheckPtsRemovedDone() bool
	TransferLeadership(database string, nodeId uint64, oldMasterPtId, newMasterPtId uint32) error
	HierarchicalStorage(db string, ptId uint32, shardID uint64) bool
	IndexHierarchicalStorage(db string, ptId uint32, shardID uint64) bool

	RaftMessage
	CreateDDLBasePlans(planType hybridqp.DDLType, db string, ptIDs []uint32, tr *influxql.TimeRange) DDLBasePlans
	CreateConsumeIterator(ident util.MeasurementIdent, pts []uint32, opt *query.ProcessorOptions) ([]record.Iterator, func())
	SetMetaClient(m metaclient.MetaClient)

	RegisterOnPTLoaded(id CallbackKey, f PtLoadFunc)
	UninstallOnPTLoaded(id CallbackKey)
	RegisterOnPTOffload(id CallbackKey, f PtOffLoadFunc)
	UninstallOnPTOffload(id CallbackKey)

	MergeShards(meta.MergeShards) error
	ClearRepCold(req *msgservice.SendClearEventsRequest) error
	GetDatabase(database string) map[uint32]*DBPTInfo
	OpenShardLazy(sh Shard) error

	GetLastIndex(db string, ptId uint32) (uint64, error)
	GetRPPTWriteStat(db, rp string) (meta.PTMstWriteStatus, error)
	GetTask(typ scheduler.TaskType, id uint64) (scheduler.Task, error)
	CompactFiles(typ scheduler.TaskType, id uint64, info *immutable.CompactedFileInfo) error
	GetShardIDs(db string, ptId uint32, tr *influxql.TimeRange) ([]uint64, error)
	IsColStore(db, rp, mst string) bool
	GetDBPtIds(db string) []uint32
	GetColStorePK(db, rp, mst string) (record.Schemas, bool)
}

type RaftMessage interface {
	SendRaftMessage(database string, ptId uint64, msg raftpb.Message) error
}

type DDLBasePlans interface {
	Execute(mstKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (interface{}, error)
	AddPlan(plan interface{})
	Stop()
}

type StorageService interface {
	Write(db, rp, mst string, ptId uint32, shardID uint64, writeData func() error) error
	WriteDataFunc(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte, index *raftlog.SnapShotter) error
	GetNodeId() uint64
}
