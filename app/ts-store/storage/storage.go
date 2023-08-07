/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/influxdata/influxdb/pkg/limiter"
	retention2 "github.com/influxdata/influxdb/services/retention"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/castor"
	"github.com/openGemini/openGemini/services/downsample"
	"github.com/openGemini/openGemini/services/hierarchical"
	"github.com/openGemini/openGemini/services/retention"
	"go.uber.org/zap"
)

// Service represents a service attached to the server.
type Service interface {
	Open() error
	Close() error
}

type StoreEngine interface {
	RefEngineDbPt(string, uint32) error
	UnrefEngineDbPt(string, uint32)
	ExecuteDelete(*netstorage.DeleteRequest) error
	GetShardSplitPoints(string, uint32, uint64, []int64) ([]string, error)
	SeriesCardinality(string, []uint32, []string, influxql.Expr) ([]meta.MeasurementCardinalityInfo, error)
	SeriesExactCardinality(string, []uint32, []string, influxql.Expr) (map[string]uint64, error)
	SeriesKeys(string, []uint32, []string, influxql.Expr) ([]string, error)
	TagValues(string, []uint32, map[string][][]byte, influxql.Expr) (netstorage.TablesTagSets, error)
	TagValuesCardinality(string, []uint32, map[string][][]byte, influxql.Expr) (map[string]uint64, error)
	SendSysCtrlOnNode(*netstorage.SysCtrlRequest) error
	GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int
	PreOffload(*meta.DbPtInfo) error
	RollbackPreOffload(*meta.DbPtInfo) error
	PreAssign(uint64, *meta.DbPtInfo) error
	Offload(*meta.DbPtInfo) error
	Assign(uint64, *meta.DbPtInfo) error
	GetConnId() uint64
}

type Storage struct {
	path string

	metaClient *metaclient.Client
	node       *metaclient.Node

	engine netstorage.Engine

	stop chan struct{}

	Services []Service

	log     *logger.Logger
	loadCtx *metaclient.LoadCtx

	WriteLimit limiter.Fixed
}

func (s *Storage) GetPath() string {
	return s.path
}

func (s *Storage) appendRetentionPolicyService(c retention2.Config) {
	if !c.Enabled {
		return
	}

	srv := retention.NewService(time.Duration(c.CheckInterval))
	srv.Engine = s.engine
	srv.MetaClient = s.metaClient
	s.Services = append(s.Services, srv)
}

func (s *Storage) appendHierarchicalService(c retention2.Config) {
	if !c.Enabled {
		return
	}

	srv := hierarchical.NewService(time.Duration(c.CheckInterval))
	srv.Engine = s.engine
	srv.MetaClient = s.metaClient
	s.Services = append(s.Services, srv)
}

func (s *Storage) appendDownSamplePolicyService(c retention2.Config) {
	if !c.Enabled {
		return
	}
	srv := downsample.NewService(time.Duration(c.CheckInterval))
	srv.Engine = s.engine
	srv.MetaClient = s.metaClient
	s.Services = append(s.Services, srv)
}

func (s *Storage) appendAnalysisService(c config.Castor) {
	if !c.Enabled {
		return
	}
	srv := castor.NewService(c)
	s.Services = append(s.Services, srv)
}

func OpenStorage(path string, node *metaclient.Node, cli *metaclient.Client, conf *config.TSStore) (*Storage, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("cannot determine absolute path for %q: %w", path, err)
	}

	newEngineFn := netstorage.GetNewEngineFunction(conf.Data.Engine)
	if newEngineFn == nil {
		return nil, fmt.Errorf("unknown tsm engine:%v", conf.Data.Engine)
	}

	loadCtx := metaclient.LoadCtx{}
	loadCtx.LoadCh = make(chan *metaclient.DBPTCtx)
	opt := netstorage.NewEngineOptions()
	opt.ImmTableMaxMemoryPercentage = conf.Data.ImmTableMaxMemoryPercentage
	opt.WriteColdDuration = time.Duration(conf.Data.WriteColdDuration)
	opt.ShardMutableSizeLimit = int64(conf.Data.ShardMutableSizeLimit)
	opt.NodeMutableSizeLimit = int64(conf.Data.NodeMutableSizeLimit)
	opt.MaxWriteHangTime = time.Duration(conf.Data.MaxWriteHangTime)
	opt.MemDataReadEnabled = conf.Data.MemDataReadEnabled
	opt.CompactThroughput = int64(conf.Data.CompactThroughput)
	opt.CompactThroughputBurst = int64(conf.Data.CompactThroughputBurst)
	opt.CompactRecovery = conf.Data.CompactRecovery
	opt.SnapshotThroughput = int64(conf.Data.SnapshotThroughput)
	opt.SnapshotThroughputBurst = int64(conf.Data.SnapshotThroughputBurst)
	opt.MaxConcurrentCompactions = conf.Data.MaxConcurrentCompactions
	opt.MaxFullCompactions = conf.Data.MaxFullCompactions
	opt.FullCompactColdDuration = time.Duration(conf.Data.CompactFullWriteColdDuration)
	opt.CacheDataBlock = conf.Data.CacheDataBlock
	opt.CacheMetaBlock = conf.Data.CacheMetaBlock
	opt.EnableMmapRead = conf.Data.EnableMmapRead
	opt.ReadCacheLimit = uint64(conf.Data.ReadCacheLimit)
	opt.WalSyncInterval = time.Duration(conf.Data.WalSyncInterval)
	opt.WalEnabled = conf.Data.WalEnabled
	opt.WalReplayParallel = conf.Data.WalReplayParallel
	opt.WalReplayAsync = conf.Data.WalReplayAsync
	opt.CompactionMethod = conf.Data.CompactionMethod
	opt.OpenShardLimit = conf.Data.OpenShardLimit
	opt.LazyLoadShardEnable = conf.Data.LazyLoadShardEnable
	opt.ThermalShardStartDuration = time.Duration(conf.Data.ThermalShardStartDuration)
	opt.ThermalShardEndDuration = time.Duration(conf.Data.ThermalShardEndDuration)
	opt.DownSampleWriteDrop = conf.Data.DownSampleWriteDrop
	opt.MaxDownSampleTaskConcurrency = conf.Data.MaxDownSampleTaskConcurrency
	opt.MaxSeriesPerDatabase = conf.Data.MaxSeriesPerDatabase

	// init clv config
	clv.InitConfig(conf.ClvConfig)
	// init chunkReader resource allocator.
	if e := resourceallocator.InitResAllocator(int64(conf.Data.ChunkReaderThreshold), int64(conf.Data.MinChunkReaderConcurrency), int64(conf.Data.MinShardsConcurrency),
		resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, conf.Meta.PtNumPerNode); e != nil {
		return nil, e
	}
	// init shards parallelism resource allocator.
	if e := resourceallocator.InitResAllocator(int64(conf.Data.MaxShardsParallelismNum), 0, int64(conf.Data.MinShardsConcurrency),
		0, resourceallocator.ShardsParallelismRes, time.Duration(conf.Data.MaxWaitResourceTime), conf.Meta.PtNumPerNode); e != nil {
		return nil, e
	}
	// init series parallelism resource allocator.
	if e := resourceallocator.InitResAllocator(int64(conf.Data.MaxSeriesParallelismNum), 0, int64(conf.Data.MinShardsConcurrency),
		0, resourceallocator.SeriesParallelismRes, time.Duration(conf.Data.MaxWaitResourceTime), conf.Meta.PtNumPerNode); e != nil {
		return nil, e
	}

	executor.IgnoreEmptyTag = conf.Common.IgnoreEmptyTag

	eng, err := newEngineFn(conf.Data.DataDir, conf.Data.WALDir, opt, &loadCtx)
	if err != nil {
		return nil, err
	}

	cpuNum := cgroup.AvailableCPUs()
	minWriteConcurrentLimit, maxWriteConcurrentLimit := cpuNum, 8*cpuNum
	conf.Data.WriteConcurrentLimit = util.IntLimit(minWriteConcurrentLimit, maxWriteConcurrentLimit, conf.Data.WriteConcurrentLimit)

	s := &Storage{
		path:       path,
		metaClient: cli,
		node:       node,
		engine:     eng,
		stop:       make(chan struct{}),
		loadCtx:    &loadCtx,
		WriteLimit: limiter.NewFixed(conf.Data.WriteConcurrentLimit),
	}

	s.log = logger.NewLogger(errno.ModuleStorageEngine)

	if !config.GetHaEnable() {
		if err := s.engine.Open(s.metaClient.ShardDurations, s.metaClient.DBBriefInfos, s.metaClient); err != nil {
			return nil, fmt.Errorf("err open engine %s", err)
		}
	}
	s.engine.SetReadOnly(conf.Data.Readonly)
	// Append services.
	s.appendRetentionPolicyService(conf.Retention)
	s.appendDownSamplePolicyService(conf.DownSample)
	s.appendHierarchicalService(conf.HierarchicalStore)
	s.appendAnalysisService(conf.Analysis)

	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return nil, fmt.Errorf("open service: %s", err)
		}
	}

	return s, nil
}

func (s *Storage) WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error {
	db = stringinterner.InternSafe(db)
	rp = stringinterner.InternSafe(rp)
	return s.Write(db, rp, rows[0].Name, ptId, shardID, func() error {
		return s.engine.WriteRows(db, rp, ptId, shardID, rows, binaryRows)
	})
}

func (s *Storage) WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error {
	atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, 1)
	defer atomic.AddInt64(&statistics.PerfStat.WriteActiveRequests, -1)
	db = stringinterner.InternSafe(db)
	rp = stringinterner.InternSafe(rp)
	return s.Write(db, rp, mst, ptId, shardID, func() error {
		return s.engine.WriteRec(db, rp, ptId, shardID, rec, binaryRec)
	})
}

func (s *Storage) Write(db, rp, mst string, ptId uint32, shardID uint64, writeData func() error) error {
	defer func(start time.Time) {
		d := time.Since(start).Nanoseconds()
		atomic.AddInt64(&statistics.PerfStat.WriteStorageDurationNs, d)
	}(time.Now())

	var timeRangeInfo *meta.ShardTimeRangeInfo
	err := writeData()
	err2, ok := err.(*errno.Error)
	if !ok {
		return err
	}
	switch err2.Errno() {
	case errno.PtNotFound:
		// case ha enabled: db pt is created in create database
		if config.GetHaEnable() {
			return err
		}
		enableTagArray, err := s.metaClient.TagArrayEnabledFromServer(db)
		if err != nil {
			return err
		}
		s.engine.CreateDBPT(db, ptId, enableTagArray)
		fallthrough
	case errno.ShardNotFound:
		// get index meta data, shard meta data
		startT := time.Now()
		timeRangeInfo, err = s.metaClient.GetShardRangeInfo(db, rp, shardID)
		if err != nil {
			return err
		}
		// all rows belongs to the same shard/engine type, we can get engine type from the first one.
		mstInfo, err := s.metaClient.GetMeasurementInfoStore(db, rp, influx.GetOriginMstName(mst))
		if err != nil {
			return err
		}
		err = s.engine.CreateShard(db, rp, ptId, shardID, timeRangeInfo, mstInfo)
		if err != nil {
			return err
		}
		atomic.AddInt64(&statistics.PerfStat.WriteCreateShardNs, time.Since(startT).Nanoseconds())
		err = writeData()
		return err
	default:
	}
	return err2
}

func (s *Storage) VerifyNodeId(id uint64) error {
	if s.metaClient.NodeID() != id {
		return fmt.Errorf("node id invalid, need send task to %d, but my id is %d", id, s.metaClient.NodeID())
	}

	return nil
}

func (s *Storage) ReportLoad() {
	for {
		select {
		case <-s.stop:
			s.log.Info("close storage")
			return
		case rCtx := <-s.loadCtx.LoadCh:
			loads := []*proto.DBPtStatus{rCtx.DBPTStat}
			rCtxes := []*metaclient.DBPTCtx{rCtx}
			for i := 0; i < 10; i++ {
				select {
				case rCtx := <-s.loadCtx.LoadCh:
					loads = append(loads, rCtx.DBPTStat)
					rCtxes = append(rCtxes, rCtx)
					s.log.Info("get load from dbPT", zap.String("load", rCtx.String()))
				default:
				}
			}

			n := len(loads)
			err := s.metaClient.ReportShardLoads(loads)
			if err != nil {
				s.log.Warn("report load failed", zap.Error(err))
			}

			for i := 0; i < n; i++ {
				s.loadCtx.PutReportCtx(rCtxes[i])
			}
		}
	}
}

func (s *Storage) MustClose() {
	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		util.MustClose(service)
	}
	_ = s.engine.Close()
	close(s.stop)
}

func (s *Storage) ExecuteDelete(req *netstorage.DeleteRequest) error {
	switch req.Type {
	case netstorage.DatabaseDelete:
		return s.engine.DeleteDatabase(req.Database, req.PtId)
	case netstorage.RetentionPolicyDelete:
		return s.engine.DropRetentionPolicy(req.Database, req.Rp, req.PtId)
	case netstorage.MeasurementDelete:
		// imply delete measurement
		return s.engine.DropMeasurement(req.Database, req.Rp, req.Measurement, req.ShardIds)
	}
	return nil
}

func (s *Storage) GetShardSplitPoints(db string, pt uint32, shardID uint64, idxes []int64) ([]string, error) {
	return s.engine.GetShardSplitPoints(db, pt, shardID, idxes)
}

func (s *Storage) RefEngineDbPt(db string, ptId uint32) error {
	return s.engine.DbPTRef(db, ptId)
}

func (s *Storage) UnrefEngineDbPt(db string, ptId uint32) {
	s.engine.DbPTUnref(db, ptId)
}

func (s *Storage) GetShardDownSampleLevel(db string, ptId uint32, shardID uint64) int {
	return s.engine.GetShardDownSampleLevel(db, ptId, shardID)
}

func (s *Storage) CreateLogicPlan(ctx context.Context, db string, ptId uint32, shardID uint64, sources influxql.Sources, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	plan, err := s.engine.CreateLogicalPlan(ctx, db, ptId, shardID, sources, schema.(*executor.QuerySchema))
	return plan, err
}

func (s *Storage) ScanWithSparseIndex(ctx context.Context, db string, ptId uint32, shardIDS []uint64, schema hybridqp.Catalog) (hybridqp.IShardsFragments, error) {
	filesFragments, err := s.engine.ScanWithSparseIndex(ctx, db, ptId, shardIDS, schema.(*executor.QuerySchema))
	return filesFragments, err
}

func (s *Storage) TagValues(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (netstorage.TablesTagSets, error) {

	return s.engine.TagValues(db, ptIDs, tagKeys, condition)
}

func (s *Storage) TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr) (map[string]uint64, error) {
	return s.engine.TagValuesCardinality(db, ptIDs, tagKeys, condition)
}

func (s *Storage) SeriesKeys(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]string, error) {
	ms := stringSlice2BytesSlice(measurements)

	return s.engine.SeriesKeys(db, ptIDs, ms, condition)
}

func (s *Storage) SeriesCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) ([]meta.MeasurementCardinalityInfo, error) {
	ms := stringSlice2BytesSlice(measurements)
	return s.engine.SeriesCardinality(db, ptIDs, ms, condition)
}

func (s *Storage) SendSysCtrlOnNode(req *netstorage.SysCtrlRequest) error {
	return s.engine.SysCtrl(req)
}

func (s *Storage) SeriesExactCardinality(db string, ptIDs []uint32, measurements []string, condition influxql.Expr) (map[string]uint64, error) {
	ms := stringSlice2BytesSlice(measurements)

	return s.engine.SeriesExactCardinality(db, ptIDs, ms, condition)
}

func (s *Storage) GetEngine() netstorage.Engine {
	return s.engine
}

func (s *Storage) PreOffload(ptInfo *meta.DbPtInfo) error {
	return s.engine.PreOffload(ptInfo.Db, ptInfo.Pti.PtId)
}

func (s *Storage) RollbackPreOffload(ptInfo *meta.DbPtInfo) error {
	return s.engine.RollbackPreOffload(ptInfo.Db, ptInfo.Pti.PtId)
}

func (s *Storage) PreAssign(opId uint64, ptInfo *meta.DbPtInfo) error {
	return s.engine.PreAssign(opId, ptInfo.Db, ptInfo.Pti.PtId, ptInfo.Shards, ptInfo.DBBriefInfo, s.metaClient)
}

func (s *Storage) Offload(ptInfo *meta.DbPtInfo) error {
	return s.engine.Offload(ptInfo.Db, ptInfo.Pti.PtId)
}

func (s *Storage) Assign(opId uint64, ptInfo *meta.DbPtInfo) error {
	return s.engine.Assign(opId, ptInfo.Db, ptInfo.Pti.PtId, ptInfo.Pti.Ver, ptInfo.Shards, ptInfo.DBBriefInfo, s.metaClient)
}

func (s *Storage) GetConnId() uint64 {
	return s.node.ConnId
}

func stringSlice2BytesSlice(s []string) [][]byte {
	ret := make([][]byte, 0, len(s))
	for _, name := range s {
		ret = append(ret, []byte(name))
	}
	return ret
}
