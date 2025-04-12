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

package coordinator

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

const (
	MaxConcurrencyInOnePt int = 8
)

// ClusterShardMapper implements a ShardMapper for Remote shards.
type ClusterShardMapper struct {
	//Node   *meta.Node
	Logger *logger.Logger
	// Remote execution timeout
	Timeout time.Duration
	meta.MetaClient
	NetStore netstorage.Storage
}

func (csm *ClusterShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions, condition influxql.Expr) (query.ShardGroup, error) {
	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	csming := NewClusterShardMapping(csm, tmin, tmax)
	if err := csm.mapShards(csming, sources, tmin, tmax, condition, &opt); err != nil {
		return nil, err
	}
	return csming, nil
}

func (csm *ClusterShardMapper) Close() error {
	return nil
}

func (csm *ClusterShardMapper) mapMstShards(s *influxql.Measurement, csming *ClusterShardMapping, tmin, tmax time.Time, condition influxql.Expr, opt *query.SelectOptions) error {
	source, shardKeyInfo, measurements, engineTypes, err := csm.getTargetShardMsg(s)
	if err != nil {
		return err
	}

	// Retrieve the list of shards for this database. This list of
	// shards is always the same regardless of which measurement we are
	// using.
	if _, ok := csming.ShardMap[source]; !ok {
		groups, err := csm.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
		if err != nil {
			return err
		}
		if len(groups) == 0 {
			csming.ShardMap[source] = nil
			return nil
		}
		shardInfosByPtID := make(map[uint32][]executor.ShardInfo)
		//firstSetTimeRange := true
		for i, g := range groups {
			// ShardGroupsByTimeRange would get all shards with different engine type in the TimeRange,
			// we only need to process shards with engine type in engineTypes or equals to engineType.
			if !engineTypes[g.EngineType] {
				continue
			}
			if shardKeyInfo == nil {
				shardKeyInfo = measurements[0].GetShardKey(groups[i].ID)
			}
			aliveShardIdxes := csm.MetaClient.GetAliveShards(s.Database, &groups[i], true)
			var shs []meta2.ShardInfo
			if opt.HintType == hybridqp.FullSeriesQuery || opt.HintType == hybridqp.SpecificSeriesQuery {
				shs, csming.seriesKey = groups[i].TargetShardsHintQuery(measurements[0], shardKeyInfo, condition, opt, aliveShardIdxes)
			} else {
				shs = groups[i].TargetShards(measurements[0], shardKeyInfo, condition, aliveShardIdxes)
			}

			csm.updateShardInfosByPtID(s, g, shs, &shardInfosByPtID)
		}
		csming.ShardMap[source] = shardInfosByPtID
	}
	return nil
}

func (csm *ClusterShardMapper) updateShardInfosByPtID(s *influxql.Measurement, sg meta2.ShardGroupInfo, shs []meta2.ShardInfo,
	shardInfosByPtID *map[uint32][]executor.ShardInfo) {
	var ptID uint32
	for shIdx := range shs {
		if len(shs[shIdx].Owners) > 0 {
			ptID = shs[shIdx].Owners[0]
		} else {
			csm.Logger.Warn("shard has no owners", zap.Uint64("shardID", shs[shIdx].ID))
			continue
		}
		shardPath := ""
		if s.EngineType == config.COLUMNSTORE {
			shardPath = obs.GetShardPath(shs[shIdx].ID, shs[shIdx].IndexID, ptID, sg.StartTime, sg.EndTime, s.Database, s.RetentionPolicy)
		}
		(*shardInfosByPtID)[ptID] = append((*shardInfosByPtID)[ptID], executor.ShardInfo{ID: shs[shIdx].ID, Path: shardPath, Version: sg.Version})
	}
}

func (csm *ClusterShardMapper) getTargetShardMsg(s *influxql.Measurement) (Source, *meta2.ShardKeyInfo, []*meta2.MeasurementInfo, [config.ENGINETYPEEND]bool, error) {
	source := Source{
		Database:        s.Database,
		RetentionPolicy: s.RetentionPolicy,
	}
	var shardKeyInfo *meta2.ShardKeyInfo
	var engineTypes [config.ENGINETYPEEND]bool
	dbi, err := csm.MetaClient.Database(s.Database)
	if err != nil {
		return source, nil, nil, engineTypes, err
	}
	if len(dbi.ShardKey.ShardKey) > 0 {
		shardKeyInfo = &dbi.ShardKey
	}
	measurements, err := csm.MetaClient.GetMeasurements(s)
	if err != nil || len(measurements) == 0 {
		return source, nil, nil, engineTypes, err
	}
	for _, m := range measurements {
		if !engineTypes[m.EngineType] {
			engineTypes[m.EngineType] = true
			// Set engine type for measurement.
			s.EngineType = m.EngineType
			s.IndexRelation = &m.IndexRelation
			s.ObsOptions = m.ObsOptions
			s.IsTimeSorted = m.IsTimeSorted()
		}
	}
	return source, shardKeyInfo, measurements, engineTypes, nil
}

func (csm *ClusterShardMapper) mapShards(csming *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time, condition influxql.Expr, opt *query.SelectOptions) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			if err := csm.mapMstShards(s, csming, tmin, tmax, condition, opt); err != nil {
				return err
			}
		case *influxql.SubQuery:
			if err := csm.mapShards(csming, s.Statement.Sources, tmin, tmax, condition, opt); err != nil {
				return err
			}
		case *influxql.Join:
			if err := csm.mapShards(csming, influxql.Sources{s.LSrc}, tmin, tmax, condition, opt); err != nil {
				return err
			}
			if err := csm.mapShards(csming, influxql.Sources{s.RSrc}, tmin, tmax, condition, opt); err != nil {
				return err
			}
		case *influxql.BinOp:
			err := csm.mapShards(csming, influxql.Sources{s.LSrc}, tmin, tmax, condition, opt)
			if errno.Equal(err, errno.ErrMeasurementNotFound) && influxql.AllowNilMst(s.OpType) {
				s.NilMst = influxql.LNilMst
			} else if err != nil {
				return err
			}
			err = csm.mapShards(csming, influxql.Sources{s.RSrc}, tmin, tmax, condition, opt)
			if errno.Equal(err, errno.ErrMeasurementNotFound) && influxql.AllowNilMst(s.OpType) {
				if s.NilMst == influxql.LNilMst {
					return err
				} else {
					s.NilMst = influxql.RNilMst
				}
			} else if err != nil {
				return err
			}
		}
	}
	if in, ok := condition.(*influxql.InCondition); ok {
		if err := csm.mapShards(csming, in.Stmt.Sources, tmin, tmax, in.Stmt.Condition, opt); err != nil {
			return err
		}
	}
	return nil
}

// ClusterShardMapping maps data sources to a list of shard information.
type ClusterShardMapping struct {
	ShardMapper *ClusterShardMapper
	NetStore    netstorage.Storage

	MetaClient meta.MetaClient

	// Remote execution timeout
	Timeout time.Duration

	ShardMap map[Source]map[uint32][]executor.ShardInfo // {source: {ptId: []ShardInfo}},

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time

	// use for spec or full series hint query
	seriesKey []byte
	Logger    *logger.Logger
}

func NewClusterShardMapping(csm *ClusterShardMapper, tmin, tmax time.Time) *ClusterShardMapping {
	csming := &ClusterShardMapping{
		ShardMapper: csm,
		ShardMap:    make(map[Source]map[uint32][]executor.ShardInfo),
		MetaClient:  csm.MetaClient,
		Timeout:     csm.Timeout,
		NetStore:    csm.NetStore,
		Logger:      csm.Logger,
		MinTime:     tmin,
		MaxTime:     tmax,
		seriesKey:   make([]byte, 0),
	}
	return csming
}

func (csm *ClusterShardMapping) GetSeriesKey() []byte {
	return csm.seriesKey
}

func (csm *ClusterShardMapping) NodeNumbers() int {
	nods, _ := csm.MetaClient.DataNodes()
	if len(nods) == 0 {
		return 1
	}
	return len(nods)
}

func (csm *ClusterShardMapping) getSchema(database string, retentionPolicy string, mst string) (map[string]int32, map[string]struct{}, error) {
	startTime := time.Now()

	var metaFields map[string]int32
	var metaDimensions map[string]struct{}
	var err error

	for {
		metaFields, metaDimensions, err = csm.MetaClient.Schema(database, retentionPolicy, mst)
		if err != nil {
			if IsRetriedError(err) {
				if time.Since(startTime).Seconds() < DMLTimeOutSecond {
					csm.Logger.Warn("retry get schema", zap.String("database", database), zap.String("measurement", mst),
						zap.String("shardMapping", "cluster"))
					time.Sleep(DMLRetryInternalMillisecond * time.Millisecond)
					continue
				} else {
					panic(err)
				}
			} else {
				csm.Logger.Warn("get field schema failed from metaClient", zap.String("database", database),
					zap.String("measurement", mst), zap.Any("err", err), zap.String("shardMapping", "cluster"))
				return nil, nil, fmt.Errorf("get schema failed")
			}
		}
		break
	}
	return metaFields, metaDimensions, err
}

func IsRetriedError(err error) (isSpecial bool) {
	if errno.Equal(err, errno.PtNotFound) ||
		errno.Equal(err, errno.DBPTClosed) ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "read message type: EOF") ||
		strings.Contains(err.Error(), "write: connection timed out") {
		return true
	}
	return false
}

func (csm *ClusterShardMapping) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, schema *influxql.Schema, err error) {
	source := Source{
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	}

	shardIDsByNodeID := csm.ShardMap[source]
	if shardIDsByNodeID == nil {
		return nil, nil, nil, nil
	}
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	schema = &influxql.Schema{MinTime: math.MaxInt64, MaxTime: math.MinInt64}
	measurements, err := csm.MetaClient.GetMeasurements(m)
	if err != nil {
		return nil, nil, nil, err
	}
	for i := range measurements {
		var metaFields map[string]int32
		var metaDimensions map[string]struct{}
		metaFields, metaDimensions, err = csm.getSchema(m.Database, m.RetentionPolicy, measurements[i].OriginName())
		if err != nil {
			return nil, nil, nil, err
		}
		if metaFields == nil && metaDimensions == nil {
			continue
		}
		for k, ty := range metaFields {
			fields[k] = record.ToInfluxqlTypes(int(ty))
		}
		for k := range metaDimensions {
			dimensions[k] = struct{}{}
		}
	}

	return
}

func (csm *ClusterShardMapping) MapType(m *influxql.Measurement, field string) influxql.DataType {
	measurements, err := csm.MetaClient.GetMeasurements(m)
	if err != nil {
		return influxql.Unknown
	}

	for i := range measurements {
		metaFields, metaDimensions, err := csm.getSchema(m.Database, m.RetentionPolicy, measurements[i].OriginName())
		if err != nil {
			return influxql.Unknown
		}
		for k, ty := range metaFields {
			if k == field {
				return record.ToInfluxqlTypes(int(ty))
			}
		}
		for k := range metaDimensions {
			if k == field {
				return influxql.Tag
			}
		}
	}
	return influxql.Unknown
}

func (csm *ClusterShardMapping) MapTypeBatch(m *influxql.Measurement, fields map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	measurements, err := csm.MetaClient.GetMeasurements(m)
	if err != nil {
		return err
	}
	for i := range measurements {
		metaFields, metaDimensions, err := csm.getSchema(m.Database, m.RetentionPolicy, measurements[i].OriginName())
		if err != nil {
			return err
		}

		for k := range fields {
			hasMstInfo := false
			var preField string
			if strings.HasPrefix(k, measurements[i].Name+".") {
				_, ftOk := metaFields[k]
				_, dtOK := metaDimensions[k]
				if ftOk || dtOK {
					hasMstInfo = false
				} else {
					hasMstInfo = true
					preField = k
					k = k[len(measurements[i].Name)+1:]
				}

			}
			ft, ftOk := metaFields[k]
			_, dtOK := metaDimensions[k]

			if !(ftOk || dtOK) {
				fields[k].DataType = influxql.Unknown
				continue
			}

			if ftOk && dtOK {
				return fmt.Errorf("column (%s) in measurement (%s) in both fields and tags", k, measurements[i].Name)
			}
			shardMapperExprRewriter(ftOk, hasMstInfo, fields, k, preField, ft)
		}
	}
	return nil
}

func (csm *ClusterShardMapping) distShardsToOwnerNodes(src *influxql.Measurement, shardInfosByDBPT map[uint32][]executor.ShardInfo, shardsMapByNode map[uint64]map[uint32][]executor.ShardInfo,
	sourcesMapByPtId map[uint32]influxql.Sources) (map[uint64]map[uint32][]executor.ShardInfo, map[uint32]influxql.Sources, error) {
	ptView, err := csm.MetaClient.DBPtView(src.Database)
	if err != nil {
		return nil, nil, err
	}
	for pId, shardInfos := range shardInfosByDBPT {
		nodeID := ptView[pId].Owner.NodeID
		if _, ok := shardsMapByNode[nodeID]; !ok {
			shardInfosByPtID := make(map[uint32][]executor.ShardInfo)
			sourcesMapByPtId[pId] = make(influxql.Sources, 0, 1)
			shardInfosByPtID[pId] = shardInfos
			shardsMapByNode[nodeID] = shardInfosByPtID
			sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
		} else {
			shardsMapByNode[nodeID][pId] = shardInfos
			sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
		}
	}
	return shardsMapByNode, sourcesMapByPtId, nil
}

func (csm *ClusterShardMapping) distShardsToReadNodes(src *influxql.Measurement, shardInfosByDBPT map[uint32][]executor.ShardInfo, shardsMapByNode map[uint64]map[uint32][]executor.ShardInfo,
	sourcesMapByPtId map[uint32]influxql.Sources) (map[uint64]map[uint32][]executor.ShardInfo, map[uint32]influxql.Sources, error) {
	nodes, err := csm.MetaClient.AliveReadNodes()
	if err != nil {
		return nil, nil, err
	}
	// sort ptids
	pIds := make([]uint32, 0, len(shardInfosByDBPT))
	for pId := range shardInfosByDBPT {
		pIds = append(pIds, pId)
	}
	sort.SliceStable(pIds, func(i, j int) bool { return pIds[i] < pIds[j] })

	// distribute shards to readNode in order
	step := uint64(len(nodes))
	pos := xxhash.Sum64String(src.Database+src.RetentionPolicy+src.Name) % step
	for _, pId := range pIds {
		shardInfos := shardInfosByDBPT[pId]
		pos++
		pos = pos % step
		nodeID := nodes[pos].ID
		// nodeId ->  map[pId][]ShardInfo
		if _, ok := shardsMapByNode[nodeID]; !ok {
			shardInfosByPtID := make(map[uint32][]executor.ShardInfo)
			sourcesMapByPtId[pId] = make(influxql.Sources, 0, 1)
			shardInfosByPtID[pId] = shardInfos
			shardsMapByNode[nodeID] = shardInfosByPtID
			sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
		} else {
			shardsMapByNode[nodeID][pId] = shardInfos
			sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
		}
	}
	return shardsMapByNode, sourcesMapByPtId, nil
}

func (csm *ClusterShardMapping) GetShardAndSourcesMap(sources influxql.Sources) (map[uint64]map[uint32][]executor.ShardInfo, map[uint32]influxql.Sources, error) {
	shardsMapByNode := make(map[uint64]map[uint32][]executor.ShardInfo) // {"nodeId": {"ptId": []ShardInfo } }
	sourcesMapByPtId := make(map[uint32]influxql.Sources)               // {"ptId": influxql.Sources }
	var err error
	for _, src := range sources {
		switch src := src.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        src.Database,
				RetentionPolicy: src.RetentionPolicy,
			}
			shardInfosByDBPT := csm.ShardMap[source]
			if shardInfosByDBPT == nil {
				continue
			}
			if !src.IsRWSplit() {
				shardsMapByNode, sourcesMapByPtId, err = csm.distShardsToOwnerNodes(src, shardInfosByDBPT, shardsMapByNode, sourcesMapByPtId)
			} else {
				shardsMapByNode, sourcesMapByPtId, err = csm.distShardsToReadNodes(src, shardInfosByDBPT, shardsMapByNode, sourcesMapByPtId)
			}
			if err != nil {
				return nil, nil, err
			}
		case *influxql.SubQuery:
			return nil, nil, fmt.Errorf("subquery is not supported in logical plan creation")
		default:
			return nil, nil, fmt.Errorf("unknown source in logical plan creation")
		}
	}
	return shardsMapByNode, sourcesMapByPtId, nil
}

func distShardsByMaxConcurrency(ptID uint32, shardInfos []executor.ShardInfo, ascending bool) []executor.PtQuery {
	ptQuerysLen := MaxConcurrencyInOnePt
	if len(shardInfos) < MaxConcurrencyInOnePt {
		ptQuerysLen = len(shardInfos)
	}
	if ptQuerysLen == 0 {
		return nil
	}

	shardsNumInPt := len(shardInfos) / ptQuerysLen
	ptQuerys := make([]executor.PtQuery, 0, ptQuerysLen)
	for i := 0; i < len(shardInfos); i++ {
		j := i % ptQuerysLen
		if j >= len(ptQuerys) {
			ptQuerys = append(ptQuerys, executor.PtQuery{
				PtID:       ptID,
				ShardInfos: make([]executor.ShardInfo, 0, shardsNumInPt),
			})
		}
		if ascending {
			ptQuerys[j].ShardInfos = append(ptQuerys[j].ShardInfos, shardInfos[i])
		} else {
			ptQuerys[j].ShardInfos = append(ptQuerys[j].ShardInfos, shardInfos[len(shardInfos)-1-i])
		}
	}

	return ptQuerys
}

func (csm *ClusterShardMapping) RemoteQueryETraitsAndSrc(ctx context.Context, opts *query.ProcessorOptions, schema hybridqp.Catalog,
	shardsMapByNode map[uint64]map[uint32][]executor.ShardInfo, sourcesMapByPtId map[uint32]influxql.Sources) ([]hybridqp.Trait, error) {
	eTraits := make([]hybridqp.Trait, 0, len(shardsMapByNode))
	var muList = sync.Mutex{}
	var errs error
	once := sync.Once{}
	wg := sync.WaitGroup{}
	for nodeID, shardsByPtId := range shardsMapByNode {
		for pId, sIds := range shardsByPtId {
			wg.Add(1)
			go func(nodeID uint64, ptID uint32, shardInfos []executor.ShardInfo) {
				defer wg.Done()
				src := sourcesMapByPtId[ptID]

				var err error
				var rq *executor.RemoteQuery
				if src != nil && src.HaveOnlyCSStore() && src.IsUnifyPlan() {
					ptQuerys := distShardsByMaxConcurrency(ptID, shardInfos, opts.IsAscending())
					rq, err = csm.makeRemoteQuery(ctx, src, *opts, nodeID, ptID, nil, ptQuerys)
				} else {
					rq, err = csm.makeRemoteQuery(ctx, src, *opts, nodeID, ptID, shardInfos, nil)
				}

				if err != nil {
					csm.Logger.Error("failed to createLogicalPlan", zap.Error(err), zap.String("shardMapping", "cluster"))
					if !errno.Equal(err, errno.PtNotFound) {
						once.Do(func() {
							errs = err
						})
					}
					return
				}

				muList.Lock()
				opts.Sources = src
				eTraits = append(eTraits, rq)
				muList.Unlock()
			}(nodeID, pId, sIds)
		}
	}
	wg.Wait()
	if errs != nil {
		return nil, errs
	}
	if schema.Options().(*query.ProcessorOptions).Sources == nil {
		return nil, nil
	}
	return eTraits, nil
}

func (csm *ClusterShardMapping) GetETraits(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) ([]hybridqp.Trait, error) {
	ctxValue := ctx.Value(query.QueryDurationKey)
	if ctxValue != nil {
		qDuration, ok := ctxValue.(*statistics.SQLSlowQueryStatistics)
		if ok && qDuration != nil {
			schema.Options().(*query.ProcessorOptions).Query = qDuration.GetQueryByStmtId(schema.Options().GetStmtId())
			start := time.Now()
			defer func() {
				qDuration.AddDuration("LocalIteratorDuration", time.Since(start).Nanoseconds())
			}()
		}
	}
	opts, _ := schema.Options().(*query.ProcessorOptions)
	if c, ok := ctx.Value(query.QueryIDKey).([]uint64); ok {
		opts.QueryId = c[schema.Options().GetStmtId()]
	}
	shardsMapByNode, sourcesMapByPtId, err := csm.GetShardAndSourcesMap(sources)
	if err != nil {
		return nil, err
	}
	// The time ranges of the left and right binaryOperator of the promql may be different.
	// The time ranges of the left and right sub-forms cannot be intersected with the outer time.
	if opts.IsPromQuery() {
		return csm.RemoteQueryETraitsAndSrc(ctx, opts, schema, shardsMapByNode, sourcesMapByPtId)
	}
	// Override the time constraints if they don't match each other.
	if !csm.MinTime.IsZero() && opts.StartTime < csm.MinTime.UnixNano() {
		opts.StartTime = csm.MinTime.UnixNano()
	}
	if !csm.MaxTime.IsZero() && opts.EndTime > csm.MaxTime.UnixNano() {
		opts.EndTime = csm.MaxTime.UnixNano()
	}
	return csm.RemoteQueryETraitsAndSrc(ctx, opts, schema, shardsMapByNode, sourcesMapByPtId)
}

func (csm *ClusterShardMapping) CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	eTraits, err := csm.GetETraits(ctx, sources, schema)
	if eTraits == nil || err != nil {
		return nil, err
	}
	var plan hybridqp.QueryNode
	var pErr error

	builder := executor.NewLogicalPlanBuilderImpl(schema)

	// query plan for the colstore
	if sources != nil && sources.HaveOnlyCSStore() {
		if !schema.Options().IsUnifyPlan() {
			return CreateColumnStorePlan(schema, eTraits, builder)
		}

		// generate plan of the hybrid store cursors
		plan, pErr = builder.CreateColStoreCursorPlan()
		if pErr != nil {
			return nil, pErr
		}

		// generate plan of the hybrid store reader
		plan, pErr = builder.CreateColStoreReaderPlan(plan)
		if pErr != nil {
			return nil, pErr
		}
	} else {
		// query plan for the tsstore
		// push down to chunk reader.
		plan, pErr = builder.CreateSeriesPlan()
		if pErr != nil {
			return nil, pErr
		}

		plan, pErr = builder.CreateMeasurementPlan(plan)
		if pErr != nil {
			return nil, pErr
		}
	}

	//todo:create scanner plan
	plan, pErr = builder.CreateScanPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	if sources != nil && sources.HaveOnlyCSStore() && schema.Options().IsUnifyPlan() {
		plan, pErr = builder.CreatePartitionPlan(plan)
	} else {
		plan, pErr = builder.CreateShardPlan(plan)
	}
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateNodePlan(plan, eTraits)
	if pErr != nil {
		return nil, pErr
	}

	return plan.(executor.LogicalPlan), pErr
}

func (csm *ClusterShardMapping) makeRemoteQuery(ctx context.Context, src influxql.Sources, opt query.ProcessorOptions,
	nodeID uint64, ptID uint32, shardInfos []executor.ShardInfo, ptQuerys []executor.PtQuery) (*executor.RemoteQuery, error) {
	m, ok := src[0].(*influxql.Measurement)
	if !ok {
		return nil, fmt.Errorf("invalid sources, exp: *influxql.Measurement, got: %s", reflect.TypeOf(src[0]))
	}

	shardIDs := make([]uint64, len(shardInfos))
	for i := range shardInfos {
		shardIDs[i] = shardInfos[i].ID
	}
	opt.Sources = src

	analyze := false
	if span := tracing.SpanFromContext(ctx); span != nil {
		analyze = true
	}

	node, err := csm.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}
	if node.SegregateStatus != meta2.Normal {
		return nil, fmt.Errorf("makeRemoteQuery error: nodeid %d is Segerate", node.ID)
	}
	transport.NewNodeManager().Add(nodeID, node.TCPHost)
	rq := &executor.RemoteQuery{
		Database: m.Database,
		PtID:     ptID,
		NodeID:   nodeID,
		ShardIDs: shardIDs,
		PtQuerys: ptQuerys,
		Opt:      opt,
		Analyze:  analyze,
		Node:     nil,
	}
	return rq, nil
}

func (csm *ClusterShardMapping) LogicalPlanCost(m *influxql.Measurement, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	return hybridqp.LogicalPlanCost{}, nil
}

// Close clears out the list of mapped shards.
func (csm *ClusterShardMapping) Close() error {
	csm.ShardMap = nil
	return nil
}

// there are multi source return when one source input because measurement regex
func (csm *ClusterShardMapping) GetSources(sources influxql.Sources) influxql.Sources {
	var srcs influxql.Sources
	for _, src := range sources {
		switch src := src.(type) {
		case *influxql.Measurement:
			measurements, err := csm.MetaClient.GetMeasurements(src)
			if err != nil {
				return nil
			}
			for i := range measurements {
				clone := src.Clone()
				clone.Regex = nil
				clone.Name = measurements[i].Name
				srcs = append(srcs, clone)
			}
		case *influxql.SubQuery:
			srcs = append(srcs, src)
		default:
			panic("unknown measurement.")
		}
	}
	return srcs
}

// Source contains the database and retention policy source for data.
type Source struct {
	Database        string
	RetentionPolicy string
}

func shardMapperExprRewriter(ftOk, hasMstInfo bool, fields map[string]*influxql.FieldNameSpace, k, preField string, ft int32) {
	if ftOk {
		if hasMstInfo {
			fields[preField].DataType = record.ToInfluxqlTypes(int(ft))
			fields[preField].RealName = k
		} else {
			fields[k].DataType = record.ToInfluxqlTypes(int(ft))
		}
	} else {
		fields[k].DataType = influxql.Tag
	}
}

func CreateColumnStorePlan(schema hybridqp.Catalog, eTraits []hybridqp.Trait, builder *executor.LogicalPlanBuilderImpl) (hybridqp.QueryNode, error) {
	plan, pErr := builder.CreateSegmentPlan(schema)
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateSparseIndexScanPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateNodePlan(plan, eTraits)
	if pErr != nil {
		return nil, pErr
	}
	return plan.(executor.LogicalPlan), pErr
}
