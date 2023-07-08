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

package coordinator

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"go.uber.org/zap"
)

// ClusterShardMapper implements a ShardMapper for Remote shards.
type ClusterShardMapper struct {
	//Node   *meta.Node
	Logger *logger.Logger
	// Remote execution timeout
	Timeout time.Duration
	meta.MetaClient
	NetStore  netstorage.Storage
	SeriesKey []byte
}

func (csm *ClusterShardMapper) MapShards(sources influxql.Sources, t influxql.TimeRange, opt query.SelectOptions, condition influxql.Expr) (query.ShardGroup, error) {
	a := &ClusterShardMapping{
		ShardMapper: csm,
		ShardMap:    make(map[Source]map[uint32][]uint64),
		MetaClient:  csm.MetaClient,
		Timeout:     csm.Timeout,
		//Node:        csm.Node,
		NetStore: csm.NetStore,
		Logger:   csm.Logger,
	}

	tmin := time.Unix(0, t.MinTimeNano())
	tmax := time.Unix(0, t.MaxTimeNano())
	if err := csm.mapShards(a, sources, tmin, tmax, condition, &opt); err != nil {
		return nil, err
	}
	a.MinTime, a.MaxTime = tmin, tmax

	return a, nil
}

func (csm *ClusterShardMapper) Close() error {
	return nil
}

func (csm *ClusterShardMapper) GetSeriesKey() []byte {
	return csm.SeriesKey
}

func (csm *ClusterShardMapper) mapMstShards(s *influxql.Measurement, a *ClusterShardMapping, tmin, tmax time.Time, condition influxql.Expr, opt *query.SelectOptions) error {
	source := Source{
		Database:        s.Database,
		RetentionPolicy: s.RetentionPolicy,
	}
	var shardKeyInfo *meta2.ShardKeyInfo
	dbi, err := csm.MetaClient.Database(s.Database)
	if err != nil {
		return err
	}
	if len(dbi.ShardKey.ShardKey) > 0 {
		shardKeyInfo = &dbi.ShardKey
	}
	measurements, err := csm.MetaClient.GetMeasurements(s)
	if err != nil {
		return err
	}
	if len(measurements) == 0 {
		return nil // meta.ErrMeasurementNotFound(s.Name)
	}

	var engineTypes [config.ENGINETYPEEND]bool
	for _, m := range measurements {
		if !engineTypes[m.EngineType] {
			engineTypes[m.EngineType] = true
			// Set engine type for measurement.
			s.EngineType = m.EngineType
		}
	}

	// Retrieve the list of shards for this database. This list of
	// shards is always the same regardless of which measurement we are
	// using.
	if _, ok := a.ShardMap[source]; !ok {
		groups, err := csm.MetaClient.ShardGroupsByTimeRange(s.Database, s.RetentionPolicy, tmin, tmax)
		if err != nil {
			return err
		}

		if len(groups) == 0 {
			a.ShardMap[source] = nil
			return nil
		}

		shardIDsByPtID := make(map[uint32][]uint64)
		for i, g := range groups {
			// ShardGroupsByTimeRange would get all shards with different engine type in the TimeRange,
			// we only need to process shards with engine type in engineTypes or equals to engineType.
			if !engineTypes[g.EngineType] {
				continue
			}
			gTimeRange := influxql.TimeRange{Min: g.StartTime, Max: g.EndTime}
			if i == 0 {
				a.ShardsTimeRage = gTimeRange
			} else {
				if a.ShardsTimeRage.Min.After(gTimeRange.Min) {
					a.ShardsTimeRage.Min = gTimeRange.Min
				}
				if gTimeRange.Max.After(a.ShardsTimeRage.Max) {
					a.ShardsTimeRage.Max = gTimeRange.Max
				}
			}

			if shardKeyInfo == nil {
				shardKeyInfo = measurements[0].GetShardKey(groups[i].ID)
			}
			var aliveShardIdxes []int
			if !config.GetHaEnable() {
				aliveShardIdxes = csm.MetaClient.GetAliveShards(s.Database, &groups[i])
			} else {
				// all shards to query
				aliveShardIdxes = make([]int, len(groups[i].Shards))
			}
			var shs []meta2.ShardInfo
			if opt.HintType == hybridqp.FullSeriesQuery || opt.HintType == hybridqp.SpecificSeriesQuery {
				shs, csm.SeriesKey = groups[i].TargetShardsHintQuery(measurements[0], shardKeyInfo, condition, opt, aliveShardIdxes)
			} else {
				shs = groups[i].TargetShards(measurements[0], shardKeyInfo, condition, aliveShardIdxes)
			}

			for shIdx := range shs {
				var ptID uint32
				if len(shs[shIdx].Owners) > 0 {
					ptID = shs[shIdx].Owners[rand.Intn(len(shs[shIdx].Owners))]
				} else {
					csm.Logger.Warn("shard has no owners", zap.Uint64("shardID", shs[shIdx].ID))
					continue
				}
				shardIDsByPtID[ptID] = append(shardIDsByPtID[ptID], shs[shIdx].ID)
			}
		}
		a.ShardMap[source] = shardIDsByPtID
	}
	return nil
}

func (csm *ClusterShardMapper) mapShards(a *ClusterShardMapping, sources influxql.Sources, tmin, tmax time.Time, condition influxql.Expr, opt *query.SelectOptions) error {
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			if err := csm.mapMstShards(s, a, tmin, tmax, condition, opt); err != nil {
				return err
			}
		case *influxql.SubQuery:
			if err := csm.mapShards(a, s.Statement.Sources, tmin, tmax, condition, opt); err != nil {
				return err
			}
		case *influxql.Join:
			if err := csm.mapShards(a, influxql.Sources{s.LSrc}, tmin, tmax, condition, opt); err != nil {
				return err
			}
			if err := csm.mapShards(a, influxql.Sources{s.RSrc}, tmin, tmax, condition, opt); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClusterShardMapping maps data sources to a list of shard information.
type ClusterShardMapping struct {
	//Node        *meta.Node
	ShardMapper *ClusterShardMapper
	NetStore    netstorage.Storage

	MetaClient meta.MetaClient

	// Remote execution timeout
	Timeout time.Duration

	ShardMap map[Source]map[uint32][]uint64

	// MinTime is the minimum time that this shard mapper will allow.
	// Any attempt to use a time before this one will automatically result in using
	// this time instead.
	MinTime time.Time

	// MaxTime is the maximum time that this shard mapper will allow.
	// Any attempt to use a time after this one will automatically result in using
	// this time instead.
	MaxTime time.Time

	ShardsTimeRage influxql.TimeRange
	Logger         *logger.Logger
}

func (csm *ClusterShardMapping) ShardsTimeRange() influxql.TimeRange {
	return csm.ShardsTimeRage
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

func (csm *ClusterShardMapping) GetShardAndSourcesMap(sources influxql.Sources) (map[uint64]map[uint32][]uint64, map[uint32]influxql.Sources, error) {
	shardsMapByNode := make(map[uint64]map[uint32][]uint64) // {"nodeId": {"ptId": []shardId } }
	sourcesMapByPtId := make(map[uint32]influxql.Sources)   // {"ptId": influxql.Sources }
	for _, src := range sources {
		switch src := src.(type) {
		case *influxql.Measurement:
			source := Source{
				Database:        src.Database,
				RetentionPolicy: src.RetentionPolicy,
			}
			shardIDsByDBPT := csm.ShardMap[source]
			if shardIDsByDBPT == nil {
				continue
			}

			ptView, err := csm.MetaClient.DBPtView(source.Database)
			if err != nil {
				return nil, nil, err
			}
			for pId, sIds := range shardIDsByDBPT {
				nodeID := ptView[pId].Owner.NodeID
				if _, ok := shardsMapByNode[nodeID]; !ok {
					shardIDsByPtID := make(map[uint32][]uint64)
					sourcesMapByPtId[pId] = make(influxql.Sources, 0, 1)
					shardIDsByPtID[pId] = sIds
					shardsMapByNode[nodeID] = shardIDsByPtID
					sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
				} else {
					shardsMapByNode[nodeID][pId] = sIds
					sourcesMapByPtId[pId] = append(sourcesMapByPtId[pId], src)
				}
			}
		case *influxql.SubQuery:
			panic("subquery is not supported.")
		default:
			panic("unknown measurement.")
		}
	}
	return shardsMapByNode, sourcesMapByPtId, nil
}

func (csm *ClusterShardMapping) RemoteQueryETraitsAndSrc(ctx context.Context, opts *query.ProcessorOptions, schema hybridqp.Catalog,
	shardsMapByNode map[uint64]map[uint32][]uint64, sourcesMapByPtId map[uint32]influxql.Sources) ([]hybridqp.Trait, error) {
	eTraits := make([]hybridqp.Trait, 0, len(shardsMapByNode))
	var muList = sync.Mutex{}
	var errs error
	once := sync.Once{}
	wg := sync.WaitGroup{}
	for nodeID, shardsByPtId := range shardsMapByNode {
		for pId, sIds := range shardsByPtId {
			wg.Add(1)
			go func(nodeID uint64, ptID uint32, shardIDs []uint64) {
				defer wg.Done()
				src := sourcesMapByPtId[ptID]
				rq, err := csm.makeRemoteQuery(ctx, src, *opts, nodeID, ptID, shardIDs)
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
			schema.Options().(*query.ProcessorOptions).Query = qDuration.Query
			start := time.Now()
			defer func() {
				qDuration.AddDuration("LocalIteratorDuration", time.Since(start).Nanoseconds())
			}()
		}
	}
	opts, _ := schema.Options().(*query.ProcessorOptions)
	shardsMapByNode, sourcesMapByPtId, err := csm.GetShardAndSourcesMap(sources)
	if err != nil {
		return nil, err
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

	if sources != nil && sources.HaveOnlyCSStore() {
		return CreateColumnStorePlan(schema, eTraits, builder)
	}

	// push down to chunk reader.
	plan, pErr = builder.CreateSeriesPlan()
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateMeasurementPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	//todo:create scanner plan
	plan, pErr = builder.CreateScanPlan(plan)
	if pErr != nil {
		return nil, pErr
	}

	plan, pErr = builder.CreateShardPlan(plan)
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
	nodeID uint64, ptID uint32, shardIDs []uint64) (*executor.RemoteQuery, error) {
	m, ok := src[0].(*influxql.Measurement)
	if !ok {
		return nil, fmt.Errorf("invalid sources, exp: *influxql.Measurement, got: %s", reflect.TypeOf(src[0]))
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

	transport.NewNodeManager().Add(nodeID, node.TCPHost)
	rq := &executor.RemoteQuery{
		Database: m.Database,
		PtID:     ptID,
		NodeID:   nodeID,
		ShardIDs: shardIDs,
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
