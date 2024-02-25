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
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const MaxShardKey = 64 * 1024

var injestionCtxPool sync.Pool

func getInjestionCtx() *injestionCtx {
	v := injestionCtxPool.Get()
	if v == nil {
		return &injestionCtx{
			streamInfos:            []*meta2.StreamInfo{},
			streamDBs:              []*meta2.DatabaseInfo{},
			streamMSTs:             []*meta2.MeasurementInfo{},
			streamShardKeyInfos:    []*meta2.ShardKeyInfo{},
			streamWriteHelpers:     []*writeHelper{},
			streamAliveShardIdxes:  [][]int{},
			srcStreamDstShardIdMap: map[uint64]map[uint64]uint64{},
			mstShardIdRowMap:       map[string]map[uint64]*[]*influx.Row{}}
	}
	return v.(*injestionCtx)
}

func putInjestionCtx(s *injestionCtx) {
	s.Reset()
	injestionCtxPool.Put(s)
}

type PWMetaClient interface {
	Database(name string) (di *meta2.DatabaseInfo, err error)
	RetentionPolicy(database, policy string) (*meta2.RetentionPolicyInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time, version uint32, engineType config.EngineType) (*meta2.ShardGroupInfo, error)
	DBPtView(database string) (meta2.DBPtInfos, error)
	Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
	UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error
	CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *influxql.IndexRelation, engineType config.EngineType,
		colStoreInfo *meta2.ColStoreInfo, schemaInfo []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error)
	GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int
	GetStreamInfos() map[string]*meta2.StreamInfo
	GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool
	DBRepGroups(database string) []meta2.ReplicaGroup
	GetReplicaN(database string) (int, error)
}

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	// rows must be within this time range
	timeRange *util.TimeRange
	signal    chan struct{}

	timeout    time.Duration
	MetaClient PWMetaClient

	TSDBStore TSDBStore

	logger *logger.Logger
}

// NewPointsWriter returns a new instance of PointsWriter for a node.
func NewPointsWriter(timeout time.Duration) *PointsWriter {
	return &PointsWriter{
		signal:  make(chan struct{}),
		timeout: timeout,
		logger:  logger.NewLogger(errno.ModuleCoordinator),
	}
}

type ShardRow struct {
	shardInfo *meta2.ShardInfo
	rows      []*influx.Row
}
type ShardRows []ShardRow

func (srs ShardRows) Len() int           { return len(srs) }
func (srs ShardRows) Less(i, j int) bool { return srs[i].shardInfo.ID < srs[j].shardInfo.ID }
func (srs ShardRows) Swap(i, j int)      { srs[i], srs[j] = srs[j], srs[i] }

func buildColumnToIndex(r *influx.Row) {
	if r.ColumnToIndex == nil {
		r.ColumnToIndex = make(map[string]int)
	}
	index := 0
	for i := range r.Tags {
		r.ColumnToIndex[stringinterner.InternSafe(r.Tags[i].Key)] = index
		index++
	}
	for i := range r.Fields {
		r.ColumnToIndex[stringinterner.InternSafe(r.Fields[i].Key)] = index
		index++
	}
	r.ReadyBuildColumnToIndex = true
}

func reserveField(fieldToCreatePool []*proto2.FieldSchema) []*proto2.FieldSchema {
	if cap(fieldToCreatePool) > len(fieldToCreatePool) {
		fieldToCreatePool = fieldToCreatePool[:len(fieldToCreatePool)+1]
	} else {
		fieldToCreatePool = append(fieldToCreatePool, &proto2.FieldSchema{})
	}
	if fieldToCreatePool[len(fieldToCreatePool)-1] == nil {
		fieldToCreatePool[len(fieldToCreatePool)-1] = &proto2.FieldSchema{}
	}
	return fieldToCreatePool
}

func dropFieldByIndex(r *influx.Row, dropFieldIndex []int) {
	totalField := len(r.Fields)
	dropField := len(dropFieldIndex)
	if totalField == dropField {
		r.Fields = r.Fields[:0]
		return
	}

	remainField := make([]influx.Field, totalField-dropField)
	remainIdx, dropIdx, totalIdx := 0, 0, 0
	for totalIdx = 0; totalIdx < totalField && dropIdx < dropField; totalIdx++ {
		if totalIdx < dropFieldIndex[dropIdx] {
			remainField[remainIdx] = r.Fields[totalIdx]
			remainIdx++
		} else {
			dropIdx++
		}
	}

	for totalIdx < totalField {
		remainField[remainIdx] = r.Fields[totalIdx]
		remainIdx++
		totalIdx++
	}

	r.Fields = remainField
}

func dropTagByIndex(r *influx.Row, dropTagIndex []int) {
	totalTags := len(r.Tags)
	dropTags := len(dropTagIndex)
	if totalTags == dropTags {
		r.Tags = r.Tags[:0]
		return
	}

	remainTags := make([]influx.Tag, totalTags-dropTags)
	remainIdx, dropIdx, totalIdx := 0, 0, 0
	for totalIdx = 0; totalIdx < totalTags && dropIdx < dropTags; totalIdx++ {
		if totalIdx < dropTagIndex[dropIdx] {
			remainTags[remainIdx] = r.Tags[totalIdx]
			remainIdx++
		} else {
			dropIdx++
		}
	}

	for totalIdx < totalTags {
		remainTags[remainIdx] = r.Tags[totalIdx]
		remainIdx++
		totalIdx++
	}

	r.Tags = remainTags
}

// fixFields checks and fixes fields: ignore specified time fields
func fixFields(fields influx.Fields) (influx.Fields, error) {
	for i := 0; i < len(fields); i++ {
		if fields[i].Key == "time" {
			fields = append(fields[:i], fields[i+1:]...) // remove the i index item for time field
			i--
			continue
		}
		if i > 0 && fields[i-1].Key == fields[i].Key {
			if fields[i-1].Type != fields[i].Type {
				// same field key, diffrent field type
				return nil, errno.NewError(errno.ParseFieldTypeConflict, fields[i].Key)
			}
			// remove the i-1 index item
			fields = append(fields[:i-1], fields[i:]...)
			// fix the i index
			i--
			continue
		}
	}

	return fields, nil
}

// RetryWritePointRows make sure sql client got the latest metadata.
func (w *PointsWriter) RetryWritePointRows(database, retentionPolicy string, rows []influx.Row) error {
	var err error
	start := time.Now()

	for {
		err = w.writePointRows(database, retentionPolicy, rows)
		if err == nil {
			break
		}
		// if pt is offline, retry to get alive shards to write in write-available-first policy
		if !IsRetryErrorForPtView(err) {
			w.logger.Error("write point rows failed",
				zap.String("db", database),
				zap.String("rp", retentionPolicy),
				zap.Error(err))
			break
		}
		// retry until timeout
		if time.Since(start).Nanoseconds() >= w.timeout.Nanoseconds() {
			w.logger.Error("write point rows timeout",
				zap.String("db", database),
				zap.String("rp", retentionPolicy),
				zap.Error(err))
			break
		}
		w.resetRowsRouter(rows)
		time.Sleep(time.Second)
	}
	return err
}

func (w *PointsWriter) resetRowsRouter(rows []influx.Row) {
	for i := range rows {
		rows[i].Name = influx.GetOriginMstName(rows[i].Name)
		rows[i].ShardKey = rows[i].ShardKey[:0]
		rows[i].StreamId = rows[i].StreamId[:0]
	}
}

func (w *PointsWriter) writePointRows(database, retentionPolicy string, rows []influx.Row) error {
	ctx := getInjestionCtx()
	defer putInjestionCtx(ctx)
	ctx.writeHelper = newWriteHelper(w)

	err := ctx.checkDBRP(database, retentionPolicy, w)
	if err != nil {
		return err
	}

	if retentionPolicy == "" {
		retentionPolicy = ctx.db.DefaultRetentionPolicy
	}

	dstSis := ctx.getDstSis()
	exist := w.MetaClient.GetDstStreamInfos(database, retentionPolicy, dstSis)
	if exist {
		err = ctx.initStreamVar(w)
		if err != nil {
			return err
		}
	}

	partialErr, dropped, err := w.routeAndMapOriginRows(database, retentionPolicy, rows, ctx)
	if err != nil {
		return err
	}

	start := time.Now()
	if len(*ctx.getDstSis()) > 0 {
		err = w.routeAndCalculateStreamRows(ctx)
		if err != nil {
			return err
		}
	}
	atomic.AddInt64(&statistics.HandlerStat.WriteStreamRoutineDuration, time.Since(start).Nanoseconds())

	start = time.Now()
	err = w.writeShardMap(database, retentionPolicy, ctx)
	atomic.AddInt64(&statistics.HandlerStat.WriteStoresDuration, time.Since(start).Nanoseconds())

	if err != nil {
		if errno.Equal(err, errno.ErrorTagArrayFormat, errno.WriteErrorArray, errno.SeriesLimited) {
			return netstorage.PartialWriteError{Reason: err, Dropped: dropped}
		}
		return err
	}
	if partialErr != nil {
		return netstorage.PartialWriteError{Reason: partialErr, Dropped: dropped}
	}
	return partialErr
}

func (w *PointsWriter) writeShardMap(database, retentionPolicy string, ctx *injestionCtx) error {
	shardRowMap := ctx.getShardRowMap()
	var err error
	var mutex sync.Mutex
	var wg sync.WaitGroup
	var writeCtx *netstorage.WriteContext

	wg.Add(shardRowMap.Len())
	for i := range shardRowMap {
		writeCtx = ctx.allocWriteContext(shardRowMap[i].shardInfo, shardRowMap[i].rows)

		// get the streamId and dstShardId that is associated with the srcShardId.
		if streamDstShardIdMap, ok := ctx.getSrcStreamDstShardIdMap()[shardRowMap[i].shardInfo.ID]; ok {
			for streamId, dstShardId := range streamDstShardIdMap {
				writeCtx.StreamShards = append(writeCtx.StreamShards, streamId, dstShardId)
			}
		}

		go func(wCtx *netstorage.WriteContext) {
			innerErr := w.writeRowToShard(wCtx, database, retentionPolicy)
			if innerErr != nil {
				mutex.Lock()
				err = innerErr
				mutex.Unlock()
			}
			wg.Done()
		}(writeCtx)
	}
	wg.Wait()

	return err
}

func (w *PointsWriter) isPartialErr(err error) bool {
	return strings.Contains(err.Error(), "field type conflict") ||
		strings.Contains(err.Error(), "duplicate tag") ||
		errno.Equal(err, errno.TooManyTagKeys) ||
		errno.Equal(err, errno.InvalidTagKey) ||
		errno.Equal(err, errno.WritePointSchemaInvalid) ||
		errno.Equal(err, errno.WritePointHasInvalidTag) ||
		errno.Equal(err, errno.WritePointHasInvalidField) ||
		errno.Equal(err, errno.WritePointPrimaryKeyErr)
}

// routeAndMapOriginRows preprocess rows, verify rows and map to shards,
// if there is a stream aggregation, then map rows to mst.
func (w *PointsWriter) routeAndMapOriginRows(
	database, retentionPolicy string, rows []influx.Row, ctx *injestionCtx,
) (error, int, error) {
	var partialErr error // the last write partial error
	var dropped int      // number of missing points due to write failures
	var err error        // the error returned immediately

	var isDropRow bool
	var pErr error // the temporary error
	var sh *meta2.ShardInfo
	var r *influx.Row
	var originName string

	wh := ctx.getWriteHelper(w)
	for i := range rows {
		r = &rows[i]

		//check point is between rp duration
		if r.Timestamp < ctx.minTime || !w.inTimeRange(r.Timestamp) {
			errInfo := errno.NewError(errno.WritePointOutOfRP)
			w.logger.Error("write failed", zap.Error(errInfo), zap.Int64("point time", r.Timestamp), zap.Int64("duration min", ctx.minTime),
				zap.Any("time range", w.timeRange))
			partialErr = errInfo
			dropped++
			continue
		}

		if err := models.CheckTime(time.Unix(0, r.Timestamp)); err != nil {
			partialErr = err
			dropped++
			continue
		}
		sort.Stable(&r.Fields)

		if r.Fields, pErr = fixFields(r.Fields); pErr != nil {
			partialErr = pErr
			dropped++
			continue
		}

		originName = r.Name
		ctx.ms, err = wh.createMeasurement(database, retentionPolicy, r.Name)
		if err != nil {
			if errno.Equal(err, errno.InvalidMeasurement) {
				w.logger.Error("invalid measurement", zap.Error(err))
				partialErr = err
				dropped++
				continue
			}
			return nil, dropped, err
		}
		r.Name = ctx.ms.Name
		if ctx.ms.EngineType == config.COLUMNSTORE {
			wh.updatePrimaryKeyMapIfNeeded(ctx.ms.ColStoreInfo.PrimaryKey, r.Name)
		}

		if ctx.fieldToCreatePool, isDropRow, err = wh.updateSchemaIfNeeded(database, retentionPolicy, r, ctx.ms, originName, ctx.fieldToCreatePool[:0]); err != nil {
			if w.isPartialErr(err) {
				partialErr = err
				if isDropRow {
					dropped++
					continue
				}
			} else {
				return nil, dropped, err
			}
		}

		if len(*ctx.getDstSis()) > 0 {
			buildColumnToIndex(r)
		}
		updateIndexOptions(r, ctx.ms.GetIndexRelation())

		err, sh, pErr = w.updateShardGroupAndShardKey(database, retentionPolicy, r, ctx, false, nil, 0, false)
		if err != nil {
			return nil, dropped, err
		}
		if pErr != nil {
			partialErr = pErr
			dropped++
			continue
		}

		if len(*ctx.getDstSis()) > 0 {
			err = w.MapRowToMeasurement(ctx, sh.ID, originName, r)
			if err != nil {
				return nil, dropped, err
			}
		}

		ctx.setShardRow(sh, r)
		switch ctx.ms.EngineType {
		case config.TSSTORE:
			atomic.AddInt64(&statistics.HandlerStat.FieldsWritten, int64(r.Fields.Len()))
		case config.COLUMNSTORE:
			atomic.AddInt64(&statistics.HandlerStat.FieldsWritten, int64(r.Fields.Len()+r.Tags.Len()))
		default:
			return nil, dropped, errors.New("unknown engine type")
		}
	}
	return partialErr, dropped, nil
}

func (w *PointsWriter) updateSrcStreamDstShardIdMap(
	rs *[]*influx.Row, streamId, shardId uint64, srcStreamDstShardIdMap map[uint64]map[uint64]uint64,
) {
	for _, r := range *rs {
		r.StreamId = append(r.StreamId, streamId)
		m, exist := srcStreamDstShardIdMap[shardId]
		if !exist {
			m = map[uint64]uint64{}
		}
		m[streamId] = shardId
		srcStreamDstShardIdMap[shardId] = m
	}
}

func (w *PointsWriter) updateSrcStreamDstShardIdMapWithShardKey(
	rs *[]*influx.Row, si *meta2.StreamInfo, ctx *injestionCtx, shardId uint64, idx int,
) error {
	srcStreamDstShardIdMap := ctx.getSrcStreamDstShardIdMap()
	for _, r := range *rs {
		var pErr error
		err, sh, pErr := w.updateShardGroupAndShardKey(si.DesMst.Database, si.DesMst.RetentionPolicy, r, ctx, true, nil, idx, true)
		if err != nil {
			return err
		}
		if pErr != nil {
			err = pErr
			return err
		}
		r.StreamId = append(r.StreamId, si.ID)
		m, exist := srcStreamDstShardIdMap[shardId]
		if !exist {
			m = map[uint64]uint64{}
		}
		m[si.ID] = sh.ID
		srcStreamDstShardIdMap[shardId] = m
	}
	return nil
}

// routeAndCalculateStreamRows determines whether the source table and the target table of the stream are the same distribution,
// if the distribution is the same, add the streamId, otherwise start the sql layer calculation
func (w *PointsWriter) routeAndCalculateStreamRows(ctx *injestionCtx) (err error) {
	dstSis := ctx.getDstSis()
	srcStreamDstShardIdMap := ctx.getSrcStreamDstShardIdMap()
	mstShardIdRowMap := ctx.getMstShardIdRowMap()
	streamDBs := ctx.getStreamDBs()
	mis := ctx.getStreamMSTs()

	for mst, shardIdRowMap := range mstShardIdRowMap {
		var dstSisIdxes []int
		for i := 0; i < len(*dstSis); i++ {
			if (*dstSis)[i].SrcMst.Name == mst {
				dstSisIdxes = append(dstSisIdxes, i)
			}
		}
		if len(dstSisIdxes) == 0 {
			continue
		}

		var mi *meta2.MeasurementInfo
		mi, err = ctx.writeHelper.createMeasurement((*dstSis)[dstSisIdxes[0]].SrcMst.Database, (*dstSis)[dstSisIdxes[0]].SrcMst.RetentionPolicy, mst)
		if err != nil {
			return
		}

		for _, idx := range dstSisIdxes {
			for shardId, rs := range shardIdRowMap {
				if len((*dstSis)[idx].Dims) != 0 {
					// Case1: same distribution, same shard, which db set shardKey with same db and rp,
					// so dst measurement of the stream share the same shardId with src measurement.
					if len(ctx.db.ShardKey.ShardKey) > 0 && (*dstSis)[idx].SrcMst.Database == (*dstSis)[idx].DesMst.Database &&
						(*dstSis)[idx].SrcMst.RetentionPolicy == (*dstSis)[idx].DesMst.RetentionPolicy {
						w.updateSrcStreamDstShardIdMap(rs, (*dstSis)[idx].ID, shardId, srcStreamDstShardIdMap)
						continue
					}
					// Case2: same distribution, same node, which db set shardKey with same db and different rp,
					// so dst measurement of the stream share the same node with src measurement.
					// reuse shardKey, avoid calculate shardKey
					if len(ctx.db.ShardKey.ShardKey) > 0 && ((*dstSis)[idx].SrcMst.Database == (*dstSis)[idx].DesMst.Database ||
						len((*streamDBs)[idx].ShardKey.ShardKey) > 0 && strings2.SortIsEqual(ctx.db.ShardKey.ShardKey, (*streamDBs)[idx].ShardKey.ShardKey)) {
						err = w.updateSrcStreamDstShardIdMapWithShardKey(rs, (*dstSis)[idx], ctx, shardId, idx)
						if err != nil {
							return
						}
						continue
					}

					// Case3: same distribution, same node, which mst set shardKey with, reuse shardKey, avoid calculate shardKey
					if len(mi.ShardKeys) == 1 && strings2.SortIsEqual(mi.ShardKeys[0].ShardKey, (*dstSis)[idx].Dims) {
						err = w.updateSrcStreamDstShardIdMapWithShardKey(rs, (*dstSis)[idx], ctx, shardId, idx)
						if err != nil {
							return
						}
						continue
					}
				}

				// Case4: different distribution, if the source table and the target table are not belong to the same distribution,
				// the two-tier computing framework based on sql-store is adopted,
				// the following is calculated at the sql layer.
				if _, ok := ctx.stream.tasks[(*dstSis)[idx].Name]; !ok {
					task, err := newStreamTask((*dstSis)[idx], mi.Schema, (*mis)[idx].Schema)
					if err != nil {
						return err
					}
					ctx.stream.tasks[(*dstSis)[idx].Name] = task
				}
				err = ctx.stream.calculate(*rs, (*dstSis)[idx], w, ctx, idx)
				if err != nil {
					return
				}
			}
		}
	}
	return
}

func buildTagsFields(info *meta2.StreamInfo, srcSchema map[string]int32) ([]string, []string) {
	var tags []string
	var fields []string
	for _, v := range info.Dims {
		t, exist := srcSchema[v]
		if exist {
			if t == influx.Field_Type_Tag {
				tags = append(tags, v)
			} else {
				fields = append(fields, v)
			}
		}
	}
	return tags, fields
}

func (w *PointsWriter) updateShardGroupAndShardKey(
	database, retentionPolicy string, r *influx.Row, ctx *injestionCtx, stream bool, dims []string, index int, reuseShardKey bool,
) (err error, sh *meta2.ShardInfo, partialErr error) {
	var wh *writeHelper
	var di *meta2.DatabaseInfo
	var si **meta2.ShardKeyInfo
	var mi *meta2.MeasurementInfo
	var asis *[]int

	if stream {
		di = (*ctx.getStreamDBs())[index]
		mi = (*ctx.getStreamMSTs())[index]
		si = &ctx.getStreamShardKeyInfos()[index]
		wh = (*ctx.getWriteHelpers())[index]
		asis = &(*ctx.getStreamAliveShardIdxes())[index]
	} else {
		di = ctx.db
		mi = ctx.ms
		si = &ctx.shardKeyInfo
		wh = ctx.writeHelper
		asis = &ctx.aliveShardIdxes
	}

	var sameSg bool
	var sg *meta2.ShardGroupInfo
	engineType := mi.EngineType
	sg, sameSg, err = wh.createShardGroup(database, retentionPolicy, time.Unix(0, r.Timestamp), engineType)
	if err != nil {
		return
	}
	if len(*asis) == 0 {
		sameSg = false
	}

	if !sameSg {
		if len(di.ShardKey.ShardKey) > 0 {
			*si = &di.ShardKey
		} else {
			*si = mi.GetShardKey(sg.ID)
		}

		if *si == nil {
			err = errno.NewError(errno.WriteNoShardKey)
			return
		}
	}

	if !reuseShardKey {
		if stream {
			err = r.UnmarshalShardKeyByDimOrTag((*si).ShardKey, dims)
		} else if engineType == config.COLUMNSTORE {
			err = r.UnmarshalShardKeyByField((*si).ShardKey)
		} else {
			if r.ReadyBuildColumnToIndex {
				err = r.UnmarshalShardKeyByTagOp((*si).ShardKey)
			} else {
				err = r.UnmarshalShardKeyByTag((*si).ShardKey)
			}
		}
		if err != nil {
			if err != influx.ErrPointShouldHaveAllShardKey {
				return
			}
			partialErr = err
			err = nil
			return
		}

		if len(r.ShardKey) > MaxShardKey {
			partialErr = errno.NewError(errno.WritePointShardKeyTooLarge)
			w.logger.Error("write failed", zap.Error(partialErr))
			return
		}
	}

	//atomic.AddInt64(&statistics.HandlerStat.WriteUnmarshalSkDuration, time.Since(start).Nanoseconds())
	if !sameSg {
		*asis = w.MetaClient.GetAliveShards(database, sg)
	}

	if (*si).Type == influxql.RANGE {
		sh = sg.DestShard(bytesutil.ToUnsafeString(r.ShardKey))
	} else {
		if len((*si).ShardKey) > 0 && !reuseShardKey {
			r.ShardKey = r.ShardKey[len(r.Name)+1:]
		}
		sh = sg.ShardFor(meta2.HashID(r.ShardKey), *asis)
	}
	if sh == nil {
		err = errno.NewError(errno.WritePointMap2Shard)
	}
	return
}

func updateIndexOptions(r *influx.Row, indexRelation influxql.IndexRelation) {
	r.IndexOptions = r.IndexOptions[:0]
	if len(indexRelation.IndexList) > 0 && !r.ReadyBuildColumnToIndex {
		buildColumnToIndex(r)
	}
	for k, indexList := range indexRelation.IndexList {
		il, ok := selectIndexList(r.ColumnToIndex, indexList.IList)
		if ok {
			opt := influx.IndexOption{
				IndexList: il,
				Oid:       indexRelation.Oids[k],
			}
			r.IndexOptions = append(r.IndexOptions, opt)
		}
	}
}

func (w *PointsWriter) MapRowToMeasurement(ctx *injestionCtx, id uint64, mst string, r *influx.Row) error {
	var rp *[]*influx.Row
	mstShardIdRowMap := ctx.getMstShardIdRowMap()
	shards := mstShardIdRowMap[mst]
	if shards == nil {
		d := map[uint64]*[]*influx.Row{}
		rp = ctx.getPRowsPool()
		d[id] = rp
		mstShardIdRowMap[mst] = d
	} else {
		rp = shards[id]
		if rp == nil {
			rp = ctx.getPRowsPool()
			shards[id] = rp
		}
	}
	if cap(*rp) > len(*rp) {
		*rp = (*rp)[:len(*rp)+1]
		(*rp)[len(*rp)-1] = r
	} else {
		*rp = append(*rp, r)
	}
	return nil
}

// writeRowToShard writes row to a shard.
func (w *PointsWriter) writeRowToShard(ctx *netstorage.WriteContext, database, retentionPolicy string) error {
	start := time.Now()
	var err error
	var ptView meta2.DBPtInfos

RETRY:
	for {
		// retry timeout
		if time.Since(start).Nanoseconds() >= w.timeout.Nanoseconds() {
			w.logger.Error("[coordinator] write rows timeout", zap.String("db", database), zap.Uint32s("ptIds", ctx.Shard.Owners), zap.Error(err))
			break
		}
		ptView, err = w.MetaClient.DBPtView(database)
		if err != nil {
			break
		}
		for _, ptId := range ctx.Shard.Owners {
			err = w.TSDBStore.WriteRows(ctx, ptView[ptId].Owner.NodeID, ptId, database, retentionPolicy, w.timeout)
			if err != nil && errno.Equal(err, errno.ShardMetaNotFound) {
				w.logger.Error("[coordinator] store write failed", zap.String("db", database), zap.Uint32("pt", ptId), zap.Error(err))
				break RETRY
			}
			if err != nil && IsRetryErrorForPtView(err) {
				// maybe dbpt route to new node, retry get the right nodeID
				w.logger.Error("[coordinator] retry write rows", zap.String("db", database), zap.Uint32("pt", ptId), zap.Error(err))

				// The retry interval is added to avoid excessive error logs
				time.Sleep(100 * time.Millisecond)
				goto RETRY
			}
			if err != nil {
				w.logger.Error("[coordinator] write rows error", zap.String("db", database), zap.Uint32("pt", ptId), zap.Error(err))
				break
			}
		}
		break
	}

	return err
}

func (w *PointsWriter) SetStore(store Storage) {
	w.TSDBStore = NewLocalStore(store)
}

func (w *PointsWriter) inTimeRange(ts int64) bool {
	if w.timeRange == nil {
		return true
	}

	return ts > w.timeRange.Min && ts < w.timeRange.Max
}

func (w *PointsWriter) ApplyTimeRangeLimit(limit []toml.Duration) {
	if len(limit) != 2 {
		return
	}

	before, after := time.Duration(limit[0]), time.Duration(limit[1])
	if before == 0 && after == 0 {
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var update = func() {
		now := time.Now()
		if before > 0 {
			w.timeRange.Min = now.Add(-before).UnixNano()
		}

		if after > 0 {
			w.timeRange.Max = now.Add(after).UnixNano()
		}
	}

	w.timeRange = &util.TimeRange{Min: math.MinInt64, Max: math.MaxInt64}
	update()

	for {
		select {
		case <-w.signal:
			return
		case <-ticker.C:
			update()
		}
	}
}

func (w *PointsWriter) Close() {
	close(w.signal)
}

// IsRetryErrorForPtView returns true if dbpt is not on this node.
func IsRetryErrorForPtView(err error) bool {
	// Errors that need to be retried in both HA and non-HA scenarios.
	return errno.Equal(err, errno.NoConnectionAvailable) ||
		errno.Equal(err, errno.ConnectionClosed) ||
		errno.Equal(err, errno.NoNodeAvailable) ||
		errno.Equal(err, errno.SelectClosedConn) ||
		errno.Equal(err, errno.SessionSelectTimeout) ||
		errno.Equal(err, errno.OpenSessionTimeout) ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "write: connection timed out") ||
		strings.Contains(err.Error(), "use of closed network connection") || errno.Equal(err, errno.PtNotFound) ||
		errno.Equal(err, errno.DBPTClosed) || errno.Equal(err, errno.ShardMetaNotFound)
}

func selectIndexList(columnToIndex map[string]int, indexList []string) ([]uint16, bool) {
	index := make([]uint16, 0, len(indexList))
	for _, iCol := range indexList {
		v, exist := columnToIndex[iCol]
		if !exist {
			continue
		}
		index = append(index, uint16(v))
	}

	if len(index) == 0 {
		return nil, false
	}

	return index, true
}

type Storage interface {
	WriteRows(db, rp string, ptId uint32, shardID uint64, rows []influx.Row, binaryRows []byte) error
}

type LocalStore struct {
	store Storage
}

func NewLocalStore(store Storage) *LocalStore {
	return &LocalStore{
		store: store,
	}
}

func (s *LocalStore) WriteRows(ctx *netstorage.WriteContext, _ uint64, _ uint32, database, rp string, _ time.Duration) error {
	buf := ctx.Buf
	for _, ptId := range ctx.Shard.Owners {
		rows := ctx.Rows

		buf, _ = influx.FastMarshalMultiRows(buf[:0], rows) // for wal
		pos := len(buf)

		for i := range rows {
			buf = rows[i].UnmarshalIndexKeys(buf)
		}
		ctx.Buf = buf

		err := s.store.WriteRows(database, rp, ptId, ctx.Shard.ID, rows, buf[:pos])
		if err != nil {
			return err
		}
	}

	return nil
}
