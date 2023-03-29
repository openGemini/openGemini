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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	strings2 "github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
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
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta2.ShardGroupInfo, error)
	DBPtView(database string) (meta2.DBPtInfos, error)
	Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error)
	UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto2.FieldSchema) error
	CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *meta2.IndexRelation) (*meta2.MeasurementInfo, error)
	GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int
	GetStreamInfos() map[string]*meta2.StreamInfo
	GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool
}

// PointsWriter handles writes across multiple local and remote data nodes.
type PointsWriter struct {
	// rows must be within this time range
	timeRange *record.TimeRange
	signal    chan struct{}

	timeout    time.Duration
	MetaClient PWMetaClient

	TSDBStore interface {
		WriteRows(nodeID uint64, database, rp string, pt uint32, shard uint64, streamShardIdList []uint64, rows *[]influx.Row, timeout time.Duration) error
	}

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

// ShardMapping contains a mapping of shards to points.
type injestionCtx struct {
	fieldToCreatePool []*proto2.FieldSchema
	rowsPool          sync.Pool
	pRowsPool         sync.Pool
	shardRowMap       dictpool.Dict
	shardMap          dictpool.Dict

	srcStreamDstShardIdMap map[uint64]map[uint64]uint64
	mstShardIdRowMap       map[string]map[uint64]*[]*influx.Row

	streamInfos           []*meta2.StreamInfo
	streamDBs             []*meta2.DatabaseInfo
	streamMSTs            []*meta2.MeasurementInfo
	streamShardKeyInfos   []*meta2.ShardKeyInfo
	streamWriteHelpers    []*writeHelper
	streamAliveShardIdxes [][]int

	minTime         int64
	db              *meta2.DatabaseInfo
	rp              *meta2.RetentionPolicyInfo
	ms              *meta2.MeasurementInfo
	shardKeyInfo    *meta2.ShardKeyInfo
	writeHelper     *writeHelper
	aliveShardIdxes []int

	stream *Stream
}

func (s *injestionCtx) Reset() {
	s.fieldToCreatePool = s.fieldToCreatePool[:0]
	s.shardMap.Reset()
	s.shardRowMap.Reset()

	if s.srcStreamDstShardIdMap != nil {
		s.srcStreamDstShardIdMap = map[uint64]map[uint64]uint64{}
	}
	if s.mstShardIdRowMap != nil {
		s.mstShardIdRowMap = map[string]map[uint64]*[]*influx.Row{}
	}
	if s.stream != nil {
		s.stream.tasks = map[string]*streamTask{}
	}

	s.streamInfos = s.streamInfos[:0]
	s.streamDBs = s.streamDBs[:0]
	s.streamMSTs = s.streamMSTs[:0]
	for i := range s.streamAliveShardIdxes {
		s.streamAliveShardIdxes[i] = s.streamAliveShardIdxes[i][:0]
	}
	s.streamShardKeyInfos = s.streamShardKeyInfos[:0]

	for i := 0; i < len(s.streamWriteHelpers); i++ {
		s.streamWriteHelpers[i].reset()
	}
	if s.writeHelper != nil {
		s.writeHelper.reset()
	}
	s.db = nil
	s.rp = nil
	s.ms = nil
	s.minTime = 0
	s.aliveShardIdxes = s.aliveShardIdxes[:0]
	s.shardKeyInfo = nil
}

func (s *injestionCtx) initStreamDBs(length int) {
	if cap(s.streamDBs) < length {
		s.streamDBs = make([]*meta2.DatabaseInfo, length)
	} else {
		s.streamDBs = s.streamDBs[:length]
	}
}

func (s *injestionCtx) initStreamMSTs(length int) {
	if cap(s.streamMSTs) < length {
		s.streamMSTs = make([]*meta2.MeasurementInfo, length)
	} else {
		s.streamMSTs = s.streamMSTs[:length]
	}
}

// streamWriteHelpers not reset, PointsWriter stateless
func (s *injestionCtx) initStreamWriteHelpers(length int, w *PointsWriter) {
	if len(s.streamWriteHelpers) >= length {
		return
	}
	addLen := length - len(s.streamWriteHelpers)
	for i := 0; i < addLen; i++ {
		s.streamWriteHelpers = append(s.streamWriteHelpers, newWriteHelper(w))
	}
}

func (s *injestionCtx) initStreamAliveShardIdxes(length int) {
	if cap(s.streamAliveShardIdxes) < length {
		s.streamAliveShardIdxes = make([][]int, length)
	} else {
		s.streamAliveShardIdxes = s.streamAliveShardIdxes[:length]
	}
}

func (s *injestionCtx) initStreamShardKeyInfos(length int) {
	if cap(s.streamShardKeyInfos) < length {
		s.streamShardKeyInfos = make([]*meta2.ShardKeyInfo, length)
	} else {
		s.streamShardKeyInfos = s.streamShardKeyInfos[:length]
	}
}

func (s *injestionCtx) checkDBRP(database, retentionPolicy string, w *PointsWriter) (err error) {
	// check db and rp validation
	s.db, err = w.MetaClient.Database(database)
	if err != nil {
		return err
	}

	if retentionPolicy == "" {
		retentionPolicy = s.db.DefaultRetentionPolicy
	}

	s.rp, err = s.db.GetRetentionPolicy(retentionPolicy)
	if err != nil {
		return err
	}

	if s.rp.Duration > 0 {
		s.minTime = int64(fasttime.UnixTimestamp()*1e9) - s.rp.Duration.Nanoseconds()
	}
	return
}

// initStreamVar init the var needed by the stream calculation
func (s *injestionCtx) initStreamVar(w *PointsWriter) (err error) {
	dstSis := s.getDstSis()
	streamLen := len(*dstSis)
	if s.stream == nil {
		s.stream = NewStream(w.TSDBStore, w.MetaClient, w.logger, w.timeout)
	}

	s.initStreamDBs(streamLen)
	s.initStreamMSTs(streamLen)
	s.initStreamWriteHelpers(streamLen, w)
	s.initStreamAliveShardIdxes(streamLen)
	s.initStreamShardKeyInfos(streamLen)

	streamDBS := s.getStreamDBs()
	streamMSTs := s.getStreamMSTs()
	streamWHs := s.getWriteHelpers()

	for i := 0; i < streamLen; i++ {
		(*streamDBS)[i], err = w.MetaClient.Database((*dstSis)[i].DesMst.Database)
		if err != nil {
			return
		}

		(*streamMSTs)[i], err = (*streamWHs)[i].createMeasurement((*dstSis)[i].DesMst.Database, (*dstSis)[i].DesMst.RetentionPolicy, (*dstSis)[i].DesMst.Name)
		if err != nil {
			return
		}
	}
	return
}

func (s *injestionCtx) getShardRowMap() *dictpool.Dict {
	return &s.shardRowMap
}

func (s *injestionCtx) getShardMap() *dictpool.Dict {
	return &s.shardMap
}

func (s *injestionCtx) getMstShardIdRowMap() map[string]map[uint64]*[]*influx.Row {
	return s.mstShardIdRowMap
}

func (s *injestionCtx) getDstSis() *[]*meta2.StreamInfo {
	return &s.streamInfos
}

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

func (s *injestionCtx) getWriteHelpers() *[]*writeHelper {
	return &s.streamWriteHelpers
}

func (s *injestionCtx) getWriteHelper(w *PointsWriter) *writeHelper {
	if s.writeHelper == nil {
		s.writeHelper = newWriteHelper(w)
	}
	return s.writeHelper
}

func (s *injestionCtx) getStreamDBs() *[]*meta2.DatabaseInfo {
	return &s.streamDBs
}

func (s *injestionCtx) getStreamShardKeyInfos() []*meta2.ShardKeyInfo {
	return s.streamShardKeyInfos
}

func (s *injestionCtx) getStreamMSTs() *[]*meta2.MeasurementInfo {
	return &s.streamMSTs
}

func (s *injestionCtx) getStreamAliveShardIdxes() *[][]int {
	return &s.streamAliveShardIdxes
}

func (s *injestionCtx) getSrcStreamDstShardIdMap() map[uint64]map[uint64]uint64 {
	return s.srcStreamDstShardIdMap
}

func (s *injestionCtx) getRowsPool() *[]influx.Row {
	v := s.rowsPool.Get()
	if v == nil {
		return &[]influx.Row{}
	}
	return v.(*[]influx.Row)
}

func (s *injestionCtx) putRowsPool(rp *[]influx.Row) {
	*rp = (*rp)[:0]
	s.rowsPool.Put(rp)
}

func (s *injestionCtx) getPRowsPool() *[]*influx.Row {
	v := s.pRowsPool.Get()
	if v == nil {
		return &[]*influx.Row{}
	}
	return v.(*[]*influx.Row)
}

func (s *injestionCtx) putPRowsPool(rp *[]*influx.Row) {
	*rp = (*rp)[:0]
	s.pRowsPool.Put(rp)
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

func checkFields(fields influx.Fields) error {
	for i := 0; i < len(fields); i++ {
		if i > 0 && fields[i-1].Key == fields[i].Key {
			failpoint.Inject("skip-duplicate-field-check", func(val failpoint.Value) {
				if strings2.EqualInterface(val, fields[i].Key) {
					failpoint.Continue()
				}
			})

			return errno.NewError(errno.DuplicateField, fields[i].Key)
		}
	}

	return nil
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
		if !errno.Equal(err, errno.ShardMetaNotFound) {
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
		time.Sleep(time.Second)
	}
	return err
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
		if errno.Equal(err, errno.WriteMultiArray) || errno.Equal(err, errno.WriteErrorArray) {
			return netstorage.PartialWriteError{Reason: err, Dropped: dropped}
		}
		return err
	}
	if dropped > 0 {
		return netstorage.PartialWriteError{Reason: partialErr, Dropped: dropped}
	}
	return partialErr
}

func (w *PointsWriter) writeShardMap(database, retentionPolicy string, ctx *injestionCtx) error {
	shardRowMap := ctx.getShardRowMap()
	shardMap := ctx.getShardMap()
	var err error

	errC := make(chan error, shardRowMap.Len())
	for _, mapp := range shardRowMap.D {
		shardId := mapp.Key

		sh, ok := shardMap.Get(shardId).(*meta2.ShardInfo)
		if !ok {
			return errno.NewError(errno.WriteMapMetaShardInfo)
		}

		shID, err := strconv.ParseUint(shardId, 10, 64)
		if err != nil {
			return errno.NewError(errno.WriteMapMetaShardInfo)
		}

		// get the streamId and dstShardId that is associated with the srcShardId.
		var streamShardIdList []uint64
		if streamDstShardIdMap, ok := ctx.getSrcStreamDstShardIdMap()[shID]; ok {
			for streamId, dstShardId := range streamDstShardIdMap {
				streamShardIdList = append(streamShardIdList, streamId, dstShardId)
			}
		}

		rows, ok := mapp.Value.(*[]*influx.Row)
		if !ok {
			return errno.NewError(errno.WriteMapMetaShardInfo)
		}
		go func(shard *meta2.ShardInfo, streamShardIdList []uint64, db, rp string, rs *[]*influx.Row, ctx *injestionCtx) {
			err := w.writeRowToShard(shard, streamShardIdList, database, retentionPolicy, rs, ctx)
			errC <- err
		}(sh, streamShardIdList, database, retentionPolicy, rows, ctx)
	}

	for i := 0; i < shardRowMap.Len(); i++ {
		errShard := <-errC
		if errShard != nil {
			err = errShard
		}
	}
	return err
}

// routeAndMapOriginRows preprocess rows, verify rows and map to shards,
// if there is a stream aggregation, then map rows to mst.
func (w *PointsWriter) routeAndMapOriginRows(
	database, retentionPolicy string, rows []influx.Row, ctx *injestionCtx,
) (partialErr error, dropped int, err error) {
	var isDropRow bool
	var pErr error
	var sh *meta2.ShardInfo

	wh := ctx.getWriteHelper(w)
	for i := range rows {
		start := time.Now()

		r := &rows[i]

		//check point is between rp duration
		if r.Timestamp < ctx.minTime || !w.inTimeRange(r.Timestamp) {
			errInfo := errno.NewError(errno.WritePointOutOfRP)
			w.logger.Error("write failed", zap.Error(errInfo), zap.Int64("point time", r.Timestamp), zap.Int64("duration min", ctx.minTime),
				zap.Any("time range", w.timeRange))
			partialErr = errInfo
			dropped++
			continue
		}
		sort.Sort(r.Fields)

		if err := checkFields(r.Fields); err != nil {
			partialErr = err
			dropped++
			continue
		}

		originName := r.Name
		ctx.ms, err = wh.createMeasurement(database, retentionPolicy, r.Name)
		if err != nil {
			if errno.Equal(err, errno.InvalidMeasurement) {
				w.logger.Error("invalid measurement", zap.Error(err))
				partialErr = err
				dropped++
				err = nil
				continue
			}
			return
		}
		r.Name = ctx.ms.Name

		if ctx.fieldToCreatePool, isDropRow, err = wh.updateSchemaIfNeeded(database, retentionPolicy, r, ctx.ms, originName, ctx.fieldToCreatePool[:0]); err != nil {
			if strings.Contains(err.Error(), "field type conflict") || strings.Contains(err.Error(), "duplicate tag") {
				partialErr = err
				err = nil
				if isDropRow {
					dropped++
					continue
				}
			} else {
				return
			}
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteUpdateSchemaDuration, time.Since(start).Nanoseconds())

		if len(*ctx.getDstSis()) > 0 {
			buildColumnToIndex(r)
		}
		updateIndexOptions(r, ctx.ms.GetIndexRelation())

		start = time.Now()
		err, pErr, sh = w.updateShardGroupAndShardKey(database, retentionPolicy, r, ctx, false, nil, 0, false)
		if err != nil {
			return
		}
		if pErr != nil {
			partialErr = pErr
			dropped++
			continue
		}
		atomic.AddInt64(&statistics.HandlerStat.WriteCreateSgDuration, time.Since(start).Nanoseconds())

		start = time.Now()
		id := strconv.FormatInt(int64(sh.ID), 10)
		ctx.getShardMap().Set(id, sh)

		if len(*ctx.getDstSis()) > 0 {
			err = w.MapRowToMeasurement(ctx, sh.ID, originName, r)
			if err != nil {
				return
			}
		}

		if err = w.MapRowToShard(ctx, id, r); err != nil {
			return
		}
		atomic.AddInt64(&statistics.HandlerStat.FieldsWritten, int64(r.Fields.Len()))
		atomic.AddInt64(&statistics.HandlerStat.WriteMapRowsDuration, time.Since(start).Nanoseconds())
	}
	return
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
		err, pErr, sh := w.updateShardGroupAndShardKey(si.DesMst.Database, si.DesMst.RetentionPolicy, r, ctx, true, nil, idx, true)
		if err != nil {
			return err
		}
		if pErr != nil {
			err = pErr
			return err
		}
		id := strconv.FormatInt(int64(sh.ID), 10)
		ctx.getShardMap().Set(id, sh)
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
) (err error, partialErr error, sh *meta2.ShardInfo) {
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
	sg, sameSg, err = wh.createShardGroup(database, retentionPolicy, time.Unix(0, r.Timestamp))
	if err != nil {
		return
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
			if err = r.UnmarshalShardKeyByDimOrTag((*si).ShardKey, dims); err != nil {
				if err != influx.ErrPointShouldHaveAllShardKey {
					return
				}
				partialErr = err
				err = nil
				return
			}
		} else {
			if r.ReadyBuildColumnToIndex {
				err = r.UnmarshalShardKeyByTagOp((*si).ShardKey)
			} else {
				err = r.UnmarshalShardKeyByTag((*si).ShardKey)
			}
			if err != nil {
				if err != influx.ErrPointShouldHaveAllShardKey {
					return
				}
				partialErr = err
				err = nil
				return
			}
		}

		if len(r.ShardKey) > MaxShardKey {
			partialErr = errno.NewError(errno.WritePointShardKeyTooLarge)
			w.logger.Error("write failed", zap.Error(partialErr))
			return
		}
	}

	//atomic.AddInt64(&statistics.HandlerStat.WriteUnmarshalSkDuration, time.Since(start).Nanoseconds())
	if !config.GetHaEnable() && !sameSg {
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

func updateIndexOptions(r *influx.Row, indexRelation meta2.IndexRelation) {
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

func (w *PointsWriter) MapRowToShard(ctx *injestionCtx, id string, r *influx.Row) error {
	shardRowMap := ctx.getShardRowMap()
	if !shardRowMap.Has(id) {
		rp := ctx.getPRowsPool()
		shardRowMap.Set(id, rp)
	}
	rowsPool := shardRowMap.Get(id)
	rp, ok := rowsPool.(*[]*influx.Row)
	if !ok {
		return errors.New("MapRowToShard error")
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
func (w *PointsWriter) writeRowToShard(shard *meta2.ShardInfo, streamShardIdList []uint64, database, retentionPolicy string, row *[]*influx.Row, ctx *injestionCtx) error {
	start := time.Now()
	var err error
	var ptView meta2.DBPtInfos

	rps := ctx.getRowsPool()
	if cap(*rps) > len(*row) {
		*rps = (*rps)[:len(*row)]
	} else {
		t := make([]influx.Row, len(*row))
		rps = &t
	}
	for i := range *rps {
		(*rps)[i].Clone((*row)[i])
	}

RETRY:
	for {
		// retry timeout
		if time.Since(start).Nanoseconds() >= w.timeout.Nanoseconds() {
			w.logger.Error("[coordinator] write rows timeout", zap.String("db", database), zap.Uint32s("ptIds", shard.Owners), zap.Error(err))
			break
		}
		ptView, err = w.MetaClient.DBPtView(database)
		if err != nil {
			break
		}
		for _, ptId := range shard.Owners {
			err = w.TSDBStore.WriteRows(ptView[ptId].Owner.NodeID, database, retentionPolicy, ptId, shard.ID, streamShardIdList, rps, w.timeout)
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
	ctx.putPRowsPool(row)
	ctx.putRowsPool(rps)
	return err
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

	w.timeRange = &record.TimeRange{Min: math.MinInt64, Max: math.MaxInt64}
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
	if errno.Equal(err, errno.NoConnectionAvailable) ||
		errno.Equal(err, errno.ConnectionClosed) ||
		errno.Equal(err, errno.NoNodeAvailable) ||
		errno.Equal(err, errno.SelectClosedConn) ||
		errno.Equal(err, errno.SessionSelectTimeout) ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "write: connection timed out") ||
		strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	if !config.GetHaEnable() {
		// In non-HA scenarios, no need to retry.
		return false
	}
	// In HA scenarios, retry is required.
	return errno.Equal(err, errno.PtNotFound) ||
		errno.Equal(err, errno.DBPTClosed)
}

func selectIndexList(columnToIndex map[string]int, indexList []*meta2.IndexInfor) ([]uint16, bool) {
	index := make([]uint16, len(indexList))
	for i, iCol := range indexList {
		v, exist := columnToIndex[iCol.FieldName]
		if !exist {
			return nil, false
		}
		index[i] = uint16(v)
	}
	return index, true
}
