// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package writer

import (
	"math"
	"slices"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta2 "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/spdy"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"go.uber.org/zap"
)

type Storage interface {
	WriteBlobs(db, rp string, pt uint32, shard uint64, blobs *shelf.BlobGroup, nodeID uint64, timeout time.Duration) error
}

type RecordStore struct {
	metaClient meta2.MetaClient
}

func NewRecordStore(mc meta2.MetaClient) Storage {
	return &RecordStore{
		metaClient: mc,
	}
}

func (s *RecordStore) WriteBlobs(db, rp string, pt uint32, shard uint64, blobGroup *shelf.BlobGroup, nodeID uint64, timeout time.Duration) error {

	writeBlobsReq := msgservice.NewWriteBlobsRequest(db, rp, pt, shard, blobGroup)

	r := msgservice.NewRequester(0, nil, s.metaClient)
	r.SetToInsert()
	r.SetTimeout(timeout)
	err := r.InitWithNodeID(nodeID)
	if err != nil {
		return err
	}
	cb := &msgservice.WriteBlobsCallback{}

	err = r.Request(spdy.WriteBlobsRequest, writeBlobsReq, cb)
	if err != nil {
		return err
	}
	return nil
}

type RecordWriter struct {
	timeRange util.TimeRange
	timeout   time.Duration
	logger    *logger.Logger
	mc        MetaClient
	signal    chan struct{}
	store     Storage
}

func NewRecordWriter(mc MetaClient, timeout time.Duration) *RecordWriter {
	return &RecordWriter{
		timeRange: util.TimeRange{Min: math.MinInt64, Max: math.MaxInt64},
		mc:        mc,
		timeout:   timeout,
		signal:    make(chan struct{}),
	}
}

func (w *RecordWriter) Close() {
	close(w.signal)
}

func (w *RecordWriter) SetStorage(store Storage) {
	w.store = store
}

func (w *RecordWriter) WithLogger(lg *logger.Logger) {
	w.logger = lg
}

func (w *RecordWriter) RetryWriteRecords(db, rp string, recs []*MstRecord) error {
	var err error
	start := time.Now()

	for {
		err = w.writeRecords(db, rp, recs)
		if err == nil {
			break
		}

		if !errno.IsRetryErrorForPtView(err) || time.Since(start).Nanoseconds() >= w.timeout.Nanoseconds() {
			w.logger.Error("write records failed",
				zap.String("db", db),
				zap.String("rp", rp),
				zap.Duration("elapsed", time.Since(start)),
				zap.Error(err))
			break
		}

		time.Sleep(time.Second)
	}
	return err
}

func (w *RecordWriter) writeRecords(db, rp string, recs []*MstRecord) error {
	ctx, release := NewWriteContext(w.mc, db, rp)
	defer func() {
		release()
	}()

	err := ctx.meta.CheckDBRP()
	if err != nil {
		return err
	}

	err = w.updateSchema(ctx, recs)
	if err != nil {
		return err
	}

	// map data to shards by series key
	for _, rec := range recs {
		w.MapRecord(ctx, rec.Mst, &rec.Rec)
	}

	err = w.writeToStorage(ctx)
	if err != nil {
		return err
	}

	return ctx.err.Error()
}

func (w *RecordWriter) MapRecord(ctx *WriteContext, mst string, rec *record.Record) {
	times := rec.Times()
	tags, fields := record.SplitTagField(rec)

	statistics.NewHandler().FieldsWritten.Add(int64(fields.ColNums() * fields.RowNums()))

	mapper := ctx.mapper
	indexKey := ctx.indexKey
	err := ctx.meta.ResetMeasurementInfos(mst)
	if err != nil {
		ctx.err.AddDropRowError(err)
		return
	}

	for i := range rec.RowNums() {
		//check point is between rp duration
		if times[i] < ctx.meta.minTime || !w.inTimeRange(times[i]) {
			errInfo := errno.NewError(errno.WritePointOutOfRP)
			w.logger.Error("out of the valid time range", zap.Error(errInfo),
				zap.Int64("point time", times[i]),
				zap.Int64("duration min", ctx.meta.minTime),
				zap.Any("time range", w.timeRange))
			ctx.err.AddDropRowError(errInfo)
			continue
		}

		indexKey = record.UnmarshalIndexKeys(mst, tags, i, indexKey[:0])
		si, sg, aliveShards := ctx.meta.GetShardKeyAndGroupInfo(i > 0, times[i])
		if len(aliveShards) == 0 {
			ctx.err.AddDropRowError(errno.NewError(errno.WritePointMap2Shard))
			continue
		}

		shardKey := indexKey
		if si != nil && len(si.ShardKey) > 0 {
			shardKey, err = record.UnmarshalShardKeys(tags, si.ShardKey, i, shardKey)
			if err != nil {
				ctx.err.AddDropRowError(err)
				continue
			}
		}

		sh := sg.ShardFor(meta.HashID(shardKey), aliveShards)
		if sh == nil {
			ctx.err.AddDropRowError(errno.NewError(errno.WritePointMap2Shard))
			continue
		}

		ctx.meta.shards[sh.ID] = sh
		mapper.MapRecord(sh.ID, mst, indexKey, fields, i)
	}
	ctx.indexKey = indexKey
}

func (w *RecordWriter) inTimeRange(ts int64) bool {
	return ts > w.timeRange.Min && ts < w.timeRange.Max
}

func (w *RecordWriter) updateSchema(ctx *WriteContext, recs []*MstRecord) error {
	var times []int64
	var msInfo *meta.MeasurementInfo
	var err error

	for _, mr := range recs {
		rec := &mr.Rec

		msInfo, err = ctx.meta.CreateMeasurement(mr.Mst, config.TSSTORE)
		err = ctx.err.Assert(err, rec.RowNums())
		if err != nil {
			return err
		}

		err = ctx.meta.UpdateSchemaIfNeeded(rec, msInfo, mr.Mst)
		err = ctx.err.Assert(err, rec.RowNums())
		if err != nil {
			return err
		}

		mr.Mst = msInfo.Name
		times = append(times, rec.Times()...)
	}

	err = ctx.meta.CreateShardGroupIfNeeded(times)
	return ctx.err.Assert(err, 0)
}

func (w *RecordWriter) ApplyTimeRangeLimit(limit []toml.Duration) {
	if len(limit) != 2 {
		return
	}

	before, after := time.Duration(limit[0]), time.Duration(limit[1])
	if before == 0 && after == 0 {
		return
	}

	var update = func() {
		now := time.Now()
		if before > 0 {
			w.timeRange.Min = now.Add(-before).UnixNano()
		}

		if after > 0 {
			w.timeRange.Max = now.Add(after).UnixNano()
		}
	}

	update()
	util.TickerRun(time.Second, w.signal, func() {
		update()
	}, func() {})
}

func (w *RecordWriter) writeToStorage(ctx *WriteContext) error {
	errs := errno.NewErrs()
	errs.Init(len(ctx.mapper.alc.ValMap()), nil)

	ctx.mapper.Walk(func(shardID uint64, group *shelf.BlobGroup) {
		go func() {
			shard, ok := ctx.meta.shards[shardID]
			if !ok {
				errs.Dispatch(nil)
				return
			}

			err := w.writeBlobsToShard(ctx.meta.db, ctx.meta.rp, shard, group)
			errs.Dispatch(err)
		}()
	})

	return errs.Err()
}

func (w *RecordWriter) writeBlobsToShard(db, rp string, shard *meta.ShardInfo, group *shelf.BlobGroup) error {
	var logErr = func(msg string, err error) {
		w.logger.Error(msg,
			zap.String("db", db),
			zap.Uint32s("pt", shard.Owners),
			zap.Uint64("shard", shard.ID),
			zap.Error(err))
	}

	var assert = func(err error) bool {
		if errno.Equal(err, errno.ShardMetaNotFound) {
			return false
		}

		if errno.IsRetryErrorForPtView(err) {
			logErr("[RecordWriter] retry write record", err)
			return true
		}

		return false
	}

	var write = func() error {
		ptView, err := w.mc.DBPtView(db)
		if err != nil {
			return err
		}
		for _, ptId := range shard.Owners {
			err = w.store.WriteBlobs(db, rp, ptId, shard.ID, group, ptView[ptId].Owner.NodeID, w.timeout)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := RetryExecute(w.timeout, write, assert)
	if err != nil {
		logErr("[RecordWriter] write records error", err)
	}

	return err
}

func RetryExecute(timeout time.Duration, fn func() error, assert func(error) bool) error {
	start := time.Now()
	var err error

	for {
		if timeout > 0 && time.Since(start).Nanoseconds() >= timeout.Nanoseconds() {
			return err
		}

		err = fn()
		if err == nil {
			break
		}

		if !assert(err) {
			return err
		}

		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func appendField(fields []*proto.FieldSchema, name string, typ int32) []*proto.FieldSchema {
	fields = slices.Grow(fields, 1)
	idx := len(fields)
	fields = fields[:idx+1]
	if fields[idx] == nil {
		fields[idx] = &proto.FieldSchema{}
	}

	fields[idx].FieldName = &name
	fields[idx].FieldType = &typ
	return fields
}
