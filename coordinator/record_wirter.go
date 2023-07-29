/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"go.uber.org/zap"
)

type RWMetaClient interface {
	Database(name string) (di *meta.DatabaseInfo, err error)
	RetentionPolicy(database, policy string) (*meta.RetentionPolicyInfo, error)
	CreateShardGroup(database, policy string, timestamp time.Time, engineType config.EngineType) (*meta.ShardGroupInfo, error)
	DBPtView(database string) (meta.DBPtInfos, error)
	Measurement(database string, rpName string, mstName string) (*meta.MeasurementInfo, error)
	UpdateSchema(database string, retentionPolicy string, mst string, fieldToCreate []*proto.FieldSchema) error
	CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta.ShardKeyInfo, indexR *meta.IndexRelation, engineType config.EngineType, colStoreInfo *meta.ColStoreInfo) (*meta.MeasurementInfo, error)
	GetShardInfoByTime(database, retentionPolicy string, t time.Time, ptIdx int, nodeId uint64, engineType config.EngineType) (*meta.ShardInfo, error)
}

// RecMsg data structure of the message of the record.
type RecMsg struct {
	Database        string
	RetentionPolicy string
	Measurement     string
	Rec             array.Record
}

// RecordWriter handles writes the local data node.
type RecordWriter struct {
	ptNum            int // ptNum is the number of partition on the current node
	recMsgChFactor   int // recMsgChFactor based on the rule of thumb, increase the capacity of ch and reduce block.
	nodeId           uint64
	MetaClient       RWMetaClient
	errs             *errno.Errs
	logger           *logger.Logger
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
	timeout          time.Duration
	recMsgCh         chan *RecMsg
	recWriterHelpers []*recordWriterHelper

	StorageEngine interface {
		WriteRec(db, rp, mst string, ptId uint32, shardID uint64, rec *record.Record, binaryRec []byte) error
	}
}

func NewRecordWriter(timeout time.Duration, ptNum, recMsgChFactor int) *RecordWriter {
	rw := &RecordWriter{
		timeout:        timeout,
		ptNum:          ptNum,
		wg:             sync.WaitGroup{},
		errs:           errno.NewErrs(),
		logger:         logger.NewLogger(errno.ModuleCoordinator),
		recMsgChFactor: recMsgChFactor,
	}
	rw.ctx, rw.cancel = context.WithCancel(context.Background())
	rw.errs.Init(rw.ptNum, rw.cancel)
	return rw
}

func (w *RecordWriter) RetryWriteRecord(database, retentionPolicy, measurement string, rec array.Record) error {
	w.recMsgCh <- &RecMsg{
		Database:        database,
		RetentionPolicy: retentionPolicy,
		Measurement:     measurement,
		Rec:             rec,
	}
	return nil
}

func (w *RecordWriter) Open() error {
	w.logger = logger.NewLogger(errno.ModuleCoordinator)
	w.nodeId = metaclient.DefaultMetaClient.NodeID()
	w.logger.Info(fmt.Sprintf("init nodeId %d", w.nodeId))

	ptNum := w.ptNum
	w.wg.Add(ptNum)
	w.recMsgCh = make(chan *RecMsg, ptNum*w.recMsgChFactor)
	w.recWriterHelpers = make([]*recordWriterHelper, ptNum)
	for ptIdx := 0; ptIdx < ptNum; ptIdx++ {
		w.recWriterHelpers[ptIdx] = newRecordWriterHelper(w.MetaClient, w.nodeId)
		go func(idx int) {
			w.consume(idx)
		}(ptIdx)
	}
	go w.monitoring()
	return nil
}

func (w *RecordWriter) monitoring() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if err := w.errs.Err(); err != nil {
				w.logger.Error("record writer monitoring", zap.Error(errno.NewError(errno.RecordWriterFatalErr)))
				if err = w.Close(); err != nil {
					w.logger.Error("record writer closed", zap.Error(err))
				}
				return
			}
		}
	}
}

func (w *RecordWriter) Close() error {
	w.release()
	w.errs.Clean()
	w.wg.Wait()
	return nil
}

func (w *RecordWriter) release() {
	w.cancel()
	close(w.recMsgCh)
	w.recWriterHelpers = w.recWriterHelpers[:0]
}

func (w *RecordWriter) consume(ptIdx int) {
	defer w.wg.Done()
	for {
		select {
		case msg := <-w.recMsgCh:
			w.processRecord(msg, ptIdx)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *RecordWriter) processRecord(msg *RecMsg, ptIdx int) {
	var writeErr error
	defer func() {
		if err := recover(); err != nil {
			w.logger.Error("processRecord panic",
				zap.String("record writer raise stack:", string(debug.Stack())),
				zap.Error(errno.NewError(errno.RecoverPanic, err)))
			w.errs.Dispatch(errno.NewError(errno.RecoverPanic, err))
		}
		if writeErr != nil {
			w.errs.Dispatch(writeErr)
		}
		msg.Rec.Release() // The memory is released. The value of reference counting is decreased by 1.
	}()

	start := time.Now()
	writeErr = w.writeRecord(msg.Database, msg.RetentionPolicy, msg.Measurement, msg.Rec, ptIdx)
	if writeErr != nil {
		w.logger.Error("writeRecord failed", zap.Error(writeErr))
	}
	atomic.AddInt64(&statistics.HandlerStat.WriteStoresDuration, time.Since(start).Nanoseconds())
}

func (w *RecordWriter) writeRecord(db, rp, mst string, rec array.Record, ptIdx int) error {
	colNum, rowNum := rec.NumCols(), rec.NumRows()
	if colNum == 0 || rowNum == 0 {
		return nil
	}

	ctx := getWriteRecCtx()
	defer putWriteRecCtx(ctx)

	wh := w.recWriterHelpers[ptIdx]
	err := ctx.checkDBRP(db, rp, wh.metaClient)
	if err != nil {
		return err
	}

	if rp == "" {
		rp = ctx.db.DefaultRetentionPolicy
	}

	ctx.ms, err = wh.createMeasurement(db, rp, mst)
	if err != nil {
		w.logger.Error("invalid measurement", zap.Error(errno.NewError(errno.InvalidMeasurement)))
		return err
	}
	mst = ctx.ms.Name

	times, ok := rec.Column(int(colNum - 1)).(*array.Int64)
	if !ok {
		err = errno.NewError(errno.ArrowRecordTimeFieldErr)
		w.logger.Error("invalid schema", zap.Error(err))
		return err
	}
	startTime, endTime := times.Value(0), times.Value(int(rowNum-1))

	sgis, err := wh.createShardGroupsByTimeRange(db, rp, time.Unix(0, startTime), time.Unix(0, endTime), ctx.ms.EngineType)
	if err != nil {
		return err
	}
	err = w.splitAndWriteByShard(sgis, db, rp, mst, rec, ptIdx, ctx.ms.EngineType)
	if err != nil {
		return err
	}
	return nil
}

func (w *RecordWriter) splitAndWriteByShard(sgis []*meta.ShardGroupInfo, db, rp, mst string, r array.Record, ptIdx int, engineType config.EngineType) error {
	rec := record.NewRecord(record.ArrowSchemaToNativeSchema(r.Schema()), false)
	err := record.ArrowRecordToNativeRecord(r, rec)
	if err != nil {
		return err
	}
	start := 0
	var subRec *record.Record
	for i := range sgis {
		interval := SearchLowerBoundOfRec(rec, sgis[i], start)
		if interval == NotInShardDuration {
			continue
		}
		end := start + interval
		if start == 0 && end == rec.RowNums() {
			subRec = rec
		} else {
			subRec = record.NewRecord(rec.Schema, false)
			subRec.SliceFromRecord(rec, start, end)
		}
		shard, err := w.recWriterHelpers[i].GetShardByTime(db, rp, time.Unix(0, subRec.Time(0)), ptIdx, engineType)
		if err != nil {
			w.logger.Error("GetShardByTime failed", zap.Error(err))
			return err
		}
		err = w.writeRecordToShard(shard, db, rp, mst, subRec)
		if err != nil {
			w.logger.Error("writeRecordToShard failed", zap.Error(err))
			return err
		}
		start = end
	}
	return nil
}

func (w *RecordWriter) writeRecordToShard(shard *meta.ShardInfo, database, retentionPolicy, measurement string, rec *record.Record) error {
	var err error
	var toContinue bool
	start := time.Now()

	pBuf := bufferpool.GetPoints()
	defer func() {
		bufferpool.PutPoints(pBuf)
	}()

	pBuf, err = rec.Marshal(pBuf[:0])
	if err != nil {
		w.logger.Error("record marshal failed", zap.Error(err))
		return err
	}

	for {
		// retry timeout
		if time.Since(start).Nanoseconds() >= w.timeout.Nanoseconds() {
			w.logger.Error("[coordinator] write rows timeout", zap.String("db", database), zap.Uint32s("ptIds", shard.Owners), zap.Error(err))
			return err
		}

		toContinue, err = w.writeShard(shard, database, retentionPolicy, measurement, rec, pBuf)
		if toContinue {
			continue
		}
		return err
	}
}

func (w *RecordWriter) writeShard(shard *meta.ShardInfo, database, retentionPolicy, measurement string, rec *record.Record, pBuf []byte) (bool, error) {
	var err error
	for _, ptId := range shard.Owners {
		err = w.StorageEngine.WriteRec(database, retentionPolicy, measurement, ptId, shard.ID, rec, pBuf)
		if err != nil && IsRetryErrorForPtView(err) {
			// maybe dbPt route to new node, retry get the right nodeID
			w.logger.Error("[coordinator] retry write rows", zap.String("db", database), zap.Uint32("pt", ptId), zap.Error(err))

			// The retry interval is added to avoid excessive error logs
			time.Sleep(100 * time.Millisecond)
			return true, err
		}
		if err != nil {
			w.logger.Error("[coordinator] write rows error", zap.String("db", database), zap.Uint32("pt", ptId), zap.Error(err))
			return false, err
		}
	}
	return false, nil
}
