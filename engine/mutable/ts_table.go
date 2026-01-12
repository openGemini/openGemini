// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package mutable

import (
	"io"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/dictmap"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type RecordIterator interface {
	Next(dst *record.Record) (uint64, error)
}

type tsMemTableImpl struct {
}

func NewTsMemTableImpl() *tsMemTableImpl {
	return &tsMemTableImpl{}
}

func (t *tsMemTableImpl) WriteRecordForFlush(rec *record.Record, msb *immutable.MsBuilder, tbStore immutable.TablesStore, id uint64) *immutable.MsBuilder {
	var err error

	msb.StoreTimes()
	msb, err = msb.WriteRecord(id, rec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		return tbStore.NextSequence(), 0, 0, 0
	})
	if err != nil {
		logger.GetLogger().Error("failed to write record", zap.Error(err))
	}

	return msb
}

func (t *tsMemTableImpl) FlushChunks(table *MemTable, msName string, tbStore immutable.TablesStore, fileInfos chan []immutable.FileInfoExtend) {
	err := t.flushChunks(table, msName, tbStore, fileInfos)
	if err != nil {
		logger.GetLogger().Error("failed to flush chunks", zap.Error(err))
		// The error here is due to a disk operation failure or some kind of bug.
		// Since it is an asynchronous background thread, it can only panic.
		panic(err)
	}
}

func (t *tsMemTableImpl) flushChunks(table *MemTable, msName string, tbStore immutable.TablesStore, fileInfos chan []immutable.FileInfoExtend) error {
	flusher := NewRecordFlusher(tbStore, msName, fileInfos)
	defer flusher.Free()

	msInfo, ok := table.msInfoMap[msName]
	if !ok || msInfo == nil {
		return nil
	}
	sids := msInfo.GetAllSid()
	sidMap := msInfo.sidMap

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	flushSizeCount := 0
	for i := range sids {
		chunk := sidMap[sids[i]]
		chunk.SortRecord(hlp)
		rec := chunk.WriteRec.GetRecord()

		err := flusher.FlushRecord(chunk.Sid, rec)
		if err != nil {
			return err
		}
		if Statistics.PTWriteStat.ShouldCollect() {
			flushSizeCount += rec.Size()
		}
	}
	tbStore.AddPtWriteStats(msName, flushSizeCount)

	orderFiles, unOrderFiles, err := flusher.Flush()
	if err != nil {
		return err
	}

	// add both ordered/unordered files to list
	tbStore.AddBothTSSPFiles(msInfo.GetFlushed(), msName, orderFiles, unOrderFiles)
	PutSidsImpl(sids)
	return nil
}

func (t *tsMemTableImpl) FlushRecords(tbStore immutable.TablesStore, itr RecordIterator, msName string,
	fileInfos chan []immutable.FileInfoExtend) ([]immutable.TSSPFile, []immutable.TSSPFile, error) {

	flusher := NewRecordFlusher(tbStore, msName, fileInfos)
	defer flusher.Free()

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	rec := &record.Record{}
	for {
		rec.ResetDeep()
		sid, err := itr.Next(rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.GetLogger().Error("failed to iterator record. skip it", zap.Error(err))
			continue
		}
		if sid == 0 {
			logger.GetLogger().Error("invalid series id. skip it")
			continue
		}

		rec = hlp.Sort(rec)

		err = flusher.FlushRecord(sid, rec)
		if err != nil {
			return nil, nil, err
		}
	}

	return flusher.Flush()
}

func SplitRecordByTime(rec *record.Record, pool []record.Record, time int64) (*record.Record, *record.Record) {
	times := rec.Times()
	if time >= times[len(times)-1] {
		return nil, rec
	} else if time < times[0] {
		return rec, nil
	}

	n := sort.Search(len(times), func(i int) bool {
		return times[i] > time
	})

	if len(pool) < 2 {
		pool = make([]record.Record, 2)
	}

	orderRec := &pool[0]
	orderRec.Reset()
	orderRec.ReserveColVal(len(rec.Schema))

	unOrderRec := &pool[1]
	unOrderRec.Reset()
	unOrderRec.ReserveColVal(len(rec.Schema))

	orderIdx, unOrderIdx := 0, 0

	for i := range rec.Schema {
		field := rec.Schema[i]
		col := &rec.ColVals[i]

		orderCol, unOrderCol := &orderRec.ColVals[orderIdx], &unOrderRec.ColVals[unOrderIdx]

		unOrderCol.Init()
		unOrderCol.AppendColVal(col, field.Type, 0, n)
		if unOrderCol.NilCount != unOrderCol.Len {
			unOrderRec.Schema = append(unOrderRec.Schema, field)
			unOrderIdx++
		}

		orderCol.Init()
		orderCol.AppendColVal(col, field.Type, n, col.Len)
		if orderCol.NilCount != orderCol.Len {
			orderRec.Schema = append(orderRec.Schema, field)
			orderIdx++
		}
	}

	orderRec.ColVals = orderRec.ColVals[:orderIdx]
	unOrderRec.ColVals = unOrderRec.ColVals[:unOrderIdx]

	return orderRec, unOrderRec
}

func (t *tsMemTableImpl) WriteRows(table *MemTable, rowsD dictmap.DictMap[string, *[]influx.Row], wc WriteRowsCtx) error {
	var err error
	for k, rows := range rowsD {
		rs := *rows
		msName := stringinterner.InternSafe(k)

		start := time.Now()
		msInfo := table.CreateMsInfo(msName, &rs[0], nil)
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

		start = time.Now()
		var (
			exist bool
			sid   uint64
			chunk *WriteChunk
		)
		for index := range rs {
			sid = rs[index].PrimaryId
			if sid == 0 {
				continue
			}

			chunk, exist = msInfo.CreateChunk(sid)

			if !exist && table.idx != nil {
				// only range mode
				startTime := time.Now()
				err = table.idx.CreateIndex(util.Str2bytes(msName), rs[index].ShardKey, sid)
				if err != nil {
					return err
				}
				atomic.AddInt64(&Statistics.PerfStat.WriteShardKeyIdxNs, time.Since(startTime).Nanoseconds())
			}
			_, err = t.appendFields(msInfo, chunk, rs[index].Timestamp, rs[index].Fields)
			if err != nil {
				return err
			}

			wc.AddRowCountsBySid(msName, sid, 1)
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}

	return nil
}

func (t *tsMemTableImpl) appendFields(msInfo *MsInfo, chunk *WriteChunk, time int64, fields []influx.Field) (int64, error) {
	chunk.Mu.Lock()
	defer chunk.Mu.Unlock()

	writeRec := &chunk.WriteRec
	if writeRec.rec == nil {
		writeRec.init(msInfo.Schema)
	}

	sameSchema := checkSchemaIsSame(writeRec.rec.Schema, fields)
	if !sameSchema && !writeRec.schemaCopyed {
		copySchema := record.Schemas{}
		if writeRec.rec.RowNums() == 0 {
			genMsSchema(&copySchema, fields)
			sameSchema = true
		} else {
			copySchema = append(copySchema, writeRec.rec.Schema...)
		}
		oldColNums := writeRec.rec.ColNums()
		newColNums := len(copySchema)
		writeRec.rec.Schema = copySchema
		writeRec.rec.ReserveColVal(newColNums - oldColNums)
		writeRec.schemaCopyed = true
	}

	if time <= writeRec.lastAppendTime {
		writeRec.timeAsd = false
	} else {
		writeRec.lastAppendTime = time
	}
	if time < writeRec.firstAppendTime {
		writeRec.firstAppendTime = time
	}

	return record.AppendFieldsToRecord(writeRec.rec, fields, time, sameSchema)
}

func (t *tsMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mst string) error {
	return nil
}

func (t *tsMemTableImpl) Reset(table *MemTable) {

}

func (t *tsMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	msInfo.Init(row)
	return msInfo
}

type RecordFlusher struct {
	mst             string
	orderSW         *immutable.StreamWriteFile
	unorderedSW     *immutable.StreamWriteFile
	tbStore         immutable.TablesStore
	flushedRowCount int
	fileInfos       chan []immutable.FileInfoExtend

	recPool      [2]record.Record
	seq          *immutable.Sequencer
	mmsIdTime    *immutable.MmsIdTime
	hasOrderFile bool
}

func NewRecordFlusher(tbStore immutable.TablesStore, mst string, fileInfos chan []immutable.FileInfoExtend) *RecordFlusher {
	rf := &RecordFlusher{
		mst:          mst,
		tbStore:      tbStore,
		fileInfos:    fileInfos,
		hasOrderFile: false,
		seq:          tbStore.Sequencer(),
	}

	rf.hasOrderFile = tbStore.GetTableFileNum(mst, true) > 0
	if rf.hasOrderFile {
		rf.mmsIdTime = rf.seq.GetMmsIdTime(mst)
	}
	return rf
}

func (rf *RecordFlusher) Free() {
	if rf.seq != nil {
		rf.seq.UnRef()
	}
}

func (rf *RecordFlusher) getStreamWriteFile(mst string, order bool) (*immutable.StreamWriteFile, error) {
	sw := &rf.orderSW
	if !order {
		sw = &rf.unorderedSW
	}

	if *sw == nil {
		*sw = rf.tbStore.NewStreamWriteFile(mst)
		err := (*sw).InitFlushFile(rf.tbStore.NextSequence(), order)
		if err != nil {
			return nil, err
		}
	}

	return *sw, nil
}

func (rf *RecordFlusher) flushRecord(mst string, sid uint64, rec *record.Record, order bool) error {
	sw, err := rf.getStreamWriteFile(mst, order)
	if err != nil {
		return err
	}
	rf.flushedRowCount += rec.RowNums()

	return sw.WriteRecord(sid, rec)
}

func (rf *RecordFlusher) FlushRecord(sid uint64, rec *record.Record) error {
	flushTime := rf.getFlushTime(sid)

	orderRec, unOrderRec := SplitRecordByTime(rec, rf.recPool[:], flushTime)
	orderRows := orderRec.RowNums()
	if orderRows > 0 {
		err := rf.flushRecord(rf.mst, sid, orderRec, true)
		if err != nil {
			return err
		}
		atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(orderRows))
	}

	unOrderRows := unOrderRec.RowNums()
	if unOrderRows > 0 {
		err := rf.flushRecord(rf.mst, sid, unOrderRec, false)
		if err != nil {
			return err
		}
		atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(unOrderRows))

		rf.statUnordered(unOrderRec.Times(), flushTime)
	}

	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(orderRows+unOrderRows))
	return nil
}

func (rf *RecordFlusher) getFlushTime(sid uint64) int64 {
	var flushTime int64 = math.MinInt64
	if rf.hasOrderFile {
		flushTime = math.MaxInt64
		if rf.mmsIdTime != nil {
			flushTime, _ = rf.mmsIdTime.Get(sid)
		}
	}
	return flushTime
}

func (rf *RecordFlusher) statUnordered(times []int64, flushTime int64) {
	if flushTime == math.MaxInt64 {
		return
	}

	stat := Statistics.NewOOOTimeDistribution()
	for i := range times {
		stat.Add(flushTime-times[i], 1)
	}
}

func (rf *RecordFlusher) Flush() ([]immutable.TSSPFile, []immutable.TSSPFile, error) {
	orderFiles, err := rf.flush(rf.orderSW)
	if err != nil {
		return nil, nil, err
	}

	unOrderFiles, err := rf.flush(rf.unorderedSW)

	return orderFiles, unOrderFiles, err
}

func (rf *RecordFlusher) flush(sw *immutable.StreamWriteFile) ([]immutable.TSSPFile, error) {
	if sw == nil {
		return nil, nil
	}

	rf.seq.BatchUpdateCheckTime(sw.IdTimePairs(), false)

	file, err := sw.NewTSSPFile(false)
	if err != nil {
		return nil, err
	}

	rf.reportFileInfo(file)
	return []immutable.TSSPFile{file}, nil
}

func (rf *RecordFlusher) reportFileInfo(file immutable.TSSPFile) {
	if rf.fileInfos == nil {
		return
	}

	fi := rf.createFileInfo(file)
	if fi == nil {
		return
	}

	infos := []immutable.FileInfoExtend{
		{
			FileInfo: *fi,
			Name:     rf.mst,
		},
	}
	rf.fileInfos <- infos
}

func (rf *RecordFlusher) createFileInfo(file immutable.TSSPFile) *meta.FileInfo {
	minT, maxT, err := file.MinMaxTime()
	if err != nil {
		logger.GetLogger().Error("create file info failed", zap.Error(err))
		return nil
	}

	name := file.FileName()

	return &meta.FileInfo{
		Sequence:      name.GetSeq(),
		Level:         name.GetLevel(),
		Merge:         name.GetMerge(),
		Extent:        name.GetExtend(),
		ShardID:       rf.tbStore.GetShardID(),
		CreatedAt:     time.Now().UnixNano(),
		MinTime:       minT,
		MaxTime:       maxT,
		RowCount:      int64(rf.flushedRowCount),
		FileSizeBytes: file.FileSize(),
	}
}
