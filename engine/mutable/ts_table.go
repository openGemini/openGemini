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

package mutable

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
	"go.uber.org/zap"
)

type tsMemTableImpl struct {
}

func newTsMemTableImpl() *tsMemTableImpl {
	return &tsMemTableImpl{}
}

func (t *tsMemTableImpl) SetClient(_ metaclient.MetaClient) {}

func (t *tsMemTableImpl) WriteRecordForFlush(rec *record.Record, msb *immutable.MsBuilder, tbStore immutable.TablesStore, id uint64, order bool,
	lastFlushTime int64) *immutable.MsBuilder {
	var err error

	if !order && lastFlushTime == math.MaxInt64 {
		msb.StoreTimes()
	}

	msb, err = msb.WriteRecord(id, rec, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		return tbStore.NextSequence(), 0, 0, 0
	})
	if err != nil {
		panic(err)
	}

	return msb
}

func (t *tsMemTableImpl) FlushChunks(table *MemTable, dataPath, msName, _, _ string, lock *string, tbStore immutable.TablesStore, _ int64, fileInfos chan []immutable.FileInfoExtend) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok || msInfo == nil {
		return
	}
	sids := msInfo.GetAllSid()

	sidMap := msInfo.sidMap
	sidLen := len(sids)

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	var orderMsBuilder, unOrderMsBuilder *immutable.MsBuilder
	var mmsIdTime *immutable.MmsIdTime
	var flushTime int64 = math.MinInt64

	recPool := []record.Record{{}, {}}
	hasOrderFile := tbStore.GetTableFileNum(msName, true) > 0

	if hasOrderFile {
		seq := tbStore.Sequencer()
		defer func() {
			seq.UnRef()
		}()
		mmsIdTime = seq.GetMmsIdTime(msName)
	}

	for i := range sids {
		chunk := sidMap[sids[i]]
		chunk.SortRecord(hlp)
		rec := chunk.WriteRec.GetRecord()

		if hasOrderFile {
			flushTime = math.MaxInt64
			if mmsIdTime != nil {
				flushTime, _ = mmsIdTime.Get(chunk.Sid)
			}
		}

		orderRec, unOrderRec := SplitRecordByTime(rec, recPool, flushTime)
		orderRows := orderRec.RowNums()
		if orderRows > 0 {
			if orderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				orderMsBuilder = createMsBuilder(tbStore, true, lock, dataPath, msName, sidLen, orderRows, conf, config.TSSTORE)
			}
			orderMsBuilder = t.WriteRecordForFlush(orderRec, orderMsBuilder, tbStore, chunk.Sid, true, flushTime)
			atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(orderRows))
		}

		unOrderRows := unOrderRec.RowNums()
		if unOrderRows > 0 {
			if unOrderMsBuilder == nil {
				conf := immutable.GetTsStoreConfig()
				unOrderMsBuilder = createMsBuilder(tbStore, false, lock, dataPath, msName, sidLen, unOrderRows, conf, config.TSSTORE)
			}
			unOrderMsBuilder = t.WriteRecordForFlush(unOrderRec, unOrderMsBuilder, tbStore, chunk.Sid, false, flushTime)
			atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(unOrderRows))
		}

		atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(orderRows+unOrderRows))
	}

	orderFiles := t.finish(orderMsBuilder, fileInfos)
	unOrderFiles := t.finish(unOrderMsBuilder, fileInfos)

	// add both ordered/unordered files to list
	tbStore.AddBothTSSPFiles(msInfo.GetFlushed(), msName, orderFiles, unOrderFiles)
	PutSidsImpl(sids)
}

func (t *tsMemTableImpl) finish(msb *immutable.MsBuilder, fileInfos chan []immutable.FileInfoExtend) []immutable.TSSPFile {
	if msb == nil {
		return nil
	}

	if err := immutable.WriteIntoFile(msb, true, false, nil); err != nil {
		logger.GetLogger().Error("rename init file failed", zap.String("mstName", msb.Name()), zap.Error(err))
	}

	files := msb.Files
	if fileInfos != nil {
		fileInfos <- msb.FilesInfo
	}
	return files
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

func (t *tsMemTableImpl) WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error {
	var err error
	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		rs := *rows
		msName := stringinterner.InternSafe(mapp.Key)

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
			_, err = t.appendFields(table, msInfo, chunk, rs[index].Timestamp, rs[index].Fields)
			if err != nil {
				return err
			}

			wc.AddRowCountsBySid(msName, sid, 1)
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}

	return nil
}

func (t *tsMemTableImpl) appendFields(table *MemTable, msInfo *MsInfo, chunk *WriteChunk, time int64, fields []influx.Field) (int64, error) {
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

	return table.appendFieldsToRecord(writeRec.rec, fields, time, sameSchema)
}

func (t *tsMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mstsInfo *sync.Map, mst string) error {
	return nil
}

func (t *tsMemTableImpl) Reset(table *MemTable) {

}

func (t *tsMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	msInfo.Init(row)
	return msInfo
}

func (t *tsMemTableImpl) SetFlushManagerInfo(manager map[string]FlushManager, accumulateMetaIndex *sync.Map) {
}
