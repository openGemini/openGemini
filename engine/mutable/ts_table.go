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
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type tsMemTableImpl struct {
}

func newTsMemTableImpl() *tsMemTableImpl {
	return &tsMemTableImpl{}
}

func (t *tsMemTableImpl) flushChunkImp(dataPath, msName string, lockPath *string, totalChunks int, tbStore immutable.TablesStore, chunk *WriteChunk,
	writeMs *immutable.MsBuilder, finish bool, isOrder bool) *immutable.MsBuilder {
	var writeRec *record.Record
	if isOrder {
		writeRec = chunk.OrderWriteRec.GetRecord()
	} else {
		writeRec = chunk.UnOrderWriteRec.GetRecord()
	}
	if writeRec.RowNums() != 0 {
		if writeMs == nil {
			conf := immutable.GetTsStoreConfig()
			writeMs = createMsBuilder(tbStore, isOrder, lockPath, dataPath, msName, totalChunks, writeRec.Len(), conf, config.TSSTORE)
		}
		writeMs = t.WriteRecordForFlush(writeRec, writeMs, tbStore, chunk.Sid, isOrder, chunk.LastFlushTime)
	}
	if finish {
		if writeMs != nil {
			if err := immutable.WriteIntoFile(writeMs, true, false, nil); err != nil {
				writeMs = nil
				logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
				return writeMs
			}

			tbStore.AddTSSPFiles(writeMs.Name(), isOrder, writeMs.Files...)
			writeMs = nil
		}
	}

	return writeMs
}

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

func (t *tsMemTableImpl) FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok || msInfo == nil {
		return
	}
	sids := msInfo.GetAllSid()

	sidMap := msInfo.sidMap
	var orderMs, unOrderMs *immutable.MsBuilder
	sidLen := len(sids)

	hlp := record.NewSortHelper()
	defer hlp.Release()

	for i := range sids {
		chunk := sidMap[sids[i]]
		finish := i >= sidLen-1

		chunk.SortRecord(hlp)

		orderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, chunk, orderMs, finish, true)
		atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(chunk.OrderWriteRec.GetRecord().RowNums()))

		unOrderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, chunk, unOrderMs, finish, false)
		atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(chunk.UnOrderWriteRec.GetRecord().RowNums()))
		atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(chunk.OrderWriteRec.GetRecord().RowNums())+int64(chunk.UnOrderWriteRec.GetRecord().RowNums()))
	}

	PutSidsImpl(sids)
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

			if chunk.LastFlushTime == math.MinInt64 {
				chunk.LastFlushTime = wc.GetLastFlushTime(msName, sid)
			}

			if !exist && table.idx != nil && (chunk.LastFlushTime == math.MinInt64 || chunk.LastFlushTime == math.MaxInt64) {
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

	var writeRec *WriteRec
	if time > chunk.LastFlushTime {
		writeRec = &chunk.OrderWriteRec
	} else {
		writeRec = &chunk.UnOrderWriteRec
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

func (t *tsMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mstsInfo map[string]*meta.MeasurementInfo, mst string) error {
	return nil
}

func (t *tsMemTableImpl) Reset(table *MemTable) {

}

func (t *tsMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	msInfo.Init(row)
	return msInfo
}

func (t *tsMemTableImpl) ApplyConcurrency(table *MemTable, f func(msName string)) {
	var wg sync.WaitGroup
	wg.Add(len(table.msInfoMap))
	for k := range table.msInfoMap {
		concurLimiter <- struct{}{}
		go func(msName string) {
			f(msName)
			concurLimiter.Release()
			wg.Done()
		}(k)
	}
	wg.Wait()
}
