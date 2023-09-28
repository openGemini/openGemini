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
	"github.com/openGemini/openGemini/engine/immutable/colstore"
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

const defaultWriteRecNum = 8

type csMemTableImpl struct {
	mu         sync.RWMutex
	primaryKey map[string]record.Schemas // mst -> primary key
}

func (c *csMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	if rec != nil {
		msInfo.Name = name
		msInfo.Schema = rec.Schema
		return msInfo
	}
	msInfo.Name = row.Name
	msInfo.concurrencyChunks = &rowChunks{writeRowChunksToken: make(chan struct{}, defaultWriteRecNum),
		rowChunksStatus: make([]int, defaultWriteRecNum)}
	genMsSchemaForColumnStore(&msInfo.Schema, row.Fields, row.Tags)
	return msInfo
}

func (c *csMemTableImpl) ApplyConcurrency(table *MemTable, f func(msName string)) {
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

func (c *csMemTableImpl) flushChunkImp(dataPath, msName string, lockPath *string, tbStore immutable.TablesStore,
	chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder) *immutable.MsBuilder {
	writeRec := chunk.WriteRec.GetRecord()
	writeMs = WriteRecordForFlush(writeRec, writeMs, tbStore, 0, true, math.MinInt64, c.primaryKey[msName])
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(writeRec.RowNums()))

	if writeMs != nil {
		if err := WriteIntoFile(writeMs, true, writeMs.GetPKInfoNum() != 0); err != nil {
			writeMs = nil
			logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
			return writeMs
		}

		tbStore.AddTSSPFiles(writeMs.Name(), false, writeMs.Files...)
		if writeMs.GetPKInfoNum() != 0 {
			for i, file := range writeMs.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendIndexSuffix(immutable.RemoveTsspSuffix(dataFilePath))
				tbStore.AddPKFile(writeMs.Name(), indexFilePath, writeMs.GetPKRecord(i), writeMs.GetPKMark(i))
			}
		}

		writeMs = nil
	}

	return nil
}

func (c *csMemTableImpl) updatePrimaryKey(mst string, pk record.Schemas) {
	c.mu.RLock()
	_, ok := c.primaryKey[mst]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.primaryKey[mst]; !ok {
			c.primaryKey[mst] = pk
		}
	}
}

func (c *csMemTableImpl) FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok {
		return
	}

	JoinWriteRec(table, msName)
	chunk := msInfo.writeChunk
	chunk.SortRecord()

	rec := chunk.WriteRec.GetRecord()
	if rec.RowNums() == 0 {
		return
	}
	conf := immutable.GetColStoreConfig()
	writeMs := createMsBuilder(tbStore, true, lock, dataPath, msName, 1, rec.Len(), conf)
	writeMs.NewPKIndexWriter()
	c.flushChunkImp(dataPath, msName, lock, tbStore, chunk, writeMs)
}

func (c *csMemTableImpl) WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error {
	var err error
	var idx int
	var writeChunk *WriteChunkForColumnStore
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
		err = c.UpdatePrimaryKey(msName, wc.MstsInfo)
		if err != nil {
			return err
		}

		createRowChunks(msInfo, wc.MstsInfo[msName].ColStoreInfo.SortKey)
		msInfo.concurrencyChunks.writeRowChunksToken <- struct{}{}
		writeChunk, idx = getFreeWriteChunk(msInfo)
		for index := range rs {
			_, err = c.appendFields(table, writeChunk, rs[index].Timestamp, rs[index].Fields, rs[index].Tags)
			if err != nil {
				return err
			}
		}

		// update the row count for each mst
		if wc.MsRowCount != nil {
			if value, ok := wc.MsRowCount.Load(msName); ok {
				atomic.AddInt64(value.(*int64), int64(len(*rows)))
			} else {
				count := int64(len(*rows))
				wc.MsRowCount.Store(msName, &count)
			}
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
		msInfo.concurrencyChunks.rowChunksStatus[idx] = 0
		<-msInfo.concurrencyChunks.writeRowChunksToken
	}
	return err
}

func (c *csMemTableImpl) appendFields(table *MemTable, chunk *WriteChunkForColumnStore, time int64, fields []influx.Field, tags []influx.Tag) (int64, error) {
	chunk.Mu.Lock()
	defer chunk.Mu.Unlock()

	sameSchema := checkSchemaIsSameWithTag(chunk.WriteRec.rec.Schema, fields, tags)
	if !sameSchema && !chunk.WriteRec.schemaCopyed {
		copySchema := record.Schemas{}
		if chunk.WriteRec.rec.RowNums() == 0 {
			genMsSchemaForColumnStore(&copySchema, fields, tags)
			sameSchema = true
		} else {
			copySchema = append(copySchema, chunk.WriteRec.rec.Schema...)
		}
		oldColNums := chunk.WriteRec.rec.ColNums()
		newColNums := len(copySchema)
		chunk.WriteRec.rec.Schema = copySchema
		chunk.WriteRec.rec.ReserveColVal(newColNums - oldColNums)
		chunk.WriteRec.schemaCopyed = true
	}

	return c.appendTagsFieldsToRecord(table, chunk.WriteRec.rec, fields, tags, time, sameSchema)
}

func (c *csMemTableImpl) appendTagsFieldsToRecord(t *MemTable, rec *record.Record, fields []influx.Field, tags []influx.Tag, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	var err error
	recSchemaIdx, pointSchemaIdx, tagSchemaIdx := 0, 0, 0
	filedsSchemaLen, tagSchemaLen := len(fields), len(tags)
	if sameSchema {
		for pointSchemaIdx < filedsSchemaLen && tagSchemaIdx < tagSchemaLen {
			if fields[pointSchemaIdx].Key < tags[tagSchemaIdx].Key {
				if err = t.appendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
					return size, err
				}
				pointSchemaIdx++
				recSchemaIdx++
				continue
			}

			c.appendTagToCol(&rec.ColVals[recSchemaIdx], &tags[tagSchemaIdx], &size)
			tagSchemaIdx++
			recSchemaIdx++
		}

		for pointSchemaIdx < filedsSchemaLen {
			if err = t.appendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
				return size, err
			}
			pointSchemaIdx++
			recSchemaIdx++
		}

		for tagSchemaIdx < tagSchemaLen {
			c.appendTagToCol(&rec.ColVals[recSchemaIdx], &tags[tagSchemaIdx], &size)
			tagSchemaIdx++
			recSchemaIdx++
		}

		rec.ColVals[len(fields)+len(tags)].AppendInteger(time)
		size += int64(util.Int64SizeBytes)
		return size, nil
	}

	// slow path
	// schema less

	return size, errors.New("column store schemaless not support yet")
}

func (c *csMemTableImpl) appendTagToCol(col *record.ColVal, tag *influx.Tag, size *int64) {
	col.AppendString(tag.Value)
	*size += int64(len(tag.Value))
}

func (c *csMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mstsInfo map[string]*meta.MeasurementInfo, mst string) error {
	start := time.Now()
	msInfo := table.CreateMsInfo(mst, nil, rec)
	atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

	err := c.UpdatePrimaryKey(mst, mstsInfo)
	if err != nil {
		return err
	}
	start = time.Now()
	msInfo.CreateWriteChunkForColumnStore(mstsInfo[mst].ColStoreInfo.SortKey)
	msInfo.writeChunk.Mu.Lock()
	defer msInfo.writeChunk.Mu.Unlock()
	msInfo.writeChunk.WriteRec.rec.AppendRec(rec, 0, rec.RowNums())
	atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	return err
}

func (c *csMemTableImpl) UpdatePrimaryKey(msName string, mstsInfo map[string]*meta.MeasurementInfo) error {
	c.mu.RLock()
	_, ok := c.primaryKey[msName]
	c.mu.RUnlock()
	if ok {
		return nil
	}

	if mstInfo, ok := mstsInfo[msName]; ok && mstInfo != nil {
		primaryKey := make(record.Schemas, len(mstInfo.ColStoreInfo.PrimaryKey))
		for i, pk := range mstInfo.ColStoreInfo.PrimaryKey {
			if pk == record.TimeField {
				primaryKey[i] = record.Field{Name: pk, Type: influx.Field_Type_Int}
			} else {
				primaryKey[i] = record.Field{Name: pk, Type: record.ToPrimitiveType(mstInfo.Schema[pk])}
			}
			c.updatePrimaryKey(msName, primaryKey)
		}
	} else {
		return errors.New("measurements info not found")
	}
	return nil
}

func (c *csMemTableImpl) Reset(t *MemTable) {
	for _, msInfo := range t.msInfoMap {
		if msInfo.writeChunk == nil {
			continue
		}
		msInfo.writeChunk.WriteRec.rec.ResetForReuse()
		writeRecPool.Put(msInfo.writeChunk.WriteRec.rec)
	}
}

func JoinWriteRec(table *MemTable, msName string) {
	// arrow flight
	if table.msInfoMap[msName].writeChunk.WriteRec.rec != nil {
		return
	}
	// line protocol
	var rec *record.Record
	writeChunk := table.msInfoMap[msName].writeChunk
	rks := table.msInfoMap[msName].concurrencyChunks
	writeChunk.WriteRec.rec = rks.writeChunks[0].WriteRec.rec
	for i := 1; i < len(rks.writeChunks) && rks.writeChunks[i] != nil; i++ {
		rec = rks.writeChunks[i].WriteRec.rec
		if rec != nil {
			writeChunk.WriteRec.rec.AppendRec(rec, 0, rec.RowNums())
		}
	}
	table.msInfoMap[msName].writeChunk = writeChunk
}

func SortAndDedup(table *MemTable, msName string) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok {
		return
	}
	JoinWriteRec(table, msName)
	msInfo.writeChunk.SortRecord()
}

func createRowChunks(msi *MsInfo, sortKeys []string) {
	msi.mu.Lock()
	if msi.concurrencyChunks.writeChunks != nil {
		msi.mu.Unlock()
		return
	}
	msi.concurrencyChunks.writeChunks = make([]*WriteChunkForColumnStore, defaultWriteRecNum)
	msi.writeChunk = &WriteChunkForColumnStore{WriteRec: WriteRec{}}
	msi.writeChunk.sortKeys = GetPrimaryKeys(msi.Schema, sortKeys)
	msi.mu.Unlock()
}

func getFreeWriteChunk(msi *MsInfo) (*WriteChunkForColumnStore, int) {
	msi.mu.Lock()
	defer msi.mu.Unlock()
	i := checkStatus(msi)
	if i == -1 {
		logger.GetLogger().Info("there is no free writeChunk")
	}
	if msi.concurrencyChunks.writeChunks[i] == nil {
		msi.concurrencyChunks.writeChunks[i] = &WriteChunkForColumnStore{}
		msi.concurrencyChunks.writeChunks[i].WriteRec.initForReuse(msi.Schema)
	}
	msi.concurrencyChunks.rowChunksStatus[i] = 1
	return msi.concurrencyChunks.writeChunks[i], i
}

func checkStatus(msi *MsInfo) int {
	for i := range msi.concurrencyChunks.rowChunksStatus {
		if msi.concurrencyChunks.rowChunksStatus[i] == 0 {
			return i
		}
	}
	return -1
}

type rowChunks struct {
	writeChunks         []*WriteChunkForColumnStore
	rowChunksStatus     []int
	writeRowChunksToken chan struct{}
}

func SetWriteChunk(msi *MsInfo, rec *record.Record) {
	writeChunk := &WriteChunkForColumnStore{WriteRec: WriteRec{rec: rec}}
	for i := range msi.concurrencyChunks.writeChunks {
		if msi.concurrencyChunks.writeChunks[i] == nil {
			msi.concurrencyChunks.writeChunks[i] = writeChunk
			return
		}
	}
}

func (r *rowChunks) GetWriteChunks() []*WriteChunkForColumnStore {
	return r.writeChunks
}
