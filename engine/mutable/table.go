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

package mutable

import (
	"errors"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

type WriteRowsCtx struct {
	GetLastFlushTime  func(msName string, sid uint64) int64
	AddRowCountsBySid func(msName string, sid uint64, rowCounts int64)
	MstsInfo          map[string]*meta.MeasurementInfo
}

type WriteRec struct {
	rec            *record.Record
	lastAppendTime int64
	timeAsd        bool
	schemaCopyed   bool
}

type WriteChunk struct {
	Mu              sync.Mutex
	Sid             uint64
	LastFlushTime   int64
	OrderWriteRec   WriteRec
	UnOrderWriteRec WriteRec
}

type WriteChunkForColumnStore struct {
	Mu          sync.Mutex
	WriteRec    WriteRec
	primaryKeys []record.PrimaryKey
	sortKeys    []record.PrimaryKey
}

var writeRecPool pool.FixedPool

func InitWriteRecPool(size int) {
	writeRecPool.Reset(size, func() interface{} {
		return &record.Record{}
	}, nil)
}
func (writeRec *WriteRec) init(schema []record.Field) {
	if writeRec.rec == nil {
		writeRec.rec = record.NewRecordBuilder(schema)
	} else {
		writeRec.rec.ResetWithSchema(schema)
	}
	writeRec.lastAppendTime = math.MinInt64
	writeRec.timeAsd = true
	writeRec.schemaCopyed = false
}

func (writeRec *WriteRec) initForReuse(schema []record.Field) {
	rec, ok := writeRecPool.Get().(*record.Record)
	if !ok {
		rec = &record.Record{}
	}
	writeRec.rec = rec
	writeRec.rec.ResetWithSchema(schema)
	writeRec.lastAppendTime = math.MinInt64
	writeRec.timeAsd = true
}

func (writeRec *WriteRec) GetRecord() *record.Record {
	return writeRec.rec
}

func (writeRec *WriteRec) SetLastAppendTime(v int64) {
	if v > writeRec.lastAppendTime {
		writeRec.lastAppendTime = v
	}
}

func (writeRec *WriteRec) SetWriteRec(rec *record.Record) {
	writeRec.rec = rec
}

func (chunk *WriteChunk) Init(sid uint64, schema []record.Field) {
	chunk.Sid = sid
	chunk.LastFlushTime = math.MinInt64
	chunk.OrderWriteRec.init(schema)
	chunk.UnOrderWriteRec.init(schema)
}

type MsInfo struct {
	mu         sync.RWMutex
	Name       string // measurement name with version
	Schema     record.Schemas
	sidMap     map[uint64]*WriteChunk
	chunkBufs  []WriteChunk
	writeChunk *WriteChunkForColumnStore
	TimeAsd    bool
}

func (msi *MsInfo) Init(row *influx.Row) {
	msi.Name = row.Name
	genMsSchema(&msi.Schema, row.Fields)
	msi.TimeAsd = true
	msi.sidMap = make(map[uint64]*WriteChunk)
}

func (msi *MsInfo) allocChunk() *WriteChunk {
	if cap(msi.chunkBufs) == len(msi.chunkBufs) {
		msi.chunkBufs = make([]WriteChunk, 0, 64)
	}
	msi.chunkBufs = msi.chunkBufs[:len(msi.chunkBufs)+1]

	return &msi.chunkBufs[len(msi.chunkBufs)-1]
}

func (msi *MsInfo) CreateChunk(sid uint64) (*WriteChunk, bool) {
	msi.mu.Lock()
	chunk, ok := msi.sidMap[sid]
	if !ok {
		// init chunk buffer
		chunk = msi.allocChunk()
		chunk.Init(sid, msi.Schema)
		msi.sidMap[sid] = chunk
	}
	msi.mu.Unlock()
	return chunk, ok
}

func (msi *MsInfo) CreateWriteChunkForColumnStore(primaryKeys, sortKeys []string) {
	msi.mu.Lock()
	if msi.writeChunk != nil {
		msi.mu.Unlock()
		return
	}
	msi.writeChunk = &WriteChunkForColumnStore{}
	msi.writeChunk.WriteRec.initForReuse(msi.Schema)
	msi.writeChunk.primaryKeys = GetPrimaryKeys(msi.Schema, primaryKeys)
	msi.writeChunk.sortKeys = GetPrimaryKeys(msi.Schema, sortKeys)
	msi.mu.Unlock()
}

func (msi *MsInfo) GetWriteChunk() *WriteChunkForColumnStore {
	return msi.writeChunk
}

func (msi *MsInfo) SetWriteChunk(writeChunk *WriteChunkForColumnStore) {
	msi.writeChunk = writeChunk
}

func GetPrimaryKeys(schema []record.Field, primaryKeys []string) []record.PrimaryKey {
	pk := make([]record.PrimaryKey, 0, len(primaryKeys))
	var filed record.PrimaryKey
	for i := range primaryKeys {
		for j := range schema {
			if primaryKeys[i] == schema[j].Name {
				filed.Key = schema[j].Name
				filed.Type = int32(schema[j].Type)
				pk = append(pk, filed)
			}
		}
	}
	return pk
}

type MTable interface {
	initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo
	ApplyConcurrency(table *MemTable, f func(msName string, ids []uint64))
	SortAndDedup(table *MemTable, msName string, ids []uint64)
	sortWriteRec(hlp *record.SortHelper, wRec *WriteRec, pk, sk []record.PrimaryKey)
	FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore, sids []uint64)
	WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error
	WriteCols(table *MemTable, rec *record.Record, mstsInfo map[string]*meta.MeasurementInfo, mst string) error
	Reset(table *MemTable)
}

type tsMemTableImpl struct {
}

func (t *tsMemTableImpl) ApplyConcurrency(table *MemTable, f func(msName string, sids []uint64)) {
	var wg sync.WaitGroup
	wg.Add(len(table.msInfoMap))
	for k := range table.msInfoMap {
		concurLimiter <- struct{}{}
		go func(msName string) {
			sids := table.GetSids(msName)
			sort.Sort(uint64Sids(sids))
			f(msName, sids)
			table.PutSids(sids)
			concurLimiter.Release()
			wg.Done()
		}(k)
	}
	wg.Wait()
}

func (t *tsMemTableImpl) SortAndDedup(table *MemTable, msName string, sids []uint64) {
	if table.msInfoMap[msName].TimeAsd {
		return
	}

	hlp := record.NewSortHelper()
	defer hlp.Release()

	sidMap := table.msInfoMap[msName].sidMap
	for _, v := range sids {
		writeChunk := sidMap[v]
		writeChunk.Mu.Lock()
		t.sortWriteRec(hlp, &writeChunk.OrderWriteRec, nil, nil)
		t.sortWriteRec(hlp, &writeChunk.UnOrderWriteRec, nil, nil)
		writeChunk.Mu.Unlock()
	}
}

func (t *tsMemTableImpl) sortWriteRec(hlp *record.SortHelper, wRec *WriteRec, pk, sk []record.PrimaryKey) {
	if !wRec.timeAsd {
		wRec.rec = hlp.Sort(wRec.rec)
		wRec.timeAsd = true
	}
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
			writeMs = createMsBuilder(tbStore, isOrder, lockPath, dataPath, msName, totalChunks, writeRec.Len(), conf)
		}
		writeMs = WriteRecordForFlush(writeRec, writeMs, tbStore, chunk.Sid, isOrder, chunk.LastFlushTime, nil)
	}
	if finish {
		if writeMs != nil {
			if err := WriteIntoFile(writeMs, true, false); err != nil {
				if os.IsNotExist(err) {
					writeMs = nil
					logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
					return writeMs
				}
				panic(err)
			}

			tbStore.AddTSSPFiles(writeMs.Name(), isOrder, writeMs.Files...)
			writeMs = nil
		}
	}

	return writeMs
}

func (t *tsMemTableImpl) FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore, sids []uint64) {
	sidMap := table.msInfoMap[msName].sidMap
	var orderMs, unOrderMs *immutable.MsBuilder
	sidLen := len(sids)
	for i := range sids {
		if i < sidLen-1 {
			orderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], orderMs, false, true)
			atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(sidMap[sids[i]].OrderWriteRec.GetRecord().RowNums()))

			unOrderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], unOrderMs, false, false)
			atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(sidMap[sids[i]].UnOrderWriteRec.GetRecord().RowNums()))
			atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(sidMap[sids[i]].OrderWriteRec.GetRecord().RowNums())+int64(sidMap[sids[i]].UnOrderWriteRec.GetRecord().RowNums()))
		} else {
			orderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], orderMs, true, true)
			atomic.AddInt64(&Statistics.PerfStat.FlushOrderRowsCount, int64(sidMap[sids[i]].OrderWriteRec.GetRecord().RowNums()))

			unOrderMs = t.flushChunkImp(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], unOrderMs, true, false)
			atomic.AddInt64(&Statistics.PerfStat.FlushUnOrderRowsCount, int64(sidMap[sids[i]].UnOrderWriteRec.GetRecord().RowNums()))
			atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(sidMap[sids[i]].OrderWriteRec.GetRecord().RowNums())+int64(sidMap[sids[i]].UnOrderWriteRec.GetRecord().RowNums()))
		}
	}
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
		msInfo := table.CreateMsInfo(msName, &rs[0], nil, false)
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
		msInfo.TimeAsd = false
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

type csMemTableImpl struct {
	mu         sync.RWMutex
	primaryKey map[string]record.Schemas // mst -> primary key
}

func (c *csMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	if rec != nil {
		msInfo.Name = name
		msInfo.Schema = rec.Schema
		msInfo.TimeAsd = true
		return msInfo
	}
	msInfo.Name = row.Name
	genMsSchemaForColumnStore(&msInfo.Schema, row.Fields, row.Tags)
	return msInfo
}

func (c *csMemTableImpl) ApplyConcurrency(table *MemTable, f func(msName string, ids []uint64)) {
	var wg sync.WaitGroup
	wg.Add(len(table.msInfoMap))
	for k := range table.msInfoMap {
		concurLimiter <- struct{}{}
		go func(msName string) {
			f(msName, nil)
			concurLimiter.Release()
			wg.Done()
		}(k)
	}
	wg.Wait()
}

func (c *csMemTableImpl) SortAndDedup(table *MemTable, msName string, ids []uint64) {
	hlp := record.NewSortHelper()
	defer hlp.Release()

	writeChunk := table.msInfoMap[msName].writeChunk
	writeChunk.Mu.Lock()
	c.sortWriteRec(hlp, &writeChunk.WriteRec, writeChunk.primaryKeys, writeChunk.sortKeys)
	writeChunk.Mu.Unlock()
}

func (c *csMemTableImpl) sortWriteRec(hlp *record.SortHelper, wRec *WriteRec, pk, sk []record.PrimaryKey) {
	wRec.rec = hlp.SortForColumnStore(wRec.rec, hlp.SortData, pk, sk)
}

func (c *csMemTableImpl) flushChunkImp(dataPath, msName string, lockPath *string, tbStore immutable.TablesStore,
	chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder) *immutable.MsBuilder {
	writeRec := chunk.WriteRec.GetRecord()
	writeMs = WriteRecordForFlush(writeRec, writeMs, tbStore, 0, true, math.MinInt64, c.primaryKey[msName])
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(writeRec.RowNums()))

	if writeMs != nil {
		if err := WriteIntoFile(writeMs, true, writeMs.GetPKInfoNum() != 0); err != nil {
			if os.IsNotExist(err) {
				writeMs = nil
				logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
				return writeMs
			}
			panic(err)
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

func (c *csMemTableImpl) FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore, ids []uint64) {
	msInfo := table.msInfoMap[msName]
	rec := msInfo.writeChunk.WriteRec.GetRecord()
	if rec.RowNums() == 0 {
		return
	}
	conf := immutable.GetColStoreConfig()
	writeMs := createMsBuilder(tbStore, true, lock, dataPath, msName, 1, rec.Len(), conf)
	writeMs.NewPKIndexWriter()
	c.flushChunkImp(dataPath, msName, lock, tbStore, msInfo.writeChunk, writeMs)
}

func (c *csMemTableImpl) WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error {
	var err error
	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		rs := *rows
		msName := stringinterner.InternSafe(mapp.Key)

		start := time.Now()
		msInfo := table.CreateMsInfo(msName, &rs[0], nil, true)
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

		start = time.Now()
		err = c.UpdatePrimaryKey(msName, wc.MstsInfo)
		if err != nil {
			return err
		}
		msInfo.CreateWriteChunkForColumnStore(wc.MstsInfo[msName].ColStoreInfo.PrimaryKey, wc.MstsInfo[msName].ColStoreInfo.SortKey)

		for index := range rs {
			_, err = c.appendFields(table, msInfo.writeChunk, rs[index].Timestamp, rs[index].Fields, rs[index].Tags)
			if err != nil {
				return err
			}
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
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
	msInfo := table.CreateMsInfo(mst, nil, rec, false)
	atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

	err := c.UpdatePrimaryKey(mst, mstsInfo)
	if err != nil {
		return err
	}
	start = time.Now()
	msInfo.CreateWriteChunkForColumnStore(mstsInfo[mst].ColStoreInfo.PrimaryKey, mstsInfo[mst].ColStoreInfo.SortKey)
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
				primaryKey[i] = record.Field{Name: pk, Type: int(mstInfo.Schema[pk])}
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

func WriteIntoFile(msb *immutable.MsBuilder, tmp bool, withPKIndex bool) error {
	f, err := msb.NewTSSPFile(tmp)
	if err != nil {
		panic(err)
	}
	if f != nil {
		msb.Files = append(msb.Files, f)
	}

	if !withPKIndex {
		err = immutable.RenameTmpFiles(msb.Files)
	} else {
		err = immutable.RenameTmpFilesWithPKIndex(msb.Files)
	}

	if err != nil {
		return err
	}
	return nil
}

func WriteRecordForFlush(rec *record.Record, msb *immutable.MsBuilder, tbStore immutable.TablesStore, id uint64, order bool, lastFlushTime int64, schema record.Schemas) *immutable.MsBuilder {
	var err error

	if !order && lastFlushTime == math.MaxInt64 {
		msb.StoreTimes()
	}

	msb, err = msb.WriteRecord(id, rec, schema, func(fn immutable.TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
		return tbStore.NextSequence(), 0, 0, 0
	})
	if err != nil {
		panic(err)
	}

	return msb
}

func createMsBuilder(tbStore immutable.TablesStore, order bool, lockPath *string, dataPath string, msName string, totalChunks int, size int, conf *immutable.Config) *immutable.MsBuilder {
	seq := tbStore.Sequencer()
	defer seq.UnRef()

	FileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, order, lockPath)
	msb := immutable.NewMsBuilder(dataPath, msName, lockPath, conf, totalChunks, FileName, tbStore.Tier(), seq, size)
	return msb
}

type MemTable struct {
	mu  sync.RWMutex
	ref int32
	idx *ski.ShardKeyIndex

	msInfoMap map[string]*MsInfo // measurements schemas, {"cpu_0001": *MsInfo}
	msInfos   []MsInfo           // pre-allocation

	log     *zap.Logger
	memSize int64
	MTable  MTable //public method in MemTable
}

type MemTables struct {
	readEnable  bool
	activeTbl   *MemTable
	snapshotTbl *MemTable
}

func (m *MemTables) Init(activeTbl, snapshotTbl *MemTable, readEnable bool) {
	m.activeTbl = activeTbl
	m.snapshotTbl = snapshotTbl
	m.readEnable = readEnable
}

func (m *MemTables) Ref() {
	refMemTable(m.activeTbl)
	refMemTable(m.snapshotTbl)
}

func (m *MemTables) UnRef() {
	unrefMemTable(m.activeTbl)
	unrefMemTable(m.snapshotTbl)
}

func (m *MemTables) Values(msName string, id uint64, tr util.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	if !m.readEnable {
		return nil
	}

	var getValues = func(mt *MemTable) *record.Record {
		if mt == nil {
			return nil
		}
		return mt.values(msName, id, tr, schema, ascending)
	}

	snapshotRec := getValues(m.snapshotTbl)
	activeRec := getValues(m.activeTbl)
	if activeRec == nil {
		return snapshotRec
	} else if snapshotRec == nil {
		return activeRec
	}

	var mergeRecord record.Record
	if ascending {
		mergeRecord.MergeRecord(activeRec, snapshotRec)
	} else {
		mergeRecord.MergeRecordDescend(activeRec, snapshotRec)
	}
	return &mergeRecord
}

var memTablePool sync.Pool
var memTablePoolCh = make(chan *MemTable, 4)

func GetMemTable(engineType config.EngineType) *MemTable {
	select {
	case v := <-memTablePoolCh:
		atomic.AddInt32(&v.ref, 1)
		return getMTable(v, engineType)
	default:
		if v := memTablePool.Get(); v != nil {
			memTbl, ok := v.(*MemTable)
			if !ok {
				panic("GetMemTable memTablePool get value isn't *MemTable type")
			}
			atomic.AddInt32(&memTbl.ref, 1)
			return getMTable(memTbl, engineType)
		}
		return NewMemTable(engineType)
	}
}

func getMTable(mmTable *MemTable, engineType config.EngineType) *MemTable {
	switch engineType {
	case config.TSSTORE:
		mmTable.MTable = &tsMemTableImpl{}
	case config.COLUMNSTORE:
		mmTable.MTable = &csMemTableImpl{
			primaryKey: make(map[string]record.Schemas),
		}
	default:
		panic("UnKnown engine type")
	}
	return mmTable
}

func (t *MemTable) SetIdx(idx *ski.ShardKeyIndex) {
	t.idx = idx
}

func (t *MemTable) Ref() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *MemTable) UnRef() {
	t.PutMemTable()
}

func (t *MemTable) PutMemTable() {
	if atomic.AddInt32(&t.ref, -1) == 0 {
		t.Reset()
		select {
		case memTablePoolCh <- t:
		default:
			memTablePool.Put(t)
		}
	}
}

func refMemTable(mt *MemTable) {
	if mt != nil {
		mt.Ref()
	}
}

func unrefMemTable(mt *MemTable) {
	if mt != nil {
		mt.UnRef()
	}
}

type uint64Sids []uint64

func (s uint64Sids) Len() int { return len(s) }
func (s uint64Sids) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s uint64Sids) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// nolint
func (t *MemTable) AddMemSize(size int64) {
	atomic.AddInt64(&t.memSize, size)
}

func (t *MemTable) GetMemSize() int64 {
	return atomic.LoadInt64(&t.memSize)
}

type SidsPool struct {
	pool chan []uint64
}

var sidsPool SidsPool

func InitMutablePool(size int) {
	sidsPool = SidsPool{pool: make(chan []uint64, size)}
}

func GetSidsImpl(size int) []uint64 {
	select {
	case sids := <-sidsPool.pool:
		if cap(sids) >= size {
			return sids[:0]
		}
		break
	default:
		break
	}
	return make([]uint64, 0, size)
}

func PutSidsImpl(sids []uint64) {
	select {
	case sidsPool.pool <- sids:
	default:
		break
	}
}

func (t *MemTable) PutSids(sids []uint64) {
	if len(sids) != 0 {
		PutSidsImpl(sids)
	}
}

func (t *MemTable) GetSids(msName string) []uint64 {
	info, ok := t.msInfoMap[msName]
	if !ok || info == nil {
		return nil
	}

	sids := GetSidsImpl(len(info.sidMap))
	for k := range info.sidMap {
		sids = append(sids, k)
	}
	return sids
}

func NewMemTable(engineType config.EngineType) *MemTable {
	wb := &MemTable{
		log:       logger.GetLogger(),
		ref:       1,
		msInfoMap: make(map[string]*MsInfo),
	}

	return getMTable(wb, engineType)
}

func (t *MemTable) NeedFlush() bool {
	return atomic.LoadInt64(&t.memSize) > GetSizeLimit()
}

func (t *MemTable) SetMsInfo(name string, msInfo *MsInfo) {
	t.msInfoMap[name] = msInfo
}

func (t *MemTable) GetMsInfo(name string) (*MsInfo, error) {
	msInfo, ok := t.msInfoMap[name]
	if !ok {
		return nil, errors.New("msInfoMap have not this info")
	}
	return msInfo, nil
}

func genMsSchema(msSchema *record.Schemas, fields []influx.Field) {
	schemaLen := len(fields) + 1
	if schemaLen > cap(*msSchema) {
		*msSchema = make(record.Schemas, schemaLen)
	} else {
		*msSchema = (*msSchema)[:schemaLen]
	}

	for i := range fields {
		(*msSchema)[i].Type = int(fields[i].Type)
		(*msSchema)[i].Name = stringinterner.InternSafe(fields[i].Key)
	}
	// append time column
	(*msSchema)[schemaLen-1].Type = influx.Field_Type_Int
	(*msSchema)[schemaLen-1].Name = record.TimeField
}

func genMsSchemaForColumnStore(msSchema *record.Schemas, fields []influx.Field, tags []influx.Tag) {
	schemaLen := len(fields) + len(tags)
	if schemaLen > cap(*msSchema) {
		*msSchema = make(record.Schemas, schemaLen)
	} else {
		*msSchema = (*msSchema)[:schemaLen]
	}

	// fast path
	if len(tags) == 0 {
		for i := range fields {
			(*msSchema)[i].Type = int(fields[i].Type)
			(*msSchema)[i].Name = stringinterner.InternSafe(fields[i].Key)
		}
	} else {
		msSchema = updateMsSchema(msSchema, fields, tags)
		sort.Sort(msSchema)
	}

	// append time column
	timeSchema := record.Field{Type: influx.Field_Type_Int,
		Name: record.TimeField}
	*msSchema = append(*msSchema, timeSchema)
}

func updateMsSchema(msSchema *record.Schemas, fields []influx.Field, tags []influx.Tag) *record.Schemas {
	for i := range tags {
		(*msSchema)[i].Type = influx.Field_Type_String
		(*msSchema)[i].Name = stringinterner.InternSafe(tags[i].Key)
	}

	for i := range fields {
		(*msSchema)[i+len(tags)].Type = int(fields[i].Type)
		(*msSchema)[i+len(tags)].Name = stringinterner.InternSafe(fields[i].Key)
	}

	return msSchema
}

func checkSchemaIsSame(msSchema record.Schemas, fields []influx.Field) bool {
	if len(fields) != len(msSchema)-1 {
		return false
	}
	for i := range fields {
		if msSchema[i].Name != fields[i].Key {
			return false
		}
	}
	return true
}

func checkSchemaIsSameWithTag(msSchema record.Schemas, fields []influx.Field, tags []influx.Tag) bool {
	if len(fields)+len(tags) != len(msSchema)-1 {
		return false
	}

	// fast path
	if len(tags) == 0 {
		for i := range fields {
			if msSchema[i].Name != fields[i].Key {
				return false
			}
		}
	} else {
		idxField, idxTag, idx := 0, 0, 0
		for idxField < len(fields) && idxTag < len(tags) {
			if msSchema[idx].Name != fields[idxField].Key && msSchema[idx].Name != tags[idxTag].Key {
				return false
			} else if msSchema[idx].Name != fields[idxField].Key {
				idx++
				idxTag++
			} else {
				idx++
				idxField++
			}
		}

		for idxField < len(fields) {
			if msSchema[idx].Name != fields[idxField].Key {
				return false
			}
			idx++
			idxField++
		}

		for idxTag < len(tags) {
			if msSchema[idx].Name != tags[idxTag].Key {
				return false
			}
			idx++
			idxTag++
		}
	}
	return true
}

func (t *MemTable) appendFieldToCol(col *record.ColVal, field *influx.Field, size *int64) error {
	if field.Type == influx.Field_Type_Int || field.Type == influx.Field_Type_UInt {
		col.AppendInteger(int64(field.NumValue))
		*size += int64(util.Int64SizeBytes)
	} else if field.Type == influx.Field_Type_Float {
		col.AppendFloat(field.NumValue)
		*size += int64(util.Float64SizeBytes)
	} else if field.Type == influx.Field_Type_Boolean {
		if field.NumValue == 0 {
			col.AppendBoolean(false)
		} else {
			col.AppendBoolean(true)
		}
		*size += int64(util.BooleanSizeBytes)
	} else if field.Type == influx.Field_Type_String {
		col.AppendString(field.StrValue)
		*size += int64(len(field.StrValue))
	} else {
		return errors.New("unsupport data type")
	}
	return nil
}

func (t *MemTable) appendFieldsToRecord(rec *record.Record, fields []influx.Field, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	if sameSchema {
		for i := range fields {
			if err := t.appendFieldToCol(&rec.ColVals[i], &fields[i], &size); err != nil {
				return size, err
			}
		}
		rec.ColVals[len(fields)].AppendInteger(time)
		size += int64(util.Int64SizeBytes)
		return size, nil
	}

	// slow path
	recSchemaIdx, pointSchemaIdx := 0, 0
	recSchemaLen, pointSchemaLen := rec.ColNums()-1, len(fields)
	appendColIdx := rec.ColNums()
	oldRowNum, oldColNum := rec.RowNums(), rec.ColNums()
	for recSchemaIdx < recSchemaLen && pointSchemaIdx < pointSchemaLen {
		if rec.Schema[recSchemaIdx].Name == fields[pointSchemaIdx].Key {
			if err := t.appendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
				return size, err
			}
			recSchemaIdx++
			pointSchemaIdx++
		} else if rec.Schema[recSchemaIdx].Name < fields[pointSchemaIdx].Key {
			// table field exists but point field not exist, exist field
			rec.ColVals[recSchemaIdx].PadColVal(rec.Schema[recSchemaIdx].Type, 1)
			recSchemaIdx++
		} else {
			// point field exists but table field not exist, new field
			rec.ReserveSchemaAndColVal(1)
			rec.Schema[appendColIdx].Name = stringinterner.InternSafe(fields[pointSchemaIdx].Key)
			rec.Schema[appendColIdx].Type = int(fields[pointSchemaIdx].Type)
			rec.ColVals[appendColIdx].PadColVal(int(fields[pointSchemaIdx].Type), oldRowNum)
			if err := t.appendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
				return size, err
			}
			pointSchemaIdx++
			appendColIdx++
		}
	}

	// table field exists but point field not exist, exist field
	for recSchemaIdx < recSchemaLen {
		rec.ColVals[recSchemaIdx].PadColVal(rec.Schema[recSchemaIdx].Type, 1)
		recSchemaIdx++
	}
	// point field exists but table field not exist, new field
	rec.ReserveSchemaAndColVal(pointSchemaLen - pointSchemaIdx)
	for pointSchemaIdx < pointSchemaLen {
		rec.Schema[appendColIdx].Name = stringinterner.InternSafe(fields[pointSchemaIdx].Key)
		rec.Schema[appendColIdx].Type = int(fields[pointSchemaIdx].Type)
		rec.ColVals[appendColIdx].PadColVal(int(fields[pointSchemaIdx].Type), oldRowNum)
		if err := t.appendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
			return size, err
		}
		pointSchemaIdx++
		appendColIdx++
	}

	// check if added new field
	newColNum := rec.ColNums()
	if oldColNum != newColNum {
		sort.Sort(rec)
	}
	rec.ColVals[newColNum-1].AppendInteger(time)
	size += int64(util.Int64SizeBytes)

	return size, nil
}

func (t *MemTable) allocMsInfo() *MsInfo {
	size := len(t.msInfos)
	if cap(t.msInfos) == size {
		t.msInfos = make([]MsInfo, 0, 64)
		size = 0
	}
	t.msInfos = t.msInfos[:size+1]
	return &t.msInfos[size]
}

func (t *MemTable) CreateMsInfo(name string, row *influx.Row, rec *record.Record, needTag bool) *MsInfo {
	t.mu.RLock()
	msInfo, ok := t.msInfoMap[name]
	t.mu.RUnlock()

	if ok {
		return msInfo
	}

	t.mu.Lock()
	msInfo, ok = t.msInfoMap[name]
	if !ok {
		msInfo = t.allocMsInfo()
		msInfo = t.MTable.initMsInfo(msInfo, row, rec, name)
		t.msInfoMap[name] = msInfo
	}
	t.mu.Unlock()

	return msInfo
}

func (t *MemTable) Reset() {
	t.MTable.Reset(t)
	t.memSize = 0
	t.msInfos = make([]MsInfo, 0, len(t.msInfoMap))
	t.msInfoMap = make(map[string]*MsInfo, len(t.msInfoMap))
	t.idx = nil
	t.MTable = nil
}

func (t *MemTable) getSortedRecSafe(msName string, id uint64, tr util.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	// check measurement exist or not
	t.mu.RLock()
	msInfo, ok := t.msInfoMap[msName]
	t.mu.RUnlock()
	if !ok {
		return nil
	}

	// check sid exist or not
	msInfo.mu.RLock()
	chunk, ok := msInfo.sidMap[id]
	msInfo.mu.RUnlock()
	if !ok {
		return nil
	}

	hlp := record.NewSortHelper()
	defer hlp.Release()

	var rec *record.Record
	chunk.Mu.Lock()
	t.MTable.sortWriteRec(hlp, &chunk.OrderWriteRec, nil, nil)
	t.MTable.sortWriteRec(hlp, &chunk.UnOrderWriteRec, nil, nil)

	if chunk.OrderWriteRec.rec.RowNums() == 0 {
		rec = chunk.UnOrderWriteRec.rec.CopyWithCondition(ascending, tr, schema)
		chunk.Mu.Unlock()
		return rec
	}

	if chunk.UnOrderWriteRec.rec.RowNums() == 0 {
		rec = chunk.OrderWriteRec.rec.CopyWithCondition(ascending, tr, schema)
		chunk.Mu.Unlock()
		return rec
	}

	rec = &record.Record{}
	rec.MergeRecord(chunk.OrderWriteRec.rec, chunk.UnOrderWriteRec.rec)
	chunk.Mu.Unlock()

	return rec.CopyWithCondition(ascending, tr, schema)
}

func (t *MemTable) values(msName string, id uint64, tr util.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	// column of sid need sort and dedupe
	rec := t.getSortedRecSafe(msName, id, tr, schema, ascending)
	if rec != nil {
		sort.Sort(rec)
	}
	return rec
}

func (t *MemTable) GetMaxTimeBySidNoLock(msName string, sid uint64) int64 {
	msInfo, ok := t.msInfoMap[msName]
	if !ok {
		return math.MinInt64
	}
	chunk, ok := msInfo.sidMap[sid]
	if !ok {
		return math.MinInt64
	}

	if chunk.UnOrderWriteRec.lastAppendTime > chunk.OrderWriteRec.lastAppendTime {
		return chunk.UnOrderWriteRec.lastAppendTime
	}
	return chunk.OrderWriteRec.lastAppendTime
}
