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
	mu                  sync.RWMutex
	primaryKey          map[string]record.Schemas // mst -> primary key
	timeClusterDuration map[string]time.Duration  // mst -> time cluster duration
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
	chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder, tcLocation int8) *immutable.MsBuilder {
	writeRec := chunk.WriteRec.GetRecord()
	writeMs = WriteRecordForFlush(writeRec, writeMs, tbStore, 0, true, math.MinInt64, c.primaryKey[msName])
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(writeRec.RowNums()))

	if writeMs != nil {
		if err := immutable.WriteIntoFile(writeMs, true, writeMs.GetPKInfoNum() != 0); err != nil {
			writeMs = nil
			logger.GetLogger().Error("rename init file failed", zap.String("mstName", msName), zap.Error(err))
			return writeMs
		}

		tbStore.AddTSSPFiles(writeMs.Name(), false, writeMs.Files...)
		if writeMs.GetPKInfoNum() != 0 {
			for i, file := range writeMs.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendIndexSuffix(immutable.RemoveTsspSuffix(dataFilePath))
				tbStore.AddPKFile(writeMs.Name(), indexFilePath, writeMs.GetPKRecord(i), writeMs.GetPKMark(i), tcLocation)
			}
		}

		writeMs = nil
	}

	return nil
}

func (c *csMemTableImpl) updateMstInfo(mst string, pk record.Schemas, duration time.Duration) {
	c.mu.RLock()
	_, okPrimary := c.primaryKey[mst]
	_, okTC := c.timeClusterDuration[mst]
	c.mu.RUnlock()
	if !okPrimary || !okTC {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, ok := c.primaryKey[mst]; !ok {
			c.primaryKey[mst] = pk
		}
		if _, ok := c.timeClusterDuration[mst]; !ok {
			c.timeClusterDuration[mst] = duration
		}
	}
}

func (c *csMemTableImpl) FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore) {
	msInfo, ok := table.msInfoMap[msName]
	if !ok {
		return
	}
	MergeSchema(table, msName)
	JoinWriteRec(table, msName)
	chunk := msInfo.writeChunk
	chunk.SortRecord(c.timeClusterDuration[msName])
	var tcLocation int8 = colstore.DefaultTCLocation
	if c.timeClusterDuration[msName] > 0 {
		tcLocation = 0
	}

	rec := chunk.WriteRec.GetRecord()
	if rec.RowNums() == 0 {
		return
	}
	conf := immutable.GetColStoreConfig()
	writeMs := createMsBuilder(tbStore, true, lock, dataPath, msName, 1, rec.Len(), conf)
	writeMs.SetTCLocation(tcLocation)
	writeMs.NewPKIndexWriter()
	c.flushChunkImp(dataPath, msName, lock, tbStore, chunk, writeMs, tcLocation)
}

func MergeSchema(table *MemTable, msName string) {
	// arrow flight
	if table.msInfoMap[msName].writeChunk.WriteRec.rec != nil {
		return
	}
	// line protocol
	schemaMap := dictpool.Dict{}
	concurrencyChunks := table.msInfoMap[msName].concurrencyChunks
	schemaChanged := false
	for i := 0; i < len(concurrencyChunks.writeChunks) && concurrencyChunks.writeChunks[i] != nil; i++ {
		if concurrencyChunks.writeChunks[i].sameSchema {
			continue
		}
		schemaChanged = true
		for j := range table.msInfoMap[msName].Schema {
			schemaMap.Set(table.msInfoMap[msName].Schema[j].Name, struct{}{})
		}
		break
	}

	if !schemaChanged {
		return
	}

	schema := table.msInfoMap[msName].Schema[:len(table.msInfoMap[msName].Schema)-1]
	for i := 0; i < len(concurrencyChunks.writeChunks) && concurrencyChunks.writeChunks[i] != nil; i++ {
		if concurrencyChunks.writeChunks[i].sameSchema {
			continue
		}
		s := concurrencyChunks.writeChunks[i].WriteRec.rec.Schema
		for j := 0; j < len(s); j++ {
			if !schemaMap.Has(s[j].Name) {
				ref := record.Field{Name: s[j].Name, Type: s[j].Type}
				schemaMap.Set(s[j].Name, struct{}{})
				schema = append(schema, ref)
			}
		}
	}
	// if schema changed
	sort.Sort(schema)
	schema = append(schema, record.Field{Name: record.TimeField, Type: influx.Field_Type_Int})
	table.msInfoMap[msName].Schema = schema
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
		err = c.UpdateMstInfo(msName, wc.MstsInfo)
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
			c.mu.Lock()
			UpdateMstRowCount(wc.MsRowCount, msName, int64(len(*rows)))
			c.mu.Unlock()
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
		chunk.sameSchema = false
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

func (c *csMemTableImpl) genRowFields(fields influx.Fields, tags []influx.Tag) influx.Fields {
	tf := make([]influx.Field, len(tags))
	for i := range tags {
		tf[i].Key = tags[i].Key
		tf[i].StrValue = tags[i].Value
		tf[i].Type = influx.Field_Type_String
	}
	fields = append(fields, tf...)
	return fields
}

func (c *csMemTableImpl) appendTagsFieldsToRecord(t *MemTable, rec *record.Record, fields []influx.Field, tags []influx.Tag, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	var err error
	if sameSchema {
		recSchemaIdx, pointSchemaIdx, tagSchemaIdx := 0, 0, 0
		filedsSchemaLen, tagSchemaLen := len(fields), len(tags)
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
	return c.appendToRecordWithSchemaLess(t, rec, fields, tags, time)
}

func (c *csMemTableImpl) appendToRecordWithSchemaLess(t *MemTable, rec *record.Record, fields influx.Fields, tags []influx.Tag, time int64) (int64, error) {
	fields = c.genRowFields(fields, tags)
	sort.Sort(&fields)
	return t.appendFieldsToRecordSlow(rec, fields, time)
}

func (c *csMemTableImpl) appendTagToCol(col *record.ColVal, tag *influx.Tag, size *int64) {
	col.AppendString(tag.Value)
	*size += int64(len(tag.Value))
}

func (c *csMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mstsInfo map[string]*meta.MeasurementInfo, mst string) error {
	start := time.Now()
	msInfo := table.CreateMsInfo(mst, nil, rec)
	atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

	err := c.UpdateMstInfo(mst, mstsInfo)
	if err != nil {
		return err
	}
	start = time.Now()
	msInfo.CreateWriteChunkForColumnStore(mstsInfo[mst].ColStoreInfo.SortKey)
	msInfo.writeChunk.Mu.Lock()
	defer msInfo.writeChunk.Mu.Unlock()
	c.appendRec(msInfo, rec, &msInfo.writeChunk.WriteRec)
	atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	return err
}

func (c *csMemTableImpl) appendRec(msInfo *MsInfo, rec *record.Record, writeRec *WriteRec) {
	// update schema
	sameSchema, idx := checkSchemaIsSameForCol(writeRec.rec.Schema, rec.Schema)
	// if needed pad
	if !sameSchema {
		//extend schema and colVal
		padCol(rec, writeRec, idx)
	}
	// append
	msInfo.writeChunk.WriteRec.rec.AppendRec(rec, 0, rec.RowNums())
}

func padCol(rec *record.Record, writeRec *WriteRec, idx []int) {
	oldRowNum, oldColNum := writeRec.rec.RowNums(), writeRec.rec.ColNums()
	writeRec.rec.ReserveSchemaAndColVal(len(idx))
	for i := 0; i < len(idx); i++ {
		writeRec.rec.Schema[oldColNum+i].Name = stringinterner.InternSafe(rec.Schema[idx[i]].Name)
		writeRec.rec.Schema[oldColNum+i].Type = rec.Schema[idx[i]].Type
		writeRec.rec.ColVals[oldColNum+i].PadColVal(rec.Schema[idx[i]].Type, oldRowNum)
	}
	sort.Sort(writeRec.rec)
}

func checkSchemaIsSameForCol(msSchema, fields record.Schemas) (bool, []int) {
	oldSchemaLen, schemaLen := len(msSchema), len(fields)
	oldSchemaIdx, schemaIdx := 0, 0
	result := make([]int, 0, len(fields))
	for oldSchemaIdx < oldSchemaLen && schemaIdx < schemaLen {
		if msSchema[oldSchemaIdx].Name == fields[schemaIdx].Name {
			oldSchemaIdx++
			schemaIdx++
		} else if msSchema[oldSchemaIdx].Name < fields[schemaIdx].Name {
			oldSchemaIdx++
		} else {
			result = append(result, schemaIdx)
			schemaIdx++
		}
	}

	for schemaIdx < schemaLen {
		result = append(result, schemaIdx)
		schemaIdx++
	}
	if len(result) != 0 {
		return false, result
	}
	return true, nil
}

func (c *csMemTableImpl) UpdateMstInfo(msName string, mstsInfo map[string]*meta.MeasurementInfo) error {
	c.mu.RLock()
	_, okPrimary := c.primaryKey[msName]
	_, okTC := c.timeClusterDuration[msName]
	c.mu.RUnlock()
	if okPrimary && okTC {
		return nil
	}

	mstInfo, ok := mstsInfo[msName]
	if !ok || mstInfo == nil {
		return errors.New("measurements info not found")
	}

	primaryKey := make(record.Schemas, len(mstInfo.ColStoreInfo.PrimaryKey))
	for i, pk := range mstInfo.ColStoreInfo.PrimaryKey {
		if pk == record.TimeField {
			primaryKey[i] = record.Field{Name: pk, Type: influx.Field_Type_Int}
		} else {
			primaryKey[i] = record.Field{Name: pk, Type: record.ToPrimitiveType(mstInfo.Schema[pk])}
		}
	}
	c.updateMstInfo(msName, primaryKey, mstInfo.ColStoreInfo.TimeClusterDuration)

	return nil
}

func (c *csMemTableImpl) Reset(t *MemTable) {
	for _, msInfo := range t.msInfoMap {
		if msInfo.writeChunk != nil {
			msInfo.writeChunk.WriteRec.rec.ResetForReuse()
			writeRecPool.Put(msInfo.writeChunk.WriteRec.rec)
		}
		if msInfo.concurrencyChunks != nil {
			for i := range msInfo.concurrencyChunks.writeChunks {
				if msInfo.concurrencyChunks.writeChunks[i] != nil {
					msInfo.concurrencyChunks.writeChunks[i].WriteRec.rec.ResetForReuse()
					writeRecPool.Put(msInfo.concurrencyChunks.writeChunks[i].WriteRec.rec)
				}
			}
		}
	}
}

func JoinWriteRec(table *MemTable, msName string) {
	// arrow flight
	if table.msInfoMap[msName].writeChunk.WriteRec.rec != nil {
		return
	}
	// line protocol
	var rec *record.Record
	table.msInfoMap[msName].writeChunk.WriteRec.initForReuse(table.msInfoMap[msName].Schema)
	writeChunk := table.msInfoMap[msName].writeChunk
	rks := table.msInfoMap[msName].concurrencyChunks
	for i := 0; i < len(rks.writeChunks) && rks.writeChunks[i] != nil; i++ {
		rec = rks.writeChunks[i].WriteRec.rec
		if rec != nil {
			writeChunk.WriteRec.rec.AppendRec(rec, 0, rec.RowNums())
		}
	}
	table.msInfoMap[msName].writeChunk = writeChunk
}

func createRowChunks(msi *MsInfo, sortKeys []string) {
	msi.mu.Lock()
	if msi.concurrencyChunks.writeChunks != nil {
		msi.mu.Unlock()
		return
	}
	msi.concurrencyChunks.writeChunks = make([]*WriteChunkForColumnStore, defaultWriteRecNum)
	msi.writeChunk = &WriteChunkForColumnStore{WriteRec: WriteRec{}, sameSchema: true}
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
		msi.concurrencyChunks.writeChunks[i].sameSchema = true
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
