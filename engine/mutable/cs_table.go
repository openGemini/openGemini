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
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
	"go.uber.org/zap"
)

const defaultWriteRecNum = 8

type FlushManager interface {
	SetDBInfo(db string, rp string)
	flushChunk(primaryKey record.Schemas, msName string, indexRelation *influxql.IndexRelation, tbStore immutable.TablesStore,
		chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder, tcLocation int8)
	updateAccumulateMetaIndex(accumulateMetaIndex *immutable.AccumulateMetaIndex)
	getAccumulateMetaIndex() *immutable.AccumulateMetaIndex
}

type writeDetached struct {
	accumulateMetaIndex *immutable.AccumulateMetaIndex
	firstFlush          bool
	localBFCount        int64 //the block count of a local bloomFilter file
}

func (w *writeDetached) SetDBInfo(string, string) {}

func (w *writeDetached) updateAccumulateMetaIndex(accumulateMetaIndex *immutable.AccumulateMetaIndex) {
	w.accumulateMetaIndex = accumulateMetaIndex
}

func (w *writeDetached) getAccumulateMetaIndex() *immutable.AccumulateMetaIndex {
	return w.accumulateMetaIndex
}

func (w *writeDetached) flushChunk(pkSchema record.Schemas, msName string, indexRelation *influxql.IndexRelation, tbStore immutable.TablesStore,
	chunk *WriteChunkForColumnStore, writeMs *immutable.MsBuilder, tcLocation int8) {
	writeRec := chunk.WriteRec.GetRecord()
	w.WriteDetached(writeRec, writeMs, 0, pkSchema)
	atomic.AddInt64(&Statistics.PerfStat.FlushRowsCount, int64(writeRec.RowNums()))
}

func (w *writeDetached) WriteDetached(rec *record.Record, msb *immutable.MsBuilder, id uint64, pkSchema record.Schemas) {
	msb.SetLocalBfCount(w.localBFCount)
	err := msb.WriteDetached(id, rec, pkSchema, w.firstFlush, w.accumulateMetaIndex)
	if err != nil {
		panic(err)
	}
	w.localBFCount = msb.GetLocalBfCount()
	w.firstFlush = false
}

type CSMemTableImpl struct {
	db, rp              string
	mu                  sync.RWMutex
	flushManager        map[string]FlushManager // mst -> flush detached or attached
	accumulateMetaIndex *sync.Map               //mst -> immutable.AccumulateMetaIndex, record metaIndex for detached store
}

func NewCSMemTableImpl(db, rp string) *CSMemTableImpl {
	return &CSMemTableImpl{
		db:                  db,
		rp:                  rp,
		mu:                  sync.RWMutex{},
		flushManager:        make(map[string]FlushManager),
		accumulateMetaIndex: &sync.Map{},
	}
}

func (c *CSMemTableImpl) SetDbRp(db, rp string) {
	c.db = db
	c.rp = rp
}

func (c *CSMemTableImpl) initMsInfo(msInfo *MsInfo, row *influx.Row, rec *record.Record, name string) *MsInfo {
	if rec != nil {
		msInfo.Name = name
		msInfo.Schema = make(record.Schemas, len(rec.Schema))
		copy(msInfo.Schema, rec.Schema)
		return msInfo
	}
	msInfo.Name = row.Name
	msInfo.concurrencyChunks = &rowChunks{writeRowChunksToken: make(chan struct{}, defaultWriteRecNum),
		rowChunksStatus: make([]int, defaultWriteRecNum)}
	genMsSchemaForColumnStore(&msInfo.Schema, row.Fields, row.Tags)
	return msInfo
}

func (c *CSMemTableImpl) SetFlushManagerInfo(manager map[string]FlushManager, accumulateMetaIndex *sync.Map) {
	c.flushManager = manager
	c.accumulateMetaIndex = accumulateMetaIndex
}

func (c *CSMemTableImpl) getAccumulateMetaIndex(name string) *immutable.AccumulateMetaIndex {
	aMetaIndex, ok := c.accumulateMetaIndex.Load(name)
	if !ok {
		return &immutable.AccumulateMetaIndex{}
	}
	return aMetaIndex.(*immutable.AccumulateMetaIndex)
}

func (c *CSMemTableImpl) updateAccumulateMetaIndexInfo(name string, index *immutable.AccumulateMetaIndex) {
	c.accumulateMetaIndex.Store(name, index)
}

func (c *CSMemTableImpl) getFlushManager(detachedEnabled bool, writeMs *immutable.MsBuilder, recSchema record.Schemas, msName,
	lockPath string) FlushManager {
	c.mu.RLock()
	fManager, ok := c.flushManager[msName]
	c.mu.RUnlock()
	aMetaIndex := c.getAccumulateMetaIndex(msName)
	if !ok {
		if detachedEnabled {
			localBFCount := sparseindex.GetLocalBloomFilterBlockCnts(writeMs.Path, msName, lockPath, recSchema, writeMs.GetIndexBuilder().GetBfFirstSchemaIdx(), writeMs.GetFullTextIdx())
			fManager = &writeDetached{
				accumulateMetaIndex: aMetaIndex,
				firstFlush:          aMetaIndex.GetBlockId() == 0,
				localBFCount:        localBFCount,
			}
		} else {
			fManager = &WriteAttached{}
		}
		c.mu.Lock()
		c.flushManager[msName] = fManager
		c.mu.Unlock()
		return fManager
	}
	fManager.updateAccumulateMetaIndex(aMetaIndex)
	return fManager
}

func (c *CSMemTableImpl) FlushChunks(table *MemTable, dataPath, msName, db, rp string, lock *string, tbStore immutable.TablesStore, msRowCount int64, fileInfos chan []immutable.FileInfoExtend) {
	mstIdent := colstore.NewMeasurementIdent(db, rp)
	mstIdent.SetName(msName)

	MergeSchema(table, msName)
	JoinWriteRec(table, msName)

	if immutable.GetDetachedFlushEnabled() || config.GetProductType() == config.LogKeeper {
		c.FlushChunksDetached(table, dataPath, mstIdent, lock, tbStore, msRowCount, fileInfos)
		return
	}

	msInfo, ok := table.msInfoMap[msName]
	if !ok {
		return
	}

	mst, ok := colstore.MstManagerIns().GetByIdent(mstIdent)
	if !ok || mst == nil {
		logger.GetLogger().Error("measurement is not exits", zap.String("mst", msName))
		return
	}

	chunk := msInfo.writeChunk
	rec := chunk.WriteRec.GetRecord()
	if rec.RowNums() == 0 {
		return
	}

	indexRelation := mst.IndexRelation()
	at := NewWriteAttached(msName, mst.PrimaryKey(), mst.SortKey(), &indexRelation)
	at.FlushRecord(tbStore, chunk.WriteRec.GetRecord())
}

func (c *CSMemTableImpl) FlushChunksDetached(table *MemTable, dataPath string, ident colstore.MeasurementIdent, lock *string, tbStore immutable.TablesStore, msRowCount int64, fileInfos chan []immutable.FileInfoExtend) {
	msInfo, ok := table.msInfoMap[ident.Name]
	if !ok {
		return
	}

	rec := msInfo.writeChunk.WriteRec.GetRecord()
	if rec.RowNums() == 0 {
		return
	}

	// add seq id col if needed
	c.updateSeqIdCol(msRowCount-int64(msInfo.writeChunk.WriteRec.rec.RowNums()), rec)

	var indexRelation influxql.IndexRelation
	var timeClusterDuration time.Duration

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok || mst == nil {
		logger.GetLogger().Error("measurement is not exits", zap.String("mst", ident.Name))
		return
	}

	indexRelation = mst.IndexRelation()
	timeClusterDuration = mst.ColStoreInfo().TimeClusterDuration

	primaryKey := mst.PrimaryKey()
	chunk := msInfo.writeChunk
	chunk.SortRecord(timeClusterDuration)
	timeSorted := chunk.TimeSorted()

	conf := immutable.GetColStoreConfig()
	writeMs := c.createMsBuilder(tbStore, lock, dataPath, ident.Name, 1, rec.Len(), conf, config.COLUMNSTORE, mst.MeasurementInfo(), logstore.IsFullTextIdx(&indexRelation))
	if writeMs == nil {
		return
	}
	var tcLocation = colstore.DefaultTCLocation
	if timeClusterDuration > 0 {
		tcLocation = 0
	}
	writeMs.SetTCLocation(tcLocation)
	writeMs.SetTimeSorted(timeSorted)
	writeMs.NewPKIndexWriter()
	writeMs.NewIndexWriterBuilder(rec.Schema, indexRelation)

	fManager := c.getFlushManager(immutable.GetDetachedFlushEnabled(), writeMs, rec.Schema, ident.Name, *lock)
	fManager.SetDBInfo(ident.DB, ident.RP)
	fManager.flushChunk(primaryKey, ident.Name, &indexRelation, tbStore, chunk, writeMs, tcLocation) // column store flush one chunk each time
	if fileInfos != nil {
		fileInfos <- writeMs.FilesInfo
	}
	//after finishing flush update accumulate metaIndex info to csMemTableImpl
	c.updateAccumulateMetaIndexInfo(ident.Name, fManager.getAccumulateMetaIndex())
}

func (c *CSMemTableImpl) createMsBuilder(tbStore immutable.TablesStore, lockPath *string, dataPath string,
	msName string, totalChunks int, size int, conf *immutable.Config, engineType config.EngineType, mstInfo *meta.MeasurementInfo, fullTextIdx bool) *immutable.MsBuilder {
	seq := tbStore.Sequencer()
	defer seq.UnRef()

	FileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, true, lockPath)
	var msb *immutable.MsBuilder
	var err error
	if immutable.GetDetachedFlushEnabled() {
		lock := fileops.FileLockOption(*lockPath)
		dir := filepath.Join(dataPath, msName)
		_ = fileops.MkdirAll(dir, 0750, lock)
		bfCols := mstInfo.IndexRelation.GetBloomFilterColumns()
		msb, err = immutable.NewDetachedMsBuilder(dataPath, msName, lockPath, conf, totalChunks, FileName, tbStore.Tier(),
			seq, size, engineType, mstInfo.ObsOptions, bfCols, fullTextIdx)
		if err != nil {
			logger.GetLogger().Error("new detached msBuilder error", zap.Error(err))
			return nil
		}
	} else {
		msb = immutable.NewMsBuilder(dataPath, msName, lockPath, conf, totalChunks, FileName, tbStore.Tier(), seq, size, engineType, mstInfo.ObsOptions, tbStore.GetShardID())
	}
	msb.SetFullTextIdx(fullTextIdx)
	return msb
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

func (c *CSMemTableImpl) WriteRows(table *MemTable, rowsD *dictpool.Dict, ctx WriteRowsCtx) error {
	var err error
	var idx int
	var writeChunk *WriteChunkForColumnStore

	ident := colstore.NewMeasurementIdent(c.db, c.rp)

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
		ident.SetName(msName)
		mstInfo, ok := colstore.MstManagerIns().GetByIdent(ident)
		if !ok {
			logger.GetLogger().Warn("mstInfo is nil", zap.String("ident", ident.String()))
			return errors.New("measurement info is not found")
		}

		createRowChunks(msInfo, mstInfo.ColStoreInfo().SortKey)
		msInfo.concurrencyChunks.writeRowChunksToken <- struct{}{}
		// only writeRowChunksToken is available will getFreeWriteChunk
		writeChunk, idx = getFreeWriteChunk(msInfo)
		for index := range rs {
			_, err = c.appendFields(table, writeChunk, rs[index].Timestamp, rs[index].Fields, rs[index].Tags)
			if err != nil {
				return err
			}
		}

		// update the row count for each mst
		if ctx.MsRowCount != nil {
			c.mu.Lock()
			UpdateMstRowCount(ctx.MsRowCount, msName, int64(len(*rows)))
			c.mu.Unlock()
		}
		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
		msInfo.concurrencyChunks.rowChunksStatus[idx] = 0
		<-msInfo.concurrencyChunks.writeRowChunksToken
	}
	return err
}

func (c *CSMemTableImpl) updateSeqIdCol(startSeqId int64, rec *record.Record) {
	if !config.IsLogKeeper() {
		return
	}
	record.UpdateSeqIdCol(startSeqId, rec)
}

func (c *CSMemTableImpl) appendFields(table *MemTable, chunk *WriteChunkForColumnStore, time int64, fields []influx.Field, tags []influx.Tag) (int64, error) {
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

func (c *CSMemTableImpl) genRowFields(fields influx.Fields, tags []influx.Tag) influx.Fields {
	tf := make([]influx.Field, len(tags))
	for i := range tags {
		tf[i].Key = tags[i].Key
		tf[i].StrValue = tags[i].Value
		tf[i].Type = influx.Field_Type_String
	}
	fields = append(fields, tf...)
	return fields
}

func (c *CSMemTableImpl) appendTagsFieldsToRecord(t *MemTable, rec *record.Record, fields []influx.Field, tags []influx.Tag, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	var err error
	if sameSchema {
		recSchemaIdx, pointSchemaIdx, tagSchemaIdx := 0, 0, 0
		filedsSchemaLen, tagSchemaLen := len(fields), len(tags)
		for pointSchemaIdx < filedsSchemaLen && tagSchemaIdx < tagSchemaLen {
			if fields[pointSchemaIdx].Key < tags[tagSchemaIdx].Key {
				if err = record.AppendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
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
			if err = record.AppendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
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

func (c *CSMemTableImpl) appendToRecordWithSchemaLess(t *MemTable, rec *record.Record, fields influx.Fields, tags []influx.Tag, time int64) (int64, error) {
	fields = c.genRowFields(fields, tags)
	sort.Sort(&fields)
	return record.AppendFieldsToRecordSlow(rec, fields, time)
}

func (c *CSMemTableImpl) appendTagToCol(col *record.ColVal, tag *influx.Tag, size *int64) {
	col.AppendString(tag.Value)
	*size += int64(len(tag.Value))
}

func (c *CSMemTableImpl) WriteCols(table *MemTable, rec *record.Record, mst string) error {
	start := time.Now()
	mst = stringinterner.InternSafe(mst)
	msInfo := table.CreateMsInfo(mst, nil, rec)
	atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

	mi, ok := colstore.MstManagerIns().Get(c.db, c.rp, mst)
	if !ok {
		logger.GetLogger().Info("mstInfo is nil", zap.String("mst name", mst))
		return errors.New("measurement info is not found")
	}

	start = time.Now()
	msInfo.CreateWriteChunkForColumnStore(mi.ColStoreInfo().SortKey)
	msInfo.writeChunk.Mu.Lock()
	defer msInfo.writeChunk.Mu.Unlock()
	c.appendRec(msInfo, rec, &msInfo.writeChunk.WriteRec)
	atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	return nil
}

func (c *CSMemTableImpl) appendRec(msInfo *MsInfo, rec *record.Record, writeRec *WriteRec) {
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

func (c *CSMemTableImpl) Reset(t *MemTable) {
	for _, msInfo := range t.msInfoMap {
		if msInfo.writeChunk != nil {
			msInfo.writeChunk.WriteRec.rec.ReuseForWrite()
			writeRecPool.Put(msInfo.writeChunk.WriteRec.rec)
		}
		if msInfo.concurrencyChunks != nil {
			for i := range msInfo.concurrencyChunks.writeChunks {
				if msInfo.concurrencyChunks.writeChunks[i] != nil {
					msInfo.concurrencyChunks.writeChunks[i].WriteRec.rec.ReuseForWrite()
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
	msi.writeChunk.sortKeys = GetSortKeys(msi.Schema, sortKeys)
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
		msi.concurrencyChunks.writeChunks[i] = NewWriteChunkForColumnStore()
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
