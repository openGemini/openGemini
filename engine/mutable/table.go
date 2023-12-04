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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type WriteRowsCtx struct {
	GetLastFlushTime  func(msName string, sid uint64) int64
	AddRowCountsBySid func(msName string, sid uint64, rowCounts int64)
	MstsInfo          map[string]*meta.MeasurementInfo
	MsRowCount        *sync.Map
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

func (chunk *WriteChunk) Init(sid uint64, schema []record.Field) {
	chunk.Sid = sid
	chunk.LastFlushTime = math.MinInt64
	chunk.OrderWriteRec.init(schema)
	chunk.UnOrderWriteRec.init(schema)
}

func (chunk *WriteChunk) SortRecordNoLock(hlp *record.SortHelper) {
	chunk.OrderWriteRec.SortRecord(hlp)
	chunk.UnOrderWriteRec.SortRecord(hlp)
}

func (chunk *WriteChunk) SortRecord(hlp *record.SortHelper) {
	chunk.Mu.Lock()
	chunk.SortRecordNoLock(hlp)
	chunk.Mu.Unlock()
}

type WriteChunkForColumnStore struct {
	Mu         sync.Mutex
	WriteRec   WriteRec
	sortKeys   []record.PrimaryKey
	sameSchema bool
}

func (chunk *WriteChunkForColumnStore) SortRecord(tcDuration time.Duration) {
	hlp := record.NewSortHelper()
	chunk.Mu.Lock()
	chunk.WriteRec.rec = hlp.SortForColumnStore(chunk.WriteRec.rec, chunk.sortKeys, false, tcDuration)
	chunk.Mu.Unlock()
	hlp.Release()
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

func (writeRec *WriteRec) SortRecord(hlp *record.SortHelper) {
	if !writeRec.timeAsd {
		writeRec.rec = hlp.Sort(writeRec.rec)
		writeRec.timeAsd = true
	}
}

type MsInfo struct {
	mu                sync.RWMutex
	Name              string // measurement name with version
	Schema            record.Schemas
	sidMap            map[uint64]*WriteChunk
	chunkBufs         []WriteChunk
	writeChunk        *WriteChunkForColumnStore
	concurrencyChunks *rowChunks
}

func (msi *MsInfo) Init(row *influx.Row) {
	msi.Name = row.Name
	genMsSchema(&msi.Schema, row.Fields)
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
		chunk = msi.allocChunk()
		chunk.Init(sid, msi.Schema)
		msi.sidMap[sid] = chunk
	}
	msi.mu.Unlock()

	return chunk, ok
}

func (msi *MsInfo) CreateWriteChunkForColumnStore(sortKeys []string) {
	msi.mu.Lock()
	if msi.writeChunk != nil {
		msi.mu.Unlock()
		return
	}
	msi.writeChunk = &WriteChunkForColumnStore{}
	msi.writeChunk.WriteRec.initForReuse(msi.Schema)
	msi.writeChunk.sortKeys = GetPrimaryKeys(msi.Schema, sortKeys)
	msi.mu.Unlock()
}

func (msi *MsInfo) GetWriteChunk() *WriteChunkForColumnStore {
	return msi.writeChunk
}

func (msi *MsInfo) GetRowChunks() *rowChunks {
	return msi.concurrencyChunks
}

func (msi *MsInfo) SetWriteChunk(writeChunk *WriteChunkForColumnStore) {
	msi.writeChunk = writeChunk
}

func (msi *MsInfo) GetAllSid() []uint64 {
	sids := GetSidsImpl(len(msi.sidMap))
	for k := range msi.sidMap {
		sids = append(sids, k)
	}
	sort.Slice(sids, func(i, j int) bool {
		return sids[i] < sids[j]
	})
	return sids
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
	ApplyConcurrency(table *MemTable, f func(msName string))
	FlushChunks(table *MemTable, dataPath, msName string, lock *string, tbStore immutable.TablesStore)
	WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error
	WriteCols(table *MemTable, rec *record.Record, mstsInfo map[string]*meta.MeasurementInfo, mst string) error
	Reset(table *MemTable)
}

// StoreMstRowCount is used to persist the rowcount value for mst-level pre-aggregation.
func StoreMstRowCount(countFile string, rowCount int) error {
	str := strconv.Itoa(rowCount)

	return os.WriteFile(countFile, []byte(str), 0640)
}

// LoadMstRowCount is used to load the rowcount value for mst-level pre-aggregation.
func LoadMstRowCount(countFile string) (int, error) {
	data, err := os.ReadFile(countFile)
	if err != nil {
		return 0, err
	}

	rowCount, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

func createMsBuilder(tbStore immutable.TablesStore, order bool, lockPath *string, dataPath string, msName string, totalChunks int, size int, conf *immutable.Config, engineType config.EngineType) *immutable.MsBuilder {
	seq := tbStore.Sequencer()
	defer seq.UnRef()

	FileName := immutable.NewTSSPFileName(tbStore.NextSequence(), 0, 0, 0, order, lockPath)
	msb := immutable.NewMsBuilder(dataPath, msName, lockPath, conf, totalChunks, FileName, tbStore.Tier(), seq, size, engineType)
	return msb
}

type MemTableReleaseHook func(t *MemTable)

type MemTable struct {
	mu  sync.RWMutex
	ref int32
	idx *ski.ShardKeyIndex

	msInfoMap map[string]*MsInfo // measurements schemas, {"cpu_0001": *MsInfo}
	msInfos   []MsInfo           // pre-allocation

	memSize int64
	MTable  MTable //public method in MemTable

	releaseHook MemTableReleaseHook
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

func (t *MemTable) initMTable(engineType config.EngineType) {
	switch engineType {
	case config.TSSTORE:
		t.MTable = newTsMemTableImpl()
	case config.COLUMNSTORE:
		t.MTable = &csMemTableImpl{
			primaryKey:          make(map[string]record.Schemas),
			timeClusterDuration: make(map[string]time.Duration),
			indexRelation:       make(map[string]*index.Relation),
		}
	default:
		panic("UnKnown engine type")
	}
}

func (t *MemTable) SetReleaseHook(hook MemTableReleaseHook) {
	t.releaseHook = hook
}

func (t *MemTable) SetIdx(idx *ski.ShardKeyIndex) {
	t.idx = idx
}

func (t *MemTable) Ref() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *MemTable) UnRef() {
	if atomic.AddInt32(&t.ref, -1) == 0 {
		t.release()
	}
}

func (t *MemTable) release() {
	if t.releaseHook != nil {
		t.Reset()
		t.releaseHook(t)
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

func NewMemTable(engineType config.EngineType) *MemTable {
	wb := &MemTable{
		ref:       1,
		msInfoMap: make(map[string]*MsInfo),
	}

	wb.initMTable(engineType)
	return wb
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
	return t.appendFieldsToRecordSlow(rec, fields, time)
}

func (t *MemTable) appendFieldsToRecordSlow(rec *record.Record, fields []influx.Field, time int64) (int64, error) {
	var size int64
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

func (t *MemTable) CreateMsInfo(name string, row *influx.Row, rec *record.Record) *MsInfo {
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
	chunk.SortRecordNoLock(hlp)

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

func UpdateMstRowCount(msRowCount *sync.Map, mstName string, rowCount int64) {
	if count, ok := msRowCount.Load(mstName); ok {
		atomic.AddInt64(count.(*int64), rowCount)
	} else {
		msRowCount.Store(mstName, &rowCount)
	}
}
