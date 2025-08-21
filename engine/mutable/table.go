// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/savsgio/dictpool"
)

type SeriesRowCountFunc func(msName string, sid uint64, rowCounts int64)

type WriteRowsCtx struct {
	AddRowCountsBySid SeriesRowCountFunc
	MsRowCount        *util.SyncMap[string, *int64]
}

func (ctx *WriteRowsCtx) SetSeriesRowCountFunc(fn SeriesRowCountFunc) {
	ctx.AddRowCountsBySid = fn
}

func (ctx *WriteRowsCtx) SetMsRowCount(v *util.SyncMap[string, *int64]) {
	ctx.MsRowCount = v
}

func GetMsInfo(name string, mstsInfo *sync.Map) (*meta.MeasurementInfo, bool) {
	msInfo, ok := mstsInfo.Load(name)
	if !ok {
		return nil, false
	}
	return msInfo.(*meta.MeasurementInfo), true
}

type WriteRec struct {
	rec             *record.Record
	firstAppendTime int64
	lastAppendTime  int64
	timeAsd         bool
	schemaCopyed    bool
}

type WriteChunk struct {
	Mu       sync.Mutex
	Sid      uint64
	WriteRec WriteRec
}

func (chunk *WriteChunk) Init(sid uint64) {
	chunk.Sid = sid
}

func (chunk *WriteChunk) SortRecordNoLock(hlp *record.ColumnSortHelper) {
	chunk.WriteRec.SortRecord(hlp)
}

func (chunk *WriteChunk) SortRecord(hlp *record.ColumnSortHelper) {
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

func NewWriteChunkForColumnStore() *WriteChunkForColumnStore {
	return &WriteChunkForColumnStore{}
}

func (chunk *WriteChunkForColumnStore) SortRecord(tcDuration time.Duration) {
	hlp := record.NewSortHelper()
	chunk.Mu.Lock()
	chunk.WriteRec.rec = hlp.SortForColumnStore(chunk.WriteRec.rec, chunk.sortKeys, false, tcDuration)
	chunk.Mu.Unlock()
	hlp.Release()
}

func (chunk *WriteChunkForColumnStore) TimeSorted() bool {
	if len(chunk.sortKeys) == 0 {
		return false
	}
	return chunk.sortKeys[0].Key == record.TimeField
}

var writeRecPool pool.FixedPool

func initWriteRecPool(size int) {
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
	writeRec.firstAppendTime = math.MaxInt64
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
	writeRec.firstAppendTime = math.MaxInt64
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

func (writeRec *WriteRec) SortRecord(hlp *record.ColumnSortHelper) {
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
	flushed           bool
}

func (msi *MsInfo) Init(row *influx.Row) {
	msi.Name = row.Name
	genMsSchema(&msi.Schema, row.Fields)
	msi.sidMap = make(map[uint64]*WriteChunk)
}

func (msi *MsInfo) GetFlushed() *bool {
	return &msi.flushed
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
		chunk.Init(sid)
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
	msi.writeChunk = NewWriteChunkForColumnStore()
	msi.writeChunk.WriteRec.initForReuse(msi.Schema)
	msi.writeChunk.sortKeys = GetSortKeys(msi.Schema, sortKeys)
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

func GetSortKeys(schema []record.Field, primaryKeys []string) []record.PrimaryKey {
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
	FlushChunks(table *MemTable, dataPath, msName, db, rp string, lock *string, tbStore immutable.TablesStore, msRowCount int64, fileInfos chan []immutable.FileInfoExtend)
	WriteRows(table *MemTable, rowsD *dictpool.Dict, wc WriteRowsCtx) error
	WriteCols(table *MemTable, rec *record.Record, mst string) error
	SetFlushManagerInfo(manager map[string]FlushManager, accumulateMetaIndex *sync.Map)
	Reset(table *MemTable)
}

// StoreMstRowCount is used to persist the rowcount value for mst-level pre-aggregation.
func StoreMstRowCount(countFile string, rowCount int) error {
	str := strconv.Itoa(rowCount)

	return os.WriteFile(countFile, []byte(str), 0600)
}

// LoadMstRowCount is used to load the rowcount value for mst-level pre-aggregation.
func LoadMstRowCount(countFile string) (int, error) {
	data, err := os.ReadFile(filepath.Clean(countFile))
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
	msb := immutable.NewMsBuilder(dataPath, msName, lockPath, conf, totalChunks, FileName, util.Hot, seq, size, engineType, tbStore.GetObsOption(), tbStore.GetShardID())
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

func NewMemTables(readEnable bool) *MemTables {
	return &MemTables{
		readEnable: readEnable,
	}
}

func (m *MemTables) Init(activeTbl, snapshotTbl *MemTable) {
	m.activeTbl = activeTbl
	m.snapshotTbl = snapshotTbl
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
		t.MTable = NewTsMemTableImpl()
	case config.COLUMNSTORE:
		t.MTable = &CSMemTableImpl{
			flushManager:        make(map[string]FlushManager),
			accumulateMetaIndex: &sync.Map{},
		}
	default:
		panic("UnKnown engine type")
	}
}

func (t *MemTable) ApplyConcurrency(f func(msName string)) {
	var wg sync.WaitGroup
	wg.Add(len(t.msInfoMap))
	for k := range t.msInfoMap {
		concurLimiter <- struct{}{}
		go func(msName string) {
			f(msName)
			concurLimiter.Release()
			wg.Done()
		}(k)
	}
	wg.Wait()
}

func (t *MemTable) SetReleaseHook(hook MemTableReleaseHook) {
	t.releaseHook = hook
}

func (t *MemTable) SetIdx(idx *ski.ShardKeyIndex) {
	t.idx = idx
}

func (t *MemTable) SetDbRp(db, rp string) {
	tb, ok := t.MTable.(*CSMemTableImpl)
	if ok {
		tb.SetDbRp(db, rp)
	}
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
	return atomic.LoadInt64(&t.memSize) > config.GetShardMemTableSizeLimit()
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
	if !ok || chunk == nil || chunk.WriteRec.lastAppendTime < tr.Min || chunk.WriteRec.firstAppendTime > tr.Max {
		return nil
	}

	hlp := record.NewColumnSortHelper()
	defer hlp.Release()

	chunk.Mu.Lock()
	writeRec := chunk.WriteRec.rec
	if writeRec == nil || writeRec.RowNums() == 0 {
		chunk.Mu.Unlock()
		return nil
	}

	chunk.SortRecordNoLock(hlp)
	var rec = chunk.WriteRec.rec.Copy(ascending, &tr, schema)
	chunk.Mu.Unlock()
	return rec
}

func (t *MemTable) values(msName string, id uint64, tr util.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	// column of sid need sort and dedupe
	rec := t.getSortedRecSafe(msName, id, tr, schema, ascending)
	if rec != nil {
		sort.Sort(rec)
	}
	return rec
}

func UpdateMstRowCount(msRowCount *util.SyncMap[string, *int64], mstName string, rowCount int64) {
	if count, ok := msRowCount.Load(mstName); ok {
		atomic.AddInt64(count, rowCount)
	} else {
		msRowCount.Store(mstName, &rowCount)
	}
}

var concurLimiter = limiter.NewFixed(8)

func initConcurLimiter(limit int) {
	concurLimiter = limiter.NewFixed(limit)
}

func Init(cpuNum int) {
	initConcurLimiter(cpuNum)
	initWriteRecPool(cpuNum)
	InitMutablePool(cpuNum)
	logstore.InitSkipIndexPool(cpuNum)
	NewMemTablePoolManager().Init()
}
