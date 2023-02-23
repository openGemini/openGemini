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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/ski"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

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

func (writeRec *WriteRec) sortAndDedupe(sortAux *record.SortAux) {
	if writeRec.rec.RowNums() > 1 {
		sortAux.InitRecord(writeRec.rec.Schemas())
		hlp := &record.SortHelper{}
		hlp.Sort(writeRec.rec, sortAux)
		writeRec.rec, sortAux.SortRec = sortAux.SortRec, writeRec.rec
	}
}

func (writeRec *WriteRec) GetRecord() *record.Record {
	return writeRec.rec
}

func (chunk *WriteChunk) Init(sid uint64, schema []record.Field) {
	chunk.Sid = sid
	chunk.LastFlushTime = math.MinInt64
	chunk.OrderWriteRec.init(schema)
	chunk.UnOrderWriteRec.init(schema)
}

type MsInfo struct {
	mu        sync.RWMutex
	Name      string // measurement name with version
	Schema    record.Schemas
	sidMap    map[uint64]*WriteChunk
	chunkBufs []WriteChunk
	TimeAsd   bool
}

func (msi *MsInfo) Init(row *influx.Row) {
	msi.Name = row.Name
	genMsSchema(&msi.Schema, row.Fields)
	msi.TimeAsd = true
	msi.sidMap = make(map[uint64]*WriteChunk, 1024)
}

func (msi *MsInfo) allocChunk() *WriteChunk {
	if cap(msi.chunkBufs) == len(msi.chunkBufs) {
		msi.chunkBufs = make([]WriteChunk, 0, 1024)
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

type MemTable struct {
	mu   sync.RWMutex
	ref  int32
	conf *Config
	path string
	idx  *ski.ShardKeyIndex

	msInfoMap     map[string]*MsInfo // measurements schemas, {"cpu_0001": *MsInfo}
	concurLimiter limiter.Fixed

	log     *zap.Logger
	memSize int64
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

func (m *MemTables) Values(msName string, id uint64, tr record.TimeRange, schema record.Schemas, ascending bool) *record.Record {
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

func GetMemTable(path string) *MemTable {
	select {
	case v := <-memTablePoolCh:
		atomic.AddInt32(&v.ref, 1)
		v.path = path
		return v
	default:
		if v := memTablePool.Get(); v != nil {
			memTbl, ok := v.(*MemTable)
			if !ok {
				panic("GetMemTable memTablePool get value isn't *MemTable type")
			}
			atomic.AddInt32(&memTbl.ref, 1)
			memTbl.path = path
			return memTbl
		}
		return NewMemTable(NewConfig(), path)
	}
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
	Statistics.MutableStat.AddMutableSize(t.path, size)
}

func (t *MemTable) GetMemSize() int64 {
	return atomic.LoadInt64(&t.memSize)
}

func (t *MemTable) ApplyConcurrency(f func(msName string, sids []uint64)) {
	var wg sync.WaitGroup
	wg.Add(len(t.msInfoMap))
	for k := range t.msInfoMap {
		t.concurLimiter <- struct{}{}
		go func(msName string) {
			sids := make([]uint64, 0, 128)
			sids = t.GetSids(msName, sids)
			sort.Sort(uint64Sids(sids))
			f(msName, sids)
			t.concurLimiter.Release()
			wg.Done()
		}(k)
	}
	wg.Wait()
}

func (t *MemTable) GetSids(msName string, sids []uint64) []uint64 {
	info, ok := t.msInfoMap[msName]
	if !ok || info == nil {
		return nil
	}

	for k := range info.sidMap {
		sids = append(sids, k)
	}
	return sids
}

type SortAuxPool struct {
	cache chan *record.SortAux
	pool  *sync.Pool
}

var sortAuxPool = newSortAuxPool()

func newSortAuxPool() *SortAuxPool {
	n := cpu.GetCpuNum()
	if n < 4 {
		n = 4
	}
	if n > 32 {
		n = 32
	}
	return &SortAuxPool{
		cache: make(chan *record.SortAux, n),
		pool:  &sync.Pool{},
	}
}

func (p *SortAuxPool) put(m *record.SortAux) {
	m.RowIds = m.RowIds[:0]
	m.Times = m.Times[:0]
	select {
	case p.cache <- m:
	default:
		p.pool.Put(m)
	}
}

func (p *SortAuxPool) get() *record.SortAux {
	select {
	case r := <-p.cache:
		return r
	default:
		if v := p.pool.Get(); v != nil {
			return v.(*record.SortAux)
		}
		return &record.SortAux{}
	}
}

func (t *MemTable) SortAndDedup(msName string, sids []uint64) {
	if t.msInfoMap[msName].TimeAsd {
		return
	}

	sortAux := sortAuxPool.get()
	defer sortAuxPool.put(sortAux)

	sidMap := t.msInfoMap[msName].sidMap
	for _, v := range sids {
		writeChunk := sidMap[v]
		writeChunk.Mu.Lock()
		if !writeChunk.OrderWriteRec.timeAsd {
			writeChunk.OrderWriteRec.sortAndDedupe(sortAux)
			writeChunk.OrderWriteRec.timeAsd = true
		}
		if !writeChunk.UnOrderWriteRec.timeAsd {
			writeChunk.UnOrderWriteRec.sortAndDedupe(sortAux)
			writeChunk.UnOrderWriteRec.timeAsd = true
		}
		writeChunk.Mu.Unlock()
	}
}

func (t *MemTable) FlushChunks(dataPath, msName string, lock *string, tbStore immutable.TablesStore, sids []uint64,
	f func(dataPath, msName string, lock *string, totalChunks int, tbStore immutable.TablesStore, chunk *WriteChunk,
		orderMs, unOrderMs *immutable.MsBuilder, finish bool) (*immutable.MsBuilder, *immutable.MsBuilder)) {

	sidMap := t.msInfoMap[msName].sidMap
	var orderMs, unOrderMs *immutable.MsBuilder
	sidLen := len(sids)
	for i := range sids {
		if i < sidLen-1 {
			orderMs, unOrderMs = f(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], orderMs, unOrderMs, false)
		} else {
			orderMs, unOrderMs = f(dataPath, msName, lock, sidLen, tbStore, sidMap[sids[i]], orderMs, unOrderMs, true)
		}
	}
}

func NewMemTable(conf *Config, path string) *MemTable {
	wb := &MemTable{
		conf:          conf,
		path:          path,
		log:           logger.GetLogger(),
		ref:           1,
		msInfoMap:     make(map[string]*MsInfo),
		concurLimiter: limiter.NewFixed(cpu.GetCpuNum()),
	}

	return wb
}

func (t *MemTable) NeedFlush() bool {
	return atomic.LoadInt64(&t.memSize) > int64(t.conf.sizeLimit)
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

func (t *MemTable) appendFieldToCol(col *record.ColVal, field *influx.Field, size *int64) error {
	if field.Type == influx.Field_Type_Int || field.Type == influx.Field_Type_UInt {
		col.AppendInteger(int64(field.NumValue))
		*size += int64(record.Int64SizeBytes)
	} else if field.Type == influx.Field_Type_Float {
		col.AppendFloat(field.NumValue)
		*size += int64(record.Float64SizeBytes)
	} else if field.Type == influx.Field_Type_Boolean {
		if field.NumValue == 0 {
			col.AppendBoolean(false)
		} else {
			col.AppendBoolean(true)
		}
		*size += int64(record.BooleanSizeBytes)
	} else if field.Type == influx.Field_Type_String {
		col.AppendString(field.StrValue)
		*size += int64(len(field.StrValue))
	} else {
		return errors.New("unsupport data type")
	}
	return nil
}

func (t *MemTable) appendFieldsToRecord(rec *record.Record, fields []influx.Field, time int64, sameSchema bool) (error, int64) {
	// fast path
	var size int64
	if sameSchema {
		for i := range fields {
			if err := t.appendFieldToCol(&rec.ColVals[i], &fields[i], &size); err != nil {
				return err, size
			}
		}
		rec.ColVals[len(fields)].AppendInteger(time)
		size += int64(record.Int64SizeBytes)
		return nil, size
	}

	// slow path
	recSchemaIdx, pointSchemaIdx := 0, 0
	recSchemaLen, pointSchemaLen := rec.ColNums()-1, len(fields)
	appendColIdx := rec.ColNums()
	oldRowNum, oldColNum := rec.RowNums(), rec.ColNums()
	for recSchemaIdx < recSchemaLen && pointSchemaIdx < pointSchemaLen {
		if rec.Schema[recSchemaIdx].Name == fields[pointSchemaIdx].Key {
			if err := t.appendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
				return err, size
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
				return err, size
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
			return err, size
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
	size += int64(record.Int64SizeBytes)

	return nil, size
}

func (t *MemTable) appendFields(msInfo *MsInfo, chunk *WriteChunk, time int64, fields []influx.Field) (error, int64) {
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

	return t.appendFieldsToRecord(writeRec.rec, fields, time, sameSchema)
}

func (t *MemTable) createMsInfo(name string, row *influx.Row) *MsInfo {
	t.mu.RLock()
	msInfo, ok := t.msInfoMap[name]
	t.mu.RUnlock()

	if ok {
		return msInfo
	}

	t.mu.Lock()
	msInfo, ok = t.msInfoMap[name]
	if !ok {
		msInfo = &MsInfo{}
		msInfo.Init(row)
		t.msInfoMap[name] = msInfo
	}
	t.mu.Unlock()

	return msInfo
}

func (t *MemTable) WriteRows(rowsD *dictpool.Dict, getLastFlushTime func(msName string, sid uint64) (int64, error),
	addRowCountsBySid func(msName string, sid uint64, rowCounts int64)) error {
	var err error
	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		rs := *rows
		msName := stringinterner.InternSafe(mapp.Key)

		start := time.Now()
		msInfo := t.createMsInfo(msName, &rs[0])
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

		start = time.Now()
		var (
			exist        = false
			sid   uint64 = 0
			chunk *WriteChunk
		)
		for index := range rs {
			sid = rs[index].PrimaryId
			chunk, exist = msInfo.CreateChunk(sid)

			if chunk.LastFlushTime == math.MinInt64 {
				chunk.LastFlushTime, err = getLastFlushTime(msName, sid)
				if err != nil {
					return err
				}
			}

			if !exist && t.idx != nil && (chunk.LastFlushTime == math.MinInt64 || chunk.LastFlushTime == math.MaxInt64) {
				startTime := time.Now()
				err = t.idx.CreateIndex(record.Str2bytes(msName), rs[index].ShardKey, sid)
				if err != nil {
					return err
				}
				atomic.AddInt64(&Statistics.PerfStat.WriteShardKeyIdxNs, time.Since(startTime).Nanoseconds())
			}

			err, _ = t.appendFields(msInfo, chunk, rs[index].Timestamp, rs[index].Fields)
			if err != nil {
				return err
			}

			addRowCountsBySid(msName, sid, 1)
		}

		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}

	return nil
}

func (t *MemTable) Reset() {
	t.memSize = 0
	t.msInfoMap = make(map[string]*MsInfo)
	t.idx = nil
}

func (t *MemTable) getSortedRecSafe(msName string, id uint64, tr record.TimeRange, schema record.Schemas, ascending bool) *record.Record {
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

	sortAux := sortAuxPool.get()
	defer sortAuxPool.put(sortAux)

	var rec *record.Record
	chunk.Mu.Lock()
	if !chunk.OrderWriteRec.timeAsd {
		chunk.OrderWriteRec.sortAndDedupe(sortAux)
		chunk.OrderWriteRec.timeAsd = true
	}
	if !chunk.UnOrderWriteRec.timeAsd {
		chunk.UnOrderWriteRec.sortAndDedupe(sortAux)
		chunk.UnOrderWriteRec.timeAsd = true
	}
	if chunk.OrderWriteRec.rec.RowNums() == 0 {
		rec = chunk.UnOrderWriteRec.rec
	} else if chunk.UnOrderWriteRec.rec.RowNums() == 0 {
		rec = chunk.OrderWriteRec.rec
	} else {
		rec = &record.Record{}
		rec.MergeRecord(chunk.OrderWriteRec.rec, chunk.UnOrderWriteRec.rec)
	}
	chunk.Mu.Unlock()

	return rec.CopyWithCondition(ascending, tr, schema)
}

func (t *MemTable) values(msName string, id uint64, tr record.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	// column of sid need sort and dedupe
	rec := t.getSortedRecSafe(msName, id, tr, schema, ascending)
	if rec != nil {
		sort.Sort(rec)
	}
	return rec
}

func (t *MemTable) GetConf() *Config {
	return t.conf
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
	return chunk.OrderWriteRec.lastAppendTime
}
