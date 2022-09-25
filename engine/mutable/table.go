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
		writeRec.rec.SortAndDedupe(sortAux)
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
	Schema    record.Schemas
	sidMap    map[uint64]*WriteChunk
	chunkBufs []WriteChunk
	TimeAsd   bool
}

type MemTable struct {
	mu   sync.RWMutex
	ref  int32
	conf *Config
	path string
	idx  *ski.ShardKeyIndex

	msInfoMap     map[string]*MsInfo // measurements schemas
	msInfos       []MsInfo
	concurLimiter limiter.Fixed

	log     *zap.Logger
	memSize int64
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

type uint64Sids []uint64

func (s uint64Sids) Len() int { return len(s) }
func (s uint64Sids) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s uint64Sids) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//nolint
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
	sidMap := t.msInfoMap[msName].sidMap
	for k := range sidMap {
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

func (t *MemTable) FlushChunks(dataPath, msName string, tbStore immutable.TablesStore, sids []uint64,
	f func(dataPath, msName string, totalChunks int, tbStore immutable.TablesStore, chunk *WriteChunk,
		orderMs, unOrderMs *immutable.MsBuilder, finish bool) (*immutable.MsBuilder, *immutable.MsBuilder)) {

	sidMap := t.msInfoMap[msName].sidMap
	var orderMs, unOrderMs *immutable.MsBuilder
	sidLen := len(sids)
	for i := range sids {
		if i < sidLen-1 {
			orderMs, unOrderMs = f(dataPath, msName, sidLen, tbStore, sidMap[sids[i]], orderMs, unOrderMs, false)
		} else {
			orderMs, unOrderMs = f(dataPath, msName, sidLen, tbStore, sidMap[sids[i]], orderMs, unOrderMs, true)
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
	}
	writeRec.lastAppendTime = time

	return t.appendFieldsToRecord(writeRec.rec, fields, time, sameSchema)
}

func (t *MemTable) WriteRows(rowsD *dictpool.Dict, getLastFlushTime func(msName string, sid uint64) int64,
	addRowCountsBySid func(msName string, sid uint64, rowCounts int64)) error {
	for _, mapp := range rowsD.D {
		rows, ok := mapp.Value.(*[]influx.Row)
		if !ok {
			return errors.New("can't map mmPoints")
		}
		rs := *rows
		msName := stringinterner.InternSafe(mapp.Key)
		start := time.Now()
		t.mu.RLock()
		msInfo, ok := t.msInfoMap[msName]
		t.mu.RUnlock()
		if !ok {
			t.mu.Lock()
			msInfo, ok = t.msInfoMap[msName]
			if !ok {
				if cap(t.msInfos) == len(t.msInfos) {
					capNew := (len(t.msInfos) + 1) * 2
					t.msInfos = make([]MsInfo, 0, capNew)
				}
				t.msInfos = t.msInfos[:len(t.msInfos)+1]
				msInfo = &t.msInfos[len(t.msInfos)-1]
				msInfo.TimeAsd = true
				msInfo.sidMap = make(map[uint64]*WriteChunk)
				msInfo.chunkBufs = msInfo.chunkBufs[:0]
				genMsSchema(&msInfo.Schema, rs[0].Fields)
				t.msInfoMap[msName] = msInfo
			}
			t.mu.Unlock()
		}
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())
		start = time.Now()
		msInfo.mu.Lock()
		for index := range rs {
			sid := rs[index].SeriesId
			chunkBuff, exist := msInfo.sidMap[sid]
			if !exist {
				if cap(msInfo.chunkBufs) == len(msInfo.chunkBufs) {
					capNew := (len(msInfo.chunkBufs) + 1) * 2
					msInfo.chunkBufs = make([]WriteChunk, 0, capNew)
				}
				msInfo.chunkBufs = msInfo.chunkBufs[:len(msInfo.chunkBufs)+1]
				msInfo.sidMap[sid] = &msInfo.chunkBufs[len(msInfo.chunkBufs)-1]
				chunkBuff = &msInfo.chunkBufs[len(msInfo.chunkBufs)-1]

				// init chunk buffer
				chunkBuff.Init(sid, msInfo.Schema)
				chunkBuff.LastFlushTime = getLastFlushTime(msName, sid)

				if t.idx != nil && chunkBuff.LastFlushTime == math.MinInt64 {
					startTime := time.Now()
					err := t.idx.CreateIndex(record.Str2bytes(msName), rs[index].ShardKey, sid)
					if err != nil {
						return err
					}
					atomic.AddInt64(&Statistics.PerfStat.WriteShardKeyIdxNs, time.Since(startTime).Nanoseconds())
				}
			}
			err, _ := t.appendFields(msInfo, chunkBuff, rs[index].Timestamp, rs[index].Fields)
			if err != nil {
				msInfo.mu.Unlock()
				return err
			}
			startTime := time.Now()
			addRowCountsBySid(msName, sid, 1)
			atomic.AddInt64(&Statistics.PerfStat.WriteAddSidRowCountNs, time.Since(startTime).Nanoseconds())
		}
		msInfo.mu.Unlock()
		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}

	return nil
}

func (t *MemTable) Reset() {
	t.msInfos = make([]MsInfo, 0, 1)
	t.memSize = 0
	t.msInfoMap = make(map[string]*MsInfo)
	t.idx = nil
}

func (t *MemTable) Close() error {
	return nil
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

	if ascending {
		return rec.CopyWithCondition(tr, schema)
	}
	return rec.CopyWithConditionDescend(tr, schema)
}

func (t *MemTable) Values(msName string, id uint64, tr record.TimeRange, schema record.Schemas, ascending bool) *record.Record {
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
