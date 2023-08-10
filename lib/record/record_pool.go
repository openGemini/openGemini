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

// nolint
package record

import (
	"sync"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

type RecordPool struct {
	name  RecordType
	cache chan *Record
	pool  sync.Pool
	inUse func(i int64)
	get   func(i int64)
	reUse func(i int64)
	abort func(i int64)
}

type RecordType uint8

const (
	IntervalRecordPool RecordType = iota
	FileCursorPool
	AggPool
	TsmMergePool
	TsspSequencePool
	SequenceAggPool
	SeriesPool
	ColumnReaderPool
	UnknownPool
)

func NewRecordPool(recordType RecordType) *RecordPool {
	n := cpu.GetCpuNum() * 2
	if n < 4 {
		n = 4
	}
	if n > 256 {
		n = 256
	}

	inUse := func(i int64) {}
	get := func(i int64) {}
	reuse := func(i int64) {}
	abort := func(i int64) {}
	switch recordType {
	case IntervalRecordPool:
		inUse = statistics.NewRecordStatistics().AddIntervalRecordPoolInUse
		get = statistics.NewRecordStatistics().AddIntervalRecordPoolGet
		reuse = statistics.NewRecordStatistics().AddIntervalRecordPoolGetReUse
		abort = statistics.NewRecordStatistics().AddIntervalRecordPoolAbort
	case FileCursorPool:
		inUse = statistics.NewRecordStatistics().AddFileCursorPoolInUse
		get = statistics.NewRecordStatistics().AddFileCursorPoolGet
		reuse = statistics.NewRecordStatistics().AddFileCursorPoolGetReUse
		abort = statistics.NewRecordStatistics().AddFileCursorPoolAbort
	case AggPool:
		inUse = statistics.NewRecordStatistics().AddAggPoolInUse
		get = statistics.NewRecordStatistics().AddAggPoolGet
		reuse = statistics.NewRecordStatistics().AddAggPoolGetReUse
		abort = statistics.NewRecordStatistics().AddAggPoolAbort
	case TsmMergePool:
		inUse = statistics.NewRecordStatistics().AddTsmMergePoolInUse
		get = statistics.NewRecordStatistics().AddTsmMergePoolGet
		reuse = statistics.NewRecordStatistics().AddTsmMergePoolGetReUse
		abort = statistics.NewRecordStatistics().AddTsmMergePoolAbort
	case TsspSequencePool:
		inUse = statistics.NewRecordStatistics().AddTsspSequencePoolInUse
		get = statistics.NewRecordStatistics().AddTsspSequencePoolGet
		reuse = statistics.NewRecordStatistics().AddTsspSequencePoolGetReUse
		abort = statistics.NewRecordStatistics().AddTsspSequencePoolAbort
	case SequenceAggPool:
		inUse = statistics.NewRecordStatistics().AddSequenceAggPoolInUse
		get = statistics.NewRecordStatistics().AddSequenceAggPoolGet
		reuse = statistics.NewRecordStatistics().AddSequenceAggPoolGetReUse
		abort = statistics.NewRecordStatistics().AddSequenceAggPoolAbort
	case SeriesPool:
		inUse = statistics.NewRecordStatistics().AddSeriesPoolInUse
		get = statistics.NewRecordStatistics().AddSeriesPoolGet
		reuse = statistics.NewRecordStatistics().AddSeriesPoolGetReUse
		abort = statistics.NewRecordStatistics().AddSeriesPoolAbort
	}

	return &RecordPool{
		cache: make(chan *Record, n),
		name:  recordType,
		inUse: inUse,
		get:   get,
		reUse: reuse,
		abort: abort,
	}
}

func (p *RecordPool) Get() *Record {
	p.inUse(1)
	p.get(1)
	select {
	case rec := <-p.cache:
		p.reUse(1)
		return rec
	default:
		v := p.pool.Get()
		if v != nil {
			rec, ok := v.(*Record)
			if !ok {
				return &Record{}
			}
			p.reUse(1)
			return rec
		}
		rec := &Record{}
		return rec
	}
}

func (p *RecordPool) Put(rec *Record) {
	p.inUse(-1)
	if recLen := rec.Len(); (recLen > 0 && rec.RowNums() > RecMaxRowNumForRuse) || cap(rec.Schema) > RecMaxLenForRuse {
		p.abort(1)
		return
	}
	rec.ResetDeep()
	select {
	case p.cache <- rec:
	default:
		p.pool.Put(rec)
	}
}

type CircularRecordPool struct {
	index       int
	pool        *RecordPool
	records     []*Record
	schema      Schemas
	initColMeta bool
}

func NewCircularRecordPool(recordPool *RecordPool, recordNum int, schema Schemas, initColMeta bool) *CircularRecordPool {
	statistics.NewRecordStatistics().AddCircularRecordPool(1)
	rp := &CircularRecordPool{
		index:       0,
		pool:        recordPool,
		initColMeta: initColMeta,
		records:     make([]*Record, 0, recordNum),
		schema:      schema,
	}
	return rp
}

func (p *CircularRecordPool) GetIndex() int {
	return p.index
}

func (p *CircularRecordPool) allocRecord() {
	record := p.pool.Get()
	schemaLen := len(p.schema)
	recSchemaLen := len(record.Schema)
	if schemaCap := cap(record.Schema); schemaCap < schemaLen {
		record.Schema = append(record.Schema[:recSchemaLen], make([]Field, schemaLen-recSchemaLen)...)
	}
	record.Schema = record.Schema[:schemaLen]

	if colValCap := cap(record.ColVals); colValCap < schemaLen {
		record.ColVals = append(record.ColVals[:recSchemaLen], make([]ColVal, schemaLen-recSchemaLen)...)
	}
	record.ColVals = record.ColVals[:schemaLen]

	copy(record.Schema, p.schema)
	if p.initColMeta {
		if record.RecMeta == nil {
			record.RecMeta = &RecMeta{
				Times: make([][]int64, schemaLen),
			}
		}
		if timeCap, timeLen := cap(record.RecMeta.Times), len(record.RecMeta.Times); timeCap < schemaLen {
			record.RecMeta.Times = append(record.RecMeta.Times[:timeLen], make([][]int64, schemaLen-timeLen)...)
		}
		record.RecMeta.Times = record.RecMeta.Times[:schemaLen]
	}
	p.records = append(p.records, record)
}

func (p *CircularRecordPool) Get() *Record {
	if len(p.records) < cap(p.records) {
		p.allocRecord()
	}
	r := p.records[p.index]
	r.ResetForReuse()
	if !p.initColMeta {
		r.RecMeta = nil
	}
	p.index = (p.index + 1) % cap(p.records)
	return r
}

func (p *CircularRecordPool) GetBySchema(s Schemas) *Record {
	if len(p.records) < cap(p.records) {
		p.allocRecord()
	}
	r := p.records[p.index]
	r.Schema = s
	r.ResetForReuse()
	if !p.initColMeta {
		r.RecMeta = nil
	}
	p.index = (p.index + 1) % cap(p.records)
	return r
}

func (p *CircularRecordPool) Put() {
	statistics.NewRecordStatistics().AddCircularRecordPool(-1)
	for i := 0; i < len(p.records); i++ {
		p.pool.Put(p.records[i])
	}
	p.pool = nil
	p.records = nil
}

func (p *CircularRecordPool) PutRecordInCircularPool() {
	recordNum := cap(p.records)
	p.index = (p.index - 1 + recordNum) % recordNum
}
