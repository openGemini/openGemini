// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"errors"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var recordGroupsPool = pool.NewDefaultUnionPool(func() *MemGroups {
	return &MemGroups{}
})

type MemGroups struct {
	wg               sync.WaitGroup
	groups           []MemGroup
	size             int
	seriesHashFactor int
}

func NewMemGroups(size, seriesHashFactor int) (*MemGroups, func()) {
	rgs := recordGroupsPool.Get()
	rgs.Init(size, seriesHashFactor)
	return rgs, func() {
		rgs.Reset()
		recordGroupsPool.Put(rgs)
	}
}

func (rs *MemGroups) Init(size, seriesHashFactor int) {
	rs.size = size
	rs.seriesHashFactor = seriesHashFactor
	if cap(rs.groups) < size {
		rs.groups = make([]MemGroup, size)
	}
	rs.groups = rs.groups[:size]
}

func (rs *MemGroups) ResetTime() {
	for i := range rs.groups {
		rs.groups[i].ResetTime()
	}
}

func (rs *MemGroups) Add(row *influx.Row) error {
	idx := xxhash.Sum64String(row.Name) % uint64(rs.size)
	factor := uint64(rs.seriesHashFactor)
	if factor > 1 {
		idx = (idx + xxhash.Sum64(row.IndexKey)%factor) % uint64(rs.size)
	}
	return rs.groups[idx].Add(idx, row)
}

func (rs *MemGroups) BeforeWrite() {
	for i := range rs.groups {
		if !rs.groups[i].IsEmpty() {
			rs.wg.Add(1)
			rs.groups[i].done = rs.wg.Done
		}
	}
}

func (rs *MemGroups) RowGroup(i int) *MemGroup {
	return &rs.groups[i]
}

func (rs *MemGroups) GroupNum() int {
	return len(rs.groups)
}

func (rs *MemGroups) Wait() {
	rs.wg.Wait()
}

func (rs *MemGroups) Reset() {
	for i := range rs.groups {
		rs.groups[i].Reset()
	}
}

func (rs *MemGroups) MemSize() int {
	return 0
}

func (rs *MemGroups) Error() error {
	for _, rg := range rs.groups {
		if err := rg.Error(); err != nil {
			return err
		}
	}
	return nil
}

type MemGroup struct {
	tm      time.Time
	err     error
	done    func()
	shardID uint64
	hash    uint64
	recs    []*MemRecord
	recMap  map[string]*MemRecord
}

func (g *MemGroup) ResetTime() {
	g.tm = time.Now()
}

func (g *MemGroup) MicroSince() int64 {
	return time.Since(g.tm).Microseconds()
}

func (g *MemGroup) Records() []*MemRecord {
	return g.recs
}

func (g *MemGroup) SetShardID(id uint64) {
	g.shardID = id
}

func (g *MemGroup) ShardID() uint64 {
	return g.shardID
}

func (g *MemGroup) Hash() uint64 {
	return g.hash
}

func (g *MemGroup) Add(hash uint64, row *influx.Row) error {
	if g.recMap == nil {
		g.recMap = make(map[string]*MemRecord)
	}
	g.hash = hash

	key := util.Bytes2str(row.IndexKey)
	rec, ok := g.recMap[key]
	if !ok {
		rec = g.allocRecord()
		rec.Name = row.Name
		rec.IndexKey = row.IndexKey
		rec.Rec.ResetDeep()
		g.recMap[key] = rec
	}

	return rec.appendRow(row)
}

func (g *MemGroup) allocRecord() *MemRecord {
	idx := len(g.recs)
	if cap(g.recs) == idx {
		g.recs = append(g.recs, &MemRecord{})
	} else {
		g.recs = g.recs[:idx+1]
		if g.recs[idx] == nil {
			g.recs[idx] = &MemRecord{}
		}
	}
	return g.recs[idx]
}

func (g *MemGroup) IsEmpty() bool {
	return len(g.recMap) == 0
}

func (g *MemGroup) Reset() {
	g.err = nil
	g.done = nil
	g.hash = 0
	g.shardID = 0
	g.recs = g.recs[:0]
	clear(g.recMap)
}

func (g *MemGroup) Done(err error) {
	g.err = err
	if g.done != nil {
		g.done()
	}
}

func (g *MemGroup) Error() error {
	return g.err
}

type MemRecord struct {
	Name     string
	IndexKey []byte
	Rec      Record
}

func (r *MemRecord) appendRow(row *influx.Row) error {
	sameSchema := false
	rec := &r.Rec
	if rec.RowNums() == 0 {
		sameSchema = true
		r.genMsSchema(row.Fields)
		rec.ReserveColVal(rec.Len())
	}

	_, err := AppendFieldsToRecord(rec, row.Fields, row.Timestamp, sameSchema)
	return err
}

func (r *MemRecord) genMsSchema(fields []influx.Field) {
	rec := &r.Rec
	schemaLen := len(fields) + 1
	rec.ReserveSchema(schemaLen)

	for i := range fields {
		rec.Schema[i].Type = int(fields[i].Type)
		rec.Schema[i].Name = fields[i].Key
	}

	rec.Schema[schemaLen-1].Type = influx.Field_Type_Int
	rec.Schema[schemaLen-1].Name = TimeField
}

func AppendFieldToCol(col *ColVal, field *influx.Field, size *int64) error {
	switch field.Type {
	case influx.Field_Type_Int, influx.Field_Type_UInt:
		col.AppendInteger(int64(field.NumValue))
		*size += int64(util.Int64SizeBytes)
	case influx.Field_Type_Float:
		col.AppendFloat(field.NumValue)
		*size += int64(util.Float64SizeBytes)
	case influx.Field_Type_Boolean:
		if field.NumValue == 0 {
			col.AppendBoolean(false)
		} else {
			col.AppendBoolean(true)
		}
		*size += int64(util.BooleanSizeBytes)
	case influx.Field_Type_String:
		col.AppendString(field.StrValue)
		*size += int64(len(field.StrValue))
	default:
		return errors.New("unsupported data type")
	}
	return nil
}

func AppendFieldsToRecord(rec *Record, fields []influx.Field, time int64, sameSchema bool) (int64, error) {
	// fast path
	var size int64
	if sameSchema {
		for i := range fields {
			if err := AppendFieldToCol(&rec.ColVals[i], &fields[i], &size); err != nil {
				return size, err
			}
		}
		rec.ColVals[len(fields)].AppendInteger(time)
		size += int64(util.Int64SizeBytes)
		return size, nil
	}

	// slow path
	return AppendFieldsToRecordSlow(rec, fields, time)
}

func AppendFieldsToRecordSlow(rec *Record, fields []influx.Field, time int64) (int64, error) {
	var size int64
	recSchemaIdx, pointSchemaIdx := 0, 0
	recSchemaLen, pointSchemaLen := rec.ColNums()-1, len(fields)
	appendColIdx := rec.ColNums()
	oldRowNum, oldColNum := rec.RowNums(), rec.ColNums()
	for recSchemaIdx < recSchemaLen && pointSchemaIdx < pointSchemaLen {
		if rec.Schema[recSchemaIdx].Name == fields[pointSchemaIdx].Key {
			if err := AppendFieldToCol(&rec.ColVals[recSchemaIdx], &fields[pointSchemaIdx], &size); err != nil {
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
			if err := AppendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
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
		if err := AppendFieldToCol(&rec.ColVals[appendColIdx], &fields[pointSchemaIdx], &size); err != nil {
			return size, err
		}
		pointSchemaIdx++
		appendColIdx++
	}

	// check if added new field
	newColNum := rec.ColNums()
	if oldColNum != newColNum {
		FastSortRecord(rec, oldColNum)
	}
	rec.ColVals[newColNum-1].AppendInteger(time)
	size += int64(util.Int64SizeBytes)
	return size, nil
}
