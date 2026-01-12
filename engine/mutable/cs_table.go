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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/dictmap"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	Statistics "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type CSMemTableImpl struct {
	db, rp string
	mu     sync.RWMutex
}

func NewCSMemTableImpl(db, rp string) *CSMemTableImpl {
	return &CSMemTableImpl{
		db: db,
		rp: rp,
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
	genMsSchemaForColumnStore(&msInfo.Schema, row.Fields, row.Tags)
	return msInfo
}

func (c *CSMemTableImpl) FlushChunks(table *MemTable, msName string, tbStore immutable.TablesStore, _ chan []immutable.FileInfoExtend) {
	mstIdent := util.NewMeasurementIdent(c.db, c.rp)
	mstIdent.SetName(msName)

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
	if Statistics.PTWriteStat.ShouldCollect() {
		tbStore.AddPtWriteStats(msName, rec.Size())
	}

	at := NewWriteAttached(mst)
	at.FlushRecord(tbStore, chunk.WriteRec.GetRecord())
}

func (c *CSMemTableImpl) WriteRows(table *MemTable, rowsD dictmap.DictMap[string, *[]influx.Row], ctx WriteRowsCtx) error {
	var err error
	ident := util.NewMeasurementIdent(c.db, c.rp)

	for k, rows := range rowsD {
		rs := *rows
		msName := stringinterner.InternSafe(k)

		start := time.Now()
		msInfo := table.CreateMsInfo(msName, &rs[0], nil)
		atomic.AddInt64(&Statistics.PerfStat.WriteGetMstInfoNs, time.Since(start).Nanoseconds())

		start = time.Now()
		ident.SetName(msName)
		mst, ok := colstore.MstManagerIns().GetByIdent(ident)
		if !ok {
			logger.GetLogger().Warn("measurement is nil", zap.String("ident", ident.String()))
			return errors.New("measurement info is not found")
		}

		msInfo.CreateWriteChunkForColumnStore(mst.ColStoreInfo().SortKey)
		writeChunk := msInfo.GetWriteChunk()
		for index := range rs {
			_, err = c.appendFields(writeChunk, rs[index].Timestamp, rs[index].Fields, rs[index].Tags)
			if err != nil {
				return err
			}
		}

		if ctx.MsRowCount != nil {
			c.mu.Lock()
			UpdateMstRowCount(ctx.MsRowCount, msName, int64(len(*rows)))
			c.mu.Unlock()
		}
		atomic.AddInt64(&Statistics.PerfStat.WriteMstInfoNs, time.Since(start).Nanoseconds())
	}
	return err
}

func (c *CSMemTableImpl) appendFields(chunk *WriteChunkForColumnStore, time int64, fields []influx.Field, tags []influx.Tag) (int64, error) {
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

	return c.appendTagsFieldsToRecord(chunk.WriteRec.rec, fields, tags, time, sameSchema)
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

func (c *CSMemTableImpl) appendTagsFieldsToRecord(rec *record.Record, fields []influx.Field, tags []influx.Tag, time int64, sameSchema bool) (int64, error) {
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
	return c.appendToRecordWithSchemaLess(rec, fields, tags, time)
}

func (c *CSMemTableImpl) appendToRecordWithSchemaLess(rec *record.Record, fields influx.Fields, tags []influx.Tag, time int64) (int64, error) {
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
		msInfo.writeChunk.WriteRec.rec.ReuseForWrite()
		writeRecPool.Put(msInfo.writeChunk.WriteRec.rec)
	}
}
