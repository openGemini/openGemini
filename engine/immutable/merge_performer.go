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

package immutable

import (
	"errors"
	"fmt"
	"math"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
)

type mergePerformer struct {
	mh   *record.MergeHelper
	ur   *UnorderedReader
	sw   *StreamWriteFile
	cw   *columnWriter
	stat *statistics.MergeStatItem

	// New file after merge
	mergedFiles TSSPFiles

	// Is the last ordered file?
	// The remaining unordered data that does not intersect the series
	// needs to be written into this file
	lastFile bool

	// The series of the current ordered data does not exist in the unordered data
	noUnorderedSeries bool

	// The column in the current ordered data does not exist in the unordered data
	noUnorderedColumn bool

	// current series ID
	sid uint64

	// current column schema
	ref *record.Field

	// schema of unordered data
	unorderedSchemas record.Schemas

	// all times of ordered data in the current series
	orderTimes []int64

	// Maximum time after data merge
	maxMergedTime int64

	// merged time column
	mergedTimes *record.ColVal
}

func NewMergePerformer(ur *UnorderedReader, stat *statistics.MergeStatItem) *mergePerformer {
	return &mergePerformer{
		mh:          record.NewMergeHelper(),
		ur:          ur,
		mergedFiles: TSSPFiles{},
		mergedTimes: &record.ColVal{},
		stat:        stat,
	}
}

func (p *mergePerformer) Reset(sw *StreamWriteFile, last bool) {
	p.sid = 0
	p.sw = sw
	p.cw = newColumnWriter(sw, MaxRowsPerSegment())
	p.lastFile = last
}

func (p *mergePerformer) Handle(col *record.ColVal, times []int64, lastSeg bool) error {
	// Unordered data does not contain the data of the series
	if p.noUnorderedSeries {
		return p.write(p.ref, col, times, lastSeg)
	}

	maxOrderTime := times[len(times)-1]
	if p.lastFile && lastSeg {
		maxOrderTime = math.MaxInt64
	}

	minOrderTime := times[0]
	if minOrderTime == p.orderTimes[0] {
		// first segment
		minOrderTime = math.MinInt64
	}

	unorderedCol, unorderedTimes, err := p.readUnordered(minOrderTime, maxOrderTime)
	if err != nil {
		return err
	}
	if unorderedCol != nil {
		record.CheckCol(unorderedCol)
	}

	return p.merge(col, unorderedCol, times, unorderedTimes, p.ref, lastSeg)
}

func (p *mergePerformer) SeriesChanged(sid uint64, orderTimes []int64) error {
	p.stat.OrderSeriesCount++

	if err := p.finishSeries(sid); err != nil {
		return err
	}

	if err := p.ur.InitSeriesTimes(sid); err != nil {
		return err
	}

	maxOrderTime := orderTimes[len(orderTimes)-1]
	if p.lastFile {
		maxOrderTime = math.MaxInt64
	}

	p.unorderedSchemas = p.ur.ReadSeriesSchemas(sid, maxOrderTime)
	p.noUnorderedSeries = len(p.unorderedSchemas) == 0
	p.sid = sid
	p.orderTimes = append(p.orderTimes[:0], orderTimes...)
	p.mergedTimes.Init()
	p.ref = nil
	p.maxMergedTime = math.MinInt64

	if p.noUnorderedSeries {
		p.appendMergedTime(orderTimes)
	} else {
		p.stat.IntersectSeriesCount++
	}

	p.sw.ChangeSid(p.sid)

	return nil
}

func (p *mergePerformer) ColumnChanged(ref record.Field) error {
	p.ref = &ref
	p.noUnorderedColumn = true

	sl := len(p.unorderedSchemas)
	if p.noUnorderedSeries || sl == 0 {
		return p.sw.AppendColumn(ref)
	}

	pos := 0
	for i := 0; i < sl; i++ {
		uRef := &p.unorderedSchemas[i]
		if uRef.Name < ref.Name {
			// columns that exist only in unordered data
			if err := p.writeUnorderedCol(uRef); err != nil {
				return err
			}
			pos++
			continue
		}

		if uRef.Name == ref.Name {
			pos++
			p.noUnorderedColumn = false
		}

		break
	}

	p.unorderedSchemas = p.unorderedSchemas[pos:]
	return p.sw.AppendColumn(ref)
}

func (p *mergePerformer) Finish() error {
	if err := p.finishSeries(math.MaxInt64); err != nil {
		return err
	}

	file, err := p.sw.NewTSSPFile(true)
	if err != nil {
		return err
	}

	p.AppendMergedFile(file)

	return nil
}

func (p *mergePerformer) finishSeries(sid uint64) error {
	if err := p.writeRemainCol(); err != nil {
		return err
	}

	if err := p.writeMergedTime(); err != nil {
		return err
	}

	if err := p.sw.WriteCurrentMeta(); err != nil {
		return err
	}

	return p.writeRemain(sid)
}

func (p *mergePerformer) AppendMergedFile(file TSSPFile) {
	p.mergedFiles.Append(file)
}

func (p *mergePerformer) MergedFiles() *TSSPFiles {
	return &p.mergedFiles
}

func (p *mergePerformer) CleanTmpFiles() {
	for _, f := range p.mergedFiles.Files() {
		if err := f.Remove(); err != nil {
			logger.GetLogger().Error("failed to remove tmp file", zap.String("file", f.Path()))
		}
	}

	if p.sw != nil {
		p.sw.Close(true)
	}
}

func (p *mergePerformer) writeRemainCol() error {
	if len(p.unorderedSchemas) == 0 {
		return nil
	}

	for _, item := range p.unorderedSchemas {
		if err := p.writeUnorderedCol(&item); err != nil {
			return err
		}
	}

	return nil
}

// Write the data whose sid is smaller than maxSid in the unordered data
func (p *mergePerformer) writeRemain(maxSid uint64) error {
	if !p.lastFile {
		return nil
	}

	var lastSid uint64 = 0
	err := p.ur.ReadRemain(maxSid, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		if lastSid != sid {
			if err := p.sw.WriteCurrentMeta(); err != nil {
				return err
			}
			p.sid = sid
			p.sw.ChangeSid(sid)
			lastSid = sid
		}

		if err := p.sw.AppendColumn(ref); err != nil {
			return err
		}

		return p.write(&ref, col, times, true)
	})

	if err == nil {
		err = p.sw.WriteCurrentMeta()
	}

	return err
}

func (p *mergePerformer) merge(orderCol, unorderedCol *record.ColVal,
	orderTimes, unorderedTimes []int64, ref *record.Field, lastSeg bool) error {
	// No unordered data exists in the time range
	if len(unorderedTimes) == 0 {
		p.appendMergedTime(orderTimes)
		return p.write(ref, orderCol, orderTimes, lastSeg)
	}

	p.mh.AddUnorderedCol(unorderedCol, unorderedTimes, ref.Type)
	mergedCol, mergedTimes, err := p.mh.Merge(orderCol, orderTimes, ref.Type)
	if err != nil {
		return err
	}

	p.appendMergedTime(mergedTimes)
	return p.write(ref, mergedCol, mergedTimes, lastSeg)
}

func (p *mergePerformer) readUnordered(min, max int64) (*record.ColVal, []int64, error) {
	var times []int64
	var col *record.ColVal
	var err error

	if p.noUnorderedColumn {
		times = p.ur.ReadTimes(min, max)
		col = p.newNilCol(len(times), p.ref)
	} else {
		col, times, err = p.ur.Read(p.sid, p.ref, max)
	}

	return col, times, err
}

func (p *mergePerformer) writeUnorderedCol(ref *record.Field) error {
	if err := p.sw.AppendColumn(*ref); err != nil {
		return err
	}

	batchSize := int(maxRowsPerSegment)
	tl := len(p.orderTimes)

	for i := 0; i < tl; i += batchSize {
		end := i + batchSize
		if end > tl {
			end = tl
		}

		maxOrderTime := p.orderTimes[end-1]
		if p.lastFile && end == tl {
			maxOrderTime = math.MaxInt64
		}

		unorderedCol, unorderedTimes, err := p.ur.Read(p.sid, ref, maxOrderTime)
		if err != nil {
			return err
		}

		orderCol := p.newNilCol(end-i, ref)
		err = p.merge(orderCol, unorderedCol, p.orderTimes[i:end], unorderedTimes, ref, end == tl)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *mergePerformer) appendMergedTime(times []int64) {
	max := times[len(times)-1]
	if max > p.maxMergedTime {
		p.maxMergedTime = max
		p.mergedTimes.AppendTimes(times)
	}
}

func (p *mergePerformer) writeMergedTime() error {
	if p.sid == 0 {
		return nil
	}

	if err := p.sw.AppendColumn(timeField); err != nil {
		return err
	}

	return p.cw.writeAll(p.sid, timeRef, p.mergedTimes)
}

func (p *mergePerformer) write(ref *record.Field, col *record.ColVal, times []int64, lastSeg bool) error {
	if err := p.cw.write(p.sid, ref, col, times); err != nil {
		return err
	}

	if lastSeg {
		return p.cw.flush()
	}

	return nil
}

func (p *mergePerformer) newNilCol(size int, ref *record.Field) *record.ColVal {
	if size == 0 {
		return nil
	}

	col := &record.ColVal{
		Val:          nil,
		Offset:       nil,
		BitMapOffset: 0,
		Len:          size,
		NilCount:     size,
	}

	col.FillBitmap(0)
	if ref.IsString() {
		col.Offset = make([]uint32, size)
	}
	return col
}

type columnWriter struct {
	sw         *StreamWriteFile
	remain     *record.ColVal
	remainTime *record.ColVal

	limit int
	sid   uint64
	ref   *record.Field
}

func newColumnWriter(sw *StreamWriteFile, limit int) *columnWriter {
	return &columnWriter{
		sw:         sw,
		remain:     &record.ColVal{},
		remainTime: &record.ColVal{},
		limit:      limit,
	}
}

func (cw *columnWriter) writeAll(sid uint64, ref *record.Field, col *record.ColVal) error {
	cols := col.Split(nil, cw.limit, ref.Type)

	for i := range cols {
		if err := cw.sw.WriteData(sid, *ref, cols[i], nil); err != nil {
			return err
		}
	}

	return nil
}

func (cw *columnWriter) write(sid uint64, ref *record.Field, col *record.ColVal, times []int64) error {
	failpoint.Inject("column-writer-error", func() {
		failpoint.Return(fmt.Errorf("failed to wirte column data"))
	})

	cw.sid = sid
	cw.ref = ref

	cw.remainTime.AppendTimes(times)

	// fast path
	if cw.remain.Len == 0 && (col.Len == cw.limit) {
		defer cw.remainTime.Init()
		return cw.sw.WriteData(sid, *ref, *col, cw.remainTime)
	}

	cw.remain.AppendColVal(col, ref.Type, 0, col.Len)
	if cw.remain.Len != cw.remainTime.Len {
		return errors.New("BUG: The length of the data column is different from that of the time column")
	}

	// data is less than one segment
	if cw.remain.Len < cw.limit {
		return nil
	}

	if cw.remain.Len == cw.limit {
		err := cw.sw.WriteData(sid, *ref, *cw.remain, cw.remainTime)
		cw.remain.Init()
		cw.remainTime.Init()
		return err
	}

	cols, timeCols := cw.splitRemain(ref.Type)
	for i := range cols {
		if cols[i].Len < cw.limit {
			cw.remain = &cols[i]
			cw.remainTime = &timeCols[i]
			break
		}
		if err := cw.sw.WriteData(sid, *ref, cols[i], &timeCols[i]); err != nil {
			return err
		}
	}

	return nil
}

func (cw *columnWriter) splitRemain(typ int) ([]record.ColVal, []record.ColVal) {
	cols := cw.remain.Split(nil, cw.limit, typ)
	times := cw.remainTime.Split(nil, cw.limit, influx.Field_Type_Int)
	cw.remain.Init()
	cw.remainTime.Init()

	return cols, times
}

func (cw *columnWriter) flush() error {
	if cw.remain.Len == 0 {
		return nil
	}
	defer func() {
		cw.remain.Init()
		cw.remainTime.Init()
	}()

	return cw.sw.WriteData(cw.sid, *cw.ref, *cw.remain, cw.remainTime)
}
