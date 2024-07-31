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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
)

type mergePerformer struct {
	mh   *record.MergeHelper
	ur   *UnorderedReader
	sw   *StreamWriteFile
	cw   *columnWriter
	itr  *ColumnIterator
	stat *statistics.MergeStatItem

	// Is the last ordered file?
	// The remaining unordered data that does not intersect the series
	// needs to be written into this file
	lastFile bool

	lastSeries bool

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

	// merged time column
	mergedTimes   []int64
	mergedTimeCol *record.ColVal

	nilCol record.ColVal
}

func NewMergePerformer(ur *UnorderedReader, stat *statistics.MergeStatItem) *mergePerformer {
	return &mergePerformer{
		mh:            record.NewMergeHelper(),
		ur:            ur,
		mergedTimeCol: &record.ColVal{},
		stat:          stat,
	}
}

func (p *mergePerformer) Reset(sw *StreamWriteFile, itr *ColumnIterator) {
	p.sid = itr.sid
	p.itr = itr
	p.sw = sw
	p.cw = newColumnWriter(sw, GetMaxRowsPerSegment4TsStore())
}

func (p *mergePerformer) Handle(col *record.ColVal, times []int64, lastSeg bool) error {
	// Unordered data does not contain the data of the series
	if p.noUnorderedSeries {
		return p.write(p.ref, col, times, lastSeg)
	}

	maxOrderTime := times[len(times)-1]
	if p.lastSeries && lastSeg {
		maxOrderTime = math.MaxInt64
	}

	unorderedCol, unorderedTimes, err := p.readUnordered(maxOrderTime)
	if err != nil {
		return err
	}
	if unorderedCol != nil {
		record.CheckCol(unorderedCol, p.ref.Type)
	}

	return p.merge(col, unorderedCol, times, unorderedTimes, p.ref, lastSeg)
}

func (p *mergePerformer) SeriesChanged(sid uint64, orderTimes []int64) error {
	p.stat.OrderSeriesCount++

	if len(orderTimes) == 0 {
		p.sid = 0
		return nil
	}

	maxOrderTime := orderTimes[len(orderTimes)-1]
	if p.lastSeries {
		maxOrderTime = math.MaxInt64
	}

	p.ur.InitTimes(sid, maxOrderTime)
	unorderedTimes := p.ur.ReadAllTimes()
	p.noUnorderedSeries = len(unorderedTimes) == 0

	if p.noUnorderedSeries {
		p.unorderedSchemas = nil
		p.mergedTimes = append(p.mergedTimes[:0], orderTimes...)
	} else {
		p.unorderedSchemas = p.ur.ReadSeriesSchemas(sid, maxOrderTime)
		p.mergedTimes = MergeTimes(orderTimes, unorderedTimes, p.mergedTimes[:0])
		p.stat.IntersectSeriesCount++
	}

	p.mergedTimeCol.Init()
	p.mergedTimeCol.AppendTimes(p.mergedTimes)

	p.sid = sid
	p.ref = nil
	p.sw.ChangeSid(p.sid)

	return nil
}

func (p *mergePerformer) ColumnChanged(ref *record.Field) error {
	p.ref = ref
	p.noUnorderedColumn = true

	sl := len(p.unorderedSchemas)
	if p.noUnorderedSeries || sl == 0 {
		p.ur.ChangeColumn(p.sid, ref)
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

	p.ur.ChangeColumn(p.sid, ref)
	p.unorderedSchemas = p.unorderedSchemas[pos:]
	return p.sw.AppendColumn(ref)
}

func (p *mergePerformer) Finish() (TSSPFile, error) {
	if p.lastFile {
		err := p.writeRemain(math.MaxUint64)
		if err != nil {
			return nil, err
		}
	}

	file, err := p.sw.NewTSSPFile(true)
	if err != nil {
		return nil, err
	}

	p.Close()
	p.Release()
	return file, nil
}

func (p *mergePerformer) finishSeries() error {
	if err := p.writeRemainCol(); err != nil {
		return err
	}

	if err := p.writeMergedTime(); err != nil {
		return err
	}

	if err := p.sw.WriteCurrentMeta(); err != nil {
		return err
	}

	return nil
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

	p.unorderedSchemas = nil
	return nil
}

// Write the data whose sid is smaller than maxSid in the unordered data
func (p *mergePerformer) writeRemain(maxSid uint64) error {
	var lastSid uint64 = 0
	var lastRef *record.Field

	err := p.ur.ReadRemain(maxSid, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		if lastSid != sid {
			if err := p.sw.WriteCurrentMeta(); err != nil {
				return err
			}
			p.sid = sid
			p.sw.ChangeSid(sid)
			lastSid = sid
		}

		if lastRef == nil || lastRef.Name != ref.Name {
			if err := p.sw.AppendColumn(&ref); err != nil {
				return err
			}
			lastRef = &ref
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
		return p.write(ref, orderCol, orderTimes, lastSeg)
	}

	p.mh.AddUnorderedCol(unorderedCol, unorderedTimes)
	mergedCol, mergedTimes, err := p.mh.Merge(orderCol, orderTimes, ref.Type)
	if err != nil {
		return err
	}

	return p.write(ref, mergedCol, mergedTimes, lastSeg)
}

func (p *mergePerformer) readUnordered(max int64) (*record.ColVal, []int64, error) {
	var times []int64
	var col *record.ColVal
	var err error

	if p.noUnorderedColumn {
		times = p.ur.ReadTimes(max)
		col = p.ur.AllocNilCol(len(times), p.ref)
	} else {
		col, times, err = p.ur.Read(p.sid, max)
	}

	return col, times, err
}

func (p *mergePerformer) writeUnorderedCol(ref *record.Field) error {
	p.ur.ChangeColumn(p.sid, ref)

	if err := p.sw.AppendColumn(ref); err != nil {
		return err
	}

	maxTime := p.mergedTimes[len(p.mergedTimes)-1]
	if p.lastSeries {
		maxTime = math.MaxInt64
	}

	orderCol := &p.nilCol
	FillNilCol(orderCol, len(p.mergedTimes), ref)
	unorderedCol, unorderedTimes, err := p.ur.Read(p.sid, maxTime)
	if err != nil {
		return err
	}
	return p.merge(orderCol, unorderedCol, p.mergedTimes, unorderedTimes, ref, true)
}

func (p *mergePerformer) writeMergedTime() error {
	if p.sid == 0 {
		return nil
	}

	if err := p.sw.AppendColumn(timeRef); err != nil {
		return err
	}

	return p.cw.writeAll(p.sid, timeRef, p.mergedTimeCol)
}

func (p *mergePerformer) write(ref *record.Field, col *record.ColVal, times []int64, lastSeg bool) error {
	if err := p.cw.write(p.sid, ref, col, times); err != nil {
		return err
	}

	if lastSeg {
		return p.cw.flush(p.sid, ref)
	}

	return nil
}

func (p *mergePerformer) HasSeries(sid uint64) bool {
	return p.ur.HasSeries(sid)
}

func (p *mergePerformer) WriteOriginal(fi *FileIterator) error {
	meta := fi.GetCurtChunkMeta()

	limit := uint32(fileops.DefaultBufferSize * 2)
	offset := meta.offset
	readSize := uint32(0)

	d := p.sw.writer.DataSize() - meta.offset
	meta.offset = p.sw.writer.DataSize()

	var cm *ColumnMeta
	for i := range meta.colMeta {
		cm = &meta.colMeta[i]
		for j := range cm.entries {
			cm.entries[j].offset += d
		}
	}

	var buf []byte
	var err error
	var n int

	for readSize < meta.size {
		if readSize+limit > meta.size {
			limit = meta.size - readSize
		}
		readSize += limit

		buf, err = fi.readData(offset, limit)
		if err != nil {
			return err
		}
		if len(buf) != int(limit) {
			return errno.NewError(errno.ShortRead, len(buf), limit)
		}
		offset += int64(limit)

		n, err = p.sw.writer.WriteData(buf)
		if err != nil {
			return err
		}
		if n != len(buf) {
			return errno.NewError(errno.ShortWrite, n, len(buf))
		}
	}

	return p.sw.WriteMeta(meta)
}

func (p *mergePerformer) Close() {
	p.itr.Close()
}

func (p *mergePerformer) Release() {
	p.itr = nil
	p.ur = nil
	p.sw = nil
	p.cw = nil
	p.ref = nil
}

type columnWriter struct {
	sw         *StreamWriteFile
	remain     *record.ColVal
	remainTime *record.ColVal

	limit int
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
	if col.Len <= cw.limit {
		return cw.sw.WriteData(sid, *ref, *col, nil)
	}

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

func (cw *columnWriter) flush(sid uint64, ref *record.Field) error {
	if cw.remain.Len == 0 {
		return nil
	}
	defer func() {
		cw.remain.Init()
		cw.remainTime.Init()
	}()

	return cw.sw.WriteData(sid, *ref, *cw.remain, cw.remainTime)
}

type MergePerformers struct {
	closed bool
	signal chan struct{}
	once   sync.Once

	items []*mergePerformer
	ur    *UnorderedReader

	mergedFiles *TSSPFiles
}

func NewMergePerformers(ur *UnorderedReader) *MergePerformers {
	performers := &MergePerformers{
		mergedFiles: NewTSSPFiles(),
		signal:      make(chan struct{}),
		ur:          ur,
	}
	return performers
}

func (c *MergePerformers) Len() int      { return len(c.items) }
func (c *MergePerformers) Swap(i, j int) { c.items[i], c.items[j] = c.items[j], c.items[i] }
func (c *MergePerformers) Less(i, j int) bool {
	a := c.items[i]
	b := c.items[j]
	if a.sid != b.sid {
		return a.sid < b.sid
	}

	return a.itr.minTime < b.itr.minTime
}

func (c *MergePerformers) Push(v interface{}) {
	c.items = append(c.items, v.(*mergePerformer))
}

func (c *MergePerformers) Pop() interface{} {
	l := len(c.items)
	v := c.items[l-1]
	c.items = c.items[:l-1]

	v.lastSeries = true
	for _, item := range c.items {
		if item.sid == v.sid {
			v.lastSeries = false
			break
		}
	}
	v.lastFile = len(c.items) == 0
	return v
}

func (c *MergePerformers) Close() {
	c.once.Do(func() {
		close(c.signal)
		c.closed = true
		for _, item := range c.items {
			item.Close()
		}
	})
}

func (c *MergePerformers) Release() {
	for _, item := range c.items {
		item.Release()
	}
	c.items = nil
	c.ur = nil
}

func (c *MergePerformers) Closed() bool {
	return c.closed
}

func (c *MergePerformers) Next() error {
	var sid uint64 = 0

	for {
		if c.Len() == 0 {
			break
		}

		item, ok := heap.Pop(c).(*mergePerformer)
		if !ok {
			continue
		}

		if sid > 0 && item.sid != sid {
			heap.Push(c, item)
			break
		}

		if sid == 0 {
			sid = item.sid
			err := item.writeRemain(sid)
			if err != nil {
				return err
			}

			err = item.ur.ChangeSeries(sid)
			if err != nil {
				return err
			}
		}

		itr := item.itr
		err := itr.IterCurrentChunk(item)
		if err != nil {
			return err
		}

		err = item.finishSeries()
		if err != nil {
			return err
		}

		if itr.NextChunkMeta() {
			item.sid = itr.sid
			heap.Push(c, item)
			continue
		}

		if itr.Error() != nil {
			return itr.Error()
		}

		mergedFile, err := item.Finish()
		if err != nil {
			return err
		}
		c.mergedFiles.Append(mergedFile)
	}

	return nil
}
