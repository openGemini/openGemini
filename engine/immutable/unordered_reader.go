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
	"io"
	"math"
	"sort"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type remainCallback func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error

type UnorderedColumnReader struct {
	fi      *FileIterator
	sr      *SegmentReader
	columns *UnorderedColumns

	seq uint64
	sid uint64

	// times of out-of-order data
	times []int64

	colIdx  int
	ref     *record.Field
	colMeta *ColumnMeta
	swap    *record.ColVal
	remain  []*record.ColVal
	colPool *MergeColPool
}

func newUnorderedColumnReader(f TSSPFile, lg *logger.Logger, pool *MergeColPool) *UnorderedColumnReader {
	fi := NewFileIterator(f, lg)
	_, seq := f.LevelAndSequence()

	return &UnorderedColumnReader{
		fi:      fi,
		sr:      NewSegmentReader(fi),
		swap:    &record.ColVal{},
		colPool: pool,
		seq:     seq,
		columns: NewUnorderedColumns(),
	}
}

func (r *UnorderedColumnReader) HasColumn() bool {
	return r.ref != nil
}

func (r *UnorderedColumnReader) MatchSeries(sid uint64) bool {
	return r.sid == sid && !r.columns.ReadCompleted()
}

func (r *UnorderedColumnReader) ChangeSeries(sid uint64) error {
	if r.columns.ReadCompleted() {
		r.sid = 0
	}

	if r.sid == sid {
		return nil
	}

	if r.sid > 0 {
		return nil
	}

	if !r.fi.NextChunkMeta() {
		return r.fi.err
	}
	cm := r.fi.GetCurtChunkMeta()
	r.columns.Init(cm)

	r.fi.chunkUsed++
	r.fi.curtChunkMeta = nil

	r.sid = cm.sid
	n := int(cm.columnCount) - cap(r.remain)
	if n > 0 {
		r.remain = append(r.remain[:cap(r.remain)], make([]*record.ColVal, n)...)
	}
	r.remain = r.remain[:cm.columnCount]

	err := r.initTime()
	r.columns.SetRemainLine(int(cm.columnCount) * len(r.times))
	return err
}

func (r *UnorderedColumnReader) ChangeColumn(sid uint64, ref *record.Field) {
	r.colMeta = nil
	r.ref = nil

	if r.sid != sid {
		return
	}

	col := r.columns.ChangeColumn(ref.Name)
	if col != nil {
		r.colMeta = col.meta
		r.ref = ref
		r.colIdx = col.colIdx
	}
}

func (r *UnorderedColumnReader) initTime() error {
	r.times = r.times[:0]
	r.colIdx = 0
	col, err := r.read(r.columns.TimeMeta(), math.MaxInt64, timeRef)
	if err != nil {
		return err
	}

	r.times = append(r.times, col.IntegerValues()...)
	r.colPool.Put(col)
	return nil
}

func (r *UnorderedColumnReader) ReadTime(sid uint64, maxTime int64) []int64 {
	start := r.columns.GetLineOffset(record.TimeField)
	times := r.readTime(sid, maxTime, start)
	r.columns.IncrLineOffset(record.TimeField, len(times))
	return times
}

func (r *UnorderedColumnReader) readTime(sid uint64, maxTime int64, start int) []int64 {
	if r.sid != sid {
		return nil
	}

	end := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > maxTime
	})

	if end < start {
		return nil
	}

	times := r.times[start:end]
	return times
}

func (r *UnorderedColumnReader) Read(sid uint64, maxTime int64) (*record.ColVal, []int64, error) {
	if r.sid != sid || !r.HasColumn() {
		return nil, nil, nil
	}

	start := r.columns.GetLineOffset(r.ref.Name)
	times := r.readTime(sid, maxTime, start)
	if len(times) == 0 {
		return nil, nil, nil
	}
	r.columns.IncrLineOffset(r.ref.Name, len(times))

	col, err := r.read(r.colMeta, len(times), r.ref)
	if err != nil {
		return nil, nil, err
	}

	return col, times, nil
}

func (r *UnorderedColumnReader) read(colMeta *ColumnMeta, need int, ref *record.Field) (*record.ColVal, error) {
	col := r.remain[r.colIdx]
	if col == nil {
		col = r.colPool.Get()
	}

	if col.Len == 0 || col.Len < need {
		i := r.columns.GetSegOffset(ref.Name)
		for ; i < len(colMeta.entries); i++ {
			err := r.sr.Read(colMeta.entries[i], ref, r.swap)
			if err != nil {
				return nil, err
			}
			col.AppendColVal(r.swap, ref.Type, 0, r.swap.Len)

			r.columns.IncrSegOffset(ref.Name, 1)
			if col.Len >= need {
				break
			}
		}
	}

	res, remain := r.split(col, need, ref)
	r.remain[r.colIdx] = remain
	return res, nil
}

func (r *UnorderedColumnReader) split(col *record.ColVal, rowCount int, ref *record.Field) (*record.ColVal, *record.ColVal) {
	if col == nil || col.Len <= rowCount {
		return col, nil
	}

	head, tail := r.colPool.Get(), r.colPool.Get()
	head.AppendColVal(col, ref.Type, 0, rowCount)
	tail.AppendColVal(col, ref.Type, rowCount, col.Len)

	r.colPool.Put(col)
	return head, tail
}

func (r *UnorderedColumnReader) ReadSchemas(sid uint64, maxTime int64, dst map[string]record.Field) {
	if r.sid != sid || r.times[0] > maxTime {
		return
	}

	r.columns.Walk(func(meta *ColumnMeta) {
		if _, ok := dst[meta.name]; !ok {
			dst[meta.name] = record.Field{Name: meta.name, Type: int(meta.ty)}
		}
	})
}

func (r *UnorderedColumnReader) Close() {
	r.fi.Close()
}

type UnorderedReader struct {
	log     *logger.Logger
	ctx     *UnorderedReaderContext
	readers []*UnorderedColumnReader

	sid     uint64
	ref     *record.Field
	schemas map[string]record.Field

	ofs   int
	times []int64
	swap  []int64

	timeCol record.ColVal
	nilCol  record.ColVal
}

func NewUnorderedReader(log *logger.Logger) *UnorderedReader {
	return &UnorderedReader{
		log:     log,
		ctx:     newUnorderedReaderContext(),
		schemas: make(map[string]record.Field),
	}
}

func (r *UnorderedReader) AddFiles(files []TSSPFile) {
	for _, f := range files {
		r.readers = append(r.readers, newUnorderedColumnReader(f, r.log, r.ctx.colPool))
	}
}

func (r *UnorderedReader) ChangeSeries(sid uint64) error {
	r.sid = sid
	return r.changeSeries(sid)
}

func (r *UnorderedReader) changeSeries(sid uint64) error {
	for _, reader := range r.readers {
		err := reader.ChangeSeries(sid)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *UnorderedReader) ChangeColumn(sid uint64, ref *record.Field) {
	r.ofs = 0
	r.ref = ref
	for _, reader := range r.readers {
		reader.ChangeColumn(sid, ref)
	}
}

func (r *UnorderedReader) HasSeries(sid uint64) bool {
	for _, reader := range r.readers {
		if reader.MatchSeries(sid) {
			return true
		}
	}
	return false
}

func (r *UnorderedReader) ReadRemain(sid uint64, cb remainCallback) error {
	var err error
	for {
		err = r.readRemain(sid, cb)
		if err != nil {
			break
		}
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}

	return err
}

// ReadRemain reads all remaining data that is smaller than the current series ID in the unordered data
func (r *UnorderedReader) readRemain(sid uint64, cb remainCallback) error {
	if err := r.changeSeries(sid); err != nil {
		return err
	}

	current := sid
	for i := range r.readers {
		if r.readers[i].sid > 0 && r.readers[i].sid < current {
			current = r.readers[i].sid
		}
	}

	if current == sid {
		return io.EOF
	}

	r.sid = current
	schema := r.ReadSeriesSchemas(current, math.MaxInt64)
	r.InitTimes(current, math.MaxInt64)

	times := r.times
	rows := len(times)
	if rows == 0 {
		return io.EOF
	}

	wn := 0
	for i := range schema {
		r.ChangeColumn(current, &schema[i])

		for wn < rows {
			wn += GetTsStoreConfig().maxRowsPerSegment
			if wn > rows {
				wn = rows
			}

			col, colTimes, err := r.Read(current, times[wn-1])
			if err != nil {
				return err
			}

			err = cb(current, *r.ref, col, colTimes)
			if err != nil {
				return err
			}
		}
		wn = 0
	}

	r.timeCol.Init()
	r.timeCol.AppendTimes(r.times)
	return cb(current, *timeRef, &r.timeCol, r.times)
}

func (r *UnorderedReader) ReadAllTimes() []int64 {
	return r.times
}

func (r *UnorderedReader) ReadTimes(maxTime int64) []int64 {
	ofs := r.ofs

	if ofs >= len(r.times) || r.times[ofs] > maxTime {
		return nil
	}

	end := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > maxTime
	})

	r.ofs = end
	return r.times[ofs:end]
}

func (r *UnorderedReader) AllocNilCol(size int, ref *record.Field) *record.ColVal {
	nilCol := &r.nilCol
	FillNilCol(&r.nilCol, size, ref)
	return nilCol
}

// Read reads data based on the series ID, column, and time range
func (r *UnorderedReader) Read(sid uint64, maxTime int64) (*record.ColVal, []int64, error) {
	nilTimes := r.ReadTimes(maxTime)
	if len(nilTimes) == 0 {
		return nil, nil, nil
	}
	nilCol := r.AllocNilCol(len(nilTimes), r.ref)

	for _, reader := range r.readers {
		col, times, err := reader.Read(sid, maxTime)
		if err != nil {
			return nil, nil, err
		}

		r.ctx.add(times, col)
	}

	return r.ctx.merge(nilCol, nilTimes, r.ref.Type)
}

func (r *UnorderedReader) ReadSeriesSchemas(sid uint64, maxTime int64) record.Schemas {
	tmp := r.schemas
	for k := range tmp {
		delete(tmp, k)
	}

	for _, reader := range r.readers {
		reader.ReadSchemas(sid, maxTime, tmp)
	}

	if len(tmp) == 0 {
		return nil
	}

	res := make(record.Schemas, 0, len(tmp))
	for k := range tmp {
		res = append(res, tmp[k])
	}
	sort.Sort(res)
	return res
}

// InitTimes initialize the time column of unordered data
func (r *UnorderedReader) InitTimes(sid uint64, maxTime int64) {
	r.times = r.times[:0]
	if sid != r.sid {
		return
	}

	for _, reader := range r.readers {
		times := reader.ReadTime(sid, maxTime)

		r.swap = MergeTimes(r.times, times, r.swap[:0])
		r.times, r.swap = r.swap, r.times
	}
}

func (r *UnorderedReader) Close() {
	for _, reader := range r.readers {
		reader.Close()
	}
}

func (r *UnorderedReader) CloseFile() {
	for _, reader := range r.readers {
		util.MustClose(reader.fi.r)
	}
}

type SegmentReader struct {
	fi  *FileIterator
	ctx *ReadContext
}

func NewSegmentReader(fi *FileIterator) *SegmentReader {
	return &SegmentReader{
		fi:  fi,
		ctx: NewReadContext(true),
	}
}

func (sr *SegmentReader) Read(seg Segment, ref *record.Field, col *record.ColVal) error {
	data, err := sr.fi.readData(seg.offset, seg.size)
	if err != nil {
		return err
	}

	return sr.decode(data, ref, col)
}

func (sr *SegmentReader) decode(data []byte, ref *record.Field, col *record.ColVal) error {
	if ref.Name == record.TimeField {
		return appendTimeColumnData(data, col, sr.ctx, false)
	}

	return decodeColumnData(ref, data, col, sr.ctx, false)
}

type UnorderedReaderContext struct {
	mh      *record.MergeHelper
	colPool *MergeColPool

	cols  []*record.ColVal
	times [][]int64
}

func newUnorderedReaderContext() *UnorderedReaderContext {
	return &UnorderedReaderContext{
		colPool: &MergeColPool{},
		mh:      record.NewMergeHelper(),
	}
}

func (t *UnorderedReaderContext) add(times []int64, col *record.ColVal) {
	if len(times) > 0 {
		t.cols = append(t.cols, col)
		t.times = append(t.times, times)
	}
}

func (t *UnorderedReaderContext) release() {
	for i := range t.cols {
		t.colPool.Put(t.cols[i])
	}

	t.cols = t.cols[:0]
	t.times = t.times[:0]
}

func (t *UnorderedReaderContext) merge(nilCol *record.ColVal, nilTimes []int64, typ int) (*record.ColVal, []int64, error) {
	if len(t.cols) == 0 {
		return nilCol, nilTimes, nil
	}

	defer t.release()

	for i := 0; i < len(t.cols); i++ {
		t.mh.AddUnorderedCol(t.cols[i], t.times[i])
	}

	return t.mh.Merge(nilCol, nilTimes, typ)
}

type UnorderedColumns struct {
	remainLine int
	current    *UnorderedColumn
	timeCol    *UnorderedColumn
	columns    []UnorderedColumn
	columnMap  map[string]*UnorderedColumn
}

func NewUnorderedColumns() *UnorderedColumns {
	return &UnorderedColumns{
		columns:   make([]UnorderedColumn, defaultCap),
		columnMap: make(map[string]*UnorderedColumn),
	}
}

func (c *UnorderedColumns) reset() {
	c.current = nil
	c.timeCol = nil
	c.remainLine = 0
	c.columns = c.columns[:0]

	for k := range c.columnMap {
		delete(c.columnMap, k)
	}
}

func (c *UnorderedColumns) Walk(callback func(meta *ColumnMeta)) {
	for i := 0; i < len(c.columns)-1; i++ {
		callback(c.columns[i].meta)
	}
}

func (c *UnorderedColumns) TimeMeta() *ColumnMeta {
	return c.columns[len(c.columns)-1].meta
}

func (c *UnorderedColumns) alloc() *UnorderedColumn {
	size := len(c.columns)
	if size == cap(c.columns) {
		c.columns = append(c.columns, UnorderedColumn{})
	}
	c.columns = c.columns[:size+1]
	return &c.columns[size]
}

func (c *UnorderedColumns) Init(cm *ChunkMeta) {
	c.reset()

	for i := range cm.colMeta {
		col := c.alloc()
		col.Init(&cm.colMeta[i], i)
		c.columnMap[cm.colMeta[i].name] = col
	}
	c.timeCol = &c.columns[len(c.columns)-1]
}

func (c *UnorderedColumns) ChangeColumn(name string) *UnorderedColumn {
	c.current = c.columnMap[name]
	return c.current
}

func (c *UnorderedColumns) GetLineOffset(name string) int {
	if name == record.TimeField {
		return c.timeCol.lineOffset
	}
	return c.current.lineOffset
}

func (c *UnorderedColumns) IncrLineOffset(name string, n int) {
	if name == record.TimeField {
		c.timeCol.lineOffset += n
	} else {
		c.current.lineOffset += n
	}
	c.remainLine -= n
}

func (c *UnorderedColumns) GetSegOffset(name string) int {
	if name == record.TimeField {
		return c.timeCol.segOffset
	}
	return c.current.segOffset
}

func (c *UnorderedColumns) IncrSegOffset(name string, n int) {
	if name == record.TimeField {
		c.timeCol.segOffset += n
	} else {
		c.current.segOffset += n
	}
}

func (c *UnorderedColumns) SetRemainLine(n int) {
	c.remainLine = n
}

func (c *UnorderedColumns) ReadCompleted() bool {
	return c.remainLine <= 0
}

type UnorderedColumn struct {
	lineOffset int
	segOffset  int
	colIdx     int
	meta       *ColumnMeta
}

func (c *UnorderedColumn) Init(meta *ColumnMeta, idx int) {
	c.lineOffset = 0
	c.segOffset = 0
	c.meta = meta
	c.colIdx = idx
}
