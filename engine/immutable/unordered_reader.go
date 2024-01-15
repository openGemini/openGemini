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
	"math"
	"sort"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var timeField = record.Field{Name: record.TimeField, Type: influx.Field_Type_Int}

type remainCallback func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error

type UnorderedColumnReader struct {
	f  TSSPFile
	sr *SegmentReader
	cm *ChunkMeta

	// times of out-of-order data
	times []int64

	col *record.ColVal

	lineOffset *Offset
	segOffset  *Offset
	remain     []*record.ColVal
	colPool    *MergeColPool
}

func newUnorderedColumnReader(f TSSPFile, cm *ChunkMeta, sr *SegmentReader, pool *MergeColPool) *UnorderedColumnReader {
	return &UnorderedColumnReader{
		f:          f,
		sr:         sr,
		cm:         cm,
		col:        &record.ColVal{},
		remain:     make([]*record.ColVal, cm.columnCount),
		lineOffset: newOffset(int(cm.columnCount)),
		segOffset:  newOffset(int(cm.columnCount)),
		colPool:    pool,
	}
}

func (r *UnorderedColumnReader) findColumnIndex(ref *record.Field) (int, bool) {
	for i := range r.cm.colMeta {
		if r.cm.colMeta[i].Equal(ref.Name, ref.Type) {
			return i, true
		}
	}

	return 0, false
}

func (r *UnorderedColumnReader) initTime() error {
	if len(r.times) == 0 {
		meta := &r.cm.colMeta[r.cm.columnCount-1]
		ref := &record.Field{Name: record.TimeField, Type: int(meta.ty)}

		r.changOffset(len(r.cm.colMeta) - 1)
		col, err := r.read(len(r.cm.colMeta)-1, math.MaxInt64, ref)
		if err != nil {
			return err
		}

		r.times = append(r.times, col.IntegerValues()...)
		r.colPool.Put(col)
	}
	return nil
}

func (r *UnorderedColumnReader) readTime(colIdx int, maxTime int64) ([]int64, error) {
	r.changOffset(colIdx)

	end := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > maxTime
	})

	start := r.lineOffset.value()
	if end < start {
		return nil, nil
	}

	times := r.times[start:end]
	r.lineOffset.incr(len(times))
	return times, nil
}

// reads all unordered data whose time is earlier than maxTime
func (r *UnorderedColumnReader) Read(ref *record.Field, maxTime int64) (*record.ColVal, []int64, error) {
	idx, ok := r.findColumnIndex(ref)
	if !ok {
		return nil, nil, nil
	}

	if err := r.initTime(); err != nil {
		return nil, nil, err
	}

	times, err := r.readTime(idx, maxTime)
	if err != nil || len(times) == 0 {
		return nil, nil, err
	}

	col, err := r.read(idx, len(times), ref)
	if err != nil {
		return nil, nil, err
	}

	return col, times, nil
}

func (r *UnorderedColumnReader) ReadSchema(res map[string]record.Field, maxTime int64) {
	if len(r.times) == 0 || r.times[0] > maxTime {
		return
	}

	if r.lineOffset.valueAt(0) == len(r.times) {
		return
	}

	for i := range r.cm.colMeta {
		meta := &r.cm.colMeta[i]
		if meta.IsTime() {
			continue
		}

		tmp, ok := res[meta.Name()]
		if !ok {
			res[meta.Name()] = record.Field{Type: int(meta.ty), Name: meta.Name()}
			continue
		}

		if tmp.Type != int(meta.ty) {
			panic("BUG: field type conflict")
		}
	}
}

func (r *UnorderedColumnReader) read(idx, need int, ref *record.Field) (*record.ColVal, error) {
	col := r.remain[idx]
	if col == nil {
		col = r.colPool.Get()
	}

	if col.Len == 0 || col.Len < need {
		meta := &r.cm.colMeta[idx]

		i := r.segOffset.value()
		for ; i < len(meta.entries); i++ {
			err := r.sr.Read(meta.entries[i], ref, r.col)
			if err != nil {
				return nil, err
			}
			col.AppendColVal(r.col, ref.Type, 0, r.col.Len)

			r.segOffset.incr(1)
			if col.Len >= need {
				break
			}
		}
	}

	res, remain := r.split(col, need, ref)
	r.remain[idx] = remain
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

func (r *UnorderedColumnReader) changOffset(idx int) {
	r.lineOffset.change(idx)
	r.segOffset.change(idx)
}

type UnorderedReader struct {
	log *logger.Logger

	// map key is sid
	meta    map[uint64][]*UnorderedColumnReader
	sid     []uint64
	times   []int64
	swap    []int64
	offsets map[string]int
	timeCol record.ColVal
	nilCol  record.ColVal

	ctx *UnorderedReaderContext
}

func NewUnorderedReader(log *logger.Logger) *UnorderedReader {
	return &UnorderedReader{
		log:     log,
		meta:    make(map[uint64][]*UnorderedColumnReader),
		offsets: make(map[string]int),
		ctx:     newUnorderedReadTool(),
	}
}

func (r *UnorderedReader) AddFiles(files []TSSPFile) {
	for _, f := range files {
		r.addFile(f)
	}
	sort.Slice(r.sid, func(i, j int) bool {
		return r.sid[i] < r.sid[j]
	})
}

func (r *UnorderedReader) addFile(f TSSPFile) {
	itr := NewFileIterator(f, r.log)
	sr := NewSegmentReader(itr)

	for {
		if !itr.NextChunkMeta() {
			break
		}

		cm := itr.curtChunkMeta.Clone()

		if _, ok := r.meta[cm.sid]; !ok {
			r.sid = append(r.sid, cm.sid)
		}

		r.meta[cm.sid] = append(r.meta[itr.curtChunkMeta.sid], newUnorderedColumnReader(f, cm, sr, r.ctx.colPool))

		itr.chunkUsed++
		itr.curtChunkMeta = nil
	}
}

// ReadRemain reads all remaining data that is smaller than the current series ID in the unordered data
func (r *UnorderedReader) ReadRemain(sid uint64, cb remainCallback) error {
	i := 0
	for ; i < len(r.sid); i++ {
		if r.sid[i] >= sid {
			break
		}

		if err := r.readRemain(r.sid[i], cb); err != nil {
			return err
		}

		delete(r.meta, r.sid[i])
	}

	r.sid = r.sid[i:]

	return nil
}

func (r *UnorderedReader) readRemain(sid uint64, cb remainCallback) error {
	if err := r.InitTimes(sid, math.MaxInt64); err != nil {
		return err
	}
	if len(r.times) == 0 {
		return nil
	}

	schemas := r.ReadSeriesSchemas(sid, math.MaxInt64)
	if len(schemas) == 0 {
		return nil
	}

	for i := 0; i < len(schemas); i++ {
		col, times, err := r.Read(sid, &schemas[i], math.MaxInt64)
		if err != nil {
			return err
		}

		if err := cb(sid, schemas[i], col, times); err != nil {
			return err
		}
	}

	r.timeCol.Init()
	r.timeCol.AppendTimes(r.times)
	return cb(sid, timeField, &r.timeCol, r.times)
}

func (r *UnorderedReader) ReadAllTimes() []int64 {
	return r.times
}

func (r *UnorderedReader) ReadTimes(ref *record.Field, maxTime int64) []int64 {
	ofs, ok := r.offsets[ref.Name]
	if !ok {
		ofs = 0
	}

	if ofs >= len(r.times) || r.times[ofs] > maxTime {
		return nil
	}

	end := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > maxTime
	})

	r.offsets[ref.Name] = end
	return r.times[ofs:end]
}

func (r *UnorderedReader) AllocNilCol(size int, ref *record.Field) *record.ColVal {
	nilCol := &r.nilCol
	FillNilCol(&r.nilCol, size, ref)
	return nilCol
}

// Read reads data based on the series ID, column, and time range
func (r *UnorderedReader) Read(sid uint64, ref *record.Field, maxTime int64) (*record.ColVal, []int64, error) {
	items, ok := r.meta[sid]
	if !ok || len(items) == 0 {
		return nil, nil, nil
	}
	nilTimes := r.ReadTimes(ref, maxTime)
	if len(nilTimes) == 0 {
		return nil, nil, nil
	}
	nilCol := r.AllocNilCol(len(nilTimes), ref)

	for i := 0; i < len(items); i++ {
		col, times, err := items[i].Read(ref, maxTime)
		if err != nil {
			return nil, nil, err
		}

		r.ctx.add(times, col)
	}

	return r.ctx.merge(nilCol, nilTimes, ref.Type)
}

func (r *UnorderedReader) ReadSeriesSchemas(sid uint64, maxTime int64) record.Schemas {
	meta, ok := r.meta[sid]
	if !ok {
		return nil
	}
	tmp := make(map[string]record.Field)
	for _, item := range meta {
		item.ReadSchema(tmp, maxTime)
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
func (r *UnorderedReader) InitTimes(sid uint64, maxTime int64) error {
	r.times = r.times[:0]
	for k := range r.offsets {
		delete(r.offsets, k)
	}

	meta, ok := r.meta[sid]
	if !ok || len(meta) == 0 {
		return nil
	}

	for _, item := range meta {
		if err := item.initTime(); err != nil {
			return err
		}

		times, err := item.readTime(len(item.cm.colMeta)-1, maxTime)
		if err != nil {
			return err
		}

		r.swap = MergeTimes(r.times, times, r.swap[:0])
		r.times, r.swap = r.swap, r.times
	}

	return nil
}

func (r *UnorderedReader) HasSeries(sid uint64) bool {
	_, ok := r.meta[sid]
	return ok
}

func (r *UnorderedReader) Close() {
	for _, items := range r.meta {
		for _, item := range items {
			util.MustClose(item.f)
		}
	}
}

type Offset struct {
	values  []int
	current *int
}

func newOffset(size int) *Offset {
	return &Offset{
		values:  make([]int, size),
		current: nil,
	}
}

func (o *Offset) change(idx int) {
	o.current = &o.values[idx]
}

func (o *Offset) incr(i int) {
	*o.current += i
}

func (o *Offset) value() int {
	return *o.current
}

func (o *Offset) valueAt(idx int) int {
	return o.values[idx]
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

func newUnorderedReadTool() *UnorderedReaderContext {
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
