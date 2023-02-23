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
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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
	remain     []record.ColVal
}

func newUnorderedColumnReader(f TSSPFile, cm *ChunkMeta, sr *SegmentReader) *UnorderedColumnReader {
	return &UnorderedColumnReader{
		f:          f,
		sr:         sr,
		cm:         cm,
		col:        &record.ColVal{},
		remain:     make([]record.ColVal, cm.columnCount),
		lineOffset: newOffset(int(cm.columnCount)),
		segOffset:  newOffset(int(cm.columnCount)),
	}
}

func (r *UnorderedColumnReader) findColumnIndex(ref *record.Field) (int, bool) {
	for i := range r.cm.colMeta {
		if r.cm.colMeta[i].name == ref.Name && int(r.cm.colMeta[i].ty) == ref.Type {
			return i, true
		}
	}

	return 0, false
}

func (r *UnorderedColumnReader) readTime() ([]int64, error) {
	if len(r.times) == 0 {
		meta := &r.cm.colMeta[r.cm.columnCount-1]
		ref := &record.Field{Name: record.TimeField, Type: int(meta.ty)}

		r.changOffset(len(r.cm.colMeta) - 1)
		col, err := r.read(len(r.cm.colMeta)-1, math.MaxInt64, ref)
		if err != nil {
			return nil, err
		}

		r.times = col.IntegerValues()
	}
	return r.times, nil
}

// reads all unordered data whose time is earlier than maxTime
func (r *UnorderedColumnReader) Read(ref *record.Field, maxTime int64) (*record.ColVal, []int64, error) {
	idx, ok := r.findColumnIndex(ref)
	if !ok {
		return nil, nil, nil
	}

	times, err := r.readTime()
	if err != nil {
		return nil, nil, err
	}

	if times[0] > maxTime {
		return nil, nil, nil
	}

	end := sort.Search(len(times), func(i int) bool {
		return times[i] > maxTime
	})

	r.changOffset(idx)
	start := r.lineOffset.value()
	if end <= start {
		return nil, nil, nil
	}

	col, err := r.read(idx, end-start, ref)
	if err != nil {
		return nil, nil, err
	}

	r.lineOffset.incr(col.Len)
	return col, times[start:end], nil
}

func (r *UnorderedColumnReader) ReadSchema(res map[string]record.Field, maxTime int64) {
	if r.cm.minTime() > maxTime {
		return
	}

	if len(r.times) > 0 && r.lineOffset.valueAt(0) == len(r.times) {
		return
	}

	for i := range r.cm.colMeta {
		meta := &r.cm.colMeta[i]
		if meta.name == record.TimeField {
			continue
		}

		tmp, ok := res[meta.name]
		if !ok {
			res[meta.name] = record.Field{Type: int(meta.ty), Name: meta.name}
			continue
		}

		if tmp.Type != int(meta.ty) {
			panic("BUG: field type conflict")
		}
	}
}

func (r *UnorderedColumnReader) read(idx, need int, ref *record.Field) (*record.ColVal, error) {
	r.sr.ResetContext()
	col := r.remain[idx]

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

	res, remain := r.split(&col, need, ref)
	r.remain[idx] = *remain
	return res, nil
}

func (r *UnorderedColumnReader) split(col *record.ColVal, rowCount int, ref *record.Field) (*record.ColVal, *record.ColVal) {
	if col == nil || col.Len <= rowCount {
		return col, &record.ColVal{}
	}

	dst := []record.ColVal{{}, {}}
	dst = col.SplitByIndex(dst, rowCount, ref.Type)
	return &dst[0], &dst[1]
}

func (r *UnorderedColumnReader) changOffset(idx int) {
	r.lineOffset.change(idx)
	r.segOffset.change(idx)
}

type UnorderedReader struct {
	log *logger.Logger

	// map key is sid
	meta  map[uint64][]*UnorderedColumnReader
	sid   []uint64
	times []int64
}

func NewUnorderedReader(log *logger.Logger) *UnorderedReader {
	return &UnorderedReader{
		log:  log,
		meta: make(map[uint64][]*UnorderedColumnReader),
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

		r.meta[cm.sid] = append(r.meta[itr.curtChunkMeta.sid], newUnorderedColumnReader(f, cm, sr))

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
	}

	r.sid = r.sid[i:]

	return nil
}

func (r *UnorderedReader) readRemain(sid uint64, cb remainCallback) error {
	schemas := r.ReadSeriesSchemas(sid, math.MaxInt64)
	if len(schemas) == 0 {
		return nil
	}

	timeCol := &record.ColVal{}

	for i := 0; i < len(schemas); i++ {
		col, times, err := r.Read(sid, &schemas[i], math.MaxInt64)
		if err != nil {
			return err
		}

		if i == 0 {
			timeCol.AppendIntegers(times...)
		}

		if err := cb(sid, record.Field{Name: schemas[i].Name, Type: schemas[i].Type}, col, times); err != nil {
			return err
		}
	}

	return cb(sid, timeField, timeCol, timeCol.IntegerValues())
}

// Read reads data based on the series ID, column, and time range
func (r *UnorderedReader) Read(sid uint64, ref *record.Field, maxTime int64) (*record.ColVal, []int64, error) {
	items, ok := r.meta[sid]
	if !ok || len(items) == 0 {
		return nil, nil, nil
	}

	var colList []*record.ColVal
	var timesList [][]int64

	for i := 0; i < len(items); i++ {
		col, times, err := items[i].Read(ref, maxTime)
		if err != nil {
			return nil, nil, err
		}

		if len(times) > 0 {
			colList = append(colList, col)
			timesList = append(timesList, times)
		}
	}

	if len(colList) == 0 {
		return nil, nil, nil
	}
	if len(colList) == 1 {
		return colList[0], timesList[0], nil
	}
	mh := record.NewMergeHelper()
	for i := 1; i < len(colList); i++ {
		mh.AddUnorderedCol(colList[i], timesList[i], ref.Type)
	}

	return mh.Merge(colList[0], timesList[0], ref.Type)
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

	if len(tmp) == 0 && maxTime == math.MaxInt64 {
		delete(r.meta, sid)
		return nil
	}

	res := make(record.Schemas, 0, len(tmp))
	for k := range tmp {
		res = append(res, tmp[k])
	}
	sort.Sort(res)
	return res
}

// InitSeriesTimes init the time column of a series
func (r *UnorderedReader) InitSeriesTimes(sid uint64) error {
	r.times = r.times[:0]

	meta, ok := r.meta[sid]
	if !ok || len(meta) == 0 {
		return nil
	}

	for _, item := range meta {
		times, err := item.readTime()
		if err != nil || len(times) == 0 {
			return err
		}

		r.times = mergeTimes(r.times, times, nil)
	}

	return nil
}

func (r *UnorderedReader) ReadTimes(min, max int64) []int64 {
	if len(r.times) == 0 {
		return nil
	}

	begin := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > min
	})
	end := sort.Search(len(r.times), func(i int) bool {
		return r.times[i] > max
	})

	return r.times[begin:end]
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

func mergeTimes(a []int64, b []int64, dst []int64) []int64 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	i, j := 0, 0
	la, lb := len(a), len(b)

	for {
		if i == la {
			dst = append(dst, b[j:]...)
			break
		}
		if j == lb {
			dst = append(dst, a[i:]...)
			break
		}

		if a[i] == b[j] {
			dst = append(dst, a[i])
			i++
			j++
			continue
		}

		if a[i] < b[j] {
			dst = append(dst, a[i])
			i++
			continue
		}

		// a[i] > b[j]
		dst = append(dst, b[j])
		j++
	}

	return dst
}

type SegmentReader struct {
	fi  *FileIterator
	ctx *ReadContext
}

func NewSegmentReader(fi *FileIterator) *SegmentReader {
	return &SegmentReader{
		fi: fi,
	}
}

func (sr *SegmentReader) ResetContext() {
	if sr.ctx != nil {
		sr.ctx.Release()
	}
	sr.ctx = NewReadContext(true)
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
