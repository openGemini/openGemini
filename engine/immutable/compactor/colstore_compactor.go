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

package compactor

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type CompactContext struct {
	dec       codec.BinaryDecoder
	swap      *record.Record
	fragments []int64
}

var compactCtxPool = pool.NewDefaultUnionPool(func() *CompactContext {
	return &CompactContext{
		swap: &record.Record{},
	}
})

func InitColStoreCompactor() {
	if !config.GetStoreConfig().ColumnStore.CompactEnabled {
		return
	}

	immutable.SetColStoreCompactorFactory(func(lock *string, sw *immutable.StreamWriteFile) immutable.ColStoreCompactor {
		return &ColStoreCompactor{
			fileLock: lock,
			sw:       sw,
		}
	})
}

type ColStoreCompactor struct {
	mst      *colstore.Measurement
	sw       *immutable.StreamWriteFile
	files    []immutable.TSSPFile
	fileLock *string
	iw       *immutable.ColStoreIndexWriter
}

func (c *ColStoreCompactor) createIteratorBuilder(files []immutable.TSSPFile) (*FragmentIteratorBuilder, error) {
	its := make([]*immutable.CSFileIterator, len(files))
	for i, file := range files {
		it, err := immutable.NewCSFileIterator(file, c.mst.TCDuration(), c.mst.IsClusterPKIndex())
		if err != nil {
			return nil, err
		}
		its[i] = it
	}

	return NewFragmentIteratorBuilder(its, c.mst.SortKey(), c.mst.IsUniqueEnabled()), nil
}

func (c *ColStoreCompactor) initWriter(file immutable.TSSPFile) error {
	if err := c.sw.InitCompactFile(file); err != nil {
		return err
	}

	return nil
}

func (c *ColStoreCompactor) Compact(ident util.MeasurementIdent, files []immutable.TSSPFile) ([]immutable.TSSPFile, error) {
	if len(files) == 0 {
		return nil, nil
	}

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		return nil, fmt.Errorf("mst not exists")
	}
	c.mst = mst

	if err := c.initWriter(files[0]); err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			c.sw.Clean()
		}
	}()

	schema := c.mst.MeasurementInfo().GetRecordSchema()

	c.iw = immutable.NewColStoreIndexWriter(c.sw.GetDir(), ident.Name, c.sw.GetFileName(), c.fileLock, mst.IndexRelation(), schema, c.mst.BuildPKMap())
	defer c.iw.MustClose()

	err := c.compact(files)

	success = err == nil
	return c.files, err
}

func (c *ColStoreCompactor) compact(files []immutable.TSSPFile) error {
	batchSize := immutable.GetColStoreConfig().GetMaxRowsPerSegment()
	builder, err := c.createIteratorBuilder(files)
	if err != nil {
		return err
	}

	pki := files[0].GetPkInfo()
	ctx := compactCtxPool.Get()
	pkRec := &record.Record{}
	schema := pki.GetRec().Schema
	pkRec.ResetWithSchema(schema[:schema.Len()-1])
	dec := &ctx.dec
	rec := ctx.swap
	fragments := ctx.fragments[:0]

	defer func() {
		ctx.fragments = fragments
		compactCtxPool.PutWithMemSize(ctx, 0)
	}()

	isCluster := c.mst.IsClusterPKIndex()
	pkMap := make(map[string]int)
	for i := range pkRec.Schema {
		pkMap[pkRec.Schema[i].Name] = i
	}
	if len(c.mst.SortKey()) > 1 {
		c.sw.MarkTimeDisorder()
	}

	var segCount int64 = 0
	var rowCount int64 = 0
	for {
		itr, pkBuf, err := builder.BuildNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		for {
			err = itr.AppendTo(rec, batchSize)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			err = c.WriteSegment(rec)
			rowCount += int64(rec.RowNums())
			segCount++
			if err != nil {
				return err
			}

			if !isCluster {
				pickPkRecordLine(pkRec, rec, pkMap, 0)
				fragments = append(fragments, util.MergeToInt64(uint32(segCount), uint32(rowCount)))
			}
		}

		if isCluster {
			colstore.AppendKeyToRecord(dec, pkRec, pkBuf)
			fragments = append(fragments, util.MergeToInt64(uint32(segCount), uint32(rowCount)))
		}
	}

	if !isCluster {
		pickPkRecordLine(pkRec, rec, pkMap, rec.RowNums()-1)
		fragments = append(fragments, util.MergeToInt64(uint32(segCount), uint32(rowCount)))
	}

	err = CheckPKRecord(pkRec, len(fragments))
	if err == nil {
		err = c.newTsspFile(pkRec, fragments)
	}

	return err
}

func pickPkRecordLine(pkRec *record.Record, src *record.Record, pkMap map[string]int, idx int) {
	for i := range src.Schema {
		j, ok := pkMap[src.Schema[i].Name]
		if !ok {
			continue
		}

		pkRec.ColVals[j].AppendColVal(&src.ColVals[i], src.Schema[i].Type, idx, idx+1)
	}
}

func (c *ColStoreCompactor) newTsspFile(pkRec *record.Record, fragments []int64) error {
	err := c.sw.WriteCurrentMeta()
	if err != nil {
		return err
	}

	file, err := c.sw.NewTSSPFile(true)
	if err != nil {
		return err
	}
	if file == nil {
		return fmt.Errorf("compactor file is nil")
	}
	c.files = append(c.files, file)

	err = c.writePrimaryIndex(file.Path(), pkRec, fragments)
	if err != nil {
		return err
	}

	uts := util.Bytes2Uint64Slice(util.Int64Slice2byte(fragments))
	pkMark := fragment.NewIndexFragmentVariable(uts)
	file.SetPkInfo(colstore.NewPKInfo(pkRec, pkMark, c.mst.ColStoreInfo().GetPKType(), -1))
	file.SetSkipIndexInfo(c.iw.BuildSkipIndexInfo())

	return c.iw.Flush()
}

func (c *ColStoreCompactor) writePrimaryIndex(path string, pkRec *record.Record, fragments []int64) error {
	indexFilePath := immutable.BuildPKFilePathFromTSSP(path)
	indexBuilder := colstore.NewIndexBuilder(c.fileLock, indexFilePath+immutable.GetTmpFileSuffix())

	defer indexBuilder.Reset()
	immutable.AppendFragmentsToPKRecord(pkRec, fragments)
	return indexBuilder.WriteData(pkRec, c.mst.TCLocation())
}

func (c *ColStoreCompactor) WriteSegment(rec *record.Record) error {
	timeCol := rec.TimeColumn()
	var err error

	if c.sw.Size() == 0 {
		c.sw.Init(colstore.SeriesID, rec.Schema)
	}

	for i := range rec.Schema.Len() {
		ref := rec.Schema[i]
		err = c.sw.ChangeColumn(ref)
		if err != nil {
			return err
		}

		err = c.sw.WriteData(colstore.SeriesID, ref, rec.ColVals[i], timeCol)
		if err != nil {
			return err
		}

		err = c.iw.Write(ref, &rec.ColVals[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func CheckPKRecord(rec *record.Record, segCount int) error {
	for i := range rec.Len() {
		col := &rec.ColVals[i]
		if col.NilCount > 0 {
			return errors.New("[BUG] PK record contains null values")
		}
		if col.Len != segCount {
			return fmt.Errorf("[BUG] PK record has %d rows, expected %d", col.Len, segCount)
		}
	}
	return nil
}

type FragmentIteratorBuilder struct {
	colstore.KeySorter
	its []*RowIterator
	fi  *FragmentIterator
}

func NewFragmentIteratorBuilder(its []*immutable.CSFileIterator, skSchema record.Schemas, unique bool) *FragmentIteratorBuilder {
	fi := NewFragmentIterator(skSchema, unique)
	fi.BuildCompactedSchema(its)

	ris := make([]*RowIterator, len(its))
	for i, it := range its {
		ris[i] = NewRowIterator(it, skSchema)
	}

	return &FragmentIteratorBuilder{
		its: ris,
		fi:  fi,
	}
}

func (b *FragmentIteratorBuilder) Swap(i, j int) {
	b.KeySorter.Swap(i, j)
	b.its[i], b.its[j] = b.its[j], b.its[i]
}

func (b *FragmentIteratorBuilder) BuildNext() (*FragmentIterator, []byte, error) {
	b.nextFragment()
	if len(b.its) == 0 {
		return nil, nil, io.EOF
	}

	err := b.nextSegment()
	if err != nil {
		return nil, nil, err
	}

	fi := b.fi
	fi.Reset()

	var pk []byte
	for i, it := range b.its {
		if i > 0 && !bytes.Equal(it.PrimaryKey(), pk) {
			break
		}

		pk = it.PrimaryKey()
		fi.AddRowIterator(it)
	}

	heap.Init(fi)
	return fi, pk, nil
}

func (b *FragmentIteratorBuilder) nextFragment() {
	b.Reset()
	var its = b.its[:0]
	for _, it := range b.its {
		if !it.NextFragment() {
			continue
		}

		its = append(its, it)
		b.Add(it.PrimaryKey())
	}
	b.its = its
	sort.Sort(b)
}

func (b *FragmentIteratorBuilder) nextSegment() error {
	for _, it := range b.its {
		if it.HasRemainRow() {
			continue
		}

		err := it.NextSegment()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// FragmentIterator used for iterating data in Fragment
// constructed through FragmentIteratorBuilder
type FragmentIterator struct {
	kc  colstore.KeyComparator
	ris []*RowIterator

	skSchema        record.Schemas
	compactedSchema record.Schemas

	unique bool
}

func NewFragmentIterator(skSchema record.Schemas, unique bool) *FragmentIterator {
	fi := &FragmentIterator{
		skSchema: skSchema,
		unique:   unique,
	}
	return fi
}

func (fi *FragmentIterator) Reset() {
	fi.ris = fi.ris[:0]
}

func (fi *FragmentIterator) AddRowIterator(ri *RowIterator) {
	if !ri.HasRemainRow() {
		return
	}

	fi.ris = append(fi.ris, ri)
}

func (fi *FragmentIterator) BuildCompactedSchema(its []*immutable.CSFileIterator) {
	fields := map[string]uint8{}
	for _, it := range its {
		colMeta := it.GetColMeta()
		for i := range colMeta {
			fields[colMeta[i].Name()] = colMeta[i].Type()
		}
	}

	for name, typ := range fields {
		fi.compactedSchema = append(fi.compactedSchema, record.Field{Type: int(typ), Name: name})
	}
	sort.Sort(record.CustomSchemas(fi.compactedSchema))
}

func (fi *FragmentIterator) AppendTo(dst *record.Record, batchSize int) error {
	if len(fi.ris) == 0 {
		return io.EOF
	}

	dst.ResetWithSchema(fi.compactedSchema)

	var preKey []byte
	for len(fi.ris) > 0 {
		ri, ok := heap.Pop(fi).(*RowIterator)
		if !ok {
			continue
		}

		key := ri.ks.LastKey()
		replace := fi.unique && bytes.Equal(preKey, key)

		if !replace && batchSize <= 0 {
			heap.Push(fi, ri)
			break
		}

		ri.ks.DeleteLast()
		preKey = append(preKey[:0], key...)

		if replace {
			// The unique key function is enabled, and the unique keys are the same.
			ri.ReplaceRecordRowTo(dst)
		} else {
			ri.AppendRecordRowTo(dst)
			batchSize--
		}

		if ri.HasRemainRow() {
			heap.Push(fi, ri)
			continue
		}

		err := ri.NextSegment()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		heap.Push(fi, ri)
	}

	return nil
}

func (fi *FragmentIterator) Pop() any {
	n := len(fi.ris)
	ri := fi.ris[n-1]
	fi.ris = fi.ris[:n-1]
	return ri
}

func (fi *FragmentIterator) Push(v any) {
	ri, ok := v.(*RowIterator)
	if !ok {
		return
	}
	fi.ris = append(fi.ris, ri)
}

func (fi *FragmentIterator) Len() int {
	return len(fi.ris)
}

func (fi *FragmentIterator) Swap(i, j int) {
	fi.ris[i], fi.ris[j] = fi.ris[j], fi.ris[i]
}

func (fi *FragmentIterator) Less(i, j int) bool {
	a, b := fi.ris[i], fi.ris[j]
	ka, kb := a.ks.LastKey(), b.ks.LastKey()

	if fi.unique && bytes.Equal(ka, kb) {
		return a.seq < b.seq
	}

	return fi.kc.Less(ka, kb)
}

type RowIterator struct {
	ci  *immutable.CSFileIterator
	ks  colstore.KeySorter
	seq uint64

	rec       *record.Record
	rowCount  int
	rowOffset int
	ncs       record.NilCounts
	skSchema  record.Schemas
	swap      []byte
}

func NewRowIterator(ci *immutable.CSFileIterator, skSchema record.Schemas) *RowIterator {
	_, seq := ci.File().LevelAndSequence()
	ri := &RowIterator{
		ci:       ci,
		skSchema: skSchema,
		seq:      seq,
	}
	return ri
}

func (ri *RowIterator) NextFragment() bool {
	if ri.HasRemainRow() {
		return true
	}

	return ri.ci.NextFragment()
}

func (ri *RowIterator) NextSegment() error {
	if ri.HasRemainRow() {
		return nil
	}

	err := ri.ci.NextSegment()
	if err != nil {
		return err
	}

	ri.rec = ri.ci.SegmentRecord()
	ri.rowOffset = 0
	ri.rowCount = ri.rec.RowNums()
	ri.buildSortKey()
	ri.ncs.Build(ri.rec)
	return nil
}

func (ri *RowIterator) ReplaceRecordRowTo(dst *record.Record) {
	i, j := 0, 0
	n := dst.Len() - 1
	rowIdx := ri.rowOffset

	for i < n && j < ri.rec.Len()-1 {
		si := &dst.Schema[i]
		sj := &ri.rec.Schema[j]
		srcCol := &ri.rec.ColVals[j]
		dstCol := &dst.ColVals[i]

		if si.Name == sj.Name {
			i++
			j++
			if srcCol.IsNil(rowIdx) {
				continue
			}

			dstCol.DeleteLast(si.Type)
			dstCol.AppendWithNilCount(srcCol, sj.Type, rowIdx, rowIdx+1, ri.ncs.Column(j))
		} else if si.Name < sj.Name {
			i++
		}
	}

	ri.rowOffset++
}

func (ri *RowIterator) AppendRecordRowTo(dst *record.Record) {
	i, j := 0, 0
	n := dst.Len() - 1
	m := ri.rec.Len() - 1
	rowIdx := ri.rowOffset

	for i < n && j < ri.rec.Len()-1 {
		si := &dst.Schema[i]
		sj := &ri.rec.Schema[j]

		if si.Name == sj.Name {
			dst.ColVals[i].AppendWithNilCount(&ri.rec.ColVals[j], sj.Type, rowIdx, rowIdx+1, ri.ncs.Column(j))
			i++
			j++
		} else if si.Name < sj.Name {
			dst.ColVals[i].PadColVal(si.Type, 1)
			i++
		}
	}

	for ; i < n; i++ {
		dst.ColVals[i].PadColVal(dst.Schema[i].Type, 1)
	}

	// time column
	dst.ColVals[n].AppendColVal(&ri.rec.ColVals[m], dst.Schema[n].Type, rowIdx, rowIdx+1)

	ri.rowOffset++
}

func (ri *RowIterator) HasRemainRow() bool {
	return ri.rowOffset < ri.rowCount
}

func (ri *RowIterator) PrimaryKey() []byte {
	return ri.ci.PrimaryKey()
}

func (ri *RowIterator) buildSortKey() {
	cols := record.FetchColVals(ri.rec, ri.skSchema)
	buf := ri.swap[:0]
	rows := ri.rec.RowNums()

	for i := range rows {
		ofs := len(buf)
		// inverse order
		buf = colstore.FetchKeyAtRow(buf, cols, ri.skSchema, rows-i-1, 0)
		ri.ks.Add(buf[ofs:])
	}

	ri.swap = buf
}
