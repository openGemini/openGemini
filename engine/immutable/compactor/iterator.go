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
	"errors"
	"io"
	"math"
	"sort"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
)

type CSIteratorCursor struct {
	ranges      []fragment.FragmentRange
	fragmentIdx int
	segmentIdx  int
}

func (c *CSIteratorCursor) Init(fragments []int64) {
	c.fragmentIdx = 0
	c.segmentIdx = 0
	c.ranges = make([]fragment.FragmentRange, len(fragments))
	for i, v := range fragments {
		c.ranges[i].Start = uint32(v >> 32)
		c.ranges[i].End = c.ranges[i].Start + uint32(v&math.MaxUint32)
	}
}

func (c *CSIteratorCursor) NextFragment() (int, bool) {
	if c.fragmentIdx >= len(c.ranges) {
		return 0, false
	}
	c.fragmentIdx++
	c.segmentIdx = 0
	return c.fragmentIdx - 1, true
}

func (c *CSIteratorCursor) NextSegment() (int, bool) {
	fr := c.ranges[c.fragmentIdx-1]

	if c.segmentIdx >= fr.NumberOfSegments() {
		return 0, false
	}

	c.segmentIdx++
	return int(fr.Start) + c.segmentIdx - 1, true
}

type CSFileIterator struct {
	file     immutable.TSSPFile
	skSchema record.Schemas

	pkCols   []*record.ColVal
	pkSchema record.Schemas
	ctx      *immutable.ReadContext
	cm       *immutable.ChunkMeta

	segment   *record.Record
	remainRow int
	pk        []byte

	cursor CSIteratorCursor
}

func NewCSFileIterator(file immutable.TSSPFile, skSchema record.Schemas) (*CSFileIterator, error) {
	ci := &CSFileIterator{
		file:     file,
		skSchema: skSchema,
		ctx:      immutable.NewReadContext(true),
	}

	if err := ci.initPKInfo(); err != nil {
		return nil, err
	}

	err := ci.initSchema()
	return ci, err
}

func (ci *CSFileIterator) initPKInfo() error {
	pki := ci.file.GetPkInfo()
	rec := pki.GetRec()

	schema := rec.Schema
	last := schema.Field(schema.Len() - 1)
	if last == nil || last.Name != record.FragmentField {
		return errors.New("invalid primary key record schema")
	}

	schema = schema[:schema.Len()-1]
	ci.pkCols = record.FetchColVals(rec, schema)
	ci.pkSchema = schema
	fragments := rec.ColVals[rec.Len()-1].IntegerValues()
	ci.cursor.Init(fragments)

	return nil
}

func (ci *CSFileIterator) initSchema() error {
	fi := immutable.NewFileIterator(ci.file, logger.NewLogger(errno.ModuleCompact))
	ok := fi.NextChunkMeta()
	ci.cm = fi.GetCurtChunkMeta()
	if !ok || ci.cm == nil {
		return errors.New("invalid chunk meta")
	}

	columns := ci.cm.GetColMeta()

	schema := make(record.Schemas, len(columns))
	for i := range columns {
		schema[i].Type = int(columns[i].Type())
		schema[i].Name = columns[i].Name()
	}
	ci.segment = &record.Record{}
	ci.segment.ResetWithSchema(schema)
	return nil
}

func (ci *CSFileIterator) NextFragment() error {
	if ci.remainRow > 0 {
		return nil
	}
	i, ok := ci.cursor.NextFragment()
	if !ok {
		return io.EOF
	}
	ci.pk = colstore.FetchKeyAtRow(ci.pk[:0], ci.pkCols, ci.pkSchema, i)
	return nil
}

func (ci *CSFileIterator) NextSegment() error {
	i, ok := ci.cursor.NextSegment()
	if !ok {
		return io.EOF
	}

	rec, err := ci.file.ReadAt(ci.cm, i, ci.segment, ci.ctx, fileops.IO_PRIORITY_LOW)
	if err != nil {
		return err
	}

	ci.segment = rec
	ci.remainRow = rec.RowNums()
	return nil
}

func (ci *CSFileIterator) AppendRecordRowTo(dst *record.Record, rowIdx int) {
	dst.AppendRec(ci.segment, rowIdx, rowIdx+1)
	ci.remainRow--
}

func (ci *CSFileIterator) RemainRecordRow() int {
	return ci.remainRow
}

func (ci *CSFileIterator) SegmentRecord() *record.Record {
	return ci.segment
}

type FragmentIteratorBuilder struct {
	colstore.KeySorter
	its []*CSFileIterator
	fi  *FragmentIterator
}

func NewFragmentIteratorBuilder(its []*CSFileIterator, skSchema record.Schemas) *FragmentIteratorBuilder {
	return &FragmentIteratorBuilder{
		its: its,
		fi:  NewFragmentIterator(skSchema),
	}
}

func (b *FragmentIteratorBuilder) Swap(i, j int) {
	b.KeySorter.Swap(i, j)
	b.its[i], b.its[j] = b.its[j], b.its[i]
}

func (b *FragmentIteratorBuilder) BuildNext() (*FragmentIterator, []byte, error) {
	err := b.nextFragment()
	if err != nil {
		return nil, nil, err
	}
	if len(b.its) == 0 {
		return nil, nil, io.EOF
	}

	fi := b.fi
	fi.Reset()

	var pk []byte
	for i, it := range b.its {
		if i > 0 && !bytes.Equal(it.pk, b.its[i-1].pk) {
			break
		}

		pk = it.pk
		err = fi.AddCSFileIterator(it)
		if err != nil {
			return nil, nil, err
		}
	}

	sort.Sort(fi)
	return fi, pk, nil
}

func (b *FragmentIteratorBuilder) nextFragment() error {
	b.Reset()
	var its = b.its[:0]
	for _, it := range b.its {
		err := it.NextFragment()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		its = append(its, it)
		b.Add(it.pk)
	}
	b.its = its
	sort.Sort(b)
	return nil
}

// FragmentIterator used for iterating data in Fragment
// constructed through FragmentIteratorBuilder
type FragmentIterator struct {
	colstore.OffsetKeySorter
	its      []*CSFileIterator
	skSchema record.Schemas
}

func NewFragmentIterator(skSchema record.Schemas) *FragmentIterator {
	fi := &FragmentIterator{
		skSchema: skSchema,
	}
	fi.SetDesc(true)
	return fi
}

func (fi *FragmentIterator) Reset() {
	fi.its = fi.its[:0]
}

func (fi *FragmentIterator) AddCSFileIterator(it *CSFileIterator) error {
	err := it.NextSegment()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	idx := len(fi.its)
	fi.its = append(fi.its, it)
	fi.buildSortKey(it.SegmentRecord(), idx)
	return nil
}

func (fi *FragmentIterator) AppendTo(dst *record.Record, batchSize int) error {
	if len(fi.Offsets) == 0 {
		return io.EOF
	}

	dst.ResetWithSchema(fi.its[0].SegmentRecord().Schema)

	for batchSize > 0 && len(fi.Offsets) > 0 {
		i := len(fi.Offsets) - 1
		v := fi.Offsets[i]
		n := v >> 32
		it := fi.its[n]
		rowIdx := int(v & math.MaxUint32)

		fi.deleteLast()
		it.AppendRecordRowTo(dst, rowIdx)
		batchSize--

		if it.RemainRecordRow() > 0 {
			continue
		}

		err := it.NextSegment()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		fi.buildSortKey(it.SegmentRecord(), int(n))
		sort.Sort(fi)
	}

	return nil
}

func (fi *FragmentIterator) deleteLast() {
	n := len(fi.Offsets) - 1
	fi.Offsets = fi.Offsets[:n]
	if len(fi.Keys) > 0 {
		fi.Keys = fi.Keys[:n]
	}
	if len(fi.Times) > 0 {
		fi.Times = fi.Times[:n]
	}
}

func (fi *FragmentIterator) buildSortKey(rec *record.Record, n int) {
	if colstore.OnlySortByTime(fi.skSchema) {
		times := rec.Times()
		for i := range rec.RowNums() {
			fi.Times = append(fi.Times, times[i])
			// The high 32 bits record the iterator sequence number, and the low 32 bits record the row number.
			fi.Offsets = append(fi.Offsets, int64(n<<32|i))
		}
		return
	}

	cols := record.FetchColVals(rec, fi.skSchema)
	var buf []byte
	for i := range rec.RowNums() {
		buf = colstore.FetchKeyAtRow(buf, cols, fi.skSchema, i)
		fi.Add(buf)
		buf = buf[len(buf):]
		fi.Offsets = append(fi.Offsets, int64(n<<32|i))
	}
}
