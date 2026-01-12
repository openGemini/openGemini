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

package immutable

import (
	"bytes"
	"errors"
	"io"
	"sort"

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

	var start uint32 = 0
	for i, v := range fragments {
		c.ranges[i].Start = start
		c.ranges[i].End = uint32(v)
		start = uint32(v)
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
	if c.fragmentIdx == 0 {
		return 0, false
	}
	fr := c.ranges[c.fragmentIdx-1]

	if c.segmentIdx >= fr.NumberOfSegments() {
		return 0, false
	}

	c.segmentIdx++
	return int(fr.Start) + c.segmentIdx - 1, true
}

type CSFileIterator struct {
	file       TSSPFile
	tcDuration int64
	isCluster  bool

	pkCols   []*record.ColVal
	pkSchema record.Schemas
	ctx      *ReadContext
	cm       *ChunkMeta

	segment *record.Record
	pk      []byte

	cursor CSIteratorCursor
}

func NewCSFileIterator(file TSSPFile, tcDuration int64, isCluster bool) (*CSFileIterator, error) {
	ci := &CSFileIterator{
		file:       file,
		tcDuration: tcDuration,
		ctx:        NewReadContext(true),
		isCluster:  isCluster,
	}

	if err := ci.initPKInfo(); err != nil {
		return nil, err
	}

	err := ci.initSchema()
	return ci, err
}

func (ci *CSFileIterator) File() TSSPFile {
	return ci.file
}

func (ci *CSFileIterator) PrimaryKey() []byte {
	return ci.pk
}

func (ci *CSFileIterator) GetColMeta() []ColumnMeta {
	return ci.cm.GetColMeta()
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

	fragments := []int64{int64(rec.RowNums() - 1)}
	if ci.isCluster {
		fragments = rec.ColVals[rec.Len()-1].IntegerValues()
	}
	ci.cursor.Init(fragments)

	return nil
}

func (ci *CSFileIterator) initSchema() error {
	fi := NewFileIterator(ci.file, logger.NewLogger(errno.ModuleCompact))
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

func (ci *CSFileIterator) NextFragment() bool {
	i, ok := ci.cursor.NextFragment()
	if !ok {
		return false
	}
	if ci.isCluster {
		ci.pk = colstore.FetchKeyAtRow(ci.pk[:0], ci.pkCols, ci.pkSchema, i, ci.tcDuration)
	}
	return true
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
	return nil
}

func (ci *CSFileIterator) ReadAll() *record.Record {
	return ci.segment
}

func (ci *CSFileIterator) ReadFragmentPK() map[string]any {
	idx := ci.cursor.fragmentIdx - 1
	if idx < 0 {
		return nil
	}

	pk := make(map[string]any)
	for i := range ci.pkSchema {
		name := ci.pkSchema[i].Name
		pk[name] = record.ReadRowValue(ci.pkCols[i], ci.pkSchema[i].Type, idx)
	}

	return pk
}

func (ci *CSFileIterator) SegmentRecord() *record.Record {
	return ci.segment
}

type CSConsumeIterator struct {
	its      []*CSFileIterator
	releases []func()
}

func NewCSConsumeIterator(its []*CSFileIterator) *CSConsumeIterator {
	itr := &CSConsumeIterator{
		its: its,
	}

	for _, it := range its {
		it.file.Ref()
		itr.releases = append(itr.releases, func() {
			it.file.Unref()
		})
	}

	return itr
}

func (csi *CSConsumeIterator) sort() {
	sort.Slice(csi.its, func(i, j int) bool {
		return bytes.Compare(csi.its[i].pk, csi.its[j].pk) <= 0
	})
}

func (csi *CSConsumeIterator) Release() {
	for _, fn := range csi.releases {
		fn()
	}
	clear(csi.releases)
	clear(csi.its)
}

func (csi *CSConsumeIterator) Next() (*record.ConsumeRecord, error) {
	for {
		if len(csi.its) == 0 {
			return nil, io.EOF
		}

		ci := csi.its[0]
		err := ci.NextSegment()
		if err == io.EOF {
			ok := ci.NextFragment()
			if !ok {
				// iteration completed
				csi.its = csi.its[1:]
				continue
			}
			csi.sort()
			continue
		}

		return csi.buildConsumeRecord(ci), nil
	}
}

func (csi *CSConsumeIterator) buildConsumeRecord(ci *CSFileIterator) *record.ConsumeRecord {
	rec := ci.ReadAll()
	cr := &record.ConsumeRecord{
		Rec: rec,
		PK:  ci.ReadFragmentPK(),
	}

	return cr
}

func (csi *CSConsumeIterator) SidCnt() int {
	return 0
}
