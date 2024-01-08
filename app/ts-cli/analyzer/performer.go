/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package analyzer

import (
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

var timeField = &record.Field{
	Type: influx.Field_Type_Int,
	Name: record.TimeField,
}

type ColumnIterator struct {
	fi         *immutable.FileIterator
	ref        *record.Field
	chunkMeta  *immutable.ChunkMeta
	columnMeta *immutable.ColumnMeta

	colIndex int
	segIndex int

	currentAlgo *CompressAlgo
	result      *AnalyzeResult
}

func (p *ColumnIterator) Handle(col *record.ColVal, times []int64, lastSeg bool) error {
	ret := p.result
	ref := p.ref

	offset, size := p.columnMeta.GetSegment(p.segIndex)
	p.segIndex++

	err := p.analyzeSegment(offset, size, ref)
	if err != nil {
		return err
	}

	ret.originSize[ref.Type] += len(col.Val)

	if p.ref.IsString() {
		ret.offsetSize += len(col.Offset) * 4
	}

	if p.currentAlgo != nil {
		p.currentAlgo.originSize += len(col.Val)
	}

	if col.NilCount == 0 {
		ret.noNilSegment++
	}
	if col.NilCount == col.Len {
		ret.allNilSegment++
	}

	return nil
}

func (p *ColumnIterator) ColumnChanged(ref *record.Field) error {
	p.columnMeta = &p.chunkMeta.GetColMeta()[p.colIndex]
	p.segIndex = 0
	p.ref = ref
	p.colIndex++
	return nil
}

func (p *ColumnIterator) SeriesChanged(_ uint64, times []int64) error {
	p.colIndex = 0
	p.chunkMeta = p.fi.GetCurtChunkMeta()

	ret := p.result
	//The time column must not have a null value
	ret.noNilSegment += p.chunkMeta.SegmentCount()

	colMeta := p.chunkMeta.GetColMeta()

	timeCol := colMeta[len(colMeta)-1]
	for i := 0; i < p.chunkMeta.SegmentCount(); i++ {
		offset, size := timeCol.GetSegment(i)
		err := p.analyzeSegment(offset, size, timeField)
		if err != nil {
			return err
		}
	}

	for _, item := range colMeta {
		ret.preAggSize += len(item.GetPreAgg())
	}
	ret.segmentTotal += p.chunkMeta.SegmentCount() * p.chunkMeta.Len()
	ret.columnTotal += p.chunkMeta.Len()

	if len(times) == 1 {
		ret.oneRowChunk++
	}
	ret.rowTotal += len(times)

	ret.originSize[influx.Field_Type_Int] += len(times) * 8
	ret.seriesTotal++

	return nil
}

func (p *ColumnIterator) HasSeries(sid uint64) bool {
	return true
}

func (p *ColumnIterator) WriteOriginal(fi *immutable.FileIterator) error {
	return nil
}

func (p *ColumnIterator) Finish() error {
	p.fi.Close()
	return nil
}
