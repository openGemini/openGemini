// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: hash_merge_func.gen.go.tmpl

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

package executor

type HashMergeColumn interface {
	AppendValues(col Column, start int, end int)
	SetOutPut(col Column)
}

type HashMergeResult struct {
	time []int64
	cols []HashMergeColumn
}

func (mr *HashMergeResult) AppendResult(chunk Chunk, start int, end int) {
	mr.time = append(mr.time, chunk.Time()[start:end]...)
	for i, col := range mr.cols {
		col.AppendValues(chunk.Column(i), start, end)
	}
}

type HashMergeMsg struct {
	tags   ChunkTags
	result *HashMergeResult
}

type HashMergeFloatColumn struct {
	values  []float64
	nils    []bool
	oLoc    int
	oValLoc int
}

func NewHashMergeFloatColumn() HashMergeColumn {
	return &HashMergeFloatColumn{
		values:  make([]float64, 0),
		nils:    make([]bool, 0),
		oLoc:    0,
		oValLoc: 0,
	}
}

func (m *HashMergeFloatColumn) AppendValues(col Column, start int, end int) {
	srcPoints := end - start
	valueStart, valueEnd := start, end
	if col.NilCount() != 0 {
		valueStart, valueEnd = col.GetRangeValueIndexV2(start, end)
	}
	m.values = append(m.values, col.FloatValues()[valueStart:valueEnd]...)
	dstPoints := valueEnd - valueStart
	if dstPoints == srcPoints {
		for ; start < end; start++ {
			m.nils = append(m.nils, true)
		}
		return
	}
	for ; start < end; start++ {
		if col.IsNilV2(start) {
			m.nils = append(m.nils, false)
		} else {
			m.nils = append(m.nils, true)
		}
	}
}

func (m *HashMergeFloatColumn) SetOutPut(col Column) {
	if m.nils[m.oLoc] {
		col.AppendNotNil()
		col.AppendFloatValue(m.values[m.oValLoc])
		m.oLoc++
		m.oValLoc++
	} else {
		col.AppendNil()
		m.oLoc++
	}
}

type HashMergeIntegerColumn struct {
	values  []int64
	nils    []bool
	oLoc    int
	oValLoc int
}

func NewHashMergeIntegerColumn() HashMergeColumn {
	return &HashMergeIntegerColumn{
		values:  make([]int64, 0),
		nils:    make([]bool, 0),
		oLoc:    0,
		oValLoc: 0,
	}
}

func (m *HashMergeIntegerColumn) AppendValues(col Column, start int, end int) {
	srcPoints := end - start
	valueStart, valueEnd := start, end
	if col.NilCount() != 0 {
		valueStart, valueEnd = col.GetRangeValueIndexV2(start, end)
	}
	m.values = append(m.values, col.IntegerValues()[valueStart:valueEnd]...)
	dstPoints := valueEnd - valueStart
	if dstPoints == srcPoints {
		for ; start < end; start++ {
			m.nils = append(m.nils, true)
		}
		return
	}
	for ; start < end; start++ {
		if col.IsNilV2(start) {
			m.nils = append(m.nils, false)
		} else {
			m.nils = append(m.nils, true)
		}
	}
}

func (m *HashMergeIntegerColumn) SetOutPut(col Column) {
	if m.nils[m.oLoc] {
		col.AppendNotNil()
		col.AppendIntegerValue(m.values[m.oValLoc])
		m.oLoc++
		m.oValLoc++
	} else {
		col.AppendNil()
		m.oLoc++
	}
}

type HashMergeStringColumn struct {
	values  []string
	nils    []bool
	oLoc    int
	oValLoc int
}

func NewHashMergeStringColumn() HashMergeColumn {
	return &HashMergeStringColumn{
		values:  make([]string, 0),
		nils:    make([]bool, 0),
		oLoc:    0,
		oValLoc: 0,
	}
}

func (m *HashMergeStringColumn) AppendValues(col Column, start int, end int) {
	srcPoints := end - start
	valueStart, valueEnd := start, end
	if col.NilCount() != 0 {
		valueStart, valueEnd = col.GetRangeValueIndexV2(start, end)
	}
	m.values = col.StringValuesRangeV2(m.values, valueStart, valueEnd)
	dstPoints := valueEnd - valueStart
	if dstPoints == srcPoints {
		for ; start < end; start++ {
			m.nils = append(m.nils, true)
		}
		return
	}
	for ; start < end; start++ {
		if col.IsNilV2(start) {
			m.nils = append(m.nils, false)
		} else {
			m.nils = append(m.nils, true)
		}
	}
}

func (m *HashMergeStringColumn) SetOutPut(col Column) {
	if m.nils[m.oLoc] {
		col.AppendNotNil()
		col.AppendStringValue(m.values[m.oValLoc])
		m.oLoc++
		m.oValLoc++
	} else {
		col.AppendNil()
		m.oLoc++
	}
}

type HashMergeBooleanColumn struct {
	values  []bool
	nils    []bool
	oLoc    int
	oValLoc int
}

func NewHashMergeBooleanColumn() HashMergeColumn {
	return &HashMergeBooleanColumn{
		values:  make([]bool, 0),
		nils:    make([]bool, 0),
		oLoc:    0,
		oValLoc: 0,
	}
}

func (m *HashMergeBooleanColumn) AppendValues(col Column, start int, end int) {
	srcPoints := end - start
	valueStart, valueEnd := start, end
	if col.NilCount() != 0 {
		valueStart, valueEnd = col.GetRangeValueIndexV2(start, end)
	}
	m.values = append(m.values, col.BooleanValues()[valueStart:valueEnd]...)
	dstPoints := valueEnd - valueStart
	if dstPoints == srcPoints {
		for ; start < end; start++ {
			m.nils = append(m.nils, true)
		}
		return
	}
	for ; start < end; start++ {
		if col.IsNilV2(start) {
			m.nils = append(m.nils, false)
		} else {
			m.nils = append(m.nils, true)
		}
	}
}

func (m *HashMergeBooleanColumn) SetOutPut(col Column) {
	if m.nils[m.oLoc] {
		col.AppendNotNil()
		col.AppendBooleanValue(m.values[m.oValLoc])
		m.oLoc++
		m.oValLoc++
	} else {
		col.AppendNil()
		m.oLoc++
	}
}
