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

package executor

import (
	"testing"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func mockCastorOp(input []Chunk, out Chunk, p ...interface{}) error { return nil }

func Test_WideIterator_MultiColumnChunk(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f2", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndex(seriesIdx...)
	ch1.Column(0).AppendFloatValues(0.0)
	ch1.Column(0).AppendNilsV2(true)
	ch1.Column(1).AppendFloatValues(0.0)
	ch1.Column(1).AppendNilsV2(true)
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{lastChunk: false}

	itr.Next(ie, p)
	if p.err == nil || !errno.Equal(p.err, errno.OnlySupportSingleField) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}

func Test_WideIterator_ColumnTypeChange(t *testing.T) {
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{lastChunk: false}

	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndex(seriesIdx...)
	ch1.Column(0).AppendFloatValues(0.0)
	ch1.Column(0).AppendNilsV2(true)
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr.Next(ie, p)

	row2 := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	cb2 := NewChunkBuilder(row2)
	ch2 := cb2.NewChunk("castor")
	ch2.AppendTime(0)
	ch2.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=2")}, seriesIdx)
	ch2.AppendIntervalIndex(seriesIdx...)
	ch2.Column(0).AppendIntegerValues(0)
	ch2.Column(0).AppendNilsV2(true)
	ch2.CheckChunk()
	ie.InputPoint.Chunk = ch2
	itr.Next(ie, p)

	if p.err == nil || !errno.Equal(p.err, errno.DtypeNotMatch) {
		t.Fatal("input chunk data type change but not detected")
	}
}

func Test_WideIterator_DataSizeTooLarge(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	data := make([]int64, maxRow+1)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(data...)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndex(seriesIdx...)
	ch1.Column(0).AppendIntegerValues(data...)
	ch1.Column(0).AppendNilsV2(true)
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{lastChunk: false}

	itr.Next(ie, p)
	if p.err == nil || !errno.Equal(p.err, errno.DataTooMuch) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}
