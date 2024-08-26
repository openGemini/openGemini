// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func mockCastorOp(input []executor.Chunk, out executor.Chunk, p ...interface{}) error { return nil }

func Test_WideIterator_MultiColumnChunk(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
		influxql.VarRef{Val: "f2", Type: influxql.Float},
	)
	cb := executor.NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.Column(1).AppendFloatValue(0.0)
	ch1.Column(1).AppendNotNil()
	ch1.CheckChunk()
	ie := &executor.IteratorEndpoint{
		InputPoint:  executor.EndPointPair{Chunk: ch1},
		OutputPoint: executor.EndPointPair{Chunk: ch1},
	}
	itr := executor.NewWideIterator(mockCastorOp)
	p := &executor.IteratorParams{}

	itr.Next(ie, p)
	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.OnlySupportSingleField) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}

func Test_WideIterator_ColumnTypeChange(t *testing.T) {
	itr := executor.NewWideIterator(mockCastorOp)
	p := &executor.IteratorParams{}

	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := executor.NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &executor.IteratorEndpoint{
		InputPoint:  executor.EndPointPair{Chunk: ch1},
		OutputPoint: executor.EndPointPair{Chunk: ch1},
	}
	itr.Next(ie, p)

	row2 := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	cb2 := executor.NewChunkBuilder(row2)
	ch2 := cb2.NewChunk("castor")
	ch2.AppendTime(0)
	ch2.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=2")}, seriesIdx)
	ch2.AppendIntervalIndexes(seriesIdx)
	ch2.Column(0).AppendIntegerValue(0)
	ch2.Column(0).AppendNotNil()
	ch2.CheckChunk()
	ie.InputPoint.Chunk = ch2
	itr.Next(ie, p)

	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.DtypeNotMatch) {
		t.Fatal("input chunk data type change but not detected")
	}
}

func Test_WideIterator_DataSizeTooLarge(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	data := make([]int64, executor.UDAFMaxRow+1)
	cb := executor.NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTimes(data)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]executor.ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendIntegerValues(data)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &executor.IteratorEndpoint{
		InputPoint:  executor.EndPointPair{Chunk: ch1},
		OutputPoint: executor.EndPointPair{Chunk: ch1},
	}
	itr := executor.NewWideIterator(mockCastorOp)
	p := &executor.IteratorParams{}

	itr.Next(ie, p)
	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.DataTooMuch) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}
