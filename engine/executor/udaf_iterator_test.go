// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/services/castor"
	"github.com/stretchr/testify/assert"
)

func mockCastorOp(input []Chunk, out Chunk, p ...interface{}) error { return nil }

func mockCastorOpError(input []Chunk, out Chunk, p ...interface{}) error {
	return errors.New("test error")
}

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
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.Column(1).AppendFloatValue(0.0)
	ch1.Column(1).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{}

	itr.Next(ie, p)
	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.OnlySupportSingleField) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}

func Test_WideIterator_ColumnTypeChange(t *testing.T) {
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{}

	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
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
	data := make([]int64, UDAFMaxRow+1)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTimes(data)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendIntegerValues(data)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOp)
	p := &IteratorParams{}

	itr.Next(ie, p)
	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.DataTooMuch) {
		t.Fatal("input chunk contain multiple columns but not detected")
	}
}

func Test_WideIterator_ParamIllegal(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(CastorReduce(CopyCastorADArrowRecordToChunk), []influxql.Expr{
		hybridqp.MustParseExpr("f"),
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{
		lastChunk: true,
	}
	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "castor algorithm params type assert failed,expect string", err.Error())
}

func Test_WideIterator_RecordNumTooLarge(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	for i := 0; i < 10001; i++ {
		ch1.AppendTime(0)
		ch1.AppendTagsAndIndex(*ParseChunkTags("t=" + strconv.Itoa(i)), i)
		ch1.AppendIntervalIndex(i)
		ch1.Column(0).AppendFloatValue(0.0)
		ch1.Column(0).AppendNotNil()
		ch1.CheckChunk()
	}

	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(CastorReduce(CopyCastorADArrowRecordToChunk), []influxql.Expr{
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{
		lastChunk: true,
	}
	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "too much data, maximum:10000, got:10001", err.Error())
}

func Test_WideIterator_RemoteError(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(CastorReduce(CopyCastorADArrowRecordToChunk), []influxql.Expr{
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{
		lastChunk: true,
	}
	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "exceed retry chance", err.Error())
}

func Test_WideIterator_WideReduceError(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOpError, []influxql.Expr{
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{
		lastChunk: true,
	}
	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "test error", err.Error())
}

func ErrorFn(r arrow.Record, c Chunk, fields map[string]struct{}) error {
	return errors.New("test fn error")
}

func Test_WideIterator_RecordToChunkFuncError(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(CastorReduce(ErrorFn), []influxql.Expr{
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{
		lastChunk: true,
	}
	srv, _, err := castor.MockCastorService(6665)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := castor.MockPyWorker(srv.Config.PyWorkerAddr[0]); err != nil {
		t.Fatal(err)
	}
	wait := 8 * time.Second // wait for service to build connection
	time.Sleep(wait)

	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "test fn error", err.Error())
}

func Test_WideIterator_ColumnNameFetchCorrectly(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideIterator(mockCastorOp)
	colMap := map[influxql.Expr]influxql.VarRef{
		&influxql.VarRef{Val: "id"}: {Val: "f1", Type: influxql.Float},
	}

	p := &IteratorParams{
		colMapping: colMap,
	}

	itr.Next(ie, p)

	fields := ie.InputPoint.Chunk.RowDataType().Fields()
	assert.Equal(t, fields[0].Name(), "f1")
	cacheField := itr.chunkCache[0].RowDataType().Fields()
	assert.Equal(t, cacheField[0].Name(), "id")
}

func Test_WideMultiColIterator_DataSizeTooLarge(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	data := make([]int64, UDAFMaxRow+1)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTimes(data)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendIntegerValues(data)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideMultiColIterator(mockCastorOp)
	p := &IteratorParams{}

	itr.Next(ie, p)
	err := p.GetErr()
	if err == nil || !errno.Equal(err, errno.DataTooMuch) {
		t.Fatal("unexpected error")
	}
}

func Test_WideMultiColIterator_RemoteError(t *testing.T) {
	row := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	cb := NewChunkBuilder(row)
	ch1 := cb.NewChunk("castor")
	ch1.AppendTime(0)
	seriesIdx := []int{0}
	ch1.AppendTagsAndIndexes([]ChunkTags{*ParseChunkTags("t=1")}, seriesIdx)
	ch1.AppendIntervalIndexes(seriesIdx)
	ch1.Column(0).AppendFloatValue(0.0)
	ch1.Column(0).AppendNotNil()
	ch1.CheckChunk()
	ie := &IteratorEndpoint{
		InputPoint:  EndPointPair{Chunk: ch1},
		OutputPoint: EndPointPair{Chunk: ch1},
	}
	itr := NewWideMultiColIterator(CastorReduce(CopyCastorADArrowRecordToChunk), []influxql.Expr{
		&influxql.StringLiteral{Val: "DIFFERENTIATEAD"},
		&influxql.StringLiteral{Val: "detect_base"},
		&influxql.StringLiteral{Val: "detect"},
	})
	p := &IteratorParams{}
	srv, _, err := castor.MockCastorService(6661)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	itr.Next(ie, p)

	p = &IteratorParams{
		lastChunk: true,
	}
	itr.Next(ie, p)
	err = p.GetErr()
	assert.Equal(t, "exceed retry chance", err.Error())

	p = &IteratorParams{}
	itr.Next(ie, p)
	err = p.GetErr()
	assert.Nil(t, err)
}
