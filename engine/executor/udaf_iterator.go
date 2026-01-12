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
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const UDAFMaxRow = 100000

type WideReduce func(input []Chunk, out Chunk, p ...interface{}) error

type WideIterator struct {
	isErrHappened bool
	fn            WideReduce
	rowCnt        int
	dType         influxql.DataType
	chunkCache    []Chunk
	params        []interface{}
}

func NewWideIterator(fn WideReduce, params ...interface{}) *WideIterator {
	r := &WideIterator{
		fn:            fn,
		params:        params,
		chunkCache:    []Chunk{},
		isErrHappened: false,
		dType:         influxql.Unknown,
	}
	return r
}

func (r *WideIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	if r.isErrHappened {
		p.err = nil
		return
	}

	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk
	if len(inChunk.Columns()) > 1 {
		p.err = errno.NewError(errno.OnlySupportSingleField)
		r.isErrHappened = true
		return
	}

	colDtype := inChunk.Columns()[0].DataType()
	if r.dType == influxql.Unknown {
		r.dType = colDtype
	} else if r.dType != colDtype {
		p.err = errno.NewError(errno.DtypeNotMatch, r.dType, colDtype)
		r.isErrHappened = true
		return
	}

	r.rowCnt += inChunk.NumberOfRows()
	if r.rowCnt > UDAFMaxRow {
		p.err = errno.NewError(errno.DataTooMuch, UDAFMaxRow, r.rowCnt)
		r.isErrHappened = true
		return
	}

	cloneChunk := inChunk.Clone()
	ColumnMapping(cloneChunk, p.colMapping)
	r.chunkCache = append(r.chunkCache, cloneChunk)
	if !p.lastChunk {
		return
	}
	err := r.fn(r.chunkCache, outChunk, r.params...)
	r.chunkCache = nil
	if err != nil {
		r.isErrHappened = true
		p.err = err
	}
}

func ColumnMapping(chunk Chunk, colMap map[influxql.Expr]influxql.VarRef) {
	newRowDataType := hybridqp.NewRowDataTypeImpl()
	chunk.RowDataType().CopyTo(newRowDataType)

	newColMap := make(map[string]string, len(colMap))
	for k, v := range colMap {
		if colName, ok := k.(*influxql.VarRef); ok {
			newColMap[v.Val] = colName.Val
		}
	}

	for _, field := range newRowDataType.Fields() {
		f, ok := field.Expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		if colName, ok := newColMap[f.Val]; ok {
			f.Val = colName
		}
	}
	chunk.SetRowDataType(newRowDataType)
}

type WideMultiColIterator struct {
	isErrHappened bool
	fn            WideReduce
	rowCnt        int
	dType         influxql.DataType
	chunkCache    []Chunk
	params        []interface{}
}

func NewWideMultiColIterator(fn WideReduce, params ...interface{}) *WideMultiColIterator {
	r := &WideMultiColIterator{
		fn:            fn,
		params:        params,
		chunkCache:    []Chunk{},
		isErrHappened: false,
		dType:         influxql.Unknown,
	}
	return r
}

func (r *WideMultiColIterator) Next(ie *IteratorEndpoint, p *IteratorParams) {
	if r.isErrHappened {
		p.err = nil
		return
	}

	inChunk, outChunk := ie.InputPoint.Chunk, ie.OutputPoint.Chunk

	r.rowCnt += inChunk.NumberOfRows()
	if r.rowCnt > UDAFMaxRow {
		p.err = errno.NewError(errno.DataTooMuch, UDAFMaxRow, r.rowCnt)
		r.isErrHappened = true
		return
	}

	cloneChunk := inChunk.Clone()
	ColumnMapping(cloneChunk, p.colMapping)
	r.chunkCache = append(r.chunkCache, cloneChunk)
	if !p.lastChunk {
		return
	}
	err := r.fn(r.chunkCache, outChunk, r.params...)
	r.chunkCache = nil
	if err != nil {
		r.isErrHappened = true
		p.err = err
	}
}
