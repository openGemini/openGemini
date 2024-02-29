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

package sparseindex

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
)

var _ = RegistrySKFileReaderCreator(index.MinMaxIndex, &MinMaxReaderCreator{})

type MinMaxReaderCreator struct {
}

func (creator *MinMaxReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewMinMaxIndexReader(rpnExpr, schema, option, isCache)
}

// MinMaxIndexReader:
//  1. the data record. the fragment size is 3.
//     f1 f2
//     1  4
//     2  5
//     3  6
//  2. the index record
//     1  4  -> min value
//     3  6  -> max value

type MinMaxIndexReader struct {
	initial    bool
	isCache    bool
	indexRange []*Range
	indexCols  []*ColumnRef
	indexType  []int
	rec        *record.Record
	condition  KeyCondition
	option     hybridqp.Options
	schema     record.Schemas
	// read the data of the index according to the file and index fields.
	ReadFunc func(file interface{}, rec *record.Record, isCache bool) (*record.Record, error)
	sk       SKCondition
}

func NewMinMaxIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*MinMaxIndexReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	return &MinMaxIndexReader{schema: schema, option: option, isCache: isCache, sk: sk}, nil
}

func (r *MinMaxIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	if fragId == 0 {
		for i := 0; i < r.rec.ColNums(); i++ {
			r.indexRange = append(r.indexRange, NewRange(NewFieldRef(r.indexCols, i, 0), NewFieldRef(r.indexCols, i, 1), true, true))
		}
	} else {
		for i := 0; i < r.rec.ColNums(); i++ {
			r.indexRange[i].left.row = int(fragId)
			if r.indexRange[i].left.IsNull() {
				r.indexRange[i].left = NEGATIVE_INFINITY
			}
			r.indexRange[i].right.row = int(fragId) + 1
			if r.indexRange[i].right.IsNull() {
				r.indexRange[i].right = NEGATIVE_INFINITY
			}
		}
	}
	mark, err := r.condition.CheckInRange(r.indexRange, r.indexType)
	if err != nil {
		return false, err
	}
	return mark.canBeTrue, nil
}

func (r *MinMaxIndexReader) init(file interface{}) (err error) {
	r.rec = record.NewRecord(r.schema, false)
	r.rec, err = r.ReadFunc(file, r.rec, r.isCache)
	if err != nil {
		return
	}
	for i := range r.rec.Schema {
		r.indexType = append(r.indexType, r.rec.Schema[i].Type)
	}

	r.condition, err = NewKeyCondition(nil, r.option.GetCondition(), r.rec.Schema)
	if err != nil {
		return
	}

	for i := 0; i < r.rec.ColNums(); i++ {
		r.indexCols = append(r.indexCols, NewColumnRef(r.rec.Schemas()[i].Name, r.rec.Schemas()[i].Type, r.rec.Column(i)))
	}
	r.initial = true
	return
}

func (r *MinMaxIndexReader) ReInit(file interface{}) (err error) {
	if !r.initial {
		return r.init(file)
	}
	r.indexRange = r.indexRange[:0]
	r.indexCols = r.indexCols[:0]
	r.rec.ResetForReuse()
	r.rec, err = r.ReadFunc(file, r.rec, r.isCache)
	if err != nil {
		return err
	}

	for i := 0; i < r.rec.ColNums(); i++ {
		r.indexCols = append(r.indexCols, NewColumnRef(r.rec.Schemas()[i].Name, r.rec.Schemas()[i].Type, r.rec.Column(i)))
	}
	return
}

func (r *MinMaxIndexReader) Close() error {
	return nil
}

type MinMaxWriter struct {
	*skipIndexWriter
}

func NewMinMaxWriter(dir, msName, dataFilePath, lockPath string) *MinMaxWriter {
	return &MinMaxWriter{
		newSkipIndexWriter(dir, msName, dataFilePath, lockPath),
	}
}

func (m *MinMaxWriter) Open() error {
	return nil
}

func (m *MinMaxWriter) Close() error {
	return nil
}

func (m *MinMaxWriter) CreateAttachSkipIndex(schemaIdx, rowsPerSegment []int, writeRec *record.Record) error {
	return nil
}

func (m *MinMaxWriter) CreateDetachSkipIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}
