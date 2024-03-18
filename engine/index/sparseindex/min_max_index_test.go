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

package sparseindex_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

// MinMaxIndexDataRead is used to read the data of the index according to the file and index fields.
func MinMaxIndexDataRead(file interface{}, rec *record.Record, isCache bool) (*record.Record, error) {
	dataPath, ok := file.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected the input file")
	}
	pkFile := colstore.AppendSKIndexSuffix(dataPath, rec.Schema[0].Name, index.MinMaxIndex)
	_ = pkFile
	rec = record.NewRecord(record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}, false)
	rec.ColVals[0].AppendIntegers(1, 3, 2, 4)
	return rec, nil
}

func TestMinMaxIndexReader(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	dataFile := "00000001-0001-00000001.tssp"
	reader.ReadFunc = MinMaxIndexDataRead
	// first init
	assert.Equal(t, reader.ReInit(dataFile), nil)
	ok, err := reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)
	// second init
	assert.Equal(t, reader.ReInit(dataFile), nil)
	ok, err = reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, true)
	// close
	assert.Equal(t, reader.Close(), nil)
}

func TestMinMaxIndexReader_error(t *testing.T) {
	schema := record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewMinMaxIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	dataFile := "00000001-0001-00000001.tssp"
	reader.ReadFunc = func(file interface{}, rec *record.Record, isCache bool) (*record.Record, error) {
		rec = record.NewRecord(record.Schemas{{Name: "value", Type: influx.Field_Type_Int}}, false)
		rec.ColVals[0].AppendIntegers(1, 3, 2, 4)
		return rec, nil
	}
	assert.NoError(t, reader.ReInit(dataFile))

	reader.ReadFunc = func(file interface{}, rec *record.Record, isCache bool) (*record.Record, error) {
		return nil, fmt.Errorf("mock error")
	}
	assert.EqualError(t, reader.ReInit(dataFile), "mock error")
}

func TestMinMaxIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"
	minMaxWriter := sparseindex.NewSkipIndexWriter(testCompDir, msName, "", "", index.MinMaxIndex, tokenizer.CONTENT_SPLITTER)
	err := minMaxWriter.Open()
	if err != nil {
		t.Fatal(err)
	}

	err = minMaxWriter.CreateAttachSkipIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = minMaxWriter.CreateDetachSkipIndex(nil, nil, nil, nil)

	err = minMaxWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
