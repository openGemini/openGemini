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
	"testing"

	"github.com/openGemini/openGemini/engine/index"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestSetIndexReader(t *testing.T) {
	schema := record.Schemas{{Name: "region", Type: influx.Field_Type_String}}
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	reader, err := sparseindex.NewSetIndexReader(rpnExpr, schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	dataFile := "00000001-0001-00000001.tssp"
	assert.Equal(t, reader.ReInit(dataFile), nil)
	ok, err := reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)
	assert.Equal(t, reader.Close(), nil)
}

func TestSetIndexWriter(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	msName := "cpu"
	setWriter := index.NewIndexWriter(testCompDir, msName, "", "", indextype.Set, tokenizer.CONTENT_SPLITTER)
	err := setWriter.Open()
	if err != nil {
		t.Fatal(err)
	}

	err = setWriter.CreateAttachIndex(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = setWriter.CreateDetachIndex(nil, nil, nil, nil)

	err = setWriter.Close()
	if err != nil {
		t.Fatal(err)
	}
}
