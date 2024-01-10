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
package tokenizer

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestSimpleTokenFInder(t *testing.T) {
	content := "abc,bc ,  gh, bc,bcb"
	target := "bc"

	finder := NewSimpleTokenFinder(CONTENT_SPLIT_TABLE)
	finder.InitInput([]byte(content), []byte(target))

	assert.True(t, finder.Next())
	assert.Equal(t, 4, finder.CurrentOffset())
	assert.True(t, finder.Next())
	assert.Equal(t, 14, finder.CurrentOffset())
	assert.False(t, finder.Next())

	content = "abc中de 中中 a中 中c"
	target = "中"
	finder.InitInput([]byte(content), []byte(target))

	assert.True(t, finder.Next())
	assert.Equal(t, 3, finder.CurrentOffset())
	assert.True(t, finder.Next())
	assert.Equal(t, 9, finder.CurrentOffset())
	assert.True(t, finder.Next())
	assert.Equal(t, 12, finder.CurrentOffset())
	assert.True(t, finder.Next())
	assert.Equal(t, 17, finder.CurrentOffset())
	assert.True(t, finder.Next())
	assert.Equal(t, 21, finder.CurrentOffset())
	assert.False(t, finder.Next())
}

func TestIndexOf(t *testing.T) {
	source := []byte("hi world hihi")
	target := []byte("hi")

	emptyTarget := []byte{}

	found := indexOf(source, target, 100)
	assert.Equal(t, -1, found)
	found = indexOf(source, target, -1)
	assert.Equal(t, 0, found)
	found = indexOf(source, emptyTarget, 0)
	assert.Equal(t, 0, found)
	found = indexOf(source, target, 0)
	assert.Equal(t, 0, found)
	found = indexOf(source, target, 5)
	assert.Equal(t, 9, found)
	found = indexOf(source, target, 9)
	assert.Equal(t, 9, found)
	found = indexOf(source, target, 10)
	assert.Equal(t, 11, found)
}

func TestTokenFilter(t *testing.T) {
	byte1 := []byte{49, TAGS_SPLITTER_CHAR, 50}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expr := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "tag", Type: influxql.String},
		RHS: &influxql.StringLiteral{Val: "1"},
	}
	split := make(map[string][]byte)
	split["tag"] = TAGS_SPLIT_TABLE
	tokenFilter := NewTokenFilter(schema, expr, split)
	words := [][]byte{byte1, []byte("test")}
	exist := tokenFilter.Filter(words)
	assert.Equal(t, exist, true)
	words = [][]byte{[]byte("12"), []byte("test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)

	expr = &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
		RHS: &influxql.StringLiteral{Val: "1"},
	}
	split = make(map[string][]byte)
	split["content"] = CONTENT_SPLIT_TABLE
	tokenFilter = NewTokenFilter(schema, expr, split)
	words = [][]byte{[]byte("1 2"), []byte("1,test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, true)
	words = [][]byte{[]byte("12"), []byte("test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)
	words = [][]byte{[]byte("12"), []byte("1")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, true)

	expr = &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "tag", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "1"},
		},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "1"},
		},
	}
	split = make(map[string][]byte)
	split["tag"] = TAGS_SPLIT_TABLE
	split["content"] = CONTENT_SPLIT_TABLE
	tokenFilter = NewTokenFilter(schema, expr, split)
	words = [][]byte{byte1, []byte("1,test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, true)
	words = [][]byte{[]byte("12"), []byte("1")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)
	words = [][]byte{[]byte("12"), []byte("1")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)

	expr = &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "tag", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "1"},
		},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.ANY,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "1"},
		},
	}
	split = make(map[string][]byte)
	split["tag"] = TAGS_SPLIT_TABLE
	split["content"] = CONTENT_SPLIT_TABLE
	tokenFilter = NewTokenFilter(schema, expr, split)
	words = [][]byte{byte1, []byte("1,test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)

	exprWrong := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op: influxql.AND,
			LHS: &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "tag", Type: influxql.String},
				RHS: &influxql.StringLiteral{Val: "1"},
			},
			RHS: &influxql.BinaryExpr{
				Op:  influxql.ANY,
				LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
				RHS: &influxql.StringLiteral{Val: "1"},
			},
		},
	}
	split = make(map[string][]byte)
	split["tag"] = TAGS_SPLIT_TABLE
	split["content"] = CONTENT_SPLIT_TABLE
	tokenFilter = NewTokenFilter(schema, exprWrong, split)
	words = [][]byte{byte1, []byte("1,test")}
	exist = tokenFilter.Filter(words)
	assert.Equal(t, exist, false)
}

func TestTokenFilterByEmptyString(t *testing.T) {
	byte1 := []byte{49, TAGS_SPLITTER_CHAR, 50}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	expr := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "tag", Type: influxql.String},
		RHS: &influxql.StringLiteral{Val: ""},
	}
	split := make(map[string][]byte)
	split["tag"] = TAGS_SPLIT_TABLE
	tokenFilter := NewTokenFilter(schema, expr, split)
	words := [][]byte{byte1, []byte("test")}
	exist := tokenFilter.Filter(words)
	assert.Equal(t, exist, false)
}
