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
package logstore

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestIndexDataReader(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	_, span := tracing.NewTrace("root")
	opt := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	version := uint32(4)
	indexReader, err := NewIndexReader(nil, version, tmpDir, util.TimeRange{Min: int64(0), Max: int64(100)}, opt)
	indexReader.StartSpan(span)
	assert.Equal(t, nil, err)
	m, err := indexReader.Get()
	assert.Equal(t, 0, int(m[0].minSeq))
	assert.Equal(t, len(contents), int(m[0].maxSeq))
	offsets := make([]int64, 0)
	lengths := make([]int64, 0)
	for _, v := range m {
		offsets = append(offsets, v.contentBlockOffset)
		lengths = append(lengths, int64(v.contentBlockLength))
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dataR, _ := NewDataReader(nil, tmpDir, version, util.TimeRange{Min: int64(0), Max: int64(100)}, opt, offsets, lengths, schema, true, nil, -1)
	dataR.StartSpan(span)
	r, err := dataR.Next()
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, r.RowNums())
}

func TestIndexDataReaderBySpan(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	indexReader, err := NewIndexReader(nil, 4, tmpDir, util.TimeRange{Min: int64(0), Max: int64(100)}, opt)
	assert.Equal(t, nil, err)
	m, err := indexReader.Get()
	assert.Equal(t, 0, int(m[0].minSeq))
	assert.Equal(t, len(contents), int(m[0].maxSeq))
	offsets := make([]int64, 0)
	lengths := make([]int64, 0)
	for _, v := range m {
		offsets = append(offsets, v.contentBlockOffset)
		lengths = append(lengths, int64(v.contentBlockLength))
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	dataR, _ := NewDataReader(nil, tmpDir, 4, util.TimeRange{Min: int64(0), Max: int64(100)}, opt, offsets, lengths, schema, true, nil, -1)
	r, err := dataR.Next()
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, r.RowNums())
}

func TestIndexDataReaderByCache(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	for i := 0; i < 2; i++ {
		indexReader, err := NewIndexReader(nil, 4, tmpDir, util.TimeRange{Min: int64(0), Max: int64(100)}, opt)
		assert.Equal(t, nil, err)
		m, _ := indexReader.Get()
		assert.Equal(t, 0, int(m[0].minSeq))
		assert.Equal(t, len(contents), int(m[0].maxSeq))
		offsets := make([]int64, 0)
		lengths := make([]int64, 0)
		for _, v := range m {
			offsets = append(offsets, v.contentBlockOffset)
			lengths = append(lengths, int64(v.contentBlockLength))
		}
		dataR, _ := NewDataReader(nil, tmpDir, 4, util.TimeRange{Min: int64(0), Max: int64(100)}, opt, offsets, lengths, schema, true, nil, -1)
		r, err := dataR.Next()
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, r.RowNums())
		min, max := indexReader.GetMinMaxTime()
		assert.Equal(t, 0, int(min))
		assert.Equal(t, 2, int(max))
		indexReader.Close()
	}
}

func TestIndexDataReaderByCacheForUnnest(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"shenzhen,1", "shanghai,2"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "key1", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "shenzhen"},
		},
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "tag"},
		record.Field{Type: influx.Field_Type_String, Name: "content"},
		record.Field{Type: influx.Field_Type_String, Name: "key1"},
		record.Field{Type: influx.Field_Type_String, Name: "value1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	unnest := &influxql.Unnest{
		Expr: &influxql.Call{
			Name: "match_all",
			Args: []influxql.Expr{
				&influxql.VarRef{Val: "([a-z]+),([0-9]+)", Type: influxql.String},
				&influxql.VarRef{Val: "content", Type: influxql.String},
			},
		},
		Aliases: []string{"key1", "value1"},
		DstType: []influxql.DataType{influxql.String, influxql.String},
	}
	for i := 0; i < 2; i++ {
		indexReader, err := NewIndexReader(nil, 4, tmpDir, util.TimeRange{Min: int64(0), Max: int64(100)}, opt)
		assert.Equal(t, nil, err)
		m, _ := indexReader.Get()
		assert.Equal(t, 0, int(m[0].minSeq))
		assert.Equal(t, len(contents), int(m[0].maxSeq))
		offsets := make([]int64, 0)
		lengths := make([]int64, 0)
		for _, v := range m {
			offsets = append(offsets, v.contentBlockOffset)
			lengths = append(lengths, int64(v.contentBlockLength))
		}
		dataR, _ := NewDataReader(nil, tmpDir, 4, util.TimeRange{Min: int64(0), Max: int64(100)}, opt, offsets, lengths, schema, true, unnest, -1)
		r, err := dataR.Next()
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, r.RowNums())
		min, max := indexReader.GetMinMaxTime()
		assert.Equal(t, 0, int(min))
		assert.Equal(t, 2, int(max))
		indexReader.Close()
	}
}
