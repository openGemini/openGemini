// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable_test

import (
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/tsreader"
	"github.com/openGemini/openGemini/lib/errno"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
	"github.com/stretchr/testify/require"
)

func TestChunkIterators_Next(t *testing.T) {
	var begin int64 = 1e12
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	recSid := uint64(100)
	recRows := 10
	defaultSchema := getDefaultSchemas()
	// test he case where the dictionary order of the col name is greater than time col
	defaultSchema = append(defaultSchema, record.Field{Type: influx.Field_Type_String, Name: "yCol"})
	mh.addRecord(recSid, rg.generate(defaultSchema, recRows))
	require.NoError(t, mh.saveToOrder())
	mh.addRecord(recSid, rg.generate(defaultSchema, recRows))
	require.NoError(t, mh.saveToUnordered())

	type fields struct {
		files    []immutable.TSSPFile
		dropping int64
		signal   chan struct{}
		lg       *Log.Logger
		schema   record.Schemas
	}
	defaultFileds := fields{
		files:    append(mh.store.Order["mst"].Files(), mh.store.OutOfOrder["mst"].Files()...),
		dropping: 0,
		signal:   make(chan struct{}),
		lg:       Log.NewLogger(errno.ModuleUnknown),
	}

	tests := []struct {
		name   string
		fields func(f fields) fields
	}{
		{
			name: "case1: iterating without specified schema",
			fields: func(f fields) fields {
				return f
			},
		},
		{
			name: "case2: iterating with a specified but full schema",
			fields: func(f fields) fields {
				schemas := getDefaultSchemas()
				schemas = append(schemas, record.Field{Type: influx.Field_Type_String, Name: "yCol"})
				sort.Sort(schemas)
				schemas = append(schemas, record.Field{
					Type: influx.Field_Type_Int,
					Name: record.TimeField,
				})
				f.schema = schemas
				return f
			},
		},
		{
			name: "case3: iterating with a specified schema that exists",
			fields: func(f fields) fields {
				schemas := record.Schemas{
					record.Field{Type: influx.Field_Type_Float, Name: "float"},
					record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
				}
				f.schema = schemas
				return f
			},
		},
		{
			name: "case4: iterating with specified schemas",
			fields: func(f fields) fields {
				schemas := record.Schemas{
					record.Field{Type: influx.Field_Type_Float, Name: "a_does_not_exist"},
					record.Field{Type: influx.Field_Type_Float, Name: "float"},
					record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
				}
				f.schema = schemas
				return f
			},
		},
		{
			name: "case5: iterating with schema that does not exist and is smaller than all columns",
			fields: func(f fields) fields {
				schemas := record.Schemas{
					record.Field{Type: influx.Field_Type_Float, Name: "a_does_not_exist"},
					record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
				}
				f.schema = schemas
				return f
			},
		},
		{
			name: "case6: iterating with schema that does not exist and is greater than all columns",
			fields: func(f fields) fields {
				schemas := record.Schemas{
					record.Field{Type: influx.Field_Type_Float, Name: "z_does_not_exist"},
					record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
				}
				f.schema = schemas
				return f
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.fields(defaultFileds)

			chunkIterators := immutable.NewChunkIterators(f.files, f.dropping, f.signal, f.lg, f.schema)
			sid, rec, err := chunkIterators.Next()
			for sid != 0 || rec != nil {
				require.NoError(t, err)

				if f.schema != nil {
					require.True(t, rec.Schema.Equal(f.schema)) // data retrieval according to the specified schema
				} else {
					schema := getDefaultSchemas()
					schema = append(schema, record.Field{Type: influx.Field_Type_String, Name: "yCol"})
					sort.Sort(schema)
					schema = append(schema, record.Field{
						Type: influx.Field_Type_Int,
						Name: record.TimeField,
					})
					require.True(t, rec.Schema.Equal(schema)) // data retrieval according to the default schema
				}

				require.Equal(t, recRows*2, rec.ColVals[len(rec.ColVals)-1].Len)
				require.Equal(t, recSid, sid)
				sid, rec, err = chunkIterators.Next()
			}
			chunkIterators.Close()
		})
	}
}

func TestSeriesIdIterator_Next(t *testing.T) {
	var begin int64 = 1e12
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	recSid1 := uint64(100)
	recSid2 := uint64(200)
	recRows := 10
	mh.addRecord(recSid1, rg.generate(getDefaultSchemas(), recRows))
	mh.addRecord(recSid2, rg.generate(getDefaultSchemas(), recRows))
	require.NoError(t, mh.saveToOrder())
	mh.addRecord(recSid1, rg.generate(getDefaultSchemas(), recRows))
	mh.addRecord(recSid2, rg.generate(getDefaultSchemas(), recRows))
	require.NoError(t, mh.saveToUnordered())

	type fields struct {
		orderFiles      []immutable.TSSPFile
		outOfOrderFiles []immutable.TSSPFile
		sids            []uint64
		startTime       int64
		endTime         int64
		refs            record.Schemas
	}

	defaultFields := fields{
		orderFiles:      mh.store.Order["mst"].Files(),
		outOfOrderFiles: mh.store.OutOfOrder["mst"].Files(),
		sids:            []uint64{recSid1},
		startTime:       0,
		endTime:         time.Now().Add(time.Hour).UnixNano(),
		refs: record.Schemas{
			record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
			record.Field{Type: influx.Field_Type_Float, Name: "float"},
			record.Field{Type: influx.Field_Type_Int, Name: "int"},
			record.Field{Type: influx.Field_Type_String, Name: "string"},
			record.Field{Type: influx.Field_Type_Int, Name: "time"},
		},
	}
	tests := []struct {
		name   string
		fields func(f fields) fields
	}{
		{
			name: "case1: set a full schema to read data with the specified sid",
			fields: func(f fields) fields {
				return f
			},
		},
		{
			name: "case2: set a specific schema to read specific columns of data with the specified sid",
			fields: func(f fields) fields {
				f.refs = record.Schemas{
					record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
					record.Field{Type: influx.Field_Type_Int, Name: "time"},
				}
				return f
			},
		},
		{
			name: "case3: set the time range to 0, and no data can be read",
			fields: func(f fields) fields {
				f.endTime = 0
				return f
			},
		},
		{
			name: "case4: set the time col is missed",
			fields: func(f fields) fields {
				f.refs = record.Schemas{
					record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
				}
				return f
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.fields(defaultFields)

			tr := util.TimeRange{Min: f.startTime, Max: f.endTime}

			items := make([]index.SeriesIDElem, len(f.sids))
			for i, sid := range f.sids {
				item := index.SeriesIDElem{SeriesID: sid, Expr: nil}
				items[i] = item
			}
			seriesIDIterator := &MockSeriesIDIterator{items: items, index: 0}

			iterator := tsreader.NewSeriesIdIterator(f.orderFiles, f.outOfOrderFiles, seriesIDIterator, tr, f.refs)
			sid, rec, err := iterator.Next()
			for sid != 0 || rec != nil {
				require.NoError(t, err)
				if f.refs != nil {
					require.True(t, rec.Schema.Equal(f.refs)) // data retrieval according to the specified schema
				} else {
					defaultSchemas := getDefaultSchemas()
					defaultSchemas = append(defaultSchemas, record.Field{
						Type: influx.Field_Type_Int,
						Name: record.TimeField,
					})
					sort.Sort(defaultSchemas)
					require.True(t, rec.Schema.Equal(defaultSchemas)) // data retrieval according to the default schema
				}
				if f.endTime == 0 || !containsTimeCol(f.refs) {
					require.Equal(t, 0, rec.ColVals[len(rec.ColVals)-1].Len)
				} else {
					require.Equal(t, recRows, rec.ColVals[len(rec.ColVals)-1].Len) // data of the same time merged
				}
				require.Equal(t, f.sids[0], sid) // data retrieval according to the specified sid
				sid, rec, err = iterator.Next()
			}
			iterator.Close()
		})
	}
}

func containsTimeCol(refs record.Schemas) bool {
	return refs.FieldIndex(record.TimeField) >= 0
}

type MockSeriesIDIterator struct {
	items []index.SeriesIDElem
	index int
}

func (m *MockSeriesIDIterator) Next() (index.SeriesIDElem, error) {
	if m.index >= len(m.items) {
		return index.SeriesIDElem{}, nil
	}
	elem := m.items[m.index]
	m.index++
	return elem, nil
}

func (m *MockSeriesIDIterator) Ids() *uint64set.Set {
	return nil
}

func (m *MockSeriesIDIterator) Close() error {
	return nil
}
