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

package colstore_test

import (
	"sort"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func assertFetchResult(t *testing.T, rec *record.Record, pkSchema record.Schemas, expSize int) {
	pks := &colstore.PrimaryKeyFetcher{}
	pkRec, offsets, pkSlice := pks.Fetch(rec, pkSchema)
	require.Equal(t, expSize, pkRec.RowNums())
	require.Equal(t, expSize, len(pkSlice))

	for i := range pkSlice {
		ofs := offsets.AppendToIfExists(string(pkSlice[i]), nil)
		require.True(t, len(ofs) > 0)
	}
}

func TestPrimaryKeyFetcher(t *testing.T) {
	rec := &record.Record{}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "f1"},
		record.Field{Type: influx.Field_Type_Int, Name: "i1"},
		record.Field{Type: influx.Field_Type_String, Name: "s1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "t1"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "b1"},
	}
	rec.ResetWithSchema(schema)
	rec.ColVals[0].AppendFloats(1.1, 1.1, 1.2, 1.3, 1.3)
	rec.ColVals[1].AppendIntegers(4, 4, 2, 2, 3)
	rec.ColVals[2].AppendStrings("f1", "f8", "f1", "f4", "f8")
	rec.ColVals[3].AppendStrings("t8", "t1", "t1", "t8", "t4")
	rec.ColVals[4].AppendBooleans(true, false, false, true, false)

	assertFetchResult(t, rec, rec.Schema, 5)

	for i := range 4 {
		assertFetchResult(t, rec, record.Schemas{schema[i]}, 3)
	}

	assertFetchResult(t, rec, record.Schemas{schema[4]}, 2)
}

func TestPrimaryKeyFetcher_sortNil(t *testing.T) {
	t.Run("float int", func(t *testing.T) {
		rec := &record.Record{}
		schema := record.Schemas{
			record.Field{Type: influx.Field_Type_Int, Name: "i1"},
			record.Field{Type: influx.Field_Type_Float, Name: "f1"},
		}
		rec.ResetWithSchema(schema)
		col0 := &rec.ColVals[0]
		col1 := &rec.ColVals[1]

		col0.AppendIntegers(1, 1)
		col0.AppendIntegerNulls(4)
		col0.AppendIntegers(3)

		col1.AppendFloatNulls(3)
		col1.AppendFloats(7, 8)
		col1.AppendFloatNulls(2)
		col1.AppendIntegers(3)

		assertFetchResult(t, rec, rec.Schema, 6)
	})

	t.Run("string boolean", func(t *testing.T) {
		rec := &record.Record{}
		schema := record.Schemas{
			record.Field{Type: influx.Field_Type_String, Name: "s1"},
			record.Field{Type: influx.Field_Type_Boolean, Name: "b1"},
		}
		rec.ResetWithSchema(schema)
		col0 := &rec.ColVals[0]
		col1 := &rec.ColVals[1]

		col0.AppendStrings("a", "a")
		col0.AppendStringNulls(4)
		col0.AppendStrings("c", "d")

		col1.AppendBooleanNulls(3)
		col1.AppendBooleans(true, false, true)
		col1.AppendBooleanNulls(1)
		col1.AppendBooleans(true)

		assertFetchResult(t, rec, rec.Schema, 6)
	})
}

func TestFetchKeyAtRow(t *testing.T) {
	rec := &record.Record{}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "f1"},
		record.Field{Type: influx.Field_Type_Int, Name: "i1"},
	}
	rec.ResetWithSchema(schema)
	rec.ColVals[0].AppendFloats(1.1, 1.1, 1.2, 1.3, 1.3)
	rec.ColVals[1].AppendIntegers(4, 4, 2, 2, 3)

	var recovered = false
	func() {
		defer func() {
			if e := recover(); e != nil {
				recovered = true
			}
		}()
		colstore.FetchKeyAtRow(nil, []*record.ColVal{&rec.ColVals[0]}, schema, 1)
	}()
	require.True(t, recovered)

	buf := colstore.FetchKeyAtRow(nil, []*record.ColVal{nil}, record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "not_exists"},
	}, 1)
	require.True(t, len(buf) == 2)
	require.True(t, buf[1] == 0)
}

func TestOffsetKeySorter(t *testing.T) {
	oks := &colstore.OffsetKeySorter{}
	oks.Offsets = append(oks.Offsets, 1, 3, 2)
	oks.Times = append(oks.Times, 1, 3, 2)

	sort.Sort(oks)
	require.Equal(t, []int64{1, 2, 3}, oks.Offsets)
	require.Equal(t, []int64{1, 2, 3}, oks.Times)

	oks.Keys = append(oks.Keys, []byte{influx.Field_Type_String, 1, 0, 0, 0, 1, 'c'})
	oks.Keys = append(oks.Keys, []byte{influx.Field_Type_String, 1, 0, 0, 0, 1, 'b'})
	oks.Keys = append(oks.Keys, []byte{influx.Field_Type_String, 1, 0, 0, 0, 1, 'a'})

	sort.Sort(oks)
	require.Equal(t, []int64{3, 2, 1}, oks.Offsets)
}

func TestColValBytesAtRow(t *testing.T) {
	_, ok := colstore.ColValBytesAtRow(&record.ColVal{}, 1, 100)
	require.True(t, ok)
}

func BenchmarkPrimaryKeyFetcher(b *testing.B) {
	pks := &colstore.PrimaryKeyFetcher{}
	rec := &record.Record{}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "f1"},
		record.Field{Type: influx.Field_Type_Int, Name: "i1"},
		record.Field{Type: influx.Field_Type_String, Name: "s1"},
		record.Field{Type: influx.Field_Type_Tag, Name: "t1"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "b1"},
		record.Field{Type: influx.Field_Type_Int, Name: "i2"},
	}
	rec.ResetWithSchema(schema)
	for i := range 8000 {
		rec.ColVals[0].AppendFloats(1.1)
		rec.ColVals[1].AppendIntegers(1)
		rec.ColVals[2].AppendStrings("f1")
		rec.ColVals[3].AppendStrings("t8")
		rec.ColVals[4].AppendBooleans(true)
		rec.ColVals[5].AppendIntegers(int64(i))
	}

	b.Run("v1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pks.Fetch(rec, rec.Schema)
		}
	})
}
