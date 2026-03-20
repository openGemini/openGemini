// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shelf_test

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestWalIterator_AllPaths(t *testing.T) {
	f := initConfig(2)
	defer f()

	lock1 := "1"
	wal := shelf.NewWal(t.TempDir(), &lock1, nil)
	defer wal.MustClose()

	// construct block0 to wal -> mstA: 1 row, fields a, b, c
	config.GetShelfMode().WalCompressMode = 1
	rowA := buildRow(100, "mstA", 10)
	schemas0 := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	rec := buildRecordWithSchema(schemas0, 999)
	err := writeRecordToWal(wal, rec, uint64(100), rowA.IndexKey)
	require.NoError(t, err)
	wal.BackgroundSync()

	// construct block1 to wal -> mstB: 1 row, fields a, c
	config.GetShelfMode().WalCompressMode = 0
	rowB := buildRow(100, "mstB", 10)
	schemas1 := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	rec = buildRecordWithSchema(schemas1, 888)
	err = writeRecordToWal(wal, rec, uint64(100), rowB.IndexKey)
	require.NoError(t, err)
	wal.BackgroundSync()

	// construct block2 to wal -> mstA: 1 row, fields b, c, d
	config.GetShelfMode().WalCompressMode = 2
	schemas2 := record.Schemas{
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
		{Type: influx.Field_Type_Boolean, Name: "d"},
	}
	rec = buildRecordWithSchema(schemas2, 999)
	// sid is not created
	err = writeRecordToWal(wal, rec, 0, rowA.IndexKey)
	require.NoError(t, err)
	wal.BackgroundSync()

	// First Next -> columns from block0
	expectedSchema := append(schemas0, record.Field{Type: influx.Field_Type_Int, Name: "time"})
	wi := shelf.NewWalIterator(wal, "mstA", nil)
	defer wi.Release()

	nextRec, err := wi.Next()
	require.NoError(t, err)
	require.True(t, verifyRecResult(nextRec, expectedSchema, 1),
		"verifyRecResult failed, nextRec=%+v, expectedSchema=%+v, nextRecNums=%+v, expectedRecNums=%+v",
		nextRec, expectedSchema, nextRec.RowNums(), 1)

	// Second Next -> skip block1 (mstB), land on block2 -> columns from block2 and also have tags
	nextRec, err = wi.Next()
	require.NoError(t, err)
	expectedSchema = append(schemas2, record.Schemas{
		{Type: influx.Field_Type_Int, Name: "time"},
		{Type: influx.Field_Type_Tag, Name: "tag1"},
		{Type: influx.Field_Type_Tag, Name: "tag2"}}...)
	sort.Sort(record.CustomSchemas(expectedSchema))
	require.True(t, verifyRecResult(nextRec, expectedSchema, 1),
		"verifyRecResult failed, nextRec=%+v, expectedSchema=%+v, nextRecNums=%+v, expectedRecNums=%+v",
		nextRec, expectedSchema, nextRec.RowNums(), 1)

	// Third Next -> io.EOF
	nextRec, err = wi.Next()
	require.Equal(t, io.EOF, err)

	// construct block3 to wal -> mstA: 1 row, fields a, b, c after io.EOF
	schemas3 := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	rec = buildRecordWithSchema(schemas3, 999)
	err = writeRecordToWal(wal, rec, uint64(100), rowA.IndexKey)
	require.NoError(t, err)
	wal.BackgroundSync()
	// Last next -> columns from block3
	nextRec, err = wi.Next()
	require.NoError(t, err)
	expectedSchema = append(schemas3, record.Field{Type: influx.Field_Type_Int, Name: "time"})
	require.True(t, verifyRecResult(nextRec, expectedSchema, 1),
		"verifyRecResult failed, nextRec=%+v, expectedSchema=%+v, nextRecNums=%+v, expectedRecNums=%+v",
		nextRec, expectedSchema, nextRec.RowNums(), 1)

}

func TestWalIterators_SingleIterator(t *testing.T) {
	f := initConfig(2)
	defer f()

	lock1 := "1"
	wal := shelf.NewWal(t.TempDir(), &lock1, nil)
	defer wal.MustClose()

	// construct block0 to wal -> mstA: 10 rows
	rowA := buildRow(100, "mstA", 10)
	schemas := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	for range 10 {
		rec := buildRecordWithSchema(schemas, 999)
		err := writeRecordToWal(wal, rec, uint64(100), rowA.IndexKey)
		require.NoError(t, err)
	}
	wal.BackgroundSync()

	// create a consumeOptions, specifying the rows and columns of the query.
	// single next limit 6 rows
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "a"},
		record.Field{Type: influx.Field_Type_Float, Name: "c"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	tagSelected := record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
	}
	fieldSelected := map[string]struct{}{
		"a": {},
		"c": {},
	}
	its := shelf.NewWalIterators(nil, "mstA", schema, tagSelected, fieldSelected, 6, nil)
	defer its.Release()
	its.AddIterator(shelf.NewWalIterator(wal, "mstA", nil))

	res, err := its.Next()
	require.NoError(t, err)
	expectedSchema := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Float, Name: "c"},
		{Type: influx.Field_Type_Tag, Name: "tag1"},
	}
	verifyRecResult(res.Rec, expectedSchema, 6)

	res, err = its.Next()
	require.NoError(t, err)
	verifyRecResult(res.Rec, expectedSchema, 4)

	res, err = its.Next()
	require.Equal(t, io.EOF, err)
	require.Nil(t, res)

}

func TestWalIterators_MultiIterator(t *testing.T) {
	f := initConfig(2)
	defer f()

	lock1 := "1"
	wal1 := shelf.NewWal(t.TempDir(), &lock1, nil)
	lock2 := "2"
	wal2 := shelf.NewWal(t.TempDir(), &lock2, nil)
	defer func() {
		wal1.MustClose()
		wal2.MustClose()
	}()

	// construct block0 to wal1 -> mstA: 7 rows
	rowA := buildRow(100, "mstA", 10)
	schemas := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	for range 7 {
		rec := buildRecordWithSchema(schemas, 999)
		err := writeRecordToWal(wal1, rec, uint64(100), rowA.IndexKey)
		require.NoError(t, err)
	}
	wal1.BackgroundSync()

	// construct block0 to wal2 -> mstA: 3 rows
	for range 3 {
		rec := buildRecordWithSchema(schemas, 999)
		err := writeRecordToWal(wal2, rec, uint64(100), rowA.IndexKey)
		require.NoError(t, err)
	}
	wal2.BackgroundSync()

	// create a consumeOptions, specifying the rows and columns of the query.
	wals := make([]*shelf.Wal, 0, 2)
	wals = append(wals, wal1)
	wals = append(wals, wal2)
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "a"},
		record.Field{Type: influx.Field_Type_Float, Name: "c"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	tagSelected := record.Schemas{
		record.Field{Type: influx.Field_Type_Tag, Name: "tag1"},
	}
	fieldSelected := map[string]struct{}{
		"a": {},
		"c": {},
	}
	// single next limit 5 rows
	its := shelf.NewWalIterators(wals, "mstA", schema, tagSelected, fieldSelected, 5, nil)
	defer its.Release()

	expectedSchema := record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Float, Name: "c"},
		{Type: influx.Field_Type_Tag, Name: "tag1"},
	}
	res, err := its.Next()
	require.NoError(t, err)
	verifyRecResult(res.Rec, expectedSchema, 5)

	res, err = its.Next()
	require.NoError(t, err)
	verifyRecResult(res.Rec, expectedSchema, 5)

	// Marked as switched
	wal1.StateSwitching = true
	res, err = its.Next()
	require.Equal(t, io.EOF, err)

	// Marked-for-convert WALs are removed from iterators; new valid writes remain unreadable. (Shelf mode already blocks writes—test-only.)
	rowA = buildRow(100, "mstA", 10)
	schemas = record.Schemas{
		{Type: influx.Field_Type_String, Name: "a"},
		{Type: influx.Field_Type_Int, Name: "b"},
		{Type: influx.Field_Type_Float, Name: "c"},
	}
	for range 7 {
		rec := buildRecordWithSchema(schemas, 999)
		err := writeRecordToWal(wal1, rec, uint64(100), rowA.IndexKey)
		require.NoError(t, err)
	}
	wal1.BackgroundSync()
	_, err = its.Next()
	require.Equal(t, err, io.EOF)
}

func buildRecordWithSchema(schemas record.Schemas, value int) *record.Record {
	schemaNums := len(schemas)
	rec := &record.Record{}
	rec.ReserveSchemaAndColVal(len(schemas) + 1)

	for i, field := range schemas {
		schema := &rec.Schema[i]
		schema.Name = schemas[i].Name
		schema.Type = schemas[i].Type

		col := &rec.ColVals[i]
		switch field.Type {
		case influx.Field_Type_String:
			col.AppendString(fmt.Sprintf("foo_%08d", i))
		case influx.Field_Type_Int:
			col.AppendInteger(int64(value*1234 + i))
		case influx.Field_Type_Float:
			col.AppendFloat(float64(value)*77.23 + float64(i)*0.1)
		case influx.Field_Type_Boolean:
			col.AppendBoolean(i%2 == 0)
		}
	}

	timeCol := &rec.Schema[schemaNums]
	timeCol.Name = "time"
	timeCol.Type = influx.Field_Type_Int
	rec.TimeColumn().AppendInteger(int64(value * 100007))
	sort.Sort(rec)

	return rec
}

func verifyRecResult(recResult *record.Record, expectedSchema record.Schemas, expectedRowNums int) bool {
	return slices.Equal(recResult.Schema, expectedSchema) && (recResult.RowNums() == expectedRowNums)
}
