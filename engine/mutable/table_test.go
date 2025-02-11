// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package mutable_test

import (
	"os"
	"path"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSizeLimit(t *testing.T) {
	config.SetShardMemTableSizeLimit(0)
	require.Equal(t, int64(config.GetShardMemTableMinSize()), config.GetShardMemTableSizeLimit())

	var size int64 = 1024 * 1024 * 64
	config.SetShardMemTableSizeLimit(size)
	require.Equal(t, size, config.GetShardMemTableSizeLimit())
}

func TestSidsPool(t *testing.T) {
	mutable.InitMutablePool(cpu.GetCpuNum())

	sids := mutable.GetSidsImpl(4)
	mutable.PutSidsImpl(sids)

	sids = mutable.GetSidsImpl(5)
	mutable.PutSidsImpl(sids)

	sids = mutable.GetSidsImpl(1)
	mutable.PutSidsImpl(sids)
}

func TestStoreLoadMstRowCount(t *testing.T) {
	dir := t.TempDir()
	err := os.MkdirAll(path.Join(dir, immutable.ColumnStoreDirName, "mst_0001"), 0755)
	if err != nil {
		t.Fatal(err)
	}
	countFile := path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", immutable.CountBinFile)
	rowCount := 8192
	err = mutable.StoreMstRowCount(countFile, rowCount)
	if err != nil {
		t.Fatal(err)
	}
	count, err := mutable.LoadMstRowCount(countFile)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, count, 8192)

	countFile = path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", "count.bin")
	_, err = mutable.LoadMstRowCount(countFile)
	assert.Equal(t, err != nil, true)

	countFile = path.Join(dir, immutable.ColumnStoreDirName, "mst_0001", immutable.CountBinFile)
	file, _ := os.OpenFile(countFile, os.O_WRONLY|os.O_TRUNC, 0640)
	_, _ = file.WriteString("")
	_, err = mutable.LoadMstRowCount(countFile)
	assert.Equal(t, err != nil, true)
}

func TestMemTables_Values_Empty(t *testing.T) {
	tb := mutable.NewMemTable(config.TSSTORE)
	row := &influx.Row{}
	row.Fields = append(row.Fields, influx.Field{
		Key:      "foo",
		NumValue: 1,
		StrValue: "",
		Type:     influx.Field_Type_Int,
	})
	msInfo := tb.CreateMsInfo("mst", row, nil)
	msInfo.CreateChunk(100)

	var run = func(readEnable bool) {
		tbs := mutable.NewMemTables(1, true)
		tbs.Init(tb, nil)
		rec := tbs.Values("mst", 100, util.TimeRange{}, nil, true)
		require.Empty(t, rec)
	}

	run(true)
	run(false)
}

func TestSplitRecordByTime(t *testing.T) {
	var order, unOrder *record.Record
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "a1"},
		record.Field{Type: influx.Field_Type_Float, Name: "a2"},
		record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
	}
	rec := &record.Record{}
	rec.ResetWithSchema(schema)
	rec.Column(0).AppendIntegers(1, 2, 3, 4, 5)
	rec.Column(1).AppendFloats(1.1, 2.1, 3.1, 4.1, 5.1)
	rec.Column(2).AppendIntegers(1, 2, 3, 4, 5)

	order, unOrder = mutable.SplitRecordByTime(rec, nil, 3)
	record.CheckRecord(order)
	record.CheckRecord(unOrder)
	require.Equal(t, 2, order.RowNums())
	require.Equal(t, 3, unOrder.RowNums())
	require.Equal(t, []int64{4, 5}, order.Column(0).IntegerValues())
	require.Equal(t, []int64{1, 2, 3}, unOrder.Column(0).IntegerValues())

	rec.ResetWithSchema(schema)
	rec.Column(0).AppendIntegerNulls(3)
	rec.Column(0).AppendIntegers(4, 5)
	rec.Column(1).AppendFloats(1.1, 2.1, 3.1)
	rec.Column(1).AppendFloatNulls(2)
	rec.Column(2).AppendIntegers(1, 2, 3, 7, 8)
	order, unOrder = mutable.SplitRecordByTime(rec, nil, 4)

	record.CheckRecord(order)
	record.CheckRecord(unOrder)
	require.Equal(t, 2, order.RowNums())
	require.Equal(t, 3, unOrder.RowNums())
	require.Equal(t, 2, order.Len())
	require.Equal(t, 2, unOrder.Len())
}

func TestSplitRecordByTime_EmptyStringColumn(t *testing.T) {
	var order, unOrder *record.Record
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "a1"},
		record.Field{Type: influx.Field_Type_Int, Name: record.TimeField},
	}
	rec := &record.Record{}
	rec.ResetWithSchema(schema)
	rec.Column(0).AppendStrings("", "", "", "", "")
	rec.Column(1).AppendIntegers(1, 2, 3, 4, 5)

	order, unOrder = mutable.SplitRecordByTime(rec, nil, 3)
	record.CheckRecord(order)
	record.CheckRecord(unOrder)
	require.Equal(t, 2, order.RowNums())
	require.Equal(t, 3, unOrder.RowNums())
	require.Equal(t, 2, order.Len())
	require.Equal(t, 2, unOrder.Len())
}
