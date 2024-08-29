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

package immutable_test

import (
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUnorderedColumnReader(t *testing.T) {
	defer beforeTest(t, 0)

	var sid uint64 = 100
	var begin int64 = 1e12
	var schema record.Schemas
	var err error
	var readTimes []int64
	var col *record.ColVal

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(1e15, defaultInterval, false)

	for i := 0; i < 5; i++ {
		mh.addRecord(sid, rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 2000))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())

	require.NoError(t, ur.ChangeSeries(sid))
	ur.InitTimes(sid, math.MaxInt64)

	schema = ur.ReadSeriesSchemas(sid, math.MaxInt64)
	require.Equal(t, 4, schema.Len())
	require.Equal(t, "booleanBoolean", schema[0].String())

	times := ur.ReadTimes(math.MaxInt64)
	require.Equal(t, 6000, len(times))
	require.Equal(t, begin, times[0])
	require.Equal(t, int64(2999000000002), times[len(times)-1])

	ur.ChangeColumn(sid, &schema[1])
	col, readTimes, err = ur.Read(sid, times[99])
	require.NoError(t, err)
	require.NotEmpty(t, col)
	require.Equal(t, 100, col.Len)
	require.Equal(t, times[99], readTimes[99])

	col, readTimes, err = ur.Read(sid, times[99])
	require.NoError(t, err)
	require.Empty(t, col)

	ur.ChangeColumn(sid, &schema[0])
	col, readTimes, err = ur.Read(sid, 0)
	require.NoError(t, err)
	require.Empty(t, col)

	ur.ChangeColumn(sid, &record.Field{
		Type: influx.Field_Type_Int,
		Name: "not_exists",
	})
	col, _, err = ur.Read(sid, times[99])
	require.NoError(t, err)
	require.Equal(t, 100, col.NilCount)
	require.NoError(t, ur.ReadRemain(sid, nil))
	require.Empty(t, ur.ReadSeriesSchemas(100, 0))

	sid++
	schema = ur.ReadSeriesSchemas(sid, math.MaxInt64)
	require.Empty(t, schema)
}

func TestUnorderedColumnReader_ReadRemain(t *testing.T) {
	defer beforeTest(t, 0)
	var begin int64 = 1e15

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 10; i++ {
		mh.addRecord(uint64(100+i), rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 100))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())

	var sids = make(map[uint64]bool)
	require.NoError(t, ur.ReadRemain(105, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		sids[sid] = true
		return nil
	}))
	require.Equal(t, 5, len(sids))

	require.NoError(t, ur.ReadRemain(200, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		sids[sid] = true
		return nil
	}))
	require.Equal(t, 10, len(sids))
}

func TestUnorderedColumnReader_error(t *testing.T) {
	defer beforeTest(t, 0)

	var sid uint64 = 100
	var begin int64 = 1e12

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	rg := newRecordGenerator(1e15, defaultInterval, true)

	for i := 0; i < 5; i++ {
		mh.addRecord(sid, rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 2000))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())

	require.NoError(t, ur.ChangeSeries(sid))
	ur.InitTimes(sid+1, math.MaxInt64)
	ur.InitTimes(sid, math.MaxInt64)

	schema := ur.ReadSeriesSchemas(sid, math.MaxInt64)
	require.NotEmpty(t, schema)
	ur.ChangeColumn(sid, &schema[0])

	ur.CloseFile()
	col, readTimes, err := ur.Read(sid, begin+1)
	require.NotEmpty(t, err)
	require.Empty(t, col)
	require.Empty(t, readTimes)
}

func TestUnorderedColumns(t *testing.T) {
	defer beforeTest(t, 0)
	var sid uint64 = 100

	cols := immutable.NewUnorderedColumns()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	rg := newRecordGenerator(1e15, defaultInterval, true)

	mh.addRecord(sid, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	f := mh.store.Order["mst"].Files()[0]
	fi := immutable.NewFileIterator(f, logger.NewLogger(errno.ModuleMerge))
	require.True(t, fi.NextChunkMeta())
	cm := fi.GetCurtChunkMeta()
	cols.Init(cm)

	cols.ChangeColumn("int")
	require.Equal(t, 0, cols.GetLineOffset(record.TimeField))
	require.Equal(t, 0, cols.GetSegOffset("foo"))
}
