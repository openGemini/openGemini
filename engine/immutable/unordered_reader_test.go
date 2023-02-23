/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable_test

import (
	"math"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestUnorderedColumnReader(t *testing.T) {
	defer beforeTest(t, 0)

	var sid uint64 = 100
	var begin int64 = 1e12

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(1e15, defaultInterval, true)

	for i := 0; i < 5; i++ {
		mh.addRecord(sid, rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 2000))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())

	require.NoError(t, ur.InitSeriesTimes(sid+1))
	require.NoError(t, ur.InitSeriesTimes(sid))

	schema := ur.ReadSeriesSchemas(100+1, math.MaxInt64)
	require.Empty(t, schema)

	schema = ur.ReadSeriesSchemas(100, math.MaxInt64)
	require.Equal(t, 4, schema.Len())
	require.Equal(t, "booleanBoolean", schema[0].String())

	times := ur.ReadTimes(0, math.MaxInt64)
	require.Equal(t, 6000, len(times))
	require.Equal(t, begin, times[0])
	require.Equal(t, int64(2999000000002), times[len(times)-1])

	col, readTimes, err := ur.Read(sid+1, &schema[0], begin+1)
	require.NoError(t, err)
	require.Empty(t, col)
	require.Empty(t, readTimes)

	col, readTimes, err = ur.Read(sid, &schema[0], times[99])
	require.NoError(t, err)
	require.NotEmpty(t, col)
	require.Equal(t, 100, col.Len)

	col, readTimes, err = ur.Read(sid, &schema[0], times[99])
	require.NoError(t, err)
	require.Empty(t, col)

	col, readTimes, err = ur.Read(sid, &schema[0], 0)
	require.NoError(t, err)
	require.Empty(t, col)

	col, _, err = ur.Read(sid, &record.Field{
		Type: influx.Field_Type_Int,
		Name: "not_exists",
	}, times[99])
	require.NoError(t, err)
	require.Empty(t, col)

	err = ur.ReadRemain(sid+1, func(sid uint64, ref record.Field, col *record.ColVal, times []int64) error {
		if ref.Type == influx.Field_Type_Boolean || ref.Name == record.TimeField {
			require.Equal(t, 5900, len(times))
		} else {
			require.Equal(t, 6000, len(times))
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, ur.ReadRemain(sid, nil))

	require.Empty(t, ur.ReadSeriesSchemas(100, 0))
}

func TestUnorderedColumnReader_ReadRemain(t *testing.T) {
	defer beforeTest(t, 0)
	var begin int64 = 1e15

	mh := NewMergeTestHelper(immutable.NewConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 10; i++ {
		mh.addRecord(uint64(100+i), rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 100))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())
	for i := 0; i < 10; i++ {
		require.NoError(t, ur.InitSeriesTimes(uint64(100+i)))
	}

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

	mh := NewMergeTestHelper(immutable.NewConfig())
	rg := newRecordGenerator(1e15, defaultInterval, true)

	for i := 0; i < 5; i++ {
		mh.addRecord(sid, rg.setBegin(begin+int64(i%3)).generate(getDefaultSchemas(), 2000))
		require.NoError(t, mh.saveToUnordered())
	}

	ur := immutable.NewUnorderedReader(logger.NewLogger(errno.ModuleMerge))
	ur.AddFiles(mh.store.OutOfOrder["mst"].Files())

	require.NoError(t, ur.InitSeriesTimes(sid+1))
	require.NoError(t, ur.InitSeriesTimes(sid))

	schema := ur.ReadSeriesSchemas(100, math.MaxInt64)
	require.NotEmpty(t, schema)

	ur.Close()
	col, readTimes, err := ur.Read(sid, &schema[0], begin+1)
	require.NotEmpty(t, err)
	require.Empty(t, col)
	require.Empty(t, readTimes)
}
