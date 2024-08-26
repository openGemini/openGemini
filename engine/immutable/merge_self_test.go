/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestMergeSelf(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()
	conf := config.GetStoreConfig()
	conf.Merge.MergeSelfOnly = true

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	schema := getDefaultSchemas()
	for i := 0; i < 14; i++ {
		rg.setBegin(begin + int64(i))
		mh.addRecord(uint64(100+i%2), rg.generate(schema, 10))
		require.NoError(t, mh.saveToUnordered())
	}

	schema = append(schema, record.Field{Type: influx.Field_Type_Int, Name: "int_1"})
	rg.setBegin(begin + 100)
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToUnordered())

	schema = append(getDefaultSchemas(), record.Field{Type: influx.Field_Type_Float, Name: "float_1"})
	rg.setBegin(begin + 101)
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToUnordered())

	conf.Merge.MergeSelfOnly = false
	err := mh.store.MergeOutOfOrder(1, false, true)
	require.NoError(t, err)
	mh.store.Wait()

	require.NoError(t, mh.mergeAndCompact(true))
	require.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestMergeSelf_Stop(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()
	conf := config.GetStoreConfig()
	conf.Merge.MergeSelfOnly = true

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer func() {
		recover()
		mh.store.Close()
	}()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	schema := getDefaultSchemas()
	for i := 0; i < 8; i++ {
		rg.setBegin(begin + 1)
		mh.addRecord(100, rg.generate(schema, 10))
		require.NoError(t, mh.saveToUnordered())
	}

	var files []immutable.TSSPFile
	for _, f := range mh.store.OutOfOrder["mst"].Files() {
		files = append(files, f)
	}

	ms := immutable.NewMergeSelf(mh.store, logger.NewLogger(0))
	ms.InitEvents(&immutable.MergeContext{})

	ms.Stop()
	_, err := ms.Merge("mst", 1, files)
	require.EqualError(t, err, "compact stopped")

	ms = immutable.NewMergeSelf(mh.store, logger.NewLogger(0))
	ms.InitEvents(&immutable.MergeContext{})

	files[1], files[0] = files[0], files[1]
	_ = files[1].Close()
	_, err = ms.Merge("mst", 1, files)
	require.NoError(t, err)
}

func TestMergeOneFile(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()
	conf := config.GetStoreConfig()
	conf.Merge.MaxMergeSelfLevel = 2
	defer func() {
		conf.Merge.MaxMergeSelfLevel = 0
	}()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	schema := getDefaultSchemas()
	rg.setBegin(begin).incrBegin(-1)
	mh.addRecord(100, rg.generate(schema, 10))
	require.NoError(t, mh.saveToUnordered())

	require.NoError(t, mh.mergeAndCompact(false))
	require.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
}

func TestCompactUnordered(t *testing.T) {
	var run = func(rowCount int) {
		var begin int64 = 1e12
		defer beforeTest(t, 0)()
		conf := config.GetStoreConfig()
		conf.Compact.CorrectTimeDisorder = true
		conf.Compact.CompactionMethod = 1
		defer func() {
			conf.Compact.CompactionMethod = 0
			conf.Compact.CorrectTimeDisorder = false
		}()

		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		defer mh.store.Close()
		rg := newRecordGenerator(begin, defaultInterval, true)

		for i := 0; i < 8; i++ {
			mh.addRecord(100, rg.setBegin(begin+int64(i)).generate(getDefaultSchemas(), rowCount))
			require.NoError(t, mh.saveToOrder())
		}

		require.NoError(t, mh.mergeAndCompact(false))
		require.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
	}
	run(1)
	run(10)
}
