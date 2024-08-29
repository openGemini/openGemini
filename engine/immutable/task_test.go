// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestFullCompactOneFile(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	conf := config.GetStoreConfig()
	conf.TSSPToParquetLevel = 2
	conf.Compact.CompactRecovery = true
	defer func() {
		conf.TSSPToParquetLevel = 0
		conf.Compact.CompactRecovery = false
	}()

	for i := 0; i < 5; i++ {
		rg.setBegin(begin).incrBegin(i + 5)
		mh.addRecord(100, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToOrder())

		require.NoError(t, mh.store.FullCompact(1))
		mh.store.Wait()
	}

	require.NoError(t, mh.store.FullCompact(1))
	mh.store.Wait()
	require.Equal(t, 1, len(mh.store.Order["mst"].Files()))
}

func TestMergeWithParquetTask(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 10)()
	schemas := getDefaultSchemas()

	immutable.SetMergeFlag4TsStore(1)
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, false)

	conf := config.GetStoreConfig()
	conf.TSSPToParquetLevel = 2
	conf.Merge.MergeSelfOnly = true
	conf.Merge.MinInterval = 0
	conf.Merge.MaxMergeSelfLevel = 4
	defer func() {
		conf.Merge.MinInterval = toml.Duration(300 * time.Second)
		conf.TSSPToParquetLevel = 0
		conf.Merge.MaxMergeSelfLevel = 0
		conf.Merge.MergeSelfOnly = false
	}()

	rg.setBegin(begin)
	mh.addRecord(100, rg.generate(schemas, 1))
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 100; i++ {
		rg.setBegin(begin).incrBegin(i + 5)
		mh.addRecord(100, rg.generate(schemas, 1))
		require.NoError(t, mh.saveToUnordered())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	mh.store.Wait()
	require.NoError(t, mh.mergeAndCompact(false))
	mh.store.Wait()
}

func TestReliabilityLog(t *testing.T) {
	plan := &immutable.TSSP2ParquetPlan{
		Mst: "foo",
		Schema: map[string]uint8{
			"int1": 1, "float1": 2, "string1": 3,
		},
		Files: []string{"1", "2", "3"},
	}
	lockFile := ""

	dir := t.TempDir()
	logFile, err := immutable.SaveReliabilityLog(plan, dir, lockFile, func() string {
		return "plan.log"
	})
	require.NoError(t, err)
	require.Contains(t, logFile, "plan.log")

	other := &immutable.TSSP2ParquetPlan{}
	require.NoError(t, immutable.ReadReliabilityLog(logFile, other))
	require.Equal(t, plan, other)

	ctx := immutable.NewEventContext(&immutable.MockIndexMergeSet{GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, nil)

	require.NotEmpty(t, immutable.ReadReliabilityLog(logFile+".not_exists", other))
	require.NoError(t, immutable.ProcParquetLog(dir, &lockFile, *ctx))

	fp, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	require.NoError(t, os.Truncate(logFile, 10))
	require.NotEmpty(t, immutable.ReadReliabilityLog(logFile, other))

	require.NotEmpty(t, immutable.ProcParquetLog(logFile, &lockFile, *ctx))
}

func assertDirFileNumber(t *testing.T, dir string, exp int) {
	items, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, exp, len(items))
}

func TestTSSP2ParquetEvent(t *testing.T) {
	config.GetStoreConfig().TSSPToParquetLevel = 2
	defer func() {
		config.GetStoreConfig().TSSPToParquetLevel = 0
	}()

	shardDir := filepath.Join(t.TempDir(), "shard")
	logDir := filepath.Join(shardDir, "parquet_log")
	lock := ""
	var begin int64 = 1e12
	rg := newRecordGenerator(begin, defaultInterval, false)
	rec := rg.generate(getDefaultSchemas(), 1)

	var createEvent = func() immutable.Event {
		event := &immutable.MergeSelfParquetEvent{}
		event.Init("mst", 2)
		require.True(t, event.Enable())

		event.OnWriteRecord(rec)
		return event
	}

	event := createEvent()
	ctx := immutable.NewEventContext(&immutable.MockIndexMergeSet{GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, nil)
	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 1)
	event.OnFinish(*ctx)
	assertDirFileNumber(t, logDir, 0)

	// interrupted before OnReplaceFile
	event = createEvent()
	event.OnInterrupt()
	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 0)
	event.OnFinish(*ctx)

	// interrupted before OnFinish
	event = createEvent()
	require.NoError(t, event.OnReplaceFile(shardDir, lock))
	assertDirFileNumber(t, logDir, 1)
	event.OnInterrupt()
	assertDirFileNumber(t, logDir, 0)
	event.OnFinish(*ctx)
}

func TestMergeWithParquetTask_full(t *testing.T) {
	var begin int64 = 1e12
	defer beforeTest(t, 0)()

	conf := config.GetStoreConfig()
	conf.Merge.MergeSelfOnly = true
	conf.TSSPToParquetLevel = 2
	defer func() {
		conf.TSSPToParquetLevel = 0
	}()

	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(uint64(100), rg.generate(schemas, 10))
	require.NoError(t, mh.saveToOrder())

	for i := 0; i < 103; i++ {
		schemas = getDefaultSchemas()
		mh.addRecord(uint64(100+i%2), rg.incrBegin(1).generate(schemas, 5))
		require.NoError(t, mh.saveToUnordered())
	}

	var assertFileNumber = func(exp int) {
		files, ok := mh.store.OutOfOrder["mst"]
		require.True(t, ok)
		require.Equal(t, exp, files.Len())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(19)

	require.NoError(t, mh.mergeAndCompact(false))
	assertFileNumber(12)

	require.NoError(t, mh.store.MergeOutOfOrder(1, true, false))
	mh.store.Wait()
	assertFileNumber(2)

	require.NoError(t, mh.store.MergeOutOfOrder(1, true, false))
	mh.store.Wait()
	assertFileNumber(1)
}
