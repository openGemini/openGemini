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
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/stretchr/testify/require"
)

func beforeHotTest(t *testing.T) func() {
	fn := beforeTest(t, 0)
	conf := &config.GetStoreConfig().HotMode
	conf.Enabled = true
	conf.Duration = toml.Duration(time.Hour)
	immutable.NewHotFileManager().Run()

	return func() {
		fn()
		conf.Enabled = false
		immutable.NewHotFileManager().Stop()
	}
}

func TestBuildHotFile(t *testing.T) {
	defer beforeHotTest(t)()

	var begin = time.Now().UnixNano()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)
	require.True(t, files.Len() > 0)

	f := files.Files()[0]
	require.True(t, f.InMemSize() > 0)

	itrTSSPFile(f, func(sid uint64, rec *record.Record) {
		record.CheckRecord(rec)
	})
}

func TestBuildHotFile_expired(t *testing.T) {
	defer beforeHotTest(t)()

	var begin = time.Now().Add(-12 * time.Hour).UnixNano()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)
	require.True(t, files.Len() > 0)

	f := files.Files()[0]
	require.True(t, f.InMemSize() == 0)
	itrTSSPFile(f, func(sid uint64, rec *record.Record) {
		record.CheckRecord(rec)
	})
}

func TestBuildHotFile_FreeWindow(t *testing.T) {
	defer beforeHotTest(t)()

	var begin = time.Now().UnixNano()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	mh.addRecord(100, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	mh.addRecord(101, rg.generate(getDefaultSchemas(), 10))
	require.NoError(t, mh.saveToOrder())

	files, ok := mh.store.GetTSSPFiles("mst", true)
	require.True(t, ok)
	require.True(t, files.Len() > 1)

	f1 := files.Files()[0]
	require.True(t, f1.InMemSize() > 0)

	immutable.NewHotFileManager().SetMaxMemorySize(10)

	time.Sleep(time.Second) // wait background free
	immutable.NewHotFileManager().Free()
	require.True(t, f1.InMemSize() == 0)

	immutable.NewHotFileManager().SetMaxMemorySize(100000)
	require.NoError(t, f1.LoadIntoMemory())
	require.NotEmpty(t, f1.LoadIntoMemory())
}
