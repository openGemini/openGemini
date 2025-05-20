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

package shelf_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/stretchr/testify/require"
)

func TestAsyncCreateIndex(t *testing.T) {
	defer initConfig(2)()
	dir := t.TempDir()
	config.GetStoreConfig().Wal.WalSyncInterval = toml.Duration(config.DefaultWALSyncInterval)
	shelf.Open()

	shard, idx, _ := newShard(10, dir)
	shard.Run()
	shard.Run()

	rec := buildRecord(10, 1)
	idx.sidCache = 0

	err := writeRecordToShard(shard, rec, 0, nil)
	require.NoError(t, err)

	time.Sleep(time.Second)
	shard.Stop()
}

func TestAsyncCovertTSSP(t *testing.T) {
	defer initConfig(2)()
	dir := t.TempDir()
	shelf.Open()

	shard, _, store := newShard(10, dir)
	defer shard.Stop()
	shard.Run()
	shard.Run()

	rec := buildRecord(10, 1)
	err := writeRecordToShard(shard, rec, 10, nil)
	require.NoError(t, err)

	shard.AsyncConvertToTSSP()
	shard.ForceFlush()

	for range 10 {
		shard.AsyncConvertToTSSP()
	}

	for range 100 {
		if len(store.files) > 0 {
			break
		}
		time.Sleep(time.Second / 100)
	}
	require.True(t, len(store.files) > 0)
}

func TestFreeShard(t *testing.T) {
	defer initConfig(2)()
	dir := t.TempDir()
	config.GetStoreConfig().Wal.WalSyncInterval = toml.Duration(config.DefaultWALSyncInterval)
	config.GetStoreConfig().ShelfMode.MaxWalDuration = toml.Duration(time.Second / 2)

	shard, _, store := newShard(10, dir)
	defer shard.Stop()
	shard.Run()
	shard.Run()

	rec := buildRecord(10, 1)
	err := writeRecordToShard(shard, rec, 10, nil)
	require.NoError(t, err)

	for range 1000 {
		shard.Free()
		if len(store.files) > 0 {
			break
		}
		time.Sleep(time.Second / 10)
	}

	time.Sleep(time.Second)
	shard.Free()
	require.True(t, len(store.files) > 0)
}

func TestWriteRecordFailed(t *testing.T) {
	defer initConfig(2)()
	dir := t.TempDir()

	shard, _, _ := newShard(10, dir)
	defer shard.Stop()

	rec := buildRecord(10, 1)
	err := writeRecordToShard(shard, rec, 10, nil)
	require.NoError(t, err)

	tr := &util.TimeRange{Min: 0, Max: math.MaxInt64}

	wal := shard.GetWalReaders(nil, "foo", tr)
	require.True(t, len(wal) == 1)

	mockErr := errors.New("some error")
	p1 := gomonkey.ApplyMethod(wal[0], "WriteRecord", func() error {
		return mockErr
	})
	defer p1.Reset()

	err = writeRecordToShard(shard, rec, 10, nil)
	require.EqualError(t, err, mockErr.Error())

	p1.Reset()

	config.GetStoreConfig().ShelfMode.MaxWalFileSize = 1
	defer func() {
		config.GetStoreConfig().ShelfMode.MaxWalFileSize = config.MB * 128
	}()
	err = writeRecordToShard(shard, rec, 10, nil)
	require.NoError(t, err)

	wal = shard.GetWalReaders(nil, "foo", tr)
	require.Equal(t, 2, len(wal))
}
