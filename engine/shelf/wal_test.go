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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestWal(t *testing.T) {
	defer initConfig(2)()

	runWalTest(t)
	conf := config.GetShelfMode()

	// lz4 compress
	conf.WalCompressMode = 1
	runWalTest(t)
	conf.WalCompressMode = 0

	// snappy compress
	conf.WalCompressMode = 2
	runWalTest(t)
	conf.WalCompressMode = 0

	// hot mode
	config.GetStoreConfig().HotMode.Enabled = true
	immutable.NewHotFileManager().Run()
	defer func() {
		config.GetStoreConfig().HotMode.Enabled = false
		immutable.NewHotFileManager().Stop()
	}()

	runWalTest(t)
}

func runWalTest(t *testing.T) {
	sid := uint64(100)
	row := buildRow(1, "foo", 10)
	row.UnmarshalIndexKeys(nil)
	rec := buildRecord(10, 88)
	times := append([]int64{}, rec.Times()...)
	lock := ""

	wal := shelf.NewWal(t.TempDir(), &lock, nil)
	wal.BackgroundSync()

	err := writeRecordToWal(wal, rec, sid, row.IndexKey)
	require.NoError(t, err)

	err = writeRecordToWal(wal, rec, 0, row.IndexKey)
	require.NoError(t, err)

	err = writeRecordToWal(wal, rec, sid, row.IndexKey)
	require.NoError(t, err)
	require.NoError(t, wal.Sync())
	wal.BackgroundSync()

	require.False(t, wal.SizeLimited())

	other := &record.Record{}
	err = wal.ReadRecord(&shelf.WalCtx{}, sid, other, false)
	require.NoError(t, err)
	record.CheckRecord(other)

	require.Equal(t, rec.Schema, other.Schema)
	require.Equal(t, times[0], other.Times()[0])
	wal.MustClose()
}

func TestLZ4CompressBlock(t *testing.T) {
	data := []byte("foo1,foo2,foo1,foo2,foo1,foo2")
	buf := make([]byte, 16)
	headerSize := 16

	block, err := shelf.LZ4CompressBlock(data, buf)
	require.NoError(t, err)

	other, err := shelf.LZ4DecompressBlock(block[headerSize:], buf)
	require.NoError(t, err)
	require.Equal(t, data, other)

	binary.BigEndian.PutUint32(block[headerSize:], 10)
	_, err = shelf.LZ4DecompressBlock(block[headerSize:], buf)
	require.NotEmpty(t, err)
}

func TestSnappyCompressBlock(t *testing.T) {
	data := []byte("foo1,foo2,foo1,foo2,foo1,foo2")
	buf := make([]byte, 16)
	headerSize := 16

	block := shelf.SnappyCompressBlock(data, buf)

	other, err := shelf.SnappyDecompressBlock(block[headerSize:], buf)
	require.NoError(t, err)
	require.Equal(t, data, other)
}

func TestLoadWalFiles(t *testing.T) {
	defer initConfig(2)()

	lock := ""
	dir := t.TempDir()
	shard, idx, store := newShard(10, dir)
	idx.sidCache = 0
	idx.sidCreate = 0
	walDir := shard.GetWalDir()

	row := buildRow(1, "foo", 10)
	row.UnmarshalIndexKeys(nil)

	wal := shelf.NewWal(walDir, &lock, nil)

	var data = make(map[uint64]*record.Record)
	for i := range uint64(100) {
		rec := buildRecord(10, int(i*77+100))

		id := i
		config.GetShelfMode().WalCompressMode = 0
		if i < 3 {
			id = 0
			config.GetShelfMode().WalCompressMode = int(i)
		}

		exp, ok := data[id]
		if ok {
			exp.Merge(rec)
		} else {
			exp = &record.Record{}
			exp.Schema = append(exp.Schema[:0], rec.Schema...)
			exp.ReserveColVal(rec.Len())
			exp.AppendRec(rec, 0, rec.RowNums())
			data[id] = exp
		}

		err := writeRecordToWal(wal, rec, id, row.IndexKey)
		require.NoError(t, err)
	}

	wal.MustClose()

	idx.sidCache = 1000
	idx.sidCreate = 1000
	shard.Load()
	shard.ConvertToTSSP()
	shard.ConvertToTSSP()
	shard.Stop()

	data[1000] = data[0]

	require.True(t, len(store.files) > 0)
	for _, f := range store.files {
		itrTSSPFile(f, func(sid uint64, rec *record.Record) {
			record.CheckRecord(rec)

			exp, ok := data[sid]
			require.True(t, ok)

			require.Equal(t, exp.Times(), rec.Times())
		})
		require.NoError(t, f.Close())
	}
}

func TestTargetContain(t *testing.T) {
	defer initConfig(2)()

	dir := t.TempDir()
	shard, _, _ := newShard(10, dir)

	wal := shard.UpdateWal(&util.TimeRange{})
	require.False(t, wal.TargetContain(&MockTSSP{}))

	rec := buildRecord(10, 1)
	err := writeRecordToWal(wal, rec, 10, nil)
	require.NoError(t, err)

	shard.ForceFlush()
	shard.ConvertToTSSP()

	require.False(t, wal.TargetContain(&MockTSSP{sequence: 10}))
	require.True(t, wal.TargetContain(&MockTSSP{sequence: 1}))
	wal.MustClose()
}

func TestLoadWalError(t *testing.T) {
	defer initConfig(2)()

	sid := uint64(100)
	row := buildRow(1, "foo", 10)
	row.UnmarshalIndexKeys(nil)
	rec := buildRecord(10, 88)
	lock := ""
	wal := shelf.NewWal(t.TempDir(), &lock, nil)
	defer wal.MustClose()

	patch := gomonkey.ApplyFunc(fileops.OpenFile, func(name string, flag int, perm os.FileMode, opt ...fileops.FSOption) (fileops.File, error) {
		return nil, fmt.Errorf("some error")
	})
	defer patch.Reset()

	err := writeRecordToWal(wal, rec, sid, row.IndexKey)
	require.Error(t, err)

	require.NoError(t, wal.Flush())

	require.Error(t, wal.LoadFromDisk())
	require.NoError(t, wal.Sync())
}

func TestMemWalReader(t *testing.T) {
	defer initConfig(2)()

	sid := uint64(100)
	row := buildRow(1, "foo", 10)
	row.UnmarshalIndexKeys(nil)
	rec := buildRecord(10, 88)
	lock := ""

	wal := shelf.NewWal(t.TempDir(), &lock, nil)
	defer wal.MustClose()

	err := writeRecordToWal(wal, rec, sid, row.IndexKey)
	require.NoError(t, err)

	require.NoError(t, wal.LoadFromDisk())
	require.NoError(t, wal.Sync())

	ctx, _ := shelf.NewWalCtx()
	_, err = wal.ReadBlock(ctx, 0)
	require.NoError(t, err)

	_, err = wal.ReadBlock(ctx, 10000)
	require.EqualError(t, err, io.EOF.Error())
}

func TestWalRecordCodec(t *testing.T) {
	rec := &record.Record{}
	rec.Schema = record.Schemas{
		{Type: 1, Name: "foo"},
		{Type: 1, Name: "foo1"},
		{Type: 1, Name: "time"},
	}
	rec.ReserveColVal(3)
	rec.ColVals[0].AppendIntegers(1)
	rec.ColVals[0].AppendIntegerNull()
	rec.ColVals[1].AppendIntegerNulls(2) // all nil
	rec.AppendTime(10, 20)

	record.CheckRecord(rec)

	decoder := shelf.NewWalRecordDecoder()
	buf := shelf.EncodeRecord(nil, rec)

	other := &record.Record{}
	err := decoder.Decode(other, slices.Clone(buf))
	require.NoError(t, err)
	require.Equal(t, rec.Schema, other.Schema)

	require.Equal(t, rec.ColVals[0].IntegerValues(), other.ColVals[0].IntegerValues())
	require.Equal(t, rec.ColVals[1], rec.ColVals[1])
	require.Equal(t, rec.Times(), other.Times())
}

func TestWalRecordCodecOneRowMode(t *testing.T) {
	rec := &record.Record{}
	rec.Schema = record.Schemas{
		{Type: 1, Name: "foo"},
		{Type: 1, Name: "foo1"},
		{Type: 1, Name: "time"},
	}
	rec.ReserveColVal(3)
	rec.ColVals[0].AppendIntegers(1)
	rec.ColVals[0].AppendIntegerNull()
	rec.ColVals[1].AppendInteger(2)
	rec.ColVals[1].AppendInteger(2)
	rec.AppendTime(10, 20)

	record.CheckRecord(rec)

	decoder := shelf.NewWalRecordDecoder()
	buf := shelf.EncodeRecordRow(nil, rec, 1)

	other := &record.Record{}
	err := decoder.Decode(other, slices.Clone(buf))
	require.NoError(t, err)
	require.Equal(t, 2, other.Schema.Len())

	require.Equal(t, int64(2), other.ColVals[0].IntegerValues()[0])
	require.Equal(t, int64(20), other.Times()[0])
}

func TestDecodeColValError(t *testing.T) {
	col := &record.ColVal{}
	var buf []byte
	decoder := shelf.NewWalRecordDecoder()
	var decode = func(exp string) {
		decoder.Reset(buf)
		err := decoder.DecodeColVal(influx.Field_Type_Int, col)
		require.EqualError(t, err, exp)
	}

	decode(errno.NewError(errno.TooSmallOrOverflow, "ColVal.Len").Error())

	buf = binary.AppendUvarint(buf, 1) // ColVal.Len = 1
	decode(errno.NewError(errno.TooSmallOrOverflow, "ColVal.NilCount").Error())

	buf = binary.AppendUvarint(buf, 0)  // ColVal.NilCount = 0
	buf = append(buf, 0, 0, 0, 2, 1, 1) // ColVal.Val = []byte{1, 1}
	decode(errno.NewError(errno.TooSmallData, "ColVal.Val", 8, 6).Error())
}

func writeRecordToShard(shard *shelf.Shard, rec *record.Record, sid uint64, seriesKey []byte) error {
	wal := shard.UpdateWal(&util.TimeRange{})
	defer wal.EndWrite()

	return writeRecordToWal(wal, rec, sid, seriesKey)
}

func writeRecordToWal(wal *shelf.Wal, rec *record.Record, sid uint64, seriesKey []byte) error {
	if len(seriesKey) == 0 {
		seriesKey = []byte{0, 0, 0, 6, 0, 0}
	}

	blob := &shelf.Blob{}
	blob.WriteRecordRow(seriesKey, rec, 0)

	itr := blob.Iterator()

	_, buf, err := itr.Next()
	if err != nil {
		return err
	}

	wal.UpdateTimeRange(blob.TimeRange())
	return wal.WriteRecord(sid, seriesKey, buf)
}

func TestSeriesKeyOffsets(t *testing.T) {
	ofs := shelf.NewSeriesKeyOffsets()
	key, _, _ := ofs.Pop()
	require.Empty(t, key)

	keys := [][]byte{
		append([]byte{1, 1, 1}, make([]byte, 1024)...),
		append([]byte{2, 2, 2}, make([]byte, 1024)...),
		append([]byte{3, 3, 3}, make([]byte, 1024)...),
		append([]byte{4, 4, 4}, make([]byte, 1024)...),
	}
	offsets := []int64{1, 2, 3, 4}

	var run = func() {
		for i, key := range keys {
			ofs.Add(key, offsets[i])
		}

		for i, exp := range keys {
			key, val, _ := ofs.Pop()
			require.Equal(t, exp, key)
			require.Equal(t, offsets[i], val)
		}
	}
	run()

	config.GetShelfMode().SeriesKeyCompressThreshold = 10
	defer func() {
		config.GetShelfMode().SeriesKeyCompressThreshold = 0
	}()
	run()
}

func TestSeriesOffsets(t *testing.T) {
	ofs := shelf.NewSeriesOffsets()

	offsets := []int64{100, 99, 101}
	for _, v := range offsets {
		ofs.Add(100, v)
	}

	slices.Sort(offsets)
	data := ofs.Get(100, nil)
	require.Equal(t, offsets, data)
}

func TestMemFile(t *testing.T) {
	defer initConfig(2)()

	lock := ""
	wal := shelf.NewWal(t.TempDir(), &lock, nil)
	require.NoError(t, wal.WriteRecord(1, []byte{0, 0, 0, 6, 0, 0}, make([]byte, 10)))
	require.Equal(t, int64(0), wal.InMemSize())
	wal.FreeMemory()
	wal.MustClose()

	mf := fileops.NewMemFile(64)
	mf.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	require.Equal(t, int64(8), mf.Size())

	mf.Write(make([]byte, 100))
	require.Equal(t, int64(8), mf.Size())

	dst := make([]byte, 16)
	_, err := mf.ReadAt(dst, 10)
	require.Error(t, err)
	require.EqualError(t, err, io.EOF.Error())
}

func TestRunObsWal(t *testing.T) {
	sid := uint64(100)
	row := buildRow(1, "foo", 10)
	row.UnmarshalIndexKeys(nil)
	rec := buildRecord(10, 88)

	lock := ""
	var opt *obs.ObsOptions
	opt = &obs.ObsOptions{
		Enabled:    true,
		BucketName: "OBS_Bucket",
		Endpoint:   "obs.cn-north-7.ulanqab",
		Ak:         "123dhged",
		Sk:         "3rhgj567",
		BasePath:   "test_1",
	}
	wal := shelf.NewWal(t.TempDir(), &lock, opt)
	wal.BackgroundSync()

	err := writeRecordToWal(wal, rec, sid, row.IndexKey)
	require.Error(t, err)
}
