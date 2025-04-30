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

package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = flag.Set("loggerLevel", "ERROR")
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
}

func Test_walRowsObjectsPool(t *testing.T) {
	objs := getWalRowsObjects()
	objs.rowsDataBuff = append(objs.rowsDataBuff, byte(0), byte(1))
	objs.rows = append(objs.rows, influx.Row{}, influx.Row{})
	objs.tags = append(objs.tags, influx.Tag{}, influx.Tag{})
	objs.fields = append(objs.fields, influx.Field{}, influx.Field{})
	objs.opts = append(objs.opts, influx.IndexOption{}, influx.IndexOption{})
	objs.keys = append(objs.keys, byte(0), byte(1))
	putWalRowsObjects(objs)
	reuseCtx := getWalRowsObjects()
	require.Equal(t, 8, cap(reuseCtx.rowsDataBuff))
	require.Equal(t, 2, cap(reuseCtx.rows))
	require.Equal(t, 2, cap(reuseCtx.tags))
	require.Equal(t, 2, cap(reuseCtx.fields))
	require.Equal(t, 2, cap(reuseCtx.opts))
	require.Equal(t, 8, cap(reuseCtx.keys))
}

func TestWalReplayParallel(t *testing.T) {
	testDir := t.TempDir()
	conf := TestConfig{100, 101, time.Second, false}
	if testing.Short() && conf.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	config.SetShardMemTableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, true)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	// write data twice
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	if err = sh.Close(); err != nil {
		t.Fatal(err)
	}
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, DefaultEngineOption, config.TSSTORE, nil)
	newSh.indexBuilder = sh.indexBuilder

	// reopen shard, replay wal files
	newSh.wal.replayParallel = true
	defer func() {
		newSh.wal.replayParallel = false
	}()

	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false, nil},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				info, err := newSh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}
				if info == nil {
					return
				}
				defer func() { info.Unref() }()
				cursors := info.GetCursors()

				// step5: loop all cursors to query data from shard
				// key is indexKey, value is Record
				m := genExpectRecordsMap(rows, querySchema)
				errs := make(chan error, len(cursors))
				checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)

				close(errs)
				for i := 0; i < len(cursors); i++ {
					err = <-errs
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// step6: close shard
	err = closeShard(newSh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWalReplaySerial(t *testing.T) {
	testDir := t.TempDir()
	conf := TestConfig{100, 101, time.Second, false}
	if testing.Short() && conf.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	config.SetShardMemTableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, true)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	if err = closeShard(sh); err != nil {
		t.Fatal(err)
	}

	// reopen shard, replay wal files
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false, nil},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				info, err := sh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}
				if info == nil {
					return
				}
				defer func() { info.Unref() }()
				cursors := info.GetCursors()

				// step5: loop all cursors to query data from shard
				// key is indexKey, value is Record
				m := genExpectRecordsMap(rows, querySchema)
				errs := make(chan error, len(cursors))
				checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)

				close(errs)
				for i := 0; i < len(cursors); i++ {
					err = <-errs
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemove(t *testing.T) {
	fileNotExists := filepath.Join(t.TempDir(), "tmp", "not_exists.data")

	lock := ""
	wal := &WAL{
		log:        logger.NewLogger(errno.ModuleWal),
		walEnabled: true,
		lock:       &lock,
	}
	err := wal.Remove([]string{fileNotExists})
	require.NotEmpty(t, err)
}

func Test_NewWal_restoreLogs(t *testing.T) {
	tmpDir := t.TempDir()
	wal := &WAL{
		log: logger.NewLogger(errno.ModuleUnknown),
		logWriter: LogWriters{{
			logPath: tmpDir,
		},
		},
		logReplay: LogReplays{{}},
	}
	_ = os.WriteFile(filepath.Join(tmpDir, "1.wal"), []byte{1}, 0600)
	time.Sleep(10 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(tmpDir, "2.wal"), []byte{3}, 0600)
	time.Sleep(10 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(tmpDir, "3.wal"), []byte{2}, 0600)
	time.Sleep(10 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(tmpDir, "4.wal"), []byte{4}, 0600)
	time.Sleep(100 * time.Millisecond)
	wal.restoreLogs()

	require.Equal(t, 4, wal.logWriter[0].fileSeq)
	require.Equal(t, []string{
		filepath.Join(tmpDir, "1.wal"),
		filepath.Join(tmpDir, "2.wal"),
		filepath.Join(tmpDir, "3.wal"),
		filepath.Join(tmpDir, "4.wal"),
	}, wal.logReplay[0].fileNames)
}

func Test_NewWal_restoreLogs_error(t *testing.T) {
	tmpDir := t.TempDir()
	wal := &WAL{
		log: logger.NewLogger(errno.ModuleUnknown),
		logWriter: LogWriters{{
			logPath: tmpDir,
		},
		},
		logReplay: LogReplays{{}},
	}
	require.Panics(t, func() { wal.restoreLog(&LogWriter{logPath: ""}, &LogReplay{}) })

	_ = os.WriteFile(filepath.Join(tmpDir, "error1.wal"), []byte{1}, 0600)
	time.Sleep(10 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(tmpDir, "error2.wal"), []byte{4}, 0600)
	time.Sleep(100 * time.Millisecond)
	wal.restoreLogs()

	require.Equal(t, 0, len(wal.logReplay[0].fileNames))
}

func TestWalReplay_SeriesLimited(t *testing.T) {
	testDir := t.TempDir()
	conf := TestConfig{100, 101, time.Second, false}

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	require.NoError(t, err)

	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	config.SetShardMemTableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	for i := 0; i < 10; i++ {
		names := []string{fmt.Sprintf("cpu%d", i)}
		rows, _, _ := GenDataRecord(names, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, true)
		require.NoError(t, writeData(sh, rows, false))
	}

	require.NoError(t, closeShard(sh))

	DefaultEngineOption.MaxSeriesPerDatabase = 10
	defer func() {
		DefaultEngineOption.MaxSeriesPerDatabase = 0
	}()
	// reopen shard, replay wal files
	_ = os.RemoveAll(testDir + "/db0/index/")
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	require.NoError(t, err)
	require.NoError(t, closeShard(sh))
}

func TestWalReplayWithUnKnowType(t *testing.T) {
	testDir := t.TempDir()
	conf := TestConfig{100, 101, time.Second, false}
	if testing.Short() && conf.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	config.SetShardMemTableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, true)
	// writeWal
	var buff []byte
	buff, err = influx.FastMarshalMultiRows(buff, rows)
	if err != nil {
		t.Fatal(err)
	}
	err = sh.wal.Write(buff, WriteWalUnKnownType, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err = sh.Close(); err != nil {
		t.Fatal(err)
	}
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, DefaultEngineOption, config.TSSTORE, nil)
	newSh.indexBuilder = sh.indexBuilder

	// reopen shard, replay wal files
	newSh.wal.replayParallel = true
	defer func() {
		newSh.wal.replayParallel = false
	}()

	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false, nil},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				info, err := newSh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}
				if info == nil {
					return
				}
				defer func() { info.Unref() }()
				cursors := info.GetCursors()

				// step5: loop all cursors to query data from shard
				// key is indexKey, value is Record
				m := genExpectRecordsMap(rows, querySchema)
				errs := make(chan error, len(cursors))
				checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)

				close(errs)
				for i := 0; i < len(cursors); i++ {
					err = <-errs
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// step6: close shard
	err = closeShard(newSh)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_batchReadWalFile(t *testing.T) {
	testDir := t.TempDir()
	_, walBinary := buildRows(t, []int64{0})

	wal := &WAL{
		replayBatchSize: 256 * 1024,
		log:             logger.NewLogger(errno.ModuleWal),
	}
	cxt := context.Background()
	tmpFile := filepath.Join(testDir, "my1.wal")
	fd, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer fd.Close()
	_, err = fd.Write(walBinary)
	require.NoError(t, err)

	var callback = func(pc *walRecord) error {
		return errors.New("mock callback error")
	}

	err = wal.replayWalFile(cxt, tmpFile, callback)
	require.Errorf(t, err, "mock callback error")
}

func streamWalConf(dir string) func() {
	config.GetStoreConfig().Wal.WalUsedForStream = true
	_ = os.MkdirAll(path.Join(dir, "wal"), 0700)
	_ = os.MkdirAll(path.Join(dir, StreamWalDir), 0700)

	return func() {
		NewStreamWalManager().Free(math.MaxInt64)
		config.GetStoreConfig().Wal.WalUsedForStream = false
	}
}

func TestStreamWalManager(t *testing.T) {
	lock := ""
	dir := t.TempDir()
	defer streamWalConf(dir)()

	swm := NewStreamWalManager()

	for _, maxTime := range []int64{100, 200, 120, 190, 140} {
		walFiles := newWalFiles(maxTime, &lock, dir)
		for i := 0; i < 2; i++ {
			file := path.Join(dir, fmt.Sprintf("/wal/%d.wal", 100+i))
			require.NoError(t, os.WriteFile(file, make([]byte, 1024), 0600))
			walFiles.Add(file)
		}

		require.NoError(t, RemoveWalFiles(walFiles))
	}

	exps := []int{5, 4, 1, 1, 0}
	for i, tm := range []int64{10, 101, 191, 192, 300} {
		swm.Free(tm)
		require.Equal(t, exps[i], len(swm.files))
	}
}

func TestStreamWalManager_Load(t *testing.T) {
	lock := ""
	dir := t.TempDir()
	defer streamWalConf(dir)()

	swm := NewStreamWalManager()
	swm.InitStreamHandler(func(rows influx.Rows, fileNames []string) error { return nil })
	now := time.Now().UnixNano()
	maxTime := now - now%1e9

	buf := make([]byte, 1024)
	var createFile = func(name string) {
		file := path.Join(dir, fmt.Sprintf("/%s/%s.wal", StreamWalDir, name))
		require.NoError(t, os.WriteFile(file, buf, 0600))
	}

	createFile(fmt.Sprintf("%d_%d", maxTime, now))
	createFile(fmt.Sprintf("%d-%d", maxTime, 0))
	createFile(fmt.Sprintf("%d_%d", maxTime, 0))
	createFile(fmt.Sprintf("%d_%s", maxTime, "aaabbbcccaaabbbccca"))

	require.NoError(t, swm.Load(dir, &lock))
	require.Equal(t, 1, len(swm.loadFiles))

	_, err := swm.Replay(context.Background(), 0, 0, 0)
	require.NoError(t, err)

	swm.CleanLoadFiles()
}

func TestStreamWalManager_Reply(t *testing.T) {
	dir := t.TempDir()
	defer streamWalConf(dir)()
	var other influx.Row
	swm := NewStreamWalManager()
	swm.InitStreamHandler(func(rows influx.Rows, fileNames []string) error {
		other = rows[1]
		return nil
	})
	rows, walBinary := buildRows(t, []int64{100, 10000, 3000})

	now := time.Now().UnixNano()
	maxTime := now - now%1e9

	file := path.Join(dir, fmt.Sprintf("/%s/%s.wal", StreamWalDir, fmt.Sprintf("%d_%d", maxTime, now)))
	require.NoError(t, os.WriteFile(file, walBinary, 0600))

	lock := ""
	require.NoError(t, swm.Load(dir, &lock))
	require.Equal(t, 1, len(swm.loadFiles))

	_, err := swm.Replay(context.Background(), 8000, 0, 0)
	require.NoError(t, err)

	exp := rows[1]
	require.Equal(t, exp.Timestamp, other.Timestamp)
	require.Equal(t, exp.Name, other.Name)
	require.Equal(t, exp.Tags, other.Tags)
	require.Equal(t, exp.Fields, other.Fields)
	swm.CleanLoadFiles()
}

func buildRows(t *testing.T, times []int64) (influx.Rows, []byte) {
	rows := make(influx.Rows, len(times))

	for i := range times {
		rows[i] = influx.Row{
			Timestamp: times[i],
			Name:      "mst_0000",
			Tags: influx.PointTags{
				{
					Key:   "server",
					Value: "host1",
				},
			},
			Fields: influx.Fields{
				{
					Key:      "cpu",
					NumValue: float64(i + 1),
					Type:     influx.Field_Type_Int,
				},
			},
		}
	}

	rowsBinary, err := influx.FastMarshalMultiRows(nil, rows)
	require.NoError(t, err)
	rowsBinary = snappy.Encode(nil, rowsBinary)

	walBinary := []byte{1}
	walBinary = binary.BigEndian.AppendUint32(walBinary, uint32(len(rowsBinary)))
	walBinary = append(walBinary, rowsBinary...)
	return rows, walBinary
}
