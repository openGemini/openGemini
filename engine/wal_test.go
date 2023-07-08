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
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/stretchr/testify/require"
)

func init() {
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
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
	mutable.SetSizeLimit(3e10)

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
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, DefaultEngineOption, config.TSSTORE)
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
				cursors, err := newSh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}

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
	mutable.SetSizeLimit(3e10)

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
				cursors, err := sh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}

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
