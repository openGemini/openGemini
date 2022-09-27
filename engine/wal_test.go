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
	"os"
	"testing"
	"time"
)

func TestWalReplayParallel(t *testing.T) {
	testDir := t.TempDir()
	config := TestConfig{100, 101, time.Second, false}
	if testing.Short() && config.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir)
	if err != nil {
		t.Fatal(err)
	}
	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	sh.SetMutableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, config.seriesNum, config.pointNumPerSeries, config.interval, time.Now(), false, true, true)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	// write data twice
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	if err = closeShard(sh); err != nil {
		t.Fatal(err)
	}

	// reopen shard, replay wal files
	sh.wal.replayParallel = true
	defer func() {
		sh.wal.replayParallel = false
	}()
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir)
	if err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false},
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

func TestWalReplaySerial(t *testing.T) {
	testDir := t.TempDir()
	config := TestConfig{100, 101, time.Second, false}
	if testing.Short() && config.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir)
	if err != nil {
		t.Fatal(err)
	}
	// not flush data to snapshot
	sh.SetWriteColdDuration(3 * time.Minute)
	sh.SetMutableSizeLimit(3e10)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, config.seriesNum, config.pointNumPerSeries, config.interval, time.Now(), false, true, true)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	if err = closeShard(sh); err != nil {
		t.Fatal(err)
	}

	// reopen shard, replay wal files
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir)
	if err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false},
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
