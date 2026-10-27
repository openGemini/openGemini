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

package engine

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/syscontrol"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/smartystreets/goconvey/convey"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEngine_processReq_error_point(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(Failpoint)
	req.SetParam(map[string]string{
		"point":    "failpoint-name",
		"switchon": "true",
		"term":     "return",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)
}

func TestEngine_processReq_snapshot(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(snapshot)
	req.SetParam(map[string]string{
		"duration": "5s",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	// invalid duration param
	req.SetParam(map[string]string{
		"duration": "5x",
	})
	_, err = e.processReq(req)
	require.Error(t, err)
}

func TestEngine_processReq_backup(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.Backup)
	_, err := e.processReq(req)
	if err != nil {
		t.Fatal()
	}
	time.Sleep(3 * time.Second)
	_, err = e.processReq(req)
	require.NoError(t, err)
}

func TestEngine_processReq_backup_abort(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.AbortBackup)
	_, err := e.processReq(req)
	if err != nil {
		t.Fatal()
	}
}

func TestEngine_processReq_backup_status(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.BackupStatus)
	_, err := e.processReq(req)
	if err != nil {
		t.Fatal()
	}
}

func TestEngine_processReq_compaction(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(compactionEn)
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "true",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{
		"switchon": "true",
		"shid":     "1",
	})
	_, err = e.processReq(req)
	require.NoError(t, err)

	// without param "switchon"
	req.SetParam(map[string]string{
		"shid": "1",
	})
	_, err = e.processReq(req)
	require.Error(t, err)

	// invalid param "allshards"
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "y",
	})
	_, err = e.processReq(req)
	require.Error(t, err)
}

func TestEngine_processReq_merge(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(compmerge)
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "true",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{
		"switchon": "true",
		"shid":     "1",
	})
	_, err = e.processReq(req)
	require.NoError(t, err)

	// without param "switchon"
	req.SetParam(map[string]string{
		"shid": "1",
	})
	_, err = e.processReq(req)
	require.Error(t, err)

	// invalid param "allshards"
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "y",
	})
	_, err = e.processReq(req)
	require.Error(t, err)
}

func TestEngine_downSample_order(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(downSampleInOrder)
	req.SetParam(map[string]string{
		"order": "true",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)
	req.SetParam(map[string]string{
		"order": "false",
	})
	_, err = e.processReq(req)
	require.NoError(t, err)
}

func TestEngine_processReq_debugMode(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(verifyNode)
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{})
	_, err = e.processReq(req)
	require.Error(t, err)
}

func TestEngine_memUsageLimit(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(memUsageLimit)
	req.SetParam(map[string]string{
		"limit": "90",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			IsMemUsageExceeded()
			wg.Done()
		}()
	}
	wg.Wait()

	req.SetParam(map[string]string{})
	_, err = e.processReq(req)
	require.Error(t, err)
}

func TestEngine_getShardStatus(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"db0": {
				0: &DBPTInfo{
					shards: map[uint64]Shard{
						1: &shard{ident: &meta2.ShardIdentifier{ShardID: 1, Policy: "rp0"}},
						2: &shard{ident: &meta2.ShardIdentifier{ShardID: 2, Policy: "rp0"}},
					},
				},
				1: &DBPTInfo{}, // filter out
			},
			"db1": { // filter out
				2: &DBPTInfo{},
			},
		},
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(queryShardStatus)
	req.SetParam(map[string]string{
		"db":    "db0",
		"rp":    "rp0",
		"pt":    "0",
		"shard": "1",
	})
	result, err := e.processReq(req)
	require.NoError(t, err)
	require.Contains(t, result["db: db0, rp: rp0, pt: 0"], `ShardId: 1`)
	require.Contains(t, result["db: db0, rp: rp0, pt: 0"], `ReadOnly: false`)
	require.Contains(t, result["db: db0, rp: rp0, pt: 0"], `Opened: false`)
}

func TestEngine_backgroundReadLimiter(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(BackgroundReadLimiter)
	req.SetParam(map[string]string{
		"limit": "100k",
	})
	_, err := e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{
		"limit": "100m",
	})
	_, err = e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{
		"limit": "100g",
	})
	_, err = e.processReq(req)
	require.NoError(t, err)

	req.SetParam(map[string]string{
		"limit": "",
	})
	_, err = e.processReq(req)
	if err == nil {
		t.Error("error set BackgroundReadLimiter")
	}

	req.SetParam(map[string]string{
		"limit": "100",
	})
	_, err = e.processReq(req)
	if err == nil {
		t.Error("error set BackgroundReadLimiter")
	}
}

func TestNodeInterruptQuery(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.NodeInterruptQuery)
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestNodeInterruptQuery fail")
	}
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	if _, err := e.processReq(req); err == nil {
		t.Error("TestNodeInterruptQuery fail")
	}
}

func TestUpperMemUsePct(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.UpperMemUsePct)
	req.SetParam(map[string]string{
		"limit": "99",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestUpperMemUsePct fail")
	}
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	if _, err := e.processReq(req); err == nil {
		t.Error("TestUpperMemUsePct fail")
	}
}

func TestWriteStreamPointsEnable(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.WriteStreamPointsEnable)
	req.SetParam(map[string]string{
		"switch": "true",
	})
	if _, err := e.processReq(req); err == nil {
		t.Error("TestReSetWalEnabled fail")
	}
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestReSetWalEnabled fail")
	}
	req.SetParam(map[string]string{
		"switchon": "false",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestReSetWalEnabled fail")
	}
}

func TestSetShardGroupTimezone(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.ShardGroupTimeZone)

	t.Run("invalid param", func(t *testing.T) {
		req.SetParam(map[string]string{})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("invalid timezone", func(t *testing.T) {
		req.SetParam(map[string]string{"timezone": "abc"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("set success", func(t *testing.T) {
		req.SetParam(map[string]string{"timezone": "Asia/Shanghai"})
		_, err := e.processReq(req)
		require.Nil(t, err)
	})
}

func TestUpdateOBSAkSk(t *testing.T) {
	convey.Convey("test update obs ak sk", t, func() {
		log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
		e := EngineImpl{
			log: log,
		}
		req := &msgservice.SysCtrlRequest{}
		req.SetMod(syscontrol.ObsAkSk)

		tests := []struct {
			params      map[string]string
			isError     bool
			isFileOpsOK bool
		}{
			{map[string]string{"ak": "", "sk": ""},
				false,
				true,
			},
			{map[string]string{"ak": ""},
				true,
				true,
			},
			{map[string]string{"sk": ""},
				true,
				true,
			},
			{map[string]string{"sk": "", "ak": ""},
				true,
				false,
			},
		}
		for _, test := range tests {
			p := gomonkey.ApplyFunc(fileops.UpdateObsAkSk, func(_, _ string) error {
				if test.isFileOpsOK {
					return nil
				}
				return errors.New("can not update")
			})
			req.SetParam(test.params)
			_, err := e.processReq(req)
			if test.isError {
				convey.ShouldNotBeNil(err)
			} else {
				convey.ShouldBeNil(err)
			}
			p.Reset()
		}
	})
}

func TestUpdateObsHostName(t *testing.T) {
	convey.Convey("test update stream conf", t, func() {
		log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
		e := EngineImpl{
			log: log,
		}
		req := &msgservice.SysCtrlRequest{}
		req.SetMod(syscontrol.ObsHostName)

		p := gomonkey.ApplyFunc(fileops.StreamConfUpdate, func(_, _ string) error {
			return nil
		})
		convey.Convey("test update ok", func() {
			req.SetParam(map[string]string{
				"hostname": "",
			})
			_, err := e.processReq(req)
			convey.ShouldBeNil(err)
		})

		convey.Convey("test param missing", func() {
			req.SetParam(map[string]string{
				"test": "",
			})
			_, err := e.processReq(req)
			convey.ShouldNotBeNil(err)
		})

		p.Reset()
		convey.Convey("test update failed", func() {
			p2 := gomonkey.ApplyFunc(fileops.StreamConfUpdate, func(_, _ string) error {
				return errors.New("update stream conf failed")
			})
			defer p2.Reset()
			req.SetParam(map[string]string{
				"hostname": "",
			})
			_, err := e.processReq(req)
			convey.ShouldNotBeNil(err)
		})
	})
}

func TestUpdateStreamConf(t *testing.T) {
	e := EngineImpl{log: logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.UpdateStreamConf)

	t.Run("missing key", func(t *testing.T) {
		req.SetParam(map[string]string{})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("missing val", func(t *testing.T) {
		req.SetParam(map[string]string{"key": "dfs.output.streamcopy.exit-on-failure"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	req.SetParam(map[string]string{"key": "dfs.output.streamcopy.exit-on-failure", "val": "false"})

	convey.Convey("test StreamConfUpdate", t, func() {
		convey.Convey("StreamConfUpdate return err", func() {
			patches := gomonkey.ApplyFunc(fileops.StreamConfUpdate, func(_, _ string) error {
				return errors.ErrUnsupported
			})
			defer patches.Reset()

			_, err := e.processReq(req)
			require.Equal(t, err, errors.ErrUnsupported)
		})

		convey.Convey("StreamConfUpdate return nil", func() {
			patches := gomonkey.ApplyFunc(fileops.StreamConfUpdate, func(_, _ string) error {
				return nil
			})
			defer patches.Reset()

			_, err := e.processReq(req)
			require.Nil(t, err)
		})
	})
}

func TestLastRowCache(t *testing.T) {
	convey.Convey("force flush duration", t, func() {
		config.SetLastRowCacheConfig(config.NewLastRowCacheConfig())
		req := &msgservice.SysCtrlRequest{}
		req.SetMod(syscontrol.LastRowCache)
		e := EngineImpl{
			log: log,
		}

		req.SetParam(map[string]string{
			syscontrol.LastRowCacheEnable:        "1",
			syscontrol.LastRowCacheMaxCost:       "66666",
			syscontrol.LastRowCacheMetricsEnable: "0",
		})
		_, err := e.processReq(req)
		convey.So(err, convey.ShouldBeNil)

		req.SetParam(map[string]string{
			syscontrol.LastRowCacheEnable: "10max",
		})
		_, err = e.processReq(req)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestUpdateParquetTaskConf(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}

	tests := []struct {
		name   string
		params map[string]string
		result bool
	}{{"modify compressAlg ok", map[string]string{syscontrol.ParquetCompressAlg: "1"}, true},
		{"modify compressAlg failed", map[string]string{syscontrol.ParquetCompressAlg: "1"}, true},
		{"modify parquetLevel ok", map[string]string{syscontrol.ParquetLevel: "3?"}, false},
		{"modify parquetLevel failed", map[string]string{syscontrol.ParquetGroupLen: "65536"}, true},
		{"modify parquetGroupLen ok", map[string]string{syscontrol.ParquetGroupLen: "<><>"}, false},
		{"modify parquetGroupLen failed", map[string]string{syscontrol.ParquetPageSize: "512"}, true},
		{"modify parquetPageSize ok", map[string]string{syscontrol.ParquetPageSize: "???"}, false},
		{"modify parquetPageSize failed", map[string]string{syscontrol.ParquetItrSize: "256"}, true},
		{"modify parquetItrSize ok", map[string]string{syscontrol.ParquetItrSize: "91zzz"}, false},
		{"modify parquetItrSize failed", map[string]string{syscontrol.ParquetDictCompressEnable: "fa;lse"}, false},
		{"modify parquetDictCompressEnable ok", map[string]string{syscontrol.ParquetDictCompressEnable: "0"}, true},
		{"modify parquetDictCompressEnable failed", map[string]string{syscontrol.ParquetCompressAlg: "XXX"}, false},
		{"modify parquetEnableMst ok", map[string]string{syscontrol.ParquetEnableMst: "test1,test2"}, true},
	}
	for _, test := range tests {
		req.SetMod(syscontrol.ParquetTask)
		req.SetParam(test.params)
		_, err := e.processReq(req)
		assert2.Equal(t, test.result, err == nil, test.name)
	}
}

func TestSeCompactMemUsageLimit(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.CompactMemUsageLimit)

	t.Run("invalid limit", func(t *testing.T) {
		req.SetParam(map[string]string{"limit": "false"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("set success", func(t *testing.T) {
		old := compactMemUsageLimit
		defer func() { SetCompactMemUsageLimit(old) }()

		compactMemUsageLimit = 80
		req.SetParam(map[string]string{"limit": "88"})
		_, err := e.processReq(req)
		require.Nil(t, err)
		require.Equal(t, int64(88), compactMemUsageLimit)
	})
}

func TestForceFlushDuration(t *testing.T) {
	convey.Convey("force flush duration", t, func() {
		req := &msgservice.SysCtrlRequest{}
		req.SetMod(syscontrol.ForceFlushDuration)
		e := EngineImpl{
			log: log,
		}

		req.SetParam(map[string]string{
			"duration": "10m",
		})
		_, err := e.processReq(req)
		convey.So(err, convey.ShouldBeNil)

		req.SetParam(map[string]string{
			"duration": "10max",
		})
		_, err = e.processReq(req)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSetMergeConfConfig(t *testing.T) {
	convey.Convey("test set parquet task config", t, func() {
		config.GetStoreConfig().Merge.MaxNumOfFileToMergeSelf = []int{8, 8, 4}
		params := map[string]string{MaxMergeSelfLevel: "2", MaxNumOfFileToMergeSelf: "8,4,2"}
		err := setMergeSelfConf(params)
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("invalid param", func() {
			params := []struct {
				param map[string]string
			}{{map[string]string{MaxMergeSelfLevel: "100"}},
				{map[string]string{MaxNumOfFileToMergeSelf: "8"}},
				{map[string]string{MaxNumOfFileToMergeSelf: "8,8??,8"}},
			}
			for _, p := range params {
				convey.So(setMergeSelfConf(p.param), convey.ShouldNotBeNil)
			}
		})
	})
}

func TestReSetWalEnabled(t *testing.T) {
	dir := t.TempDir()
	defer fileops.RemoveAll(dir)
	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.WalEnabled)
	req.SetParam(map[string]string{
		"switch": "true",
	})
	if _, err := e.processReq(req); err == nil {
		t.Error("TestReSetWalEnabled fail")
	}
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestReSetWalEnabled fail")
	}
	req.SetParam(map[string]string{
		"switchon": "false",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestReSetWalEnabled fail")
	}
}

func TestDataCacheEnable(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := EngineImpl{
		log: log,
	}
	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.DataCacheEnable)
	req.SetParam(map[string]string{
		"enabled": "1",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestDataCacheEnable fail")
	}
	req.SetParam(map[string]string{
		"enabled": "0",
	})
	if _, err := e.processReq(req); err != nil {
		t.Error("TestDataCacheEnable fail")
	}
	req.SetParam(map[string]string{
		"enable": "0",
	})
	if _, err := e.processReq(req); err == nil {
		t.Error("TestDataCacheEnable fail")
	}
}

func TestSetTsspUnrefTimeout(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.TsspUnrefTimeout)

	t.Run("invalid duration", func(t *testing.T) {
		req.SetParam(map[string]string{"duration": "-1s"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("set success", func(t *testing.T) {
		tmpTimeout := config.GetStoreConfig().TsspUnrefTimeout
		config.GetStoreConfig().TsspUnrefTimeout = toml.Duration(time.Second)
		defer func() { config.GetStoreConfig().TsspUnrefTimeout = tmpTimeout }()
		req.SetParam(map[string]string{"duration": "0"})
		_, err := e.processReq(req)
		require.Nil(t, err)
		require.Equal(t, toml.Duration(0), config.GetStoreConfig().TsspUnrefTimeout)
	})
}

func TestSetDbptUnrefTimeout(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.DbptUnrefTimeout)

	t.Run("invalid duration", func(t *testing.T) {
		req.SetParam(map[string]string{"duration": "0s"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("set success", func(t *testing.T) {
		tmpTimeout := config.GetStoreConfig().DbptUnrefTimeout
		config.GetStoreConfig().DbptUnrefTimeout = toml.Duration(time.Second)
		defer func() { config.GetStoreConfig().DbptUnrefTimeout = tmpTimeout }()
		req.SetParam(map[string]string{"duration": "3s"})
		_, err := e.processReq(req)
		require.Nil(t, err)
		require.Equal(t, toml.Duration(time.Second*3), config.GetStoreConfig().DbptUnrefTimeout)
	})
}

func TestSetOffloadWaitQuery(t *testing.T) {
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	e := getEngineBeforeTest(t, dir)
	defer e.Close()

	req := &msgservice.SysCtrlRequest{}
	req.SetMod(syscontrol.OffloadWaitQuery)

	t.Run("invalid switchon", func(t *testing.T) {
		req.SetParam(map[string]string{"switchon": "fals"})
		_, err := e.processReq(req)
		require.NotNil(t, err)
	})

	t.Run("set success", func(t *testing.T) {
		tmpWait := config.GetStoreConfig().OffloadWait
		config.GetStoreConfig().OffloadWait = true
		defer func() { config.GetStoreConfig().OffloadWait = tmpWait }()
		req.SetParam(map[string]string{"switchon": "false"})
		_, err := e.processReq(req)
		require.Nil(t, err)
		require.False(t, config.GetStoreConfig().OffloadWait)
	})
}
