// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"sync"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/syscontrol"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEngine_processReq_error_point(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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

func TestEngine_processReq_compaction(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
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
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
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
