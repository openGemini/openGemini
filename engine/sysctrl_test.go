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
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
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
	require.NoError(t, e.processReq(req))
}

func TestEngine_processReq_readonly(t *testing.T) {
	log = logger.NewLogger(errno.ModuleUnknown).SetZapLogger(zap.NewNop())
	e := Engine{
		log: log,
	}
	req := &netstorage.SysCtrlRequest{}
	req.SetMod(Readonly)
	req.SetParam(map[string]string{
		"switchon": "true",
		"allnodes": "y",
	})
	require.NoError(t, e.processReq(req))
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
	require.NoError(t, e.processReq(req))

	// invalid duration param
	req.SetParam(map[string]string{
		"duration": "5x",
	})
	require.Error(t, e.processReq(req))
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
	require.NoError(t, e.processReq(req))

	req.SetParam(map[string]string{
		"switchon": "true",
		"shid":     "1",
	})
	require.NoError(t, e.processReq(req))

	// without param "switchon"
	req.SetParam(map[string]string{
		"shid": "1",
	})
	require.Error(t, e.processReq(req))

	// invalid param "allshards"
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "y",
	})
	require.Error(t, e.processReq(req))
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
	require.NoError(t, e.processReq(req))

	req.SetParam(map[string]string{
		"switchon": "true",
		"shid":     "1",
	})
	require.NoError(t, e.processReq(req))

	// without param "switchon"
	req.SetParam(map[string]string{
		"shid": "1",
	})
	require.Error(t, e.processReq(req))

	// invalid param "allshards"
	req.SetParam(map[string]string{
		"switchon":  "true",
		"allshards": "y",
	})
	require.Error(t, e.processReq(req))
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
	require.NoError(t, e.processReq(req))
	req.SetParam(map[string]string{
		"order": "false",
	})
	require.NoError(t, e.processReq(req))
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
	require.NoError(t, e.processReq(req))

	req.SetParam(map[string]string{})
	require.Error(t, e.processReq(req))
}
