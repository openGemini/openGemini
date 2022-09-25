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

	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEngine_processReq_failpoint(t *testing.T) {
	log = zap.NewNop()
	e := Engine{
		log: zap.NewNop(),
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
	log = zap.NewNop()
	e := Engine{
		log: zap.NewNop(),
	}
	req := &netstorage.SysCtrlRequest{}
	req.SetMod(Readonly)
	req.SetParam(map[string]string{
		"switchon": "true",
		"allnodes": "y",
	})
	require.NoError(t, e.processReq(req))
}
