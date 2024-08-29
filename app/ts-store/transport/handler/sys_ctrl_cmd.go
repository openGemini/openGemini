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

package handler

import (
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"go.uber.org/zap"
)

type SysCtrlCmd struct {
	BaseHandler

	req *netstorage.SysCtrlRequest
	w   spdy.Responser
}

func (h *SysCtrlCmd) Process() error {
	rsp := &netstorage.SysCtrlResponse{}

	logger.GetLogger().Info("SysCtrlRequestMessage", zap.String("cmd", h.req.Mod()))
	result, err := h.store.SendSysCtrlOnNode(h.req)
	if err != nil {
		rsp.SetErr(err.Error())
	}
	rsp.SetResult(result)
	return h.w.Response(rsp, true)
}
