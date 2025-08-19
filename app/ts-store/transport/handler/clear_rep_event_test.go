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

package handler

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/msgservice"
	netdata "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/stretchr/testify/require"
)

func Test_NewClearRepColdEvent_Success(t *testing.T) {
	h := &ClearRepColdEvent{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	var req *msgservice.SendClearEventsRequest
	var nodeid uint64 = 2
	var db string = "db0"
	var pt uint32 = 1
	req = &msgservice.SendClearEventsRequest{
		ClearEventsRequest: netdata.ClearEventsRequest{
			NodeId:   &nodeid,
			Database: &db,
			PtId:     &pt,
			Pair:     nil,
		},
	}
	err := h.Handle(resp, req)
	require.NoError(t, err)
}

func Test_NewClearRepColdEvent_Err(t *testing.T) {
	h := &ClearRepColdEvent{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	var req *msgservice.SysCtrlRequest
	err := h.Handle(resp, req)
	require.Error(t, err)
}
