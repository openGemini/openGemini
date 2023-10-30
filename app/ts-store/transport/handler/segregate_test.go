/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package handler

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/require"
)

func Test_SegregateProcessor_Success(t *testing.T) {
	h := &SegregateProcessor{
		store: NewMockStoreEngine(0),
		log:   logger.NewLogger(errno.ModuleHA),
	}
	resp := &MockNewResponser{}
	var req *netstorage.SegregateNodeRequest
	var nodeid uint64 = 2
	req = &netstorage.SegregateNodeRequest{
		SegregateNodeRequest: netdata.SegregateNodeRequest{
			NodeId: &nodeid,
		},
	}
	err := h.Handle(resp, req)
	require.NoError(t, err)
}
