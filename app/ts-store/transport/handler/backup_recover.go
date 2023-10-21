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
	"errors"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/netstorage"
	"go.uber.org/zap"
)

type BackupCmd struct {
	BaseHandler

	req *netstorage.BackupRequest
	w   spdy.Responser
}

func (h *BackupCmd) Process() error {
	rsp := &netstorage.BackupResponse{}
	logger.GetLogger().Info("BackupResponse", zap.Any("backup cmd", h.req))
	err := h.store.Backup(h.req)
	if err != nil {
		logger.GetLogger().Error("failed to process the backup request", zap.Error(err))
		var stderr *errno.Error
		switch {
		case errors.As(err, &stderr):
			_ = h.w.Response(executor.NewErrorMessage(stderr.Errno(), stderr.Error()), true)
		default:
			_ = h.w.Response(executor.NewErrorMessage(0, stderr.Error()), true)
		}
		return nil
	}
	return h.w.Response(rsp, true)
}
