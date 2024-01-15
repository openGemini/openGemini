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

package meta

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

func Test_StartReportServer(t *testing.T) {
	svc := &Service{
		store: &Store{
			cacheData: &meta.Data{
				ClusterID: 1314520,
			},
		},
		closing: make(chan struct{}),
		Logger:  logger.NewLogger(errno.ModuleUnknown),
	}
	go svc.StartReportServer()
	close(svc.closing)
	svc.closing = make(chan struct{})

	svc.runReportServer()
}
