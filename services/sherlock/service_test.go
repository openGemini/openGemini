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

package sherlock_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/services/sherlock"
)

func Test_SherlockService_Start_Stop(t *testing.T) {
	conf := config.NewSherlockConfig()
	conf.DumpPath = t.TempDir()
	svc1 := sherlock.NewService(conf)
	// case 1: disable
	svc1.Open()
	svc1.Stop()

	// case 1: enable
	conf.SherlockEnable = true
	conf.CPUConfig.Enable = true
	conf.MemoryConfig.Enable = true
	conf.GoroutineConfig.Enable = true
	svc2 := sherlock.NewService(conf)
	svc2.WithLogger(logger.NewLogger(errno.ModuleUnknown))
	svc2.Open()
	svc2.Stop()
}
