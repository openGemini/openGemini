//go:build linux || darwin || freebsd
// +build linux darwin freebsd

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

package metaclient

import (
	"fmt"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func (c *Client) Suicide(err error) {
	c.logger.Error("Suicide for fault data node", zap.Error(err))
	time.Sleep(errSleep)
	if e := syscall.Kill(syscall.Getpid(), syscall.SIGKILL); e != nil {
		panic(fmt.Sprintf("FATAL: cannot send SIGKILL to itself: %v", e))
	}
}
