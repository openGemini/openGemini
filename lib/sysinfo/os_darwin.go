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

package sysinfo

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	HW_MEMSIZE_MIB = "hw.memsize"
)

func TotalMemory() (uint64, error) {
	ms, err := unix.SysctlUint64(HW_MEMSIZE_MIB)
	if err != nil {
		return 0, fmt.Errorf("failed to get total memory: %w", err)
	}
	return ms, nil
}

func Suicide() {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		fmt.Printf("error process syscall.Kill:%v\n", err)
	}
}
