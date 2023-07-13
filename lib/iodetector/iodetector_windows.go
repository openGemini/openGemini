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

package iodetector

import (
	"fmt"
	"os"
	"time"
)

// detectIO checks the disk flushing time every second.
// If the time is not updated within 30 seconds, the process kills itself.
func (d *IODetector) detectIO() {
	ticker := time.NewTicker(1 * time.Second)
	beforeTime := time.Now()
	for {
		select {
		case <-d.detectCloseCh:
			return
		case beforeTime = <-d.detectCh:
		case <-ticker.C:
			if time.Since(beforeTime) > time.Duration(d.timeoutThreshold)*time.Second {
				// process suicide
				pid := os.Getpid()
				p, err := os.FindProcess(pid)
				if err != nil {
					fmt.Printf("FATAL: cannot find current process: %v", err)
				}
				if err := p.Kill(); err != nil {
					fmt.Printf("error process syscall.Kill:%v\n", err)
				}
				return
			}
		}
	}
}
