// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package memory_test

import (
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/memory"
	"github.com/stretchr/testify/require"
)

func TestReadSysMemory(t *testing.T) {
	total, available := memory.ReadSysMemory()
	memUsed := memory.GetMemMonitor().MemUsedPct()
	t.Log(total, available, memUsed)
	require.NotEmpty(t, total)
	require.NotEmpty(t, available)
	require.NotEmpty(t, memUsed)
}

func TestSysMem(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		time.Sleep(time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			memory.GetMemMonitor().SysMem() // t.Log(memory.GetMemMonitor().SysMem())
		}()
	}
	wg.Wait()
}
