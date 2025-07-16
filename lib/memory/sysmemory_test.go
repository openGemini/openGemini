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

package memory

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadSysMemory(t *testing.T) {
	total, available := ReadSysMemory()
	MemUsedPct()
	readSysMemInfo(nil)
	require.NotEmpty(t, total)
	require.NotEmpty(t, available)
}

func BenchmarkReadSysMemory(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		MemUsedPct()
	}
}

func BenchmarkReadSysMemory_Parallel(b *testing.B) {
	b.ReportAllocs()
	var wg sync.WaitGroup
	f := func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			MemUsedPct()
		}
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go f()
	}
	wg.Wait()
}

func TestGetMemInt64(t *testing.T) {

	type testCase struct {
		info   string
		prefix string
		left   int
		ans    int64
	}

	testCases := []testCase{
		{
			"MemTotal:       14528572 kB",
			"MemTotal:",
			0,
			14528572,
		},
		{
			"MemFree:         6194012 kB",
			"MemFree:",
			0,
			6194012,
		},
		{
			"MemAvailable:    6254489 kB",
			"MemAvailable:",
			0,
			6254489,
		},
		{
			"Buffers:           34032 kB",
			"Buffers:",
			0,
			34032,
		},
		{
			"Cached:           188576 kB",
			"Cached:",
			0,
			188576,
		},
		{
			"SwapCached:            0 kB",
			"SwapCached:",
			0,
			0,
		},
		{
			"HugePages_Total:       0",
			"HugePages_Total:",
			0,
			0,
		},
		{
			"   test:       55 kB",
			"test:",
			3,
			55,
		},
	}

	for _, tcase := range testCases {
		act := getMemInt64([]byte(tcase.info), tcase.prefix, tcase.left)
		require.Equal(t, tcase.ans, act)
	}
}
