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

package memory

import (
	"bytes"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/shirou/gopsutil/v3/mem"
)

const maxMemUse = 64 * 1024 * 1024 * 1024

var lastGetTime time.Time
var sysMemTotal, sysMemFree, totalMemoryMax int64
var readMemMu sync.Mutex

func init() {
	sysMemTotal, sysMemFree = ReadSysMemory()
	totalMemoryMax = sysMemTotal
	lastGetTime = time.Now()
}

func ReadSysMemory() (int64, int64) {
	if runtime.GOOS == "linux" {
		return ReadSysMemoryLinux()
	}
	info, _ := mem.VirtualMemory()
	return int64(info.Total / 1024), int64(info.Available / 1024)
}

func ReadSysMemoryLinux() (int64, int64) {
	var buf [256]byte
	n1 := readSysMemInfo(buf[:])
	if n1 != 0 {
		totalStart := bytes.Index(buf[:], []byte("MemTotal:")) + len("MemTotal:")
		freeStart := bytes.Index(buf[totalStart:], []byte("MemAvailable:")) + len("MemAvailable:")
		memTotal := buf[totalStart:]
		memFree := buf[freeStart+totalStart:]
		end := bytes.Index(memTotal, []byte("kB"))
		memTotal = memTotal[:end]
		end = bytes.Index(memFree, []byte("kB"))
		memFree = memFree[:end]

		memTotal = bytes.TrimSpace(memTotal)
		memFree = bytes.TrimSpace(memFree)

		total_ := bytes2Int(memTotal)
		free_ := bytes2Int(memFree)

		return total_, free_
	}
	/*
		output like:
		MemTotal:       32505856 kB
		MemFree:        28917428 kB
		MemAvailable:   29288348 kB
		Buffers:               0 kB
		Cached:           370920 kB
		SwapCached:            0 kB
		Active:          3422876 kB
	*/
	return maxMemUse, maxMemUse
}
func SysMem() (total, free int64) {
	t := time.Now()
	readMemMu.Lock()
	defer readMemMu.Unlock()
	if t.Sub(lastGetTime) < 100*time.Millisecond {
		total, free = sysMemTotal, sysMemFree
		return
	}
	total, free = ReadSysMemory()
	if total <= 0 || free <= 0 {
		total, free = totalMemoryMax, totalMemoryMax
		return
	}
	lastGetTime = t
	sysMemTotal = total
	sysMemFree = free
	return
}

func readSysMemInfo(buf []byte) int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer util.MustClose(f)

	n, err := f.ReadAt(buf, 0)
	if err != nil || n < len(buf) {
		return 0
	}
	return n
}

func bytes2Int(b []byte) int64 {
	var v int64
	for _, c := range b {
		v = v*10 + int64(c-'0')
	}
	return v
}

func MemUsedPct() float64 {
	total, available := SysMem()
	return (1 - float64(available)/float64(total)) * 100
}
