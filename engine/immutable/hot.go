// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/memory"
	"github.com/openGemini/openGemini/lib/pool"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var hotFileWriterPool *pool.UnionPool[HotFileWriter]

type HotFileWriter struct {
	fileops.FileWriter

	data []byte
	meta []byte
}

func NewHotFileWriter(w fileops.FileWriter) *HotFileWriter {
	writer := hotFileWriterPool.Get()
	writer.data = writer.data[:0]
	writer.meta = writer.meta[:0]
	writer.FileWriter = w
	return writer
}

func (w *HotFileWriter) WriteData(b []byte) (int, error) {
	w.data = append(w.data, b...)
	return w.FileWriter.WriteData(b)
}

func (w *HotFileWriter) WriteChunkMeta(b []byte) (int, error) {
	w.meta = append(w.meta, b...)
	return w.FileWriter.WriteChunkMeta(b)
}

func (w *HotFileWriter) AppendChunkMetaToData() error {
	w.data = append(w.data, w.meta...)
	return w.FileWriter.AppendChunkMetaToData()
}

func (w *HotFileWriter) BuildHotFileReader(r fileops.BasicFileReader) *HotFileReader {
	buf := make([]byte, 0, len(w.data))
	buf = append(buf, w.data...)
	return NewHotFileReader(r, buf)
}

func (w *HotFileWriter) Release() {
	w.FileWriter = nil
	hotFileWriterPool.Put(w)
}

func (w *HotFileWriter) MemSize() int {
	return len(w.data)
}

func (w *HotFileWriter) Instance() *HotFileWriter {
	return w
}

type HotFileReader struct {
	fileops.BasicFileReader

	data []byte
	size int64
}

func NewHotFileReader(r fileops.BasicFileReader, buf []byte) *HotFileReader {
	reader := &HotFileReader{
		data: buf,
		size: int64(len(buf)),
	}
	reader.BasicFileReader = r
	return reader
}

func (r *HotFileReader) ReadAt(off int64, size uint32, dstPtr *[]byte, ioPriority int) ([]byte, error) {
	end := int(off) + int(size)
	if end > len(r.data) {
		return nil, io.EOF
	}

	stat.NewHotMode().ReadTotal.Incr()
	stat.NewHotMode().ReadBytesTotal.Add(int64(size) / 1024) // KB

	if ioPriority == fileops.IO_PRIORITY_ULTRA_HIGH {
		return r.data[off:end], nil
	}

	*dstPtr = bufferpool.Resize(*dstPtr, int(size))
	dst := *dstPtr
	dst = append(dst[:0], r.data[off:end]...)
	return dst, nil
}

func (r *HotFileReader) IsMmapRead() bool {
	return false
}

func (r *HotFileReader) Size() (int64, error) {
	return r.size, nil
}

func (r *HotFileReader) Release() {
	r.BasicFileReader = nil
}

var hotFileManager *HotFileManager

func init() {
	hotFileManager = &HotFileManager{
		files: make(map[int64]*TSSPFiles),
	}
}

func NewHotFileManager() *HotFileManager {
	return hotFileManager
}

type HotFileManager struct {
	signal chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
	conf   *config.HotMode

	files       map[int64]*TSSPFiles // key is the time window
	timeWindows []int64              // reverse order

	maxMemorySize   int64
	totalMemorySize int64

	// Used only during shard loading
	loadMemorySize int64
}

func (m *HotFileManager) Run() {
	m.signal = make(chan struct{})
	m.conf = &config.GetStoreConfig().HotMode
	if !m.conf.Enabled {
		return
	}

	memoryTotal, _ := memory.SysMem()
	m.SetMaxMemorySize(config.KB * memoryTotal * m.conf.GetMemoryAllowedPercent() / 100)
	hotFileWriterPool = pool.NewUnionPool[HotFileWriter](m.conf.PoolObjectCnt, int(m.conf.MaxCacheSize),
		int(m.conf.MaxCacheSize), func() *HotFileWriter {
			return &HotFileWriter{}
		})

	m.wg.Add(1)
	go m.BackgroundFree()
}

func (m *HotFileManager) Stop() {
	close(m.signal)
	m.wg.Wait()
}

func (m *HotFileManager) SetMaxMemorySize(size int64) {
	m.maxMemorySize = size
}

func (m *HotFileManager) AddAll(files []TSSPFile) {
	for _, file := range files {
		m.Add(file)
	}
}

func (m *HotFileManager) Add(f TSSPFile) {
	memSize := f.InMemSize()
	if memSize == 0 {
		return
	}

	m.IncrMemorySize(memSize)
	window, err := m.calculateTimeWindow(f)
	if err != nil {
		f.FreeMemory()
		log.Debug("failed to calculate time window", zap.Error(err))
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	files, ok := m.files[window]
	if !ok {
		files = &TSSPFiles{}
		m.timeWindows = append(m.timeWindows, window)
		sort.Slice(m.timeWindows, func(i, j int) bool {
			return m.timeWindows[i] > m.timeWindows[j]
		})
		m.files[window] = files
	}
	files.Append(f)
}

func (m *HotFileManager) calculateTimeWindow(f TSSPFile) (int64, error) {
	minT, maxT, err := f.MinMaxTime()
	if err != nil {
		return 0, err
	}
	if !m.inHotDuration(minT, maxT) {
		return 0, fmt.Errorf("not in the hot time range. file time range: %d-%d", minT, maxT)
	}

	maxT /= 1e9 // to seconds
	maxT -= maxT % m.conf.TimeWindowSeconds()
	return maxT, nil
}

func (m *HotFileManager) IncrMemorySize(size int64) {
	atomic.AddInt64(&m.totalMemorySize, size)
}

func (m *HotFileManager) BackgroundFree() {
	hotStat := stat.NewHotMode()
	ticker := time.NewTicker(time.Second)
	defer func() {
		ticker.Stop()
		m.wg.Done()
	}()

	for {
		select {
		case <-ticker.C:
			m.Free()

			hotStat.TotalMemorySize.Store(m.totalMemorySize)
		case <-m.signal:
			return
		}
	}
}

func (m *HotFileManager) Free() {
	if m.totalMemorySize > m.maxMemorySize {
		m.freeOldestWindow()
		return
	}

	if m.conf.DurationSeconds() == 0 {
		return
	}
	window := m.getOldestWindow()
	expireAt := int64(fasttime.UnixTimestamp()) - m.conf.DurationSeconds()
	if window < expireAt {
		m.freeOldestWindow()
	}
}

func (m *HotFileManager) AllocLoadMemory(size int64) bool {
	if m.totalMemorySize+size > m.maxMemorySize {
		return false
	}

	n := atomic.AddInt64(&m.loadMemorySize, size)
	if n > m.maxMemorySize {
		atomic.AddInt64(&m.loadMemorySize, -size)
		return false
	}

	return true
}

func (m *HotFileManager) freeOldestWindow() {
	var files *TSSPFiles
	var ok bool
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		size := len(m.timeWindows)
		if size == 0 {
			return
		}

		window := m.timeWindows[size-1]
		m.timeWindows = m.timeWindows[:size-1]
		files, ok = m.files[window]
		delete(m.files, window)
	}()

	if !ok || files == nil {
		return
	}

	for _, f := range files.Files() {
		f.FreeMemory()
	}
}

func (m *HotFileManager) getOldestWindow() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	size := len(m.timeWindows)
	if size == 0 {
		return 0
	}

	return m.timeWindows[size-1]
}

func (m *HotFileManager) InHotDuration(f TSSPFile) bool {
	minT, maxT, err := f.MinMaxTime()
	if err != nil {
		return false
	}
	return m.inHotDuration(minT, maxT)
}

func (m *HotFileManager) inHotDuration(minT, maxT int64) bool {
	dur := m.conf.DurationSeconds()
	if dur == 0 {
		return true
	}

	minT /= 1e9
	maxT /= 1e9

	lower := int64(fasttime.UnixTimestamp()) - m.conf.DurationSeconds()
	upper := int64(fasttime.UnixTimestamp()) + m.conf.DurationSeconds()

	return upper > minT && lower < maxT
}

func FilesMergedTire(files []TSSPFile) uint64 {
	tire := util.Warm
	maxSize := int64(config.GetStoreConfig().HotMode.MaxFileSize)
	for _, f := range files {
		if f.FileSize() >= maxSize {
			return util.Warm
		}

		if f.InMemSize() > 0 {
			tire = util.Hot
		}
	}
	return uint64(tire)
}
