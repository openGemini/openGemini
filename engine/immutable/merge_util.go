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

package immutable

import (
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"go.uber.org/zap"
)

type mergeContext struct {
	mst  string
	shId uint64

	tr        record.TimeRange
	order     *mergeFileInfo
	unordered *mergeFileInfo
}

func (ctx *mergeContext) reset() {
	ctx.mst = ""
	ctx.shId = 0
	ctx.tr.Min = math.MaxInt64
	ctx.tr.Max = math.MinInt64
	ctx.order.reset()
	ctx.unordered.reset()
}

func (ctx *mergeContext) AddUnordered(f TSSPFile) bool {
	min, max, err := f.MinMaxTime()
	if err != nil {
		log.Error("failed to get min, max time")
		return false
	}

	if ctx.tr.Min > min {
		ctx.tr.Min = min
	}
	if ctx.tr.Max < max {
		ctx.tr.Max = max
	}

	ctx.unordered.add(f)

	return !(len(ctx.unordered.seq) > MaxNumOfFileToMerge || ctx.unordered.size > MaxSizeOfFileToMerge)
}

func (ctx *mergeContext) Sort() {
	sort.Sort(ctx.order)
	sort.Sort(ctx.unordered)
}

func (ctx *mergeContext) Release() {
	ctx.reset()
	mergeContextPool.Put(ctx)
}

var mergeContextPool = sync.Pool{}

func NewMergeContext(mst string) *mergeContext {
	ctx, ok := mergeContextPool.Get().(*mergeContext)
	if !ok {
		return &mergeContext{
			mst:       mst,
			order:     &mergeFileInfo{},
			unordered: &mergeFileInfo{},
			tr:        record.TimeRange{Min: math.MaxInt64, Max: math.MinInt64},
		}
	}
	ctx.mst = mst
	return ctx
}

type mergeFileInfo struct {
	size int64
	name []TSSPFileName
	path []string
	seq  []uint64
}

func (mfi *mergeFileInfo) add(f TSSPFile) {
	mfi.size += f.FileSize()
	mfi.name = append(mfi.name, f.FileName())
	mfi.path = append(mfi.path, f.Path())
	_, seq := f.LevelAndSequence()
	mfi.seq = append(mfi.seq, seq)
}

func (mfi *mergeFileInfo) reset() {
	mfi.size = 0
	mfi.name = mfi.name[:0]
	mfi.path = mfi.path[:0]
	mfi.seq = mfi.seq[:0]
}

func (mfi *mergeFileInfo) Len() int { return len(mfi.name) }

func (mfi *mergeFileInfo) Less(i, j int) bool {
	if mfi.name[i].seq != mfi.name[j].seq {
		return mfi.name[i].seq < mfi.name[j].seq
	} else {
		return mfi.name[i].extent < mfi.name[j].extent
	}
}

func (mfi *mergeFileInfo) Swap(i, j int) {
	mfi.path[i], mfi.path[j] = mfi.path[j], mfi.path[i]
	mfi.name[i], mfi.name[j] = mfi.name[j], mfi.name[i]
}

type lastMergeTime struct {
	//map key is measurement
	last map[string]time.Time

	mu sync.RWMutex
}

func NewLastMergeTime() *lastMergeTime {
	return &lastMergeTime{last: make(map[string]time.Time)}
}

func (lmt *lastMergeTime) Update(mst string) {
	lmt.mu.Lock()
	defer lmt.mu.Unlock()

	lmt.last[mst] = time.Now()
}

func (lmt *lastMergeTime) Nearly(mst string, d time.Duration) bool {
	lmt.mu.RLock()
	defer lmt.mu.RUnlock()

	v, ok := lmt.last[mst]
	if !ok {
		return false
	}

	return time.Since(v) <= d
}

type InMerge struct {
	mu     sync.Mutex
	tables map[string]struct{}
}

func (m *InMerge) Add(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[name]; ok {
		return false
	}

	m.tables[name] = struct{}{}
	return true
}

func (m *InMerge) Del(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tables, name)
}

func NewInMerge() *InMerge {
	return &InMerge{tables: make(map[string]struct{}, defaultCap)}
}

func MergeRecovery(path string, name string, ctx *mergeContext) {
	if err := recover(); err != nil {
		panicInfo := fmt.Sprintf("[Merge Panic:err:%s, name:%s, seqs:%v, path:%s] %s",
			err, name, ctx.order.seq, path, debug.Stack())
		errMsg := errno.NewError(errno.CompactPanicFail)
		log.Error(panicInfo, zap.Error(errMsg))
	}
}

func newNilCol(size int, ref *record.Field) *record.ColVal {
	if size == 0 {
		return nil
	}

	col := &record.ColVal{
		Val:          nil,
		Offset:       nil,
		BitMapOffset: 0,
		Len:          size,
		NilCount:     size,
	}

	col.FillBitmap(0)
	if ref.IsString() {
		col.Offset = make([]uint32, size)
	}
	return col
}
