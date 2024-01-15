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
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type mergeContext struct {
	mst  string
	shId uint64

	tr        util.TimeRange
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
			tr:        util.TimeRange{Min: math.MaxInt64, Max: math.MinInt64},
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

type MeasurementInProcess struct {
	mu     sync.Mutex
	tables map[string]struct{}
}

func (m *MeasurementInProcess) Add(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[name]; ok {
		return false
	}

	m.tables[name] = struct{}{}
	return true
}

func (m *MeasurementInProcess) Del(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tables, name)
}

func NewMeasurementInProcess() *MeasurementInProcess {
	return &MeasurementInProcess{tables: make(map[string]struct{}, defaultCap)}
}

func MergeRecovery(path string, name string, ctx *mergeContext) {
	if err := recover(); err != nil {
		panicInfo := fmt.Sprintf("[Merge Panic:err:%s, name:%s, seqs:%v, path:%s] %s",
			err, name, ctx.order.seq, path, debug.Stack())
		errMsg := errno.NewError(errno.CompactPanicFail)
		log.Error(panicInfo, zap.Error(errMsg))
	}
}

func FillNilCol(col *record.ColVal, size int, ref *record.Field) {
	col.Init()
	if size == 0 {
		return
	}

	col.Len = size
	col.NilCount = size
	col.FillBitmap(0)
	if !ref.IsString() {
		return
	}

	if cap(col.Offset) < size {
		col.Offset = make([]uint32, size)
	}
	col.Offset = col.Offset[:size]
}

func newNilCol(size int, ref *record.Field) *record.ColVal {
	col := &record.ColVal{}
	FillNilCol(col, size, ref)
	return col
}

func MergeTimes(a []int64, b []int64, dst []int64) []int64 {
	if len(a) == 0 {
		dst = append(dst, b...)
		return dst
	}
	if len(b) == 0 {
		dst = append(dst, a...)
		return dst
	}

	i, j := 0, 0
	la, lb := len(a), len(b)

	for {
		if i == la {
			dst = append(dst, b[j:]...)
			break
		}
		if j == lb {
			dst = append(dst, a[i:]...)
			break
		}

		if a[i] == b[j] {
			dst = append(dst, a[i])
			i++
			j++
			continue
		}

		if a[i] < b[j] {
			dst = append(dst, a[i])
			i++
			continue
		}

		// a[i] > b[j]
		dst = append(dst, b[j])
		j++
	}

	return dst
}

var hitRatioStat = statistics.NewHitRatioStatistics()

type MergeColPool struct {
	pool []*record.ColVal
}

func (p *MergeColPool) Get() *record.ColVal {
	hitRatioStat.AddMergeColValGetTotal(1)
	size := len(p.pool)
	if size == 0 {
		return &record.ColVal{}
	}

	col := p.pool[size-1]
	p.pool = p.pool[:size-1]
	if col == nil {
		return &record.ColVal{}
	}

	hitRatioStat.AddMergeColValHitTotal(1)
	return col
}

func (p *MergeColPool) Put(col *record.ColVal) {
	col.Init()
	p.pool = append(p.pool, col)
}
