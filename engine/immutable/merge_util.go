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

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var (
	LevelMergeFileNum        = []int{8, 8}
	DefaultLevelMergeFileNum = 4
)

type MergeContext struct {
	mst   string
	shId  uint64
	level uint16

	tr        util.TimeRange
	order     *mergeFileInfo
	unordered *mergeFileInfo
}

func (ctx *MergeContext) reset() {
	ctx.mst = ""
	ctx.shId = 0
	ctx.tr.Min = math.MaxInt64
	ctx.tr.Max = math.MinInt64
	ctx.order.reset()
	ctx.unordered.reset()
}

func (ctx *MergeContext) AddUnordered(f TSSPFile) {
	min, max, err := f.MinMaxTime()
	if err != nil {
		log.Error("failed to get min, max time")
		return
	}

	if ctx.tr.Min > min {
		ctx.tr.Min = min
	}
	if ctx.tr.Max < max {
		ctx.tr.Max = max
	}

	ctx.unordered.add(f)
}

func (ctx *MergeContext) UnorderedLen() int {
	return ctx.unordered.Len()
}

func (ctx *MergeContext) Limited() bool {
	conf := &config.GetStoreConfig().Merge
	return (ctx.UnorderedLen() >= conf.MaxUnorderedFileNumber) ||
		ctx.unordered.size > int64(conf.MaxUnorderedFileSize)
}

func (ctx *MergeContext) Sort() {
	sort.Sort(ctx.order)
	sort.Sort(ctx.unordered)
}

func (ctx *MergeContext) Release() {
	ctx.reset()
	mergeContextPool.Put(ctx)
}

func (ctx *MergeContext) MergeSelf() bool {
	conf := config.GetStoreConfig()

	return conf.Merge.MergeSelfOnly || conf.UnorderedOnly ||
		(conf.Merge.MaxMergeSelfLevel > ctx.level && int64(conf.Merge.MaxUnorderedFileSize) > ctx.unordered.size)
}

var mergeContextPool = sync.Pool{}

func NewMergeContext(mst string, level uint16) *MergeContext {
	ctx, ok := mergeContextPool.Get().(*MergeContext)
	if !ok {
		return &MergeContext{
			mst:       mst,
			level:     level,
			order:     &mergeFileInfo{},
			unordered: &mergeFileInfo{},
			tr:        util.TimeRange{Min: math.MaxInt64, Max: math.MinInt64},
		}
	}
	ctx.mst = mst
	ctx.level = level
	return ctx
}

func BuildMergeContext(mst string, files *TSSPFiles, full bool, lmt *lastMergeTime) []*MergeContext {
	files.RLock()
	defer files.RUnlock()

	if files.Len() == 0 || files.closing > 0 {
		return nil
	}

	var ret []*MergeContext
	var callback = func(ctx *MergeContext) {
		if ctx != nil && ctx.UnorderedLen() > 0 {
			ret = append(ret, ctx)
		}
	}

	if full {
		buildFullMergeContext(mst, files, callback)
		return ret
	}

	conf := config.GetStoreConfig()
	if conf.Merge.MergeSelfOnly || conf.UnorderedOnly {
		buildUnorderedOnlyMergeContext(mst, files, callback)
		return ret
	}

	for i := uint16(0); i < conf.Merge.MaxMergeSelfLevel; i++ {
		buildLevelMergeContext(mst, files, i, callback)
	}

	if len(ret) == 0 &&
		(files.MaxMerged() >= conf.Merge.MaxMergeSelfLevel ||
			!lmt.Nearly(mst, time.Duration(conf.Merge.MinInterval))) {
		ret = append(ret, buildNormalMergeContext(mst, files))
	}

	return ret
}

func buildUnorderedOnlyMergeContext(mst string, files *TSSPFiles, callback func(ctx *MergeContext)) {
	maxLevel := files.MaxMerged()

	for i := uint16(0); i <= maxLevel; i++ {
		buildLevelMergeContext(mst, files, i, callback)
	}
}

func buildLevelMergeContext(mst string, files *TSSPFiles, level uint16, callback func(ctx *MergeContext)) {
	ctx := NewMergeContext(mst, level)

	fileNum := DefaultLevelMergeFileNum
	if int(level) < len(LevelMergeFileNum) {
		fileNum = LevelMergeFileNum[level]
	}

	maxFileSize := int64(config.GetStoreConfig().Merge.MaxUnorderedFileSize)
	for _, f := range files.Files() {
		if f.FileSize() >= maxFileSize && ctx.UnorderedLen() == 0 {
			continue
		}

		fileMergedLevel := f.FileNameMerge()
		if fileMergedLevel != level {
			if ctx.UnorderedLen() > 0 {
				ctx = NewMergeContext(mst, level)
			}
			continue
		}

		ctx.AddUnordered(f)
		if ctx.UnorderedLen() >= fileNum {
			callback(ctx)
			ctx = NewMergeContext(mst, level)
		}
	}
}

func buildFullMergeContext(mst string, files *TSSPFiles, callback func(ctx *MergeContext)) {
	conf := &config.GetStoreConfig().Merge
	ctx := NewMergeContext(mst, math.MaxUint16)

	for _, f := range files.Files() {
		if ctx.UnorderedLen() == 0 && f.FileSize() >= int64(conf.MaxUnorderedFileSize) {
			continue
		}
		ctx.AddUnordered(f)
		if ctx.Limited() {
			callback(ctx)
			ctx = NewMergeContext(mst, math.MaxUint16)
		}
	}
}

func buildNormalMergeContext(mst string, files *TSSPFiles) *MergeContext {
	files.RLock()
	defer files.RUnlock()
	ctx := NewMergeContext(mst, math.MaxUint16)

	for _, f := range files.Files() {
		ctx.AddUnordered(f)
		if ctx.Limited() {
			break
		}
	}
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

func (m *MeasurementInProcess) Has(name string) bool {
	m.mu.Lock()
	_, ok := m.tables[name]
	m.mu.Unlock()
	return ok
}

func (m *MeasurementInProcess) Del(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tables, name)
}

func NewMeasurementInProcess() *MeasurementInProcess {
	return &MeasurementInProcess{tables: make(map[string]struct{}, defaultCap)}
}

func MergeRecovery(path string, name string, ctx *MergeContext) {
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
