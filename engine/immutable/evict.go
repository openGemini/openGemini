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
	"container/list"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

type memReaderEvictCtx struct {
	evictList     []memReaderEvictList
	evictListName []string
}

type memReaderEvictList struct {
	mu   sync.Mutex
	list list.List
}

const (
	PRELOAD = iota
	LOAD
)

func NewMemReaderEvictCtx() *memReaderEvictCtx {
	ctx := &memReaderEvictCtx{}

	ctx.evictList = make([]memReaderEvictList, CompactLevels+1)
	for i := range ctx.evictList {
		ctx.evictList[i].list.Init()
	}

	ctx.evictListName = make([]string, CompactLevels+1)
	for i := range ctx.evictList {
		ctx.evictListName[i] = strconv.Itoa(i)
	}

	return ctx
}

func (ctx *memReaderEvictCtx) evictMemReader(evictSize int64) {
	for i := len(ctx.evictList) - 1; i >= 0; i-- {
		l := levelEvictListLock(uint16(i))
		e := l.Back()
		for e != nil {
			f := e.Value.(TSSPFile)
			e = e.Prev()
			size := f.Free(false)
			if size > 0 {
				evictSize -= size
			}

			if evictSize <= 0 {
				levelEvictListUnLock(uint16(i))
				return
			}
		}
		levelEvictListUnLock(uint16(i))
	}
}

func getEvictListIdx(level uint16) uint16 {
	listLen := uint16(len(nodeEvictCtx.evictList))
	if level >= listLen {
		level = listLen - 1
	}
	return level
}

func levelEvictListLock(level uint16) *list.List {
	idx := getEvictListIdx(level)
	nodeEvictCtx.evictList[idx].mu.Lock()
	return &nodeEvictCtx.evictList[idx].list
}

func levelEvictList(level uint16) *list.List {
	idx := getEvictListIdx(level)
	return &nodeEvictCtx.evictList[idx].list
}

func levelEvictListUnLock(level uint16) {
	idx := getEvictListIdx(level)
	nodeEvictCtx.evictList[idx].mu.Unlock()
}

func levelName(level uint16) string {
	idx := getEvictListIdx(level)
	return nodeEvictCtx.evictListName[idx]
}

func getImmTableEvictSize() int64 {
	nodeSize := atomic.LoadInt64(&nodeImmTableSizeUsed)
	if nodeSize > nodeImmTableSizeLimit {
		return nodeSize - nodeImmTableSizeLimit
	}
	return 0
}

var (
	fileLoadLimiter  limiter.Fixed
	nodeEvictCtx     *memReaderEvictCtx
	nodeTableStoreGC TablesGC
)

func init() {
	n := cpu.GetCpuNum() * 4
	fileLoadLimiter = limiter.NewFixed(n)

	nodeEvictCtx = NewMemReaderEvictCtx()
	go nodeEvictCtx.runEvictMemReaders()

	nodeTableStoreGC = NewTableStoreGC()
	go nodeTableStoreGC.GC()
}

func UnrefAll(files ...*TSSPFiles) {
	if len(files) == 0 {
		return
	}

	for _, item := range files {
		UnrefTSSPFiles(item)
	}
}

func UnrefTSSPFiles(files *TSSPFiles) {
	if files == nil {
		return
	}

	for _, f := range files.Files() {
		f.Unref()
		f.UnrefFileReader()
	}
}

func UnrefFiles(files ...TSSPFile) {
	for _, f := range files {
		f.Unref()
	}
}

func UnrefFilesReader(files ...TSSPFile) {
	for _, f := range files {
		f.UnrefFileReader()
	}
}

var (
	_ TablesStore = (*MmsTables)(nil)
)

type TablesGC interface {
	Add(free bool, files ...TSSPFile)
	GC()
}

type TableStoreGC struct {
	mu          sync.RWMutex
	freeFiles   map[string]TSSPFile
	removeFiles map[string]TSSPFile
}

func NewTableStoreGC() TablesGC {
	return &TableStoreGC{
		freeFiles:   make(map[string]TSSPFile, 8),
		removeFiles: make(map[string]TSSPFile, 8),
	}
}

func (sgc *TableStoreGC) Add(free bool, files ...TSSPFile) {
	sgc.mu.Lock()
	for _, f := range files {
		fName := f.Path()
		if free {
			sgc.freeFiles[fName] = f
		} else {
			delete(sgc.freeFiles, fName)
			sgc.removeFiles[f.Path()] = f
		}
	}
	sgc.mu.Unlock()
}

func (sgc *TableStoreGC) GC() {
	timer := time.NewTicker(time.Millisecond * 200)
	for range timer.C {
		sgc.mu.Lock()
		if len(sgc.freeFiles) == 0 && len(sgc.removeFiles) == 0 {
			sgc.mu.Unlock()
			continue
		}

		for fn, f := range sgc.freeFiles {
			if !f.Inuse() {
				_ = f.Free(true)
				delete(sgc.freeFiles, fn)
			}
		}

		for fn, f := range sgc.removeFiles {
			if !f.Inuse() {
				err := f.Remove()
				if err != nil {
					log.Error("gc remove file fail", zap.String("file", fn), zap.Error(err))
				} else {
					log.Info("remove file", zap.String("file", fn))
					delete(sgc.removeFiles, fn)
				}
			}
		}
		sgc.mu.Unlock()
	}
	timer.Stop()
}
