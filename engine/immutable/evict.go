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
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/cpu"
	"go.uber.org/zap"
)

const (
	PRELOAD = iota
	LOAD
)

var (
	fileLoadLimiter  limiter.Fixed
	nodeTableStoreGC TablesGC
)

func init() {
	n := cpu.GetCpuNum() * 4
	fileLoadLimiter = limiter.NewFixed(n)

	nodeTableStoreGC = NewTableStoreGC()
	go nodeTableStoreGC.GC()
}

func UnrefFiles(files ...TSSPFile) {
	for _, f := range files {
		f.Unref()
	}
}

func RefFilesReader(files ...TSSPFile) {
	for _, f := range files {
		f.RefFileReader()
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
	Add(files ...TSSPFile)
	GC()
}

type TableStoreGC struct {
	mu          sync.RWMutex
	removeFiles map[string]TSSPFile
}

func NewTableStoreGC() TablesGC {
	return &TableStoreGC{
		removeFiles: make(map[string]TSSPFile, 8),
	}
}

func (sgc *TableStoreGC) Add(files ...TSSPFile) {
	sgc.mu.Lock()
	for _, f := range files {
		sgc.removeFiles[f.Path()] = f
	}
	sgc.mu.Unlock()
}

func (sgc *TableStoreGC) GC() {
	timer := time.NewTicker(time.Millisecond * 200)
	for range timer.C {
		sgc.mu.Lock()
		if len(sgc.removeFiles) == 0 {
			sgc.mu.Unlock()
			continue
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
