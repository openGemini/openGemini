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
	"sort"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

func (m *MmsTables) MergeOutOfOrder(shId uint64, full bool, force bool) error {
	if !m.MergeEnabled() {
		return nil
	}

	measurements := m.getMstToMerge(maxCompactor, full, force)

	for i := range measurements {
		select {
		case <-m.closed:
			log.Warn("shard closed", zap.Uint64("id", shId))
			return nil
		case <-m.stopCompMerge:
			log.Warn("stopped", zap.Uint64("id", shId))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			go func(mst string) {
				defer func() {
					compLimiter.Release()
					m.wg.Done()
				}()
				m.mergeOutOfOrder(mst, shId, full, force)
			}(measurements[i])
		}
	}

	return nil
}

func (m *MmsTables) mergeOutOfOrder(mst string, shId uint64, full bool, force bool) {
	if !m.inMerge.Add(mst) {
		return
	}
	defer func() {
		m.inMerge.Del(mst)
	}()

	contexts := m.buildMergeContext(mst, full, force)
	defer func() {
		for _, item := range contexts {
			item.Release()
		}
	}()

	for _, item := range contexts {
		m.lmt.Update(mst)
		item.shId = shId

		select {
		case <-m.closed:
			return
		case <-m.stopCompMerge:
			return
		default:
			m.execMergeContext(item)
		}
	}
}

func (m *MmsTables) execMergeContext(ctx *MergeContext) {
	stat := statistics.NewMergeStatistics()
	stat.AddActive(1)
	cLog, logEnd := logger.NewOperation(log, "MergeOutOfOrder", ctx.mst)
	tool := newMergeTool(m, cLog)

	defer func() {
		tool.Release()
		if m.compactRecovery {
			MergeRecovery(m.path, ctx.mst, ctx)
		}
		stat.AddActive(-1)
		logEnd()
	}()

	tool.initStat(ctx.mst, ctx.shId)
	if ctx.MergeSelf() {
		tool.mergeSelf(ctx)
	} else {
		tool.merge(ctx)
	}
}

func (m *MmsTables) Listen(signal chan struct{}, onClose func()) {
	go func() {
		select {
		case <-m.closed:
			onClose()
		case <-m.stopCompMerge:
			onClose()
		case <-signal:
			return
		}
	}()
}

func (m *MmsTables) replaceMergedFiles(name string, lg *zap.Logger, old []TSSPFile, new []TSSPFile) error {
	needReplaced := make(map[string]TSSPFile, len(old))
	for _, of := range old {
		for _, nf := range new {
			_, s1 := of.LevelAndSequence()
			_, s2 := nf.LevelAndSequence()

			if s1 == s2 && of.FileNameExtend() == nf.FileNameExtend() {
				needReplaced[of.Path()] = of
			}
		}
	}

	old = old[:0]
	for _, f := range needReplaced {
		old = append(old, f)
		newFileName := new[len(old)-1].FileName()
		oldFileName := f.FileName()
		lg.Debug("replace merged file",
			zap.String("old file", oldFileName.String()),
			zap.Int64("old size", f.FileSize()),
			zap.String("new file", newFileName.String()),
			zap.Int64("new size", new[len(old)-1].FileSize()))
	}

	return m.ReplaceFiles(name, old, new, true)
}

func (m *MmsTables) getFilesByPath(mst string, path []string, order bool) (*TSSPFiles, error) {
	files := NewTSSPFiles()
	files.files = make([]TSSPFile, 0, len(path))

	for _, fn := range path {
		if m.isClosed() {
			return nil, ErrCompStopped
		}
		f := m.File(mst, fn, order)
		if f == nil {
			return nil, fmt.Errorf("table %v, %v, %t not find", mst, fn, order)
		}

		files.Append(f)
	}

	return files, nil
}

func (m *MmsTables) getMstToMerge(limit int, full bool, force bool) []string {
	conf := &config.GetStoreConfig().Merge
	ret := make([]string, 0, limit)
	m.mu.RLock()
	defer m.mu.RUnlock()

	for mst, files := range m.OutOfOrder {
		if len(ret) >= limit {
			break
		}

		files.RLock()
		num := files.Len()
		files.RUnlock()

		if num == 0 || files.closing > 0 || m.inMerge.Has(mst) {
			continue
		}

		if full || force {
			ret = append(ret, mst)
			continue
		}

		if num >= DefaultLevelMergeFileNum || !m.lmt.Nearly(mst, time.Duration(conf.MinInterval)) {
			ret = append(ret, mst)
		}
	}

	return ret
}

func (m *MmsTables) buildMergeContext(mst string, full bool, force bool) []*MergeContext {
	m.mu.RLock()
	files, ok := m.OutOfOrder[mst]
	m.mu.RUnlock()

	if !ok || files == nil {
		return nil
	}

	if force {
		return []*MergeContext{buildNormalMergeContext(mst, files)}
	}

	return BuildMergeContext(mst, files, full, m.lmt)
}

func (m *MmsTables) GetOutOfOrderFileNum() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, v := range m.OutOfOrder {
		total += v.Len()
	}
	return total
}

//lint:ignore U1000 test used only
func (m *MmsTables) tableFiles(name string, order bool) *TSSPFiles {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !order {
		mmsTbls = m.OutOfOrder
	}

	return mmsTbls[name]
}

func (m *MmsTables) removeFile(f TSSPFile) {
	if f.Inuse() {
		if err := f.Rename(f.Path() + tmpFileSuffix); err != nil {
			log.Error("failed to rename file", zap.String("path", f.Path()), zap.Error(err))
			return
		}
		nodeTableStoreGC.Add(false, f)
		return
	}

	err := f.Remove()
	if err != nil {
		nodeTableStoreGC.Add(false, f)
		log.Error("failed to remove file", zap.String("path", f.Path()), zap.Error(err))
		return
	}
}

func (m *MmsTables) matchOrderFiles(ctx *MergeContext) {
	files, ok := m.getTSSPFiles(ctx.mst, true)
	if !ok {
		log.Warn("No order file is matched.", zap.String("measurement", ctx.mst))
		return
	}

	files.lock.RLock()
	defer files.lock.RUnlock()

	for _, f := range files.Files() {
		if m.isClosed() {
			return
		}
		min, max, err := f.MinMaxTime()
		if err != nil {
			continue
		}

		if ctx.order.Len() > 0 || ctx.tr.Overlaps(min, max) || min > ctx.tr.Max {
			ctx.order.add(f)
		}
	}

	if ctx.order.Len() == 0 {
		ctx.order.add(files.Files()[files.Len()-1])
	}
}

func (m *MmsTables) deleteUnorderedFiles(mst string, files []TSSPFile) {
	tfs, ok := m.getTSSPFiles(mst, false)
	if !ok {
		return
	}

	noFiles := true
	func() {
		tfs.lock.Lock()
		defer tfs.lock.Unlock()

		for _, f := range files {
			tfs.deleteFile(f)
			m.removeFile(f)
		}
		if tfs.Len() > 0 {
			noFiles = false
			sort.Sort(tfs)
		}
	}()

	if !noFiles {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tfs, ok = m.OutOfOrder[mst]
	if ok && tfs.Len() == 0 {
		delete(m.OutOfOrder, mst)
	}
}
