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

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

func (m *MmsTables) MergeOutOfOrder(shId uint64, full bool, force bool) error {
	contexts := m.createMergeContext(maxCompactor, full)

	for _, ctx := range contexts {
		if ctx.mst == "" || len(ctx.unordered.seq) == 0 {
			continue
		}
		if !m.inMerge.Add(ctx.mst) {
			log.Info("merging in progress", zap.String("name", ctx.mst))
			continue
		}
		ctx.shId = shId

		select {
		case <-m.closed:
			log.Warn("shard closed", zap.Uint64("id", shId))
			return fmt.Errorf("store closed, shard id: %v", shId)
		case <-m.stopCompMerge:
			m.inMerge.Del(ctx.mst)
			log.Info("stop merge", zap.Uint64("id", shId))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.MergeEnabled() {
				m.inMerge.Del(ctx.mst)
				m.wg.Done()
				return nil
			}

			go m.mergeOutOfOrder(ctx, force)
		}
	}

	return nil
}

func (m *MmsTables) mergeOutOfOrder(ctx *MergeContext, force bool) {
	stat := statistics.NewMergeStatistics()
	stat.AddActive(1)
	cLog, logEnd := logger.NewOperation(log, "MergeOutOfOrder", ctx.mst)
	defer func() {
		stat.AddActive(-1)
		m.wg.Done()
		compLimiter.Release()
		m.inMerge.Del(ctx.mst)
		logEnd()
		ctx.Release()
	}()

	if m.compactRecovery {
		defer MergeRecovery(m.path, ctx.mst, ctx)
	}

	tool := newMergeTool(m, cLog)
	tool.merge(ctx, force)
	tool.Release()
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

func (m *MmsTables) createMergeContext(limit int, full bool) []*MergeContext {
	ret := make([]*MergeContext, 0, limit)
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.OutOfOrder {
		if v.Len() == 0 || v.closing > 0 {
			continue
		}

		var ctx *MergeContext
		if full {
			ctx = BuildFullMergeContext(k, v)
		} else {
			ctx = BuildMergeContext(k, v)
		}
		if ctx == nil {
			continue
		}

		ret = append(ret, ctx)
		limit--
		if limit <= 0 {
			break
		}
	}

	return ret
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

		if ctx.tr.Overlaps(min, max) || min > ctx.tr.Max {
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
