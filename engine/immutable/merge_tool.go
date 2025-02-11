// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"container/heap"
	"path/filepath"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type mergeTool struct {
	mts     *MmsTables
	stat    *statistics.MergeStatItem
	lg      *logger.Logger
	zlg     *zap.Logger
	context *EventContext
}

func newMergeTool(mts *MmsTables, ctx *EventContext, lg *zap.Logger) *mergeTool {

	return &mergeTool{
		mts:     mts,
		context: ctx,
		lg:      logger.NewLogger(errno.ModuleMerge),
		zlg:     lg,
	}
}

func (mt *mergeTool) initStat(mst string, shID uint64) {
	mt.stat = statistics.NewMergeStatItem(mst, shID)
}

func (mt *mergeTool) mergePrepare(ctx *MergeContext) bool {
	mt.mts.matchOrderFiles(ctx)
	if ctx.order.Len() == 0 {
		mt.zlg.Warn("no order file is matched")
		return false
	}

	mt.zlg.Info("merge info",
		zap.String("path", mt.mts.path+"/"+ctx.mst),
		zap.Uint64("shard id", ctx.shId),
		zap.Int("unordered file count", len(ctx.unordered.seq)),
		zap.Uint64s("unordered sequences", ctx.unordered.seq),
		zap.Int64("unordered size", ctx.unordered.size),
		zap.Int("order file count", len(ctx.order.seq)),
		zap.Uint64s("order sequences", ctx.order.seq),
		zap.Int64("order file size", ctx.order.size))

	if !mt.mts.acquire(ctx.order.path) {
		mt.zlg.Warn("acquire is false, skip merge")
		return false
	}

	return true
}

func (mt *mergeTool) merge(ctx *MergeContext) {
	if !mt.mergePrepare(ctx) {
		return
	}

	orderWg, inorderWg := mt.mts.refMmsTable(ctx.mst, true)
	var unordered, order *TSSPFiles
	var err error
	success := false

	defer func() {
		mt.mts.unrefMmsTable(orderWg, inorderWg)
		mt.mts.CompactDone(ctx.order.path)
		if !success {
			statistics.NewMergeStatistics().AddErrors(1)
		}
	}()

	order, unordered, err = mt.getTSSPFiles(ctx)
	if err != nil {
		mt.zlg.Error("failed to get files", zap.Error(err))
		return
	}

	func() {
		mt.stat.StatOrderFile(ctx.order.size, ctx.order.Len())
		mt.stat.StatOutOfOrderFile(ctx.unordered.size, ctx.unordered.Len())

		mergedFiles, err := mt.execute(ctx.mst, order, unordered)
		if err != nil {
			mt.zlg.Error("failed to merge unordered files", zap.Error(err))
			return
		}

		mt.stat.StatMergedFile(SumFilesSize(mergedFiles.Files()), mergedFiles.Len())
		if err := mt.mts.replaceMergedFiles(ctx.mst, mt.zlg, order.Files(), mergedFiles.Files()); err != nil {
			mt.zlg.Error("failed to replace merged files", zap.Error(err))
			return
		}
		NewHotFileManager().AddAll(mergedFiles.Files())
		mt.mts.deleteUnorderedFiles(ctx.mst, unordered.Files())
		mt.stat.Push()
		success = true
	}()
}

func (mt *mergeTool) getTSSPFiles(ctx *MergeContext) (*TSSPFiles, *TSSPFiles, error) {
	ctx.Sort()
	order, err := mt.mts.getFilesByPath(ctx.mst, ctx.order.path, true)
	if err != nil {
		return nil, nil, err
	}

	unordered, err := mt.mts.getFilesByPath(ctx.mst, ctx.unordered.path, false)
	if err != nil {
		return nil, nil, err
	}

	return order, unordered, err
}

func (mt *mergeTool) execute(mst string, order, unordered *TSSPFiles) (*TSSPFiles, error) {
	ur := NewUnorderedReader(mt.lg)
	ur.AddFiles(unordered.Files())

	var err error
	performers := NewMergePerformers(ur)
	var tempFiles []fileops.File

	defer func() {
		performers.Close()
		ur.Close()
		performers.Release()

		if err != nil && len(tempFiles) > 0 {
			for _, f := range tempFiles {
				CleanTempFile(f)
			}
		}
	}()

	for _, f := range order.Files() {
		sw := mt.mts.NewStreamWriteFile(mst)
		if err = sw.InitMergedFile(f); err != nil {
			return nil, err
		}
		sw.SetValidate(true)
		tempFiles = append(tempFiles, sw.fd)

		p := NewMergePerformer(ur, mt.stat)

		itr := NewColumnIterator(NewFileIterator(f, mt.lg))
		p.Reset(sw, itr)
		performers.items = append(performers.items, p)
	}

	mt.mts.Listen(performers.signal, func() {
		performers.Close()
	})

	heap.Init(performers)
	for {
		if performers.Len() == 0 {
			break
		}

		if performers.Closed() {
			return nil, ErrCompStopped
		}

		err = performers.Next()
		if err != nil {
			return nil, err
		}
	}

	return performers.mergedFiles, nil
}

func (mt *mergeTool) Release() {
	mt.mts = nil
	mt.lg = nil
	mt.zlg = nil
}

func (mt *mergeTool) mergeSelf(ctx *MergeContext) {
	mt.mts.lmt.Update(ctx.mst)

	if ctx.UnorderedLen() <= 1 {
		return
	}

	if ctx.MergeSelfFast() {
		mt.mergeSelfFastMode(ctx)
		return
	}

	mt.mergeSelfStreamMode(ctx)
}

func (mt *mergeTool) mergeSelfFastMode(ctx *MergeContext) {
	files, err := mt.mts.getFilesByPath(ctx.mst, ctx.unordered.path, false)
	if err != nil {
		mt.zlg.Error("failed to get files", zap.Error(err))
		return
	}

	ms := NewMergeSelf(mt.mts, mt.lg)
	defer ms.Stop()

	success := false
	events := ms.InitEvents(ctx)
	defer func() {
		// Ensures that file locks are released even in unexpected situations
		defer events.Finish(success, mt.mts.getEventContext())
	}()

	mt.mts.Listen(ms.signal, func() {
		ms.Stop()
	})

	mergedSize := int64(0)
	begin := time.Now()

	merged, err := ms.Merge(ctx.mst, ctx.ToLevel(), files.Files())

	if err == nil {
		err = events.TriggerReplaceFile(filepath.Dir(mt.mts.path), *mt.mts.lock)
	}

	if err == nil {
		mergedSize = merged.FileSize()
		err = mt.mts.ReplaceFiles(ctx.mst, files.Files(), []TSSPFile{merged}, false)
	}

	success = err == nil
	mt.lg.Info("finish merge self",
		zap.Int64("total size(MB)", ctx.unordered.size/1024/1024),
		zap.Int64("merged size(MB)", mergedSize/1024/1024),
		zap.Float64("time use(s)", time.Since(begin).Seconds()),
		zap.Uint16("level", ctx.ToLevel()),
		zap.Any("err", err),
		zap.String("mst", ctx.mst))
}

func (mt *mergeTool) mergeSelfStreamMode(ctx *MergeContext) {
	orderWg, inorderWg := mt.mts.refMmsTable(ctx.mst, true)
	success := false

	defer func() {
		mt.mts.unrefMmsTable(orderWg, inorderWg)
		if !success {
			statistics.NewMergeStatistics().AddErrors(1)
		}
	}()

	files, err := mt.mts.getFilesByPath(ctx.mst, ctx.unordered.path, false)
	if err != nil {
		mt.zlg.Error("failed to get files", zap.Error(err))
		return
	}

	statistics.NewMergeStatistics().AddMergeSelfTotal(1)
	order := &TSSPFiles{}
	unordered := &TSSPFiles{}

	order.Append(files.Files()[0])
	unordered.Append(files.Files()[1:]...)

	func() {
		mt.stat.StatOrderFile(0, 0)
		mt.stat.StatOutOfOrderFile(ctx.unordered.size, ctx.unordered.Len())

		mergedFiles, err := mt.execute(ctx.mst, order, unordered)
		if err != nil {
			mt.zlg.Error("failed to merge unordered files", zap.Error(err))
			return
		}

		mt.stat.StatMergedFile(SumFilesSize(mergedFiles.Files()), mergedFiles.Len())
		if err := mt.mts.ReplaceFiles(ctx.mst, order.Files(), mergedFiles.Files(), false); err != nil {
			mt.zlg.Error("failed to replace merged files", zap.Error(err))
			return
		}
		NewHotFileManager().AddAll(mergedFiles.Files())
		mt.mts.deleteUnorderedFiles(ctx.mst, unordered.Files())
		mt.stat.Push()
		success = true
	}()
}

func CleanTempFile(f fileops.File) {
	name := f.Name()
	util.MustClose(f)
	err := fileops.Remove(name)
	if err != nil {
		logger.GetLogger().Error("failed to remove temporary file", zap.Error(err))
	}
}
