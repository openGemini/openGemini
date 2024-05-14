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
	"container/heap"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type mergeTool struct {
	mts  *MmsTables
	lmt  *lastMergeTime
	stat *statistics.MergeStatItem
	lg   *logger.Logger
	zlg  *zap.Logger
}

func newMergeTool(mts *MmsTables, lg *zap.Logger) *mergeTool {
	return &mergeTool{
		mts: mts,
		lmt: mts.lmt,
		lg:  logger.NewLogger(errno.ModuleMerge),
		zlg: lg,
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
		performers.wg.Done()
		performers.Close()
		ur.Close()

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
