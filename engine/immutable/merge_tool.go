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
	"sort"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	// total number of unordered files is less than this value,
	// may skip the merge operation
	mergeMinFileNum = 5

	// total size of unordered files is less than this value,
	// may skip the merge operation
	mergeMinUnorderedSize = 1 * 1024 * 1024

	mergeMaxInterval = 300 // 5min

	MergeFirstAvgSize = 10 * 1024 * 1024
	MergeFirstDstSize = 10 * 1024 * 1024
	MergeFirstRatio   = 0.5
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

func (mt *mergeTool) skip(ctx *mergeContext) bool {
	ok := len(ctx.unordered.seq) < mergeMinFileNum &&
		ctx.unordered.size < mergeMinUnorderedSize &&
		mt.lmt.Nearly(ctx.mst, time.Second*mergeMaxInterval)

	if ok {
		statistics.NewMergeStatistics().AddSkipTotal(1)
		mt.zlg.Info("new and small unordered files, merge later")
	} else {
		mt.lmt.Update(ctx.mst)
	}

	return ok
}

func (mt *mergeTool) mergeUnorderedSelf(ctx *mergeContext, unordered *TSSPFiles) bool {
	if mergeFirst(unordered.Len(), ctx.unordered.size, ctx.order.size) {
		statistics.NewMergeStatistics().AddMergeSelfTotal(1)
		mt.zlg.Info("merge first",
			zap.Int("unordered file count", len(ctx.unordered.seq)),
			zap.Uint64s("unordered sequences", ctx.unordered.seq),
			zap.Int64("unordered size", ctx.unordered.size))

		mt.mergeSelf(ctx, unordered)
		return true
	}

	return false
}

func (mt *mergeTool) mergePrepare(ctx *mergeContext, force bool) bool {
	mt.zlg.Info("merge info",
		zap.String("path", mt.mts.path+"/"+ctx.mst),
		zap.Uint64("shard id", ctx.shId),
		zap.Int("unordered file count", len(ctx.unordered.seq)),
		zap.Uint64s("unordered sequences", ctx.unordered.seq),
		zap.Int64("unordered size", ctx.unordered.size))

	mt.stat = statistics.NewMergeStatItem(ctx.mst, ctx.shId)
	if !force && mt.skip(ctx) {
		return false
	}

	mt.mts.matchOrderFiles(ctx)
	if ctx.order.Len() == 0 {
		mt.zlg.Error("no order file is matched")
		return false
	}

	mt.zlg.Info("order file info",
		zap.Int("order file count", len(ctx.order.seq)),
		zap.Uint64s("order sequences", ctx.order.seq),
		zap.Int64("order file size", ctx.order.size))

	if !mt.mts.acquire(ctx.order.path) {
		mt.zlg.Warn("acquire is false, skip merge")
		return false
	}

	return true
}

func (mt *mergeTool) merge(ctx *mergeContext, force bool) {
	if !mt.mergePrepare(ctx, force) {
		return
	}

	orderWg, inorderWg := mt.mts.refMmsTable(ctx.mst, false)
	var unordered, order *TSSPFiles
	var err error
	success := false

	defer func() {
		mt.mts.unrefMmsTable(orderWg, inorderWg)
		UnrefAll(unordered, order)
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

	if !force && mt.mergeUnorderedSelf(ctx, unordered) {
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

		defer func() {
			unordered, order = nil, nil
		}()
		UnrefAll(unordered, order)

		if err := mt.mts.replaceMergedFiles(ctx.mst, mt.zlg, order.Files(), mergedFiles.Files()); err != nil {
			mt.zlg.Error("failed to replace merged files", zap.Error(err))
			return
		}
		mt.mts.deleteUnorderedFiles(ctx.mst, unordered.Files())
		mt.stat.Push()
		success = true
	}()
}

func (mt *mergeTool) getTSSPFiles(ctx *mergeContext) (*TSSPFiles, *TSSPFiles, error) {
	order, err := mt.mts.getFilesByPath(ctx.mst, ctx.order.path, true)
	if err != nil {
		return nil, nil, err
	}

	unordered, err := mt.mts.getFilesByPath(ctx.mst, ctx.unordered.path, false)
	if err != nil {
		UnrefAll(order)
		return nil, nil, err
	}

	return order, unordered, err
}

func (mt *mergeTool) contains(ur *UnorderedReader, f TSSPFile) bool {
	for _, sid := range ur.sid {
		ok, err := f.Contains(sid)
		if err == nil && ok {
			return true
		}
	}

	return false
}

func (mt *mergeTool) execute(mst string, order, unordered *TSSPFiles) (*TSSPFiles, error) {
	ur := NewUnorderedReader(mt.lg)
	ur.AddFiles(unordered.Files())
	p := NewMergePerformer(ur, mt.stat)

	var err error
	for i, f := range order.Files() {
		last := order.Len() == (i + 1)
		if !last && !mt.contains(ur, f) {
			continue
		}

		sw := mt.mts.NewStreamWriteFile(mst)
		if err = sw.InitMergedFile(f); err != nil {
			break
		}

		p.Reset(sw, last)
		itr := NewColumnIterator(NewFileIterator(f, mt.lg))

		mt.mts.Listen(itr.signal, func() {
			itr.Close()
		})
		if err = itr.Run(p); err != nil {
			break
		}
	}

	if err != nil {
		p.CleanTmpFiles()
		return nil, err
	}

	return p.MergedFiles(), nil
}

func (mt *mergeTool) mergeSelf(ctx *mergeContext, files *TSSPFiles) {
	data, ids := mt.readUnorderedRecords(files)

	mergedFile, err := mt.saveRecords(ctx, files.Files()[0].FileName(), data, ids)
	if err != nil {
		mt.zlg.Error("new tmp file fail", zap.Error(err))
		return
	}
	if mergedFile == nil {
		mt.zlg.Info("no merged files")
		return
	}

	defer func() {
		files.files = nil
	}()
	UnrefAll(files)
	err = mt.mts.ReplaceFiles(ctx.mst, files.Files(), []TSSPFile{mergedFile}, false)
	if err != nil {
		mt.zlg.Error("failed to replace files", zap.Error(err))
	}
}

func (mt *mergeTool) readUnorderedRecords(files *TSSPFiles) (map[uint64]*record.Record, []uint64) {
	var data = make(map[uint64]*record.Record)
	var ids []uint64

	for _, f := range files.Files() {
		fi := NewFileIterator(f, mt.lg)
		itr := NewChunkIterator(fi)
		itr.WithLog(mt.lg)

		for itr.Next() {
			sid := itr.GetSeriesID()
			rec, ok := data[sid]
			tmp := itr.GetRecord()

			if !ok {
				rec = &record.Record{}
				rec.ResetWithSchema(tmp.Schema.Copy())
				data[sid] = rec
				ids = append(ids, sid)
			} else {
				rec.PadRecord(tmp)
			}

			rec.AppendRec(tmp, 0, tmp.RowNums())
		}
	}

	return data, ids
}

func (mt *mergeTool) saveRecords(ctx *mergeContext, fileName TSSPFileName,
	data map[uint64]*record.Record, ids []uint64) (TSSPFile, error) {

	sh := &record.SortHelper{}
	aux := &record.SortAux{
		RowIds:  nil,
		Times:   nil,
		SortRec: nil,
	}

	fileName.merge++
	fileName.lock = mt.mts.lock
	builder := AllocMsBuilder(mt.mts.path, ctx.mst, mt.mts.lock, mt.mts.Conf,
		len(data), fileName, 0, nil, int(ctx.unordered.size))

	var err error
	defer func(msb **MsBuilder) {
		if err != nil {
			ReleaseMsBuilder(*msb)
		}
	}(&builder)

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	for _, sid := range ids {
		rec, ok := data[sid]
		if !ok {
			continue
		}

		aux.InitRecord(rec.Schemas())
		sh.Sort(rec, aux)

		aux.SortRec, rec = rec, aux.SortRec

		builder, err = builder.WriteRecord(sid, rec, nil)
		if err != nil {
			return nil, err
		}
	}

	var mergedFile TSSPFile
	mergedFile, err = builder.NewTSSPFile(true)

	return mergedFile, err
}
