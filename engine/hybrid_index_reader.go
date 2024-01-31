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

package engine

import (
	"fmt"
	"math"
	"strconv"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/cache"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	IncDataSegmentNum = 16
)

type IndexReader interface {
	Next() (executor.IndexFrags, error)
}

type indexContext struct {
	readSegmentBatch  bool
	segmentBatchCount int
	shardPath         string
	pkIndexReader     sparseindex.PKIndexReader
	skIndexReader     sparseindex.SKIndexReader
	schema            hybridqp.Catalog
	keyCondition      sparseindex.KeyCondition
	tr                util.TimeRange
	log               *logger.Logger
}

func NewIndexContext(readBatch bool, batchCount int, schema hybridqp.Catalog, shardPath string) *indexContext {
	return &indexContext{
		readSegmentBatch:  readBatch,
		segmentBatchCount: batchCount,
		shardPath:         shardPath,
		pkIndexReader:     sparseindex.NewPKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek),
		skIndexReader:     sparseindex.NewSKIndexReader(colstore.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek),
		schema:            schema,
		tr:                util.TimeRange{Min: schema.Options().GetStartTime(), Max: schema.Options().GetEndTime()},
		log:               logger.NewLogger(errno.ModuleIndex),
	}
}

type attachedIndexReader struct {
	init         bool
	idx          int
	dataPath     string
	ctx          *indexContext
	info         *executor.AttachedIndexInfo
	skFileReader []sparseindex.SKFileReader
}

func NewAttachedIndexReader(ctx *indexContext, info *executor.AttachedIndexInfo) *attachedIndexReader {
	return &attachedIndexReader{
		info: info,
		ctx:  ctx,
	}
}

func (r *attachedIndexReader) Init() (err error) {
	mstInfo := r.ctx.schema.Options().GetMeasurements()[0]
	r.skFileReader, err = r.ctx.skIndexReader.CreateSKFileReaders(r.ctx.schema.Options(), mstInfo, true)
	if err != nil {
		return err
	}
	err = initKeyCondition(r.info.Infos()[0].GetRec().Schema, r.ctx)
	return
}

func (r *attachedIndexReader) Next() (executor.IndexFrags, error) {
	if r.info == nil || len(r.info.Files()) == 0 {
		return nil, nil
	}
	if !r.init {
		if err := r.Init(); err != nil {
			return nil, err
		}
		r.init = true
	}
	var err error
	dataFiles, pkInfos := r.info.Files(), r.info.Infos()
	frags := executor.NewAttachedFrags(r.dataPath, len(dataFiles))
	for r.idx < len(dataFiles) {
		dataFile, pkInfo := dataFiles[r.idx], pkInfos[r.idx]
		var frs fragment.FragmentRanges
		frs, err = r.ctx.pkIndexReader.Scan(dataFile.Path(), pkInfo.GetRec(), pkInfo.GetMark(), r.ctx.keyCondition)
		if err != nil {
			return nil, err
		}
		if frs.Empty() {
			r.idx++
			continue
		}
		for j := range r.skFileReader {
			if err = r.skFileReader[j].ReInit(dataFile); err != nil {
				return nil, err
			}
			frs, err = r.ctx.skIndexReader.Scan(r.skFileReader[j], frs)
			if err != nil {
				return nil, err
			}
			if frs.Empty() {
				break
			}
		}
		var fragmentCount uint32
		for j := range frs {
			fragmentCount += frs[j].End - frs[j].Start
		}
		if fragmentCount == 0 {
			r.idx++
			continue
		}
		frags.AppendIndexes(dataFile)
		frags.AppendFragRanges(frs)
		frags.AddFragCount(int64(fragmentCount))
		if r.ctx.readSegmentBatch && int(frags.FragCount()) >= r.ctx.segmentBatchCount {
			r.idx++
			return frags, nil
		}
		r.idx++
	}
	if frags.FragCount() == 0 {
		return nil, nil
	}
	return frags, nil
}

type detachedIndexReader struct {
	init         bool
	idx          int
	localPath    string
	dataPath     string
	info         *executor.DetachedIndexInfo
	ctx          *indexContext
	skFileReader []sparseindex.SKFileReader
	obsOptions   *obs.ObsOptions
}

func NewDetachedIndexReader(ctx *indexContext, obsOption *obs.ObsOptions) *detachedIndexReader {
	return &detachedIndexReader{
		obsOptions: obsOption,
		ctx:        ctx,
	}
}

func (r *detachedIndexReader) Init() (err error) {
	mst := r.ctx.schema.Options().GetMeasurements()[0]
	r.dataPath = obs.GetBaseMstPath(r.ctx.shardPath, mst.Name)
	if immutable.GetColStoreConfig().GetDetachedFlushEnabled() {
		r.localPath = obs.GetLocalMstPath(immutable.GetPrefixDataPath(), r.dataPath)
	}
	chunkCount, err := immutable.GetMetaIndexChunkCount(r.obsOptions, r.dataPath)
	if err != nil {
		return
	}
	if chunkCount == 0 {
		return
	}
	startChunkId, endChunkId := int64(0), chunkCount

	// init the obs meta index reader
	metaIndexReader, err := immutable.NewDetachedMetaIndexReader(r.dataPath, r.obsOptions)
	if err != nil {
		return
	}

	// init the obs pk meta info reader
	pkMetaInfoReader, err := immutable.NewDetachedPKMetaInfoReader(r.dataPath, r.obsOptions)
	if err != nil {
		return
	}
	pkMetaInfo, err := pkMetaInfoReader.Read()
	if err != nil {
		return
	}

	// init the obs pk meta reader
	pkMetaReader, err := immutable.NewDetachedPKMetaReader(r.dataPath, r.obsOptions)
	if err != nil {
		return
	}

	// init the obs pk data reader
	pkDataReader, err := immutable.NewDetachedPKDataReader(r.dataPath, r.obsOptions)
	if err != nil {
		return
	}
	pkDataReader.SetPkMetaInfo(pkMetaInfo)

	var miChunkIds []int64
	var miFiltered []*immutable.MetaIndex

	// init the pk meta variables
	var pkMetas []*colstore.DetachedPKMeta
	var pkDatas []*colstore.DetachedPKData
	var pkItems []*colstore.DetachedPKInfo

	// step1: init the meta index
	offsets, lengths := make([]int64, 0, chunkCount), make([]int64, 0, chunkCount)
	for i := startChunkId; i < endChunkId; i++ {
		offset, length := immutable.GetMetaIndexOffsetAndLengthByChunkId(i)
		offsets, lengths = append(offsets, offset), append(lengths, length)
	}
	metaIndexes, err := metaIndexReader.ReadMetaIndex(offsets, lengths)
	if err != nil {
		return err
	}
	for i := range metaIndexes {
		if metaIndexes[i].IsExist(r.ctx.tr) {
			miFiltered = append(miFiltered, metaIndexes[i])
			miChunkIds = append(miChunkIds, startChunkId+int64(i))
		}
	}

	if len(miFiltered) == 0 {
		return nil
	}

	// step2: init the pk items
	offsets, lengths = offsets[:0], lengths[:0]
	for _, chunkId := range miChunkIds {
		offset, length := immutable.GetPKMetaOffsetLengthByChunkId(pkMetaInfo, int(chunkId))
		offsets, lengths = append(offsets, offset), append(lengths, length)
	}
	pkMetas, err = pkMetaReader.Read(offsets, lengths)
	if err != nil {
		return err
	}
	offsets, lengths = offsets[:0], lengths[:0]
	for i := range pkMetas {
		offsets, lengths = append(offsets, int64(pkMetas[i].Offset)), append(lengths, int64(pkMetas[i].Length))
	}
	pkDatas, err = pkDataReader.Read(offsets, lengths, pkMetas)
	if err != nil {
		return err
	}
	for i := range pkDatas {
		pkItems = append(pkItems, colstore.GetPKInfoByPKMetaData(pkMetas[i], pkDatas[i], pkMetaInfo.TCLocation))
	}
	r.info = executor.NewDetachedIndexInfo(miFiltered, pkItems)

	// step3: init the key condition and sk file readers
	mstInfo := r.ctx.schema.Options().GetMeasurements()[0]
	r.skFileReader, err = r.ctx.skIndexReader.CreateSKFileReaders(r.ctx.schema.Options(), mstInfo, true)
	if err != nil {
		return err
	}
	if r.ctx.keyCondition != nil {
		return
	}

	err = initKeyCondition(r.info.Infos()[0].Data.Schema, r.ctx)
	return
}

func (r *detachedIndexReader) InitInc(frag executor.IndexFrags) (executor.IndexFrags, error) {
	queryId := r.ctx.schema.Options().GetLogQueryCurrId()
	currIter := r.ctx.schema.Options().GetIterId()
	totalCount := frag.FragCount()
	iterNum := int32(math.Ceil(float64(totalCount) / float64(IncDataSegmentNum)))
	cache.PutNodeIterNum(queryId, iterNum)
	if currIter >= iterNum {
		return nil, nil
	}
	newFrag := executor.NewDetachedFrags(frag.BasePath(), 0)
	fragR := frag.FragRanges()
	fragIndex, ok := frag.Indexes().([]*immutable.MetaIndex)
	if !ok {
		return nil, fmt.Errorf("get wrong Index frag to init increase agg query")
	}
	start := uint32((currIter) * IncDataSegmentNum)
	end := uint32(0)
	if int64((currIter+1)*IncDataSegmentNum) > totalCount {
		end = uint32(totalCount)
	} else {
		end = uint32((currIter + 1) * IncDataSegmentNum)
	}
	readCount := uint32(0)
	for ik, index := range fragIndex {
		if readCount >= end {
			break
		}
		currFrs := make([]*fragment.FragmentRange, 0)
		for _, fragRange := range fragR[ik] {
			if readCount >= end {
				break
			}
			currCount := fragRange.End - fragRange.Start
			if readCount+currCount <= start {
				readCount += currCount
				continue
			}
			currStart := fragRange.Start
			if readCount < start {
				currStart = start - readCount + fragRange.Start
			}
			if readCount+currCount <= end {
				currFrs = append(currFrs, fragment.NewFragmentRange(currStart, fragRange.End))
				readCount += currCount
				continue
			} else {
				if readCount < start {
					readCount = start
				}
				currEnd := end - readCount + currStart
				currFrs = append(currFrs, fragment.NewFragmentRange(currStart, currEnd))
				readCount = end
				break
			}
		}
		if len(currFrs) == 0 {
			continue
		}
		newFrag.AppendIndexes(index)
		newFrag.AppendFragRanges(currFrs)
	}
	newFrag.AddFragCount(int64(end - start))
	return newFrag, nil
}

func (r *detachedIndexReader) Next() (executor.IndexFrags, error) {
	if r.ctx.schema.Options().IsIncQuery() {
		currIter := r.ctx.schema.Options().GetIterId()
		queryId := r.ctx.schema.Options().GetLogQueryCurrId()
		if r.init {
			return nil, nil
		}
		var err error
		if currIter == 0 {
			frag, err := r.GetBatchFrag()
			if err != nil {
				return nil, err
			}
			if frag == nil {
				cache.PutNodeIterNum(queryId, 1)
				return nil, nil
			}
			immutable.PutDetachedSegmentTask(r.ctx.shardPath+queryId, frag)
			return r.InitInc(frag)
		} else {
			frag, ok := immutable.GetDetachedSegmentTask(r.ctx.shardPath + queryId)
			if !ok {
				frag, err = r.GetBatchFrag()
				if err != nil {
					return nil, err
				}
				if frag == nil {
					cache.PutNodeIterNum(queryId, 1)
					return nil, nil
				}
				immutable.PutDetachedSegmentTask(r.ctx.shardPath+queryId, frag)
				return r.InitInc(frag.(executor.IndexFrags))
			}
			r.init = true
			return r.InitInc(frag.(executor.IndexFrags))
		}
	} else {
		return r.GetBatchFrag()
	}
}

func (r *detachedIndexReader) GetBatchFrag() (executor.IndexFrags, error) {
	if !r.init {
		if err := r.Init(); err != nil {
			return nil, err
		}
		r.init = true
	}
	if r.info == nil || len(r.info.Files()) == 0 {
		return nil, nil
	}
	var err error
	dataFiles, pkInfos := r.info.Files(), r.info.Infos()
	frags := executor.NewDetachedFrags(r.dataPath, len(dataFiles))
	for r.idx < len(dataFiles) {
		dataFile, pkInfo := dataFiles[r.idx], pkInfos[r.idx]
		var frs fragment.FragmentRanges
		mark := fragment.NewIndexFragmentFixedSize(uint32(pkInfo.EndBlockId-pkInfo.StartBlockId), uint64(util.RowsNumPerFragment))
		frs, err = r.ctx.pkIndexReader.Scan(strconv.Itoa(int(pkInfo.StartBlockId)), pkInfo.Data, mark, r.ctx.keyCondition)
		if err != nil {
			return nil, err
		}
		if frs.Empty() {
			r.idx++
			continue
		}
		for j := range r.skFileReader {
			if err = r.skFileReader[j].ReInit(sparseindex.NewOBSFilterPath(r.localPath, r.dataPath, r.obsOptions)); err != nil {
				return nil, err
			}
			for k := range frs {
				frs[k].Start += uint32(pkInfo.StartBlockId)
				frs[k].End += uint32(pkInfo.StartBlockId)
			}
			frs, err = r.ctx.skIndexReader.Scan(r.skFileReader[j], frs)
			if err != nil {
				return nil, err
			}
			if frs.Empty() {
				break
			}
			for k := range frs {
				frs[k].Start -= uint32(pkInfo.StartBlockId)
				frs[k].End -= uint32(pkInfo.StartBlockId)
			}
		}
		var fragmentCount uint32
		for j := range frs {
			fragmentCount += frs[j].End - frs[j].Start
		}
		if fragmentCount == 0 {
			r.idx++
			continue
		}
		frags.AppendIndexes(dataFile)
		frags.AppendFragRanges(frs)
		frags.AddFragCount(int64(fragmentCount))
		if r.ctx.readSegmentBatch && int(frags.FragCount()) >= r.ctx.segmentBatchCount {
			r.idx++
			return frags, nil
		}
		r.idx++
	}
	if frags.FragCount() == 0 {
		return nil, nil
	}
	return frags, nil
}

func initKeyCondition(pkSchema record.Schemas, ctx *indexContext) error {
	tIdx := pkSchema.FieldIndex(record.TimeField)
	condition := ctx.schema.Options().GetCondition()
	timePrimaryCond := binaryfilterfunc.GetTimeCondition(ctx.tr, pkSchema, tIdx)
	timeClusterCond := binaryfilterfunc.GetTimeCondition(ctx.tr, pkSchema, tIdx)
	timeCondition := binaryfilterfunc.CombineConditionWithAnd(timePrimaryCond, timeClusterCond)
	var err error
	if ctx.keyCondition, err = sparseindex.NewKeyCondition(timeCondition, condition, pkSchema); err != nil {
		return err
	}
	return nil
}
