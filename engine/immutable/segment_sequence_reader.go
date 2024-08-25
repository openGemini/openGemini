/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/consume"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const MetaIndexConsumeNum = 16

type SegmentSequenceReader struct {
	isEnd          bool
	metaIndexID    int64
	endMetaIndexId int64
	start, end     uint64
	skIndexReader  sparseindex.SKIndexReader
	skFileReader   []sparseindex.SKFileReader
	filterBitmap   *bitmap.FilterBitmap
	filterPool     *record.CircularRecordPool
	filterOpt      *FilterOptions
	tr             util.TimeRange
	path           *sparseindex.OBSFilterPath
	cond           influxql.Expr
	schema         record.Schemas
}

func NewSegmentSequenceReader(path *sparseindex.OBSFilterPath, taskID int, count uint64, consumeInfo *consume.ConsumeInfo, schema record.Schemas, filterOpt *FilterOptions) (*SegmentSequenceReader, error) {
	var endMetaIndexId int
	if consumeInfo.GetEndCursor() != nil && len(consumeInfo.GetEndCursor().Tasks) != 0 && consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.SgID == consumeInfo.GetEndCursor().Tasks[taskID].CurrTask.SgID {
		endMetaIndexId = consumeInfo.GetEndCursor().Tasks[taskID].CurrTask.MetaIndexId
	} else {
		endMetaIndexId = -1
	}

	segment := &SegmentSequenceReader{
		filterOpt:      filterOpt,
		path:           path,
		filterBitmap:   consumeInfo.FilterBitmap,
		endMetaIndexId: int64(endMetaIndexId),
		metaIndexID:    int64(consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.MetaIndexId),
		start:          consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.BlockID,
		end:            consumeInfo.GetFromCursor().Tasks[taskID].CurrTask.BlockID + count,
		tr:             consumeInfo.GetTr(),
		cond:           consumeInfo.GetCond(),
		schema:         schema,
	}
	if segment.cond != nil {
		segment.filterPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, schema, false)
		mstIndex := &influxql.Measurement{
			IndexRelation: &consumeInfo.GetMst().IndexRelation,
		}
		segment.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)
		var err error
		segment.skFileReader, err = segment.skIndexReader.CreateSKFileReaders(consumeInfo.GetProcessOption(), mstIndex, false)
		if err != nil {
			return nil, err
		}
		for j := range segment.skFileReader {
			if err = segment.skFileReader[j].ReInit(segment.path); err != nil {
				return nil, err
			}
		}
	}
	return segment, nil
}

func (reader *SegmentSequenceReader) ConsumeDateByShard() ([]map[string]interface{}, bool, int64, uint64, int64, error) {
	chunkCount, err := GetMetaIndexChunkCount(reader.path.Option(), reader.path.RemotePath())
	if err != nil {
		return nil, false, 0, 0, 0, err
	}
	if chunkCount == 0 {
		return nil, true, 0, 0, 0, nil
	}
	pkData, err := reader.getPKData(chunkCount)
	if err != nil {
		return nil, false, 0, 0, 0, err
	}
	metaIndex, blocks, scanMetaIndex, err := reader.getMetaIndexAndBlocks(pkData, chunkCount)
	if err != nil {
		return nil, false, 0, 0, 0, err
	}

	chunkMetaReader, err := NewDetachedChunkMetaReader(reader.path.RemotePath(), reader.path.Option())
	if err != nil {
		return nil, false, 0, 0, 0, err
	}
	offset := make([]int64, 0, len(metaIndex))
	sizes := make([]int64, 0, len(metaIndex))
	for i := 0; i < len(metaIndex); i++ {
		offset = append(offset, metaIndex[i].GetOffset())
		sizes = append(sizes, int64(metaIndex[i].GetSize()))
	}
	currChunkMeta, err := chunkMetaReader.ReadChunkMeta(offset, sizes)
	if err != nil {
		return nil, false, 0, 0, 0, err
	}
	segmentMetas := make([]*SegmentMeta, 0)
	for k, chunkMeta := range currChunkMeta {
		for _, block := range blocks[k] {
			if reader.tr.Min <= chunkMeta.GetTimeRangeBy(block)[1] && reader.tr.Max >= chunkMeta.GetTimeRangeBy(block)[0] {
				segmentMetas = append(segmentMetas, NewSegmentMeta(block, chunkMeta))
			}
		}
	}

	dataReader, err := NewDetachedMetaDataReader(reader.path.RemotePath(), reader.path.Option(), true)
	if err != nil {
		return nil, false, 0, 0, 0, err
	}
	defer dataReader.Close()
	dataReader.InitReadBatch(segmentMetas, reader.schema)
	readCtx := NewReadContext(true)
	recordPool := record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, reader.schema, false)
	defer recordPool.PutRecordInCircularPool()
	logs := make([]map[string]interface{}, 0)
	var size int64
	for {
		r := recordPool.Get()
		result, err := dataReader.ReadBatch(r, readCtx)
		if err != nil {
			return nil, false, 0, 0, 0, err
		}
		if result == nil {
			break
		}
		result = reader.filterData(result)
		if IsInterfaceNil(result) || result.RowNums() == 0 {
			recordPool.PutRecordInCircularPool()
			if reader.cond != nil {
				reader.filterPool.PutRecordInCircularPool()
			}
			continue
		}
		currLogs, s := reader.getLogsBy(result)
		size += s
		logs = append(logs, currLogs...)
	}
	return logs, reader.isEnd, size, reader.end, scanMetaIndex, nil
}

func (reader *SegmentSequenceReader) filterData(rec *record.Record) *record.Record {
	if rec != nil {
		rec = FilterByTime(rec, reader.tr)
	}
	if rec != nil && reader.cond != nil {
		rec = FilterByField(rec, reader.filterPool.Get(), reader.filterOpt.options, reader.filterOpt.cond, reader.filterOpt.rowFilters, reader.filterOpt.pointTags, reader.filterBitmap, &reader.filterOpt.colAux)
	}

	return rec
}

func (reader *SegmentSequenceReader) getLogsBy(result *record.Record) ([]map[string]interface{}, int64) {
	logs := make([]map[string]interface{}, result.RowNums())
	for k := range logs {
		logs[k] = make(map[string]interface{})
	}
	var size int64
	for index, v := range result.Schema {
		if v.Name == record.SeqIDField || v.Name == influxql.ShardIDField {
			continue
		}
		size += int64(result.Column(index).GetValLen())
		switch v.Type {
		case influx.Field_Type_Int:
			cols := result.Column(index).IntegerValues()
			for k, col := range cols {
				logs[k][v.Name] = col
			}
		case influx.Field_Type_Boolean:
			cols := result.Column(index).BooleanValues()
			for k, col := range cols {
				logs[k][v.Name] = col
			}
		case influx.Field_Type_String:
			cols := result.Column(index).StringValues(nil)
			for k, col := range cols {
				logs[k][v.Name] = col
			}
		case influx.Field_Type_Float:
			cols := result.Column(index).FloatValues()
			for k, col := range cols {
				logs[k][v.Name] = col
			}
		}
	}
	return logs, size
}

func (reader *SegmentSequenceReader) getMetaIndexAndBlocks(pkData []*colstore.DetachedPKInfo, chunkCount int64) ([]*MetaIndex, [][]int, int64, error) {
	filterMetaIndexes := make([]*MetaIndex, 0)
	blocks := make([][]int, 0)
	currMetaIndexId := reader.metaIndexID
	var lastMetaIndexId, scanMetaIndexId int64
	isContinue := true
	for isContinue {
		if currMetaIndexId == chunkCount || (reader.endMetaIndexId != -1 && currMetaIndexId > reader.endMetaIndexId) {
			break
		}
		lastMetaIndexId = currMetaIndexId
		if reader.endMetaIndexId != -1 && reader.metaIndexID+int64(MetaIndexConsumeNum) > reader.endMetaIndexId {
			currMetaIndexId = reader.endMetaIndexId + 1
		} else if reader.metaIndexID+int64(MetaIndexConsumeNum) > chunkCount {
			currMetaIndexId = chunkCount
		} else {
			currMetaIndexId = reader.metaIndexID + MetaIndexConsumeNum
		}
		offsets, lengths := make([]int64, 0, currMetaIndexId), make([]int64, 0, chunkCount)
		for i := lastMetaIndexId; i < currMetaIndexId; i++ {
			offset, length := GetMetaIndexOffsetAndLengthByChunkId(i)
			offsets, lengths = append(offsets, offset), append(lengths, length)
		}

		metaIndexReader, err := NewDetachedMetaIndexReader(reader.path.RemotePath(), reader.path.Option())
		if err != nil {
			metaIndexReader.Close()
			return nil, nil, 0, err
		}
		if len(offsets) == 0 {
			return nil, nil, lastMetaIndexId, nil
		}
		metaIndexes, err := metaIndexReader.ReadMetaIndex(offsets, lengths)
		if err != nil {
			return nil, nil, 0, err
		}
		for k, v := range metaIndexes {
			if pkData[lastMetaIndexId+int64(k)].EndBlockId <= reader.end {
				if reader.endMetaIndexId != -1 && reader.endMetaIndexId >= lastMetaIndexId+int64(k) || (reader.endMetaIndexId == -1 && lastMetaIndexId+int64(k) >= chunkCount-1) {
					reader.isEnd = true
				}
			}
			if !pkData[lastMetaIndexId+int64(k)].IsBlockExist(reader.start, reader.end) {
				isContinue = false
				break
			}
			if v.IsExist(reader.tr) {
				filterMetaIndexes = append(filterMetaIndexes, v)
				currBlocks, err := reader.getBlockBy(pkData[lastMetaIndexId+int64(k)])
				if err != nil {
					metaIndexReader.Close()
					return nil, nil, 0, err
				}
				blocks = append(blocks, currBlocks)
				scanMetaIndexId = reader.metaIndexID + int64(k)
			}
		}
		metaIndexReader.Close()
	}
	return filterMetaIndexes, blocks, scanMetaIndexId, nil
}

func (reader *SegmentSequenceReader) getBlockBy(pkData *colstore.DetachedPKInfo) ([]int, error) {
	var filterStart, filterEnd uint32
	if reader.start > pkData.GetStartBlockId() {
		filterStart = uint32(reader.start)
	} else {
		filterStart = uint32(pkData.GetStartBlockId())
	}
	if reader.end < pkData.GetEndBlockId() {
		filterEnd = uint32(reader.end)
	} else {
		filterEnd = uint32(pkData.GetEndBlockId())
	}
	currBlocks := make([]int, filterEnd-filterStart)
	frs := fragment.FragmentRanges{{Start: filterStart, End: filterEnd}}
	if reader.cond != nil {
		var err error
		for j := range reader.skFileReader {
			frs, err = reader.skIndexReader.Scan(reader.skFileReader[j], frs)
			if err != nil {
				return nil, err
			}
			if frs.Empty() {
				break
			}
		}
	}
	for _, v := range frs {
		midStart := int(v.Start) - int(pkData.GetStartBlockId())
		for i := 0; i < int(v.End-v.Start); i++ {
			currBlocks[i] = midStart + i
		}
	}
	return currBlocks, nil
}

func (reader *SegmentSequenceReader) getPKData(chunkCount int64) ([]*colstore.DetachedPKInfo, error) {
	pkMetaInfo, err := ReadPKMetaInfoAll(reader.path.RemotePath(), reader.path.Option())
	if err != nil {
		return nil, err
	}

	offsets, lengths := make([]int64, 0, chunkCount), make([]int64, 0, chunkCount)
	for k := 0; k < int(chunkCount); k++ {
		offset, length := GetPKMetaOffsetLengthByChunkId(pkMetaInfo, k)
		offsets, lengths = append(offsets, offset), append(lengths, length)
	}
	pkMetas, err := ReadPKMetaAll(reader.path.RemotePath(), reader.path.Option(), offsets, lengths)
	if err != nil {
		return nil, err
	}
	offsets, lengths = offsets[:0], lengths[:0]
	for i := range pkMetas {
		offsets, lengths = append(offsets, int64(pkMetas[i].Offset)), append(lengths, int64(pkMetas[i].Length))
	}
	pkDatas, err := ReadPKDataAll(reader.path.RemotePath(), reader.path.Option(), offsets, lengths, pkMetas, pkMetaInfo)
	if err != nil {
		return nil, err
	}
	var pkItems []*colstore.DetachedPKInfo
	for i := range pkDatas {
		pkItems = append(pkItems, colstore.GetPKInfoByPKMetaData(pkMetas[i], pkDatas[i], pkMetaInfo.TCLocation))
	}
	return pkItems, nil
}

func (reader *SegmentSequenceReader) Close() {
	if reader.filterPool != nil {
		reader.filterPool.Put()
	}
}

func GetCursorsBy(path *sparseindex.OBSFilterPath, tr util.TimeRange, isAscending bool) (int, uint64, error) {
	chunkCount, err := GetMetaIndexChunkCount(path.Option(), path.RemotePath())
	if err != nil {
		return -1, 0, err
	}
	if chunkCount == 0 {
		return -1, 0, nil
	}

	offsets, lengths := make([]int64, 0, chunkCount), make([]int64, 0, chunkCount)
	for i := int64(0); i < chunkCount; i++ {
		offset, length := GetMetaIndexOffsetAndLengthByChunkId(i)
		offsets, lengths = append(offsets, offset), append(lengths, length)
	}

	metaIndexReader, err := NewDetachedMetaIndexReader(path.RemotePath(), path.Option())
	if err != nil {
		return -1, 0, err
	}
	defer metaIndexReader.Close()

	metaIndexes, err := metaIndexReader.ReadMetaIndex(offsets, lengths)
	if err != nil {
		return -1, 0, err
	}
	metaIndexK := -1
	metaIndexId := uint64(0)

	if isAscending {
		for k, m := range metaIndexes {
			if m.IsExist(tr) {
				metaIndexK = k
				metaIndexId = m.GetID()
				break
			}
		}
	} else {
		for i := len(metaIndexes) - 1; i >= 0; i-- {
			if metaIndexes[i].IsExist(tr) {
				metaIndexK = i
				metaIndexId = metaIndexes[i].GetID()
				break
			}
		}
	}
	return metaIndexK, metaIndexId, nil
}
