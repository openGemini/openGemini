/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/request"
	"go.uber.org/zap"
)

const (
	MetaIndexSegmentNum = 16
)

type DetachedMetaDataReader struct {
	isSort      bool
	ch          chan *request.StreamReader
	r           fileops.BasicFileReader
	task        map[int]*SegmentTask
	positionMap map[int64]*position
}

func NewDetachedMetaDataReader(path string, obsOpts *obs.ObsOptions, isSort bool) (*DetachedMetaDataReader, error) {
	fd, err := obs.OpenObsFile(path, DataFile, obsOpts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedMetaDataReader{r: dr, isSort: isSort}, nil
}

type position struct {
	schemaIndex int
	segmentID   int
}

type SegmentTask struct {
	fieldNum int
	data     []*request.StreamReader
}

func (reader *DetachedMetaDataReader) InitReadBatch(s []*SegmentMeta, schema record.Schemas) {
	reader.task = make(map[int]*SegmentTask, len(s))
	reader.positionMap = make(map[int64]*position, len(s))

	offset := make([]int64, 0, len(s)*len(schema))
	length := make([]int64, 0, len(s)*len(schema))
	startSegmentIndex := -1
	endSegmentIndex := -1
	var currChunkMeta *ChunkMeta
	for startSegmentIndex < len(s)-1 {
		for endSegmentIndex < len(s)-1 {
			endSegmentIndex += 1
			currChunkMeta = s[endSegmentIndex].chunkMeta
			if s[endSegmentIndex].id == currChunkMeta.segmentCount()-1 {
				break
			}
			if endSegmentIndex == len(s)-1 || currChunkMeta != s[endSegmentIndex+1].chunkMeta {
				break
			}
			if currChunkMeta.colMeta[0].entries[s[endSegmentIndex].id+1].offset > currChunkMeta.colMeta[len(currChunkMeta.colMeta)-1].entries[s[endSegmentIndex].id].offset {
				break
			}
		}
		for schemaIndex, field := range schema {
			idx := currChunkMeta.columnIndex(&field)
			if idx == -1 {
				continue
			}
			tmpIndex := startSegmentIndex
			for tmpIndex < endSegmentIndex {
				tmpIndex += 1
				offset = append(offset, currChunkMeta.colMeta[idx].entries[s[tmpIndex].id].offset)
				length = append(length, int64(currChunkMeta.colMeta[idx].entries[s[tmpIndex].id].size))
				reader.positionMap[currChunkMeta.colMeta[idx].entries[s[tmpIndex].id].offset] = &position{schemaIndex: schemaIndex, segmentID: tmpIndex}
				if _, ok := reader.task[tmpIndex]; ok {
					reader.task[tmpIndex].fieldNum += 1
				} else {
					reader.task[tmpIndex] = &SegmentTask{
						fieldNum: 1,
						data:     make([]*request.StreamReader, len(schema)),
					}
				}
			}
		}
		startSegmentIndex = endSegmentIndex
	}

	reader.ch = make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, reader.ch, MetaIndexSegmentNum)
}
func (reader *DetachedMetaDataReader) ReadBatch(dst *record.Record, decs *ReadContext) (*record.Record, error) {
	schema := dst.Schema
	timeIndex := schema.Len() - 1
	finishSegment := -1
	var err error
	for r := range reader.ch {
		if r.Err != nil {
			return nil, r.Err
		}
		if len(r.Content) < crcSize {
			err = fmt.Errorf("write wrong data")
			break
		}
		if binary.BigEndian.Uint32(r.Content[:crcSize]) != crc32.ChecksumIEEE(r.Content[crcSize:]) {
			err = fmt.Errorf("write wrong data")
			break
		}
		r.Content = r.Content[crcSize:]
		if p, exist := reader.positionMap[r.Offset]; exist {
			reader.task[p.segmentID].data[p.schemaIndex] = r
			reader.task[p.segmentID].fieldNum--
			if reader.task[p.segmentID].fieldNum == 0 {
				finishSegment = p.segmentID
				break
			}
		} else {
			log.Error("read data fail", zap.String("file", reader.r.Name()), zap.Int64("chunk meta offset", r.Offset))
		}
	}
	if err != nil {
		for range reader.ch {
		}
		return nil, err
	}
	if finishSegment == -1 {
		if len(reader.task) != 0 {
			log.Error("read loss data fail", zap.String("file", reader.r.Name()))
		}
		return nil, nil
	}
	for _, v := range reader.task[finishSegment].data {
		if v == nil {
			continue
		}
		schemaIndex := reader.positionMap[v.Offset].schemaIndex
		var err error
		col := &record.ColVal{}
		if schemaIndex == timeIndex {
			err = reader.decodeTimeColumn(col, decs, v.Content)
		} else {
			err = decodeColumnData(&schema[schemaIndex], v.Content, col, decs, false)
		}
		dst.Column(schemaIndex).AppendColVal(col, dst.Schemas()[schemaIndex].Type, 0, col.Len)
		if err != nil {
			return nil, err
		}
	}
	delete(reader.task, finishSegment)
	dst.TryPadColumn()
	return dst, nil
}

func (reader *DetachedMetaDataReader) decodeTimeColumn(timeCol *record.ColVal, decs *ReadContext, tmData []byte) error {
	err := appendTimeColumnData(tmData, timeCol, decs, false)
	if err != nil {
		log.Error("decode time column fail", zap.Error(err))
	}
	return err
}
