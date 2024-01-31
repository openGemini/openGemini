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

package logstore

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	GramTokenizerVersion       = 3
	CurrentLogTokenizerVersion = 5
)

var (
	Table = crc32.MakeTable(crc32.Castagnoli)
)

func FlushVerticalFilter(filterVerBuffer, filterLogBuffer []byte) []byte {
	bufCursor := 0
	constant := GetConstant(CurrentLogTokenizerVersion)
	for i := 0; i < int(constant.VerticalPieceCntPerFilter); i++ {
		for j := 0; j < int(constant.FilterCntPerVerticalGorup); j++ {
			offset := (j * int(constant.FilterDataDiskSize)) + (i * util.Int64SizeBytes)
			val := binary.LittleEndian.Uint64(filterLogBuffer[offset:])
			filterVerBuffer = binary.LittleEndian.AppendUint64(filterVerBuffer, val)
		}
		crc := crc32.Checksum(filterVerBuffer[bufCursor:bufCursor+int(constant.VerticalPieceMemSize)], Table)
		filterVerBuffer = binary.LittleEndian.AppendUint32(filterVerBuffer, crc)
		bufCursor += int(constant.VerticalPieceDiskSize)
	}
	return filterVerBuffer
}

// GenSchemaIdxs get bloom filter cols index in the schema
func GenSchemaIdxs(schema record.Schemas, skipIndexRelation *influxql.IndexRelation, fullTextIdx bool) []int {
	var res []int
	if fullTextIdx {
		for i := range schema {
			if schema[i].IsString() {
				res = append(res, i)
			}
		}
		return res
	}

	for i := range skipIndexRelation.IndexNames {
		if skipIndexRelation.IndexNames[i] != index.BloomFilterIndex {
			continue
		}
		res = make([]int, 0, len(skipIndexRelation.IndexList[i].IList))
		for idx := range skipIndexRelation.IndexList[i].IList {
			for j := range schema {
				if skipIndexRelation.IndexList[i].IList[idx] == schema[j].Name {
					res = append(res, j)
				}
			}
		}
	}
	return res
}

func IsFullTextIdx(indexRelation *influxql.IndexRelation) bool {
	if indexRelation == nil {
		return false
	}

	for i := range indexRelation.IndexNames {
		if indexRelation.IndexNames[i] == index.BloomFilterFullTextIndex {
			return true
		}
	}
	return false
}

var BloomFilterBufferPool sync.Pool

type multiCosBuf [][]byte

func GetBloomFilterBuf(cols int) *multiCosBuf {
	buf := BloomFilterBufferPool.Get()
	if buf == nil {
		multiBuf := make(multiCosBuf, cols)
		return &multiBuf
	}
	return buf.(*multiCosBuf)
}

func PutBloomFilterBuf(buf *multiCosBuf) {
	for i := range *buf {
		(*buf)[i] = (*buf)[i][:0]
	}
	BloomFilterBufferPool.Put(buf)
}
