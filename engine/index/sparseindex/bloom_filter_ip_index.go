// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package sparseindex

import (
	"encoding/binary"
	"hash/crc32"
	"path"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
)

var _ = RegistrySKFileReaderCreator(uint32(index.BloomFilterIp), &BloomFilterIpReaderCreator{})

type BloomFilterIpReaderCreator struct{}

func (b *BloomFilterIpReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewBloomFilterIndexReaderWithIndexType(rpnExpr, schema, option, isCache, index.BloomFilterIp)
}

type BloomFilterIpIndexWriter struct {
	*skipIndexWriter
}

func NewBloomFilterIpWriter(dir, msName, dataFilePath, lockPath string, tokens string) *BloomFilterIpIndexWriter {
	return &BloomFilterIpIndexWriter{
		newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}
}

func (b *BloomFilterIpIndexWriter) Open() error {
	return nil
}

func (b *BloomFilterIpIndexWriter) Close() error {
	return nil
}

func (b *BloomFilterIpIndexWriter) getSkipIndexFilePath(fieldName string, detached bool) string {
	if detached {
		return GetBloomFilterFilePath(b.dir, b.msName, fieldName)
	}
	return path.Join(b.dir, b.msName, colstore.AppendSecondaryIndexSuffix(b.dataFilePath, fieldName, index.BloomFilterIp, 0)+tmpFileSuffix)
}

func (b *BloomFilterIpIndexWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	var err error
	var data []byte
	var skipIndexFilePath string
	for _, i := range schemaIdx {
		skipIndexFilePath = b.getSkipIndexFilePath(writeRec.Schema[i].Name, false)
		data = b.GenBloomFilterData(&writeRec.ColVals[i], rowsPerSegment, writeRec.Schema[i].Type)

		err = writeSkipIndexToDisk(data, b.lockPath, skipIndexFilePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BloomFilterIpIndexWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int,
	dataBuf [][]byte) ([][]byte, []string) {
	// leave detach
	return nil, nil
}

func (b *BloomFilterIpIndexWriter) GenBloomFilterData(src *record.ColVal, rowsPerSegment []int, refType int) []byte {
	tk := tokenizer.NewIpTokenizer()
	defer tokenizer.FreeSimpleGramTokenizer(tk)

	segCnt := len(rowsPerSegment)
	segBfSize := int(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize)
	res := make([]byte, segCnt*segBfSize)
	var segCol []record.ColVal
	segCol = src.SplitColBySize(segCol, rowsPerSegment, refType)

	start := 0
	end := 0
	for _, col := range segCol {
		end = start + segBfSize
		offs, lens := col.GetOffsAndLens()
		tk.ProcessTokenizerBatch(col.Val, res[start:end-crcSize], offs, lens)
		crc := crc32.Checksum(res[start:end-crcSize], logstore.Table)
		binary.LittleEndian.PutUint32(res[end-crcSize:end], crc)
		start = end
	}
	return res
}
