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

package sparseindex

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
)

var _ = RegistrySKFileReaderCreator(index.BloomFilterFullTextIndex, &BloomFilterFullTextReaderCreator{})

type BloomFilterFullTextReaderCreator struct {
}

func (index *BloomFilterFullTextReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewBloomFilterFullTextIndexReader(rpnExpr, schema, option, isCache)
}

type BloomFilterFullTextIndexReader struct {
	isCache bool
	version uint32
	schema  record.Schemas
	option  hybridqp.Options
	bf      rpn.SKBaseReader
	sk      SKCondition
}

func NewBloomFilterFullTextIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*BloomFilterFullTextIndexReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	return &BloomFilterFullTextIndexReader{schema: schema, option: option, isCache: isCache, version: 4, sk: sk}, nil
}

func (r *BloomFilterFullTextIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r.bf)
}

func (r *BloomFilterFullTextIndexReader) ReInit(file interface{}) (err error) {
	splitMap := make(map[string][]byte)
	expr := make([]*bloomfilter.SKRPNElement, 0, len(r.schema))
	splitMap[r.schema[0].Name] = tokenizer.CONTENT_SPLIT_TABLE
	for _, v := range r.schema {
		splitMap[v.Name] = tokenizer.CONTENT_SPLIT_TABLE
	}
	splitMap[logparser.DefaultFieldForFullText] = tokenizer.CONTENT_SPLIT_TABLE
	for _, elem := range r.sk.(*SKConditionImpl).rpn {
		if elem.RPNOp == rpn.InRange || elem.RPNOp == rpn.NotInRange {
			expr = append(expr, bloomfilter.NewSKRPNElement(elem.Key, elem.Value.(string)))
		}
	}
	if f, ok := file.(*OBSFilterPath); ok {
		fileName := BloomFilterFilePrefix + FullTextIndex + BloomFilterFileSuffix
		if f.localPath != "" {
			r.bf, err = bloomfilter.NewMultiFieldFilterReader(f.option, expr, splitMap, true, true, r.version, f.localPath, f.remotePath, fileName, fileName)
		} else {
			r.bf, err = bloomfilter.NewMultiFieldVerticalFilterReader(f.RemotePath(), f.option, expr, r.version, splitMap, fileName)
		}
		if err != nil {
			return err
		}
		return nil
	} else if f1, ok := file.(TsspFile); ok {
		index := strings.LastIndex(f1.Path(), "/")
		path := f1.Path()[:index]
		fileName := f1.Path()[index+1:]
		fileNameSlice := strings.Split(fileName, ".")
		fileName = fileNameSlice[0] + "." + BloomFilterFilePrefix + FullTextIndex + colstore.BloomFilterIndexFileSuffix
		r.bf, err = bloomfilter.NewMultiFiledLineFilterReader(path, nil, expr, r.version, splitMap, fileName)

		if err != nil {
			return err
		}
		return nil
	} else {
		return fmt.Errorf("not support file type")
	}
}

func (r *BloomFilterFullTextIndexReader) Close() error {
	return nil
}

func GetFullTextDetachFilePath(dir, msName string) string {
	return path.Join(dir, msName, BloomFilterFilePrefix+FullTextIndex+BloomFilterFileSuffix)
}

func GetFullTextAttachFilePath(dir, msName, dataFilePath string) string {
	return path.Join(dir, msName, colstore.AppendSKIndexSuffix(dataFilePath, FullTextIndex, index.BloomFilterFullTextIndex))
}

type FullTextIdxWriter struct {
	*skipIndexWriter
}

func NewFullTextIdxWriter(dir, msName, dataFilePath, lockPath string) *FullTextIdxWriter {
	return &FullTextIdxWriter{
		newSkipIndexWriter(dir, msName, dataFilePath, lockPath),
	}
}

func (f *FullTextIdxWriter) Open() error {
	return nil
}

func (f *FullTextIdxWriter) Close() error {
	return nil
}

func (f *FullTextIdxWriter) getFullTextIdxFilePath(detached bool) string {
	if detached {
		return GetFullTextDetachFilePath(f.dir, f.msName)
	}
	return GetFullTextAttachFilePath(f.dir, f.msName, f.dataFilePath)
}

func (f *FullTextIdxWriter) CreateAttachSkipIndex(schemaIdx, rowsPerSegment []int, writeRec *record.Record) error {
	indexBuf := logstore.GetIndexBuf(FullTextIdxColumnCnt)
	defer logstore.PutIndexBuf(indexBuf)

	data, skipIndexFilePaths := f.createSkipIndex(writeRec, schemaIdx, rowsPerSegment, *indexBuf, false)
	return writeSkipIndexToDisk(data[0], f.lockPath, skipIndexFilePaths[0])
}

func (f *FullTextIdxWriter) CreateDetachSkipIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return f.createSkipIndex(writeRec, schemaIdx, rowsPerSegment, dataBuf, true)
}

func (f *FullTextIdxWriter) createSkipIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, memBfData [][]byte,
	detached bool) ([][]byte, []string) {
	var skipIndexFilePaths []string
	skipIndexFilePaths = append(skipIndexFilePaths, f.getFullTextIdxFilePath(detached))
	memBfData[0] = append(memBfData[0], f.genFullTextIndexData(writeRec, schemaIdx, rowsPerSegment)...)
	return memBfData, skipIndexFilePaths
}

func (f *FullTextIdxWriter) genFullTextIndexData(writeRec *record.Record, schemaIdx, rowsPerSegment []int) []byte {
	tk := tokenizer.NewGramTokenizer(tokenizer.CONTENT_SPLITTER, 0, logstore.GramTokenizerVersion)
	segCnt := len(rowsPerSegment)
	segBfSize := int(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize)
	res := make([]byte, segCnt*segBfSize)

	start := 0
	end := 0
	colsData := f.getFullTextColsData(writeRec, schemaIdx, rowsPerSegment)
	row, col := len(colsData), len(colsData[0])
	for i := 0; i < col; i++ {
		end = start + segBfSize
		for j := 0; j < row; j++ {
			offs, lens := colsData[j][i].GetOffsAndLens()
			tk.ProcessTokenizerBatch(colsData[j][i].Val, res[start:end-crcSize], offs, lens)
		}
		crc := crc32.Checksum(res[start:end-crcSize], logstore.Table)
		binary.LittleEndian.PutUint32(res[end-crcSize:end], crc)
		start = end
	}
	return res
}

func (f *FullTextIdxWriter) getFullTextColsData(writeRec *record.Record, schemaIdx, rowsPerSegment []int) [][]record.ColVal {
	colsData := make([][]record.ColVal, 0, len(schemaIdx))
	for _, v := range schemaIdx {
		var colData []record.ColVal
		colData = writeRec.ColVals[v].SplitColBySize(colData, rowsPerSegment, writeRec.Schema[v].Type)
		colsData = append(colsData, colData)
	}
	return colsData
}
