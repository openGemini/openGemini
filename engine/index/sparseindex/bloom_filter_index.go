// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"go.uber.org/zap"
)

const (
	crcSize               = 4
	FullTextIdxColumnCnt  = 1
	tmpFileSuffix         = ".init"
	FullTextIndex         = "fullText"
	BloomFilterFileSuffix = ".idx"
	BloomFilterFilePrefix = "bloomfilter_" // bloomfilter_${columnName}.idx
)

type OBSFilterPath struct {
	localPath  string
	remotePath string
	option     *obs.ObsOptions
}

func NewOBSFilterPath(localPath, remotePath string, option *obs.ObsOptions) *OBSFilterPath {
	return &OBSFilterPath{localPath: localPath, remotePath: remotePath, option: option}
}

func (o *OBSFilterPath) LocalPath() string {
	return o.localPath
}

func (o *OBSFilterPath) RemotePath() string {
	return o.remotePath
}

func (o *OBSFilterPath) Name() string {
	return ""
}

func (o *OBSFilterPath) Option() *obs.ObsOptions {
	return o.option
}

var _ = RegistrySKFileReaderCreator(uint32(index.BloomFilter), &BloomFilterReaderCreator{})

type BloomFilterReaderCreator struct {
}

func (index *BloomFilterReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewBloomFilterIndexReader(rpnExpr, schema, option, isCache)
}

type BloomFilterIndexReader struct {
	isCache bool
	version uint32
	schema  record.Schemas
	option  hybridqp.Options
	bf      rpn.SKBaseReader
	sk      SKCondition
	span    *tracing.Span
}

func NewBloomFilterIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*BloomFilterIndexReader, error) {
	sk, err := NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	return &BloomFilterIndexReader{schema: schema, option: option, isCache: isCache, version: 4, sk: sk}, nil
}

func (r *BloomFilterIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r.bf)
}

func (r *BloomFilterIndexReader) StartSpan(span *tracing.Span) {
	r.span = span
}

func (r *BloomFilterIndexReader) ReInit(file interface{}) (err error) {
	var fullTextTokensTable []byte
	measurements := r.option.GetMeasurements()
	if len(measurements) == 0 {
		fullTextTokensTable = tokenizer.GetFullTextOption(nil).TokensTable
	} else {
		fullTextTokensTable = tokenizer.GetFullTextOption(measurements[0].IndexRelation).TokensTable
	}
	if f, ok := file.(*OBSFilterPath); ok {
		splitMap := make(map[string][]byte)
		splitMap[r.schema[0].Name] = fullTextTokensTable
		fileName := BloomFilterFilePrefix + r.schema[0].Name + BloomFilterFileSuffix
		if f.localPath != "" {
			r.bf, err = bloomfilter.NewFilterReader(f.Option(), r.option.GetCondition(), splitMap, true, true, r.version, f.localPath, f.remotePath, fileName, fileName)
		} else {
			r.bf, err = bloomfilter.NewVerticalFilterReader(f.RemotePath(), f.Option(), r.option.GetCondition(), r.version, splitMap, fileName)
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
		fileName = fileNameSlice[0] + "." + r.schema[0].Name + colstore.BloomFilterIndexFileSuffix
		splitMap := make(map[string][]byte)
		splitMap[r.schema[0].Name] = fullTextTokensTable
		r.bf, err = bloomfilter.NewLineFilterReader(path, nil, r.option.GetCondition(), r.version, splitMap, fileName)
		if err != nil {
			return err
		}
		return nil
	} else {
		return fmt.Errorf("not support file type")
	}
}

func (r *BloomFilterIndexReader) Close() error {
	return nil
}

// GetLocalBloomFilterBlockCnts get one local bloomFilter col's  block count,if not exist return 0
func GetLocalBloomFilterBlockCnts(dir, msName, lockPath string, recSchema record.Schemas, bfSchemaIdx int,
	fullTextIdx bool) int64 {
	var localPath string
	if fullTextIdx {
		localPath = GetFullTextDetachFilePath(dir, msName)
	} else {
		if bfSchemaIdx < 0 {
			logger.GetLogger().Info("bloom filter cols is not exist in the schema", zap.String("measurement name", msName))
			return 0
		}
		fieldName := recSchema[bfSchemaIdx].Name // get the first bloom filter col
		localPath = GetBloomFilterFilePath(dir, msName, fieldName)
	}

	lock := fileops.FileLockOption(lockPath)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(localPath, os.O_CREATE, 0640, lock, pri)
	defer func() {
		_ = fd.Close()
	}()
	if err != nil {
		logger.GetLogger().Error("open file fail", zap.String("name", localPath), zap.Error(err))
		panic(err)
	}

	localFileInfo, _ := fd.Stat()
	filterDataDiskSize := logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize
	return localFileInfo.Size() / filterDataDiskSize
}

func GetBloomFilterFilePath(dir, msName, fieldName string) string {
	return path.Join(dir, msName, BloomFilterFilePrefix+fieldName+BloomFilterFileSuffix)
}

type BloomFilterWriter struct {
	*skipIndexWriter
}

func NewBloomFilterWriter(dir, msName, dataFilePath, lockPath string, tokens string) *BloomFilterWriter {
	return &BloomFilterWriter{
		newSkipIndexWriter(dir, msName, dataFilePath, lockPath, tokens),
	}
}

func (b *BloomFilterWriter) Open() error {
	return nil
}

func (b *BloomFilterWriter) Close() error {
	return nil
}

func (b *BloomFilterWriter) getSkipIndexFilePath(fieldName string, detached bool) string {
	if detached {
		return GetBloomFilterFilePath(b.dir, b.msName, fieldName)
	}
	return path.Join(b.dir, b.msName, colstore.AppendSecondaryIndexSuffix(b.dataFilePath, fieldName, index.BloomFilter, 0)+tmpFileSuffix)
}

func (b *BloomFilterWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
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

func (b *BloomFilterWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int,
	dataBuf [][]byte) ([][]byte, []string) {
	skipIndexFilePaths := make([]string, len(schemaIdx))
	for k, v := range schemaIdx {
		data := b.GenBloomFilterData(&writeRec.ColVals[v], rowsPerSegment, writeRec.Schema[v].Type)
		dataBuf[k] = append(dataBuf[k], data...)
		skipIndexFilePaths[k] = b.getSkipIndexFilePath(writeRec.Schema[v].Name, true)
	}

	return dataBuf, skipIndexFilePaths
}

func (b *BloomFilterWriter) GenBloomFilterData(src *record.ColVal, rowsPerSegment []int, refType int) []byte {
	tk := tokenizer.NewGramTokenizer(b.fullTextTokens, 0, logstore.GramTokenizerVersion)

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
	tokenizer.FreeSimpleGramTokenizer(tk)
	return res
}
