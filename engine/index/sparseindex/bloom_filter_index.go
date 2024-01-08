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
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"go.uber.org/zap"
)

const (
	BloomFilterFilePrefix = "bloomfilter_" // bloomfilter_${columnName}.idx
	BloomFilterFileSuffix = ".idx"
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

var _ = RegistrySKFileReaderCreator(colstore.BloomFilterIndex, &BloomFilterReaderCreator{})

type BloomFilterReaderCreator struct {
}

func (index *BloomFilterReaderCreator) CreateSKFileReader(schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error) {
	return NewBloomFilterIndexReader(schema, option, isCache)
}

type BloomFilterIndexReader struct {
	isCache bool
	version uint32
	schema  record.Schemas
	option  hybridqp.Options
	bf      bloomfilter.BFReader
}

func NewBloomFilterIndexReader(schema record.Schemas, option hybridqp.Options, isCache bool) (*BloomFilterIndexReader, error) {
	return &BloomFilterIndexReader{schema: schema, option: option, isCache: isCache, version: 4}, nil
}

func (r *BloomFilterIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.bf.IsExist(int64(fragId))
}

func (r *BloomFilterIndexReader) ReInit(file interface{}) (err error) {
	if f, ok := file.(*OBSFilterPath); ok {
		splitMap := make(map[string][]byte)
		splitMap[r.schema[0].Name] = tokenizer.CONTENT_SPLIT_TABLE
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
		splitMap[r.schema[0].Name] = tokenizer.CONTENT_SPLIT_TABLE
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
func GetLocalBloomFilterBlockCnts(dir, msName, lockPath string, recSchema record.Schemas,
	indexRelation *influxql.IndexRelation) int64 {
	schemaIdx := logstore.GenSchemaIdxs(recSchema, indexRelation)
	if len(schemaIdx) == 0 {
		logger.GetLogger().Error("bloom filter cols is not exist in the schema", zap.String("measurement name", msName))
		return 0
	}
	fieldName := recSchema[schemaIdx[0]].Name
	filterDataDiskSize := logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize
	localPath := GetBloomFilterFilePath(dir, msName, fieldName)
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
	return localFileInfo.Size() / filterDataDiskSize
}

func GetBloomFilterFilePath(dir, msName, fieldName string) string {
	return path.Join(dir, msName, BloomFilterFilePrefix+fieldName+BloomFilterFileSuffix)
}
