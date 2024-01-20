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
	"path"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/bloomfilter"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
)

var _ = RegistrySKFileReaderCreator(colstore.BloomFilterFullTextIndex, &BloomFilterFullTextReaderCreator{})

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
		for _, rpn := range r.sk.(*SKConditionImpl).rpn {
			if rpn.Key == v.Name {
				expr = append(expr, bloomfilter.NewSKRPNElement(rpn.Key, rpn.Value.(string)))
			}
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

func GetFullTextIdxFilePath(dir, msName string) string {
	return path.Join(dir, msName, BloomFilterFilePrefix+FullTextIndex+BloomFilterFileSuffix)
}
