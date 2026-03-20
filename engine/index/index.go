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

package index

import (
	"path/filepath"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/index/textindex"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type IndexWriter interface {
	Open()
	Close() error
	Files() []string
	CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error
	CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string)
	Flush() error
	FlushSegment() error
}

type IndexWriterBuilder struct {
	bfIdx        int // if bloomFilter exist, the idx in schemaIdxes is bfIdx
	fullTextIdx  int // if bloomFilter-fullTextIdx exist, the idx in schemaIdxes is bloomFilter-fullTextIdx
	indexWriters []IndexWriter
	schemaIdxes  [][]int // each skip index contains a schemaIdx to indicate the corresponding position of the index column, but under schemaLess, its content may be empty.
}

func NewIndexWriterBuilder() *IndexWriterBuilder {
	return &IndexWriterBuilder{
		bfIdx:       -1,
		fullTextIdx: -1,
	}
}

func GetSchemaIndex(schema record.Schemas, oid uint32, IList []string) []int {
	if oid == uint32(indextype.BloomFilterFullText) {
		return schema.StringFieldIndex()
	}
	schemaMap := schema.FieldIndexMap()
	schemaIdxes := make([]int, 0, len(IList))
	for i := range IList {
		schemaIdx, ok := schemaMap[IList[i]]
		if ok {
			schemaIdxes = append(schemaIdxes, schemaIdx)
		}
	}
	return schemaIdxes
}

func NewIndexWriter(dir, msName, dataFilePath, lockPath string, indexRelation influxql.IndexRelation, i int, tokens string) IndexWriter {
	indexType := indextype.IndexType(indexRelation.Oids[i])
	switch indexType {
	case indextype.BloomFilter:
		return sparseindex.NewBloomFilterWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.BloomFilterIp:
		return sparseindex.NewBloomFilterIpWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.BloomFilterFullText:
		return sparseindex.NewBloomFilterFullTextWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.BloomFilterUniversal:
		var params *influxql.IndexParam
		if len(indexRelation.IndexParam) > i {
			params = indexRelation.IndexParam[i]
		} else {
			params = &influxql.IndexParam{
				IList: []influxql.Expr{&influxql.NumberLiteral{Val: sparseindex.DefaultFalseRate}},
			}
		}

		return sparseindex.NewUniversalGeneralBloomFilterWriter(dir, msName, dataFilePath, lockPath, tokens, params)
	case indextype.Set:
		return sparseindex.NewSetWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.MinMax:
		return sparseindex.NewMinMaxWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.Text:
		return textindex.NewTextIndexWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.Spatial:
		return sparseindex.NewSpatialWriter(filepath.Join(dir, msName), dataFilePath, lockPath, indexRelation.IndexList[i].IList)
	default:
		logger.GetLogger().Error("unknown skip index type")
		return nil
	}
}

func NewPreAggMapWriter(dir, msName, dataFilePath, lockPath string) IndexWriter {
	return sparseindex.NewPreAggMapWriter(dir, msName, dataFilePath, lockPath, "")
}

func (s *IndexWriterBuilder) NewIndexWriters(dir, msName, dataFilePath, lockPath string, schema record.Schemas, pkMap map[string]struct{},
	indexRelation influxql.IndexRelation) map[string][]IndexWriter {
	s.indexWriters = make([]IndexWriter, 0, len(indexRelation.Oids)+1)
	writerMap := make(map[string][]IndexWriter) // key is the column name

	indexWriter := NewPreAggMapWriter(dir, msName, dataFilePath, lockPath)
	schemaIdx := make([]int, 0, len(schema))
	for i, field := range schema {
		name := field.Name
		if _, exists := pkMap[name]; !exists {
			writerMap[name] = append(writerMap[name], indexWriter)
			schemaIdx = append(schemaIdx, i)
		}
	}
	s.schemaIdxes = append(s.schemaIdxes, schemaIdx)
	s.indexWriters = append(s.indexWriters, indexWriter)

	for i := range indexRelation.Oids {
		if indexRelation.Oids[i] == uint32(indextype.TimeCluster) {
			continue
		}
		var tokens string
		if i < len(indexRelation.IndexOptions) {
			indexOpt := indexRelation.IndexOptions[i]
			if indexOpt != nil && len(indexOpt.Options) != 0 {
				tokens = indexOpt.Options[0].Tokens
			}
		} else {
			tokens = tokenizer.GetFullTextOption(&indexRelation).Tokens
		}
		indexWriter := NewIndexWriter(dir, msName, dataFilePath, lockPath, indexRelation, i, tokens)
		if indexWriter == nil {
			continue
		}
		if indexRelation.Oids[i] == uint32(indextype.BloomFilter) {
			s.bfIdx = len(s.indexWriters)
		}
		if indexRelation.Oids[i] == uint32(indextype.BloomFilterFullText) {
			s.fullTextIdx = len(s.indexWriters)
		}

		iList := indexRelation.IndexList[i].IList
		for j := range iList {
			name := iList[j]
			writerMap[name] = append(writerMap[name], indexWriter)
		}
		schemaIdx := GetSchemaIndex(schema, indexRelation.Oids[i], indexRelation.IndexList[i].IList)
		s.schemaIdxes = append(s.schemaIdxes, schemaIdx)
		s.indexWriters = append(s.indexWriters, indexWriter)
	}
	return writerMap
}

// GetBfIdx get idx in schemaIdxes if bloom filter index exist bfIdx >= 0
func (s *IndexWriterBuilder) GetBfIdx() int {
	return s.bfIdx
}

func (s *IndexWriterBuilder) GetBfFirstSchemaIdx() int {
	if s.bfIdx < 0 || len(s.schemaIdxes[s.bfIdx]) <= s.bfIdx {
		return -1
	}
	return s.schemaIdxes[s.bfIdx][0]
}

// GetFullTextIdx get idx in schemaIdxes if full text index exist fullTextIdx >= 0
func (s *IndexWriterBuilder) GetFullTextIdx() int {
	return s.fullTextIdx
}

func (s *IndexWriterBuilder) GetSchemaIdxes() [][]int {
	return s.schemaIdxes
}

func (s *IndexWriterBuilder) GetSkipIndexWriters() []IndexWriter {
	return s.indexWriters
}

func (s *IndexWriterBuilder) BuildSkipIndexInfo() []*colstore.SkipIndexInfo {
	infos := make([]*colstore.SkipIndexInfo, 0)
	for _, w := range s.GetSkipIndexWriters() {
		for _, f := range w.Files() {
			infos = append(infos, colstore.NewSkipIndexInfo(f))
		}
	}
	return infos
}

func (s *IndexWriterBuilder) CloseSkipIndexWriters() {
	for _, w := range s.GetSkipIndexWriters() {
		util.MustRun(w.Close)
	}
}
