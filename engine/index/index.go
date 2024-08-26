// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/index/textindex"
	indextype "github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type IndexWriter interface {
	Open() error
	Close() error
	CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error
	CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string)
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

func NewIndexWriter(dir, msName, dataFilePath, lockPath string, indexType indextype.IndexType, tokens string) IndexWriter {
	switch indexType {
	case indextype.BloomFilter:
		return sparseindex.NewBloomFilterWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.BloomFilterFullText:
		return sparseindex.NewBloomFilterFullTextWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.Set:
		return sparseindex.NewSetWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.MinMax:
		return sparseindex.NewMinMaxWriter(dir, msName, dataFilePath, lockPath, tokens)
	case indextype.Text:
		return textindex.NewTextIndexWriter(dir, msName, dataFilePath, lockPath, tokens)
	default:
		logger.GetLogger().Error("unknown skip index type")
		return nil
	}
}

func (s *IndexWriterBuilder) NewIndexWriters(dir, msName, dataFilePath, lockPath string, schema record.Schemas,
	indexRelation influxql.IndexRelation) {
	s.indexWriters = make([]IndexWriter, 0, len(indexRelation.Oids))
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
		indexWriter := NewIndexWriter(dir, msName, dataFilePath, lockPath, indextype.IndexType(indexRelation.Oids[i]), tokens)
		if indexWriter == nil {
			continue
		}
		if indexRelation.Oids[i] == uint32(indextype.BloomFilter) {
			s.bfIdx = len(s.indexWriters)
		}
		if indexRelation.Oids[i] == uint32(indextype.BloomFilterFullText) {
			s.fullTextIdx = len(s.indexWriters)
		}
		schemaIdx := GetSchemaIndex(schema, indexRelation.Oids[i], indexRelation.IndexList[i].IList)
		s.schemaIdxes = append(s.schemaIdxes, schemaIdx)
		s.indexWriters = append(s.indexWriters, indexWriter)
	}
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
