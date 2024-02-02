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
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/logparser"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var table = crc32.MakeTable(crc32.Castagnoli)

const crcSize = 4

// SKIndexReader as a skip index read interface.
type SKIndexReader interface {
	// CreateSKFileReaders generates SKFileReaders for each index field based on the skip index information and condition
	// which is used to quickly determine whether a fragment meets the condition.
	CreateSKFileReaders(option hybridqp.Options, mstInfo *influxql.Measurement, isCache bool) ([]SKFileReader, error)
	// Scan is used to filter fragment ranges based on the secondary key in the condition.
	Scan(reader SKFileReader, rgs fragment.FragmentRanges) (fragment.FragmentRanges, error)
	// Close is used to close the SKIndexReader
	Close() error
}

type TsspFile interface {
	Name() string
	Path() string
}

// SKFileReader as an executor of skip index data reading that corresponds to the index field in the query.
type SKFileReader interface {
	// MayBeInFragment determines whether a fragment in a file meets the query condition.
	MayBeInFragment(fragId uint32) (bool, error)
	// ReInit is used to that a SKFileReader is reused among multiple files.
	ReInit(file interface{}) error
	// Close is used to close the SKFileReader
	Close() error
}

type SKIndexReaderImpl struct {
	property *IndexProperty
	logger   *logger.Logger
}

func NewSKIndexReader(rowsNumPerFragment int, coarseIndexFragment int, minRowsForSeek int) *SKIndexReaderImpl {
	return &SKIndexReaderImpl{
		property: NewIndexProperty(rowsNumPerFragment, coarseIndexFragment, minRowsForSeek),
		logger:   logger.NewLogger(errno.ModuleIndex),
	}
}

func (r *SKIndexReaderImpl) Scan(
	reader SKFileReader,
	rgs fragment.FragmentRanges,
) (fragment.FragmentRanges, error) {
	var (
		droppedFragment uint32 // Number of filtered fragments by skip index
		totalFragment   uint32 // Number of filtered fragments by primary index
		res             fragment.FragmentRanges
	)
	minMarksForSeek := (r.property.MinRowsForSeek + r.property.RowsNumPerFragment - 1) / r.property.RowsNumPerFragment
	for i := 0; i < len(rgs); i++ {
		mr := rgs[i]
		totalFragment += mr.End - mr.Start
		for j := mr.Start; j < mr.End; j++ {
			ok, err := reader.MayBeInFragment(j)
			if err != nil {
				return nil, err
			}
			if !ok {
				droppedFragment++
				continue
			}
			dataRange := fragment.NewFragmentRange(
				util.MaxUint32(rgs[i].Start, j),
				util.MinUint32(rgs[i].End, j+1),
			)
			if len(res) == 0 || int(res[len(res)-1].End-dataRange.Start) > minMarksForSeek {
				res = append(res, dataRange)
			} else {
				res[len(res)-1].End = dataRange.End
			}
		}
	}
	return res, nil
}

func (r *SKIndexReaderImpl) CreateSKFileReaders(option hybridqp.Options, mstInfo *influxql.Measurement, isCache bool) ([]SKFileReader, error) {
	skIndexRelation := mstInfo.IndexRelation
	if skIndexRelation == nil || len(skIndexRelation.Oids) == 0 || option.GetCondition() == nil {
		return nil, nil
	}

	skFieldMap := make(map[string][]string)
	for i, indexList := range skIndexRelation.IndexList {
		// time cluster takes effect on the primary key, not the skip index.
		if skIndexRelation.Oids[i] == uint32(index.TimeCluster) {
			continue
		}
		for _, field := range indexList.IList {
			if _, ok := skFieldMap[field]; !ok {
				skFieldMap[field] = []string{skIndexRelation.IndexNames[i]}
			} else {
				skFieldMap[field] = append(skFieldMap[field], skIndexRelation.IndexNames[i])
			}
		}
	}
	if len(skFieldMap) == 0 {
		return nil, nil
	}
	rpnExpr := rpn.ConvertToRPNExpr(option.GetCondition())
	skInfoMap, err := r.getSKInfoByExpr(rpnExpr, skIndexRelation, skFieldMap)
	if err != nil {
		return nil, err
	}
	return r.createSKFileReaders(skInfoMap, rpnExpr, option, isCache)
}

func (r *SKIndexReaderImpl) getSKInfoByExpr(rpnExpr *rpn.RPNExpr, skIndexRelation *influxql.IndexRelation, skFieldMap map[string][]string) (map[string]*SkInfo, error) {
	skInfoMap := make(map[string]*SkInfo)
	for _, expr := range rpnExpr.Val {
		v, ok := expr.(*influxql.VarRef)
		if !ok {
			continue
		}
		if v.Val == logparser.DefaultFieldForFullText {
			fields := skIndexRelation.GetFullTextColumns()
			if len(fields) == 0 {
				return nil, fmt.Errorf("empty fields for full text index")
			}
			schemas := make([]record.Field, 0, len(fields))
			for i := 0; i < len(fields); i++ {
				schemas = append(schemas, record.Field{Name: fields[i], Type: influx.Field_Type_String})
			}
			// TODO: indexName is used to uniquely identify an index.
			skInfoMap[index.BloomFilterFullTextIndex] = &SkInfo{fields: schemas, oid: uint32(index.BloomFilterFullText)}
			continue
		}
		indexNames, ok := skFieldMap[v.Val]
		if !ok {
			continue
		}
		for i := range indexNames {
			if info, ok := skInfoMap[indexNames[i]]; ok {
				info.fields = append(info.fields, record.Field{Name: v.Val, Type: record.ToModelTypes(v.Type)})
			} else {
				oid, ok := skIndexRelation.GetIndexOidByName(indexNames[i])
				if !ok {
					return nil, fmt.Errorf("invalid the index name %s", indexNames[i])
				}
				skInfoMap[indexNames[i]] = &SkInfo{fields: []record.Field{{Name: v.Val, Type: record.ToModelTypes(v.Type)}}, oid: oid}
			}
		}
	}
	return skInfoMap, nil
}

func (r *SKIndexReaderImpl) createSKFileReaders(skInfoMap map[string]*SkInfo, rpnExpr *rpn.RPNExpr, option hybridqp.Options, isCache bool) ([]SKFileReader, error) {
	var readers []SKFileReader
	for _, v := range skInfoMap {
		indexName, _ := index.GetIndexNameByType(index.IndexType(v.oid))
		creator, ok := GetSKFileReaderFactoryInstance().Find(indexName)
		if !ok {
			return nil, fmt.Errorf("unsupported the skip index type: %s", indexName)
		}
		reader, err := creator.CreateSKFileReader(rpnExpr, v.fields, option, isCache)
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}
	return readers, nil
}

func (r *SKIndexReaderImpl) Close() error {
	return nil
}

type SkInfo struct {
	fields record.Schemas
	oid    uint32
}

// SKFileReaderCreator is used to abstract SKFileReader implementation of multiple skip indexes in factory mode.
type SKFileReaderCreator interface {
	CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error)
}

// RegistrySKFileReaderCreator is used to registry the SKFileReaderCreator
func RegistrySKFileReaderCreator(name string, creator SKFileReaderCreator) bool {
	factory := GetSKFileReaderFactoryInstance()

	_, ok := factory.Find(name)
	if ok {
		return ok
	}

	factory.Add(name, creator)
	return true
}

type SKFileReaderCreatorFactory struct {
	creators map[string]SKFileReaderCreator
}

func NewSKFileReaderCreatorFactory() *SKFileReaderCreatorFactory {
	return &SKFileReaderCreatorFactory{creators: make(map[string]SKFileReaderCreator)}
}

func (s *SKFileReaderCreatorFactory) Add(name string, creator SKFileReaderCreator) {
	s.creators[name] = creator
}

func (s *SKFileReaderCreatorFactory) Find(name string) (SKFileReaderCreator, bool) {
	creator, ok := s.creators[name]
	return creator, ok
}

var SKFileReaderInstance *SKFileReaderCreatorFactory
var SKFileReaderOnce sync.Once

func GetSKFileReaderFactoryInstance() *SKFileReaderCreatorFactory {
	SKFileReaderOnce.Do(func() {
		SKFileReaderInstance = NewSKFileReaderCreatorFactory()
	})
	return SKFileReaderInstance
}

type SkipIndexWriter interface {
	Open() error
	Close() error
	Flush() error
	CreateSkipIndex(src *record.ColVal, rowsPerSegment []int, refType int) ([]byte, error)
	CreateFullTextIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) []byte
}

func NewSkipIndexWriter(indexType string) SkipIndexWriter {
	switch indexType {
	case index.BloomFilterIndex:
		return &BloomFilterImpl{
			bloomFilter: make([]byte, 0),
		}
	default:
		return nil
	}
}

type BloomFilterImpl struct {
	bloomFilter []byte
}

func (b *BloomFilterImpl) Open() error {
	return nil
}

func (b *BloomFilterImpl) Close() error {
	return nil
}

func (b *BloomFilterImpl) Flush() error {
	return nil
}

func (b *BloomFilterImpl) CreateSkipIndex(src *record.ColVal, rowsPerSegment []int, refType int) ([]byte, error) {
	//TODO:
	// 1. use different splitter for different column
	// 2. reusing the tokenizers
	tk := tokenizer.NewGramTokenizer(tokenizer.CONTENT_SPLITTER, 0, logstore.GramTokenizerVersion)

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
		crc := crc32.Checksum(res[start:end-crcSize], table)
		binary.LittleEndian.PutUint32(res[end-crcSize:end], crc)
		start = end
	}
	tokenizer.FreeSimpleGramTokenizer(tk)
	return res, nil
}

func (b *BloomFilterImpl) CreateFullTextIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) []byte {
	tk := tokenizer.NewGramTokenizer(tokenizer.CONTENT_SPLITTER, 0, logstore.GramTokenizerVersion)
	segCnt := len(rowsPerSegment)
	segBfSize := int(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize)
	res := make([]byte, segCnt*segBfSize)

	start := 0
	end := 0
	colsData := b.getFullTextColsData(writeRec, schemaIdx, rowsPerSegment)
	row, col := len(colsData), len(colsData[0])
	for i := 0; i < col; i++ {
		end = start + segBfSize
		for j := 0; j < row; j++ {
			offs, lens := colsData[j][i].GetOffsAndLens()
			tk.ProcessTokenizerBatch(colsData[j][i].Val, res[start:end-crcSize], offs, lens)
		}
		crc := crc32.Checksum(res[start:end-crcSize], table)
		binary.LittleEndian.PutUint32(res[end-crcSize:end], crc)
		start = end
	}
	return res
}

func (b *BloomFilterImpl) getFullTextColsData(writeRec *record.Record, schemaIdx, rowsPerSegment []int) [][]record.ColVal {
	colsData := make([][]record.ColVal, 0, len(schemaIdx))
	for _, v := range schemaIdx {
		var colData []record.ColVal
		colData = writeRec.ColVals[v].SplitColBySize(colData, rowsPerSegment, writeRec.Schema[v].Type)
		colsData = append(colsData, colData)
	}
	return colsData
}
