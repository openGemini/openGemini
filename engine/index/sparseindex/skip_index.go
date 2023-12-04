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
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

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
	skInfo := mstInfo.IndexRelation
	if mstInfo.IndexRelation == nil || len(skInfo.Oids) == 0 || option.GetCondition() == nil {
		return nil, nil
	}

	skInfoMap := make(map[string][]string)
	for i, indexList := range skInfo.IndexList {
		for _, field := range indexList.IList {
			if _, ok := skInfoMap[field]; !ok {
				skInfoMap[field] = []string{skInfo.IndexNames[i]}
			} else {
				skInfoMap[field] = append(skInfoMap[field], skInfo.IndexNames[i])
			}
		}
	}
	return r.GenSKFileReaderByExpr(option.GetCondition(), option, skInfoMap, isCache)
}

func (r *SKIndexReaderImpl) GenSKFileReaderByExpr(expr influxql.Expr, option hybridqp.Options, skInfoMap map[string][]string, isCache bool) ([]SKFileReader, error) {
	var readers []SKFileReader
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		lReaders, err := r.GenSKFileReaderByExpr(expr.LHS, option, skInfoMap, isCache)
		if err != nil {
			return nil, err
		}
		rReaders, err := r.GenSKFileReaderByExpr(expr.RHS, option, skInfoMap, isCache)
		if err != nil {
			return nil, err
		}
		readers = append(readers, lReaders...)
		readers = append(readers, rReaders...)
	case *influxql.ParenExpr:
		pReaders, err := r.GenSKFileReaderByExpr(expr.Expr, option, skInfoMap, isCache)
		if err != nil {
			return nil, err
		}
		readers = append(readers, pReaders...)
	case *influxql.VarRef:
		if sks, ok := skInfoMap[expr.Val]; ok {
			for i := range sks {
				creator, ok := GetSKFileReaderFactoryInstance().Find(sks[i])
				if !ok {
					return nil, fmt.Errorf("unsupported the skip index type: %s", sks[i])
				}
				reader, err := creator.CreateSKFileReader([]record.Field{{Name: expr.Val, Type: record.ToModelTypes(expr.Type)}}, option, isCache)
				if err != nil {
					return nil, err
				}
				readers = append(readers, reader)
			}
		}
	case *influxql.StringLiteral, *influxql.IntegerLiteral, *influxql.NumberLiteral, *influxql.BooleanLiteral:
	default:
		return nil, fmt.Errorf("unsupported the expr type: %s", expr.String())
	}
	return readers, nil
}

func (r *SKIndexReaderImpl) Close() error {
	return nil
}

// SKFileReaderCreator is used to abstract SKFileReader implementation of multiple skip indexes in factory mode.
type SKFileReaderCreator interface {
	CreateSKFileReader(schema record.Schemas, option hybridqp.Options, isCache bool) (SKFileReader, error)
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
	return nil, nil
}
