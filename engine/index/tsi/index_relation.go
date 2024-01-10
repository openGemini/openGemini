/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package tsi

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/savsgio/dictpool"
)

// IndexRelation define functions of a specific index
type IndexRelation struct {
	oid            uint32 // index id, [0:inverted index, 1:text,...]
	indexAmRoutine *IndexAmRoutine
	iBuilder       *IndexBuilder
}

func NewIndexRelation(opt *Options, primaryIndex PrimaryIndex, iBuilder *IndexBuilder) (*IndexRelation, error) {
	relation := &IndexRelation{
		oid:      GetIndexIdByType(opt.indexType),
		iBuilder: iBuilder,
	}
	var err error
	relation.indexAmRoutine, err = getIndexAmRoutine(opt, primaryIndex)
	return relation, err
}

func getIndexAmRoutine(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	switch opt.indexType {
	case MergeSet:
		return MergeSetIndexHandler(opt, primaryIndex)
	case Text:
		return TextIndexHandler(opt, primaryIndex)
	case Field:
		return FieldIndexHandler(opt, primaryIndex)
	}
	return nil, fmt.Errorf("not found index type %d", opt.indexType)
}

func (relation *IndexRelation) IndexOpen() error {
	return relation.indexAmRoutine.amOpen(relation.indexAmRoutine.index)
}

func (relation *IndexRelation) IndexBuild(name []byte, indexMap map[string]int) error {
	return relation.indexAmRoutine.amBuild(relation)
}

func (relation *IndexRelation) IndexInsert(name []byte, point interface{}) error {
	index := relation.indexAmRoutine.index
	primaryIndex := relation.iBuilder.GetPrimaryIndex()
	_, err := relation.indexAmRoutine.amInsert(index, primaryIndex, name, point)
	return err
}

func (relation *IndexRelation) IndexScan(span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}, callBack func(num int64) error) (interface{}, int64, error) {
	index := relation.indexAmRoutine.index
	primaryIndex := relation.indexAmRoutine.primaryIndex
	return relation.indexAmRoutine.amScan(index, primaryIndex, span, name, opt, callBack, groups)
}

func (relation *IndexRelation) IndexDelete(name []byte, condition influxql.Expr, tr TimeRange) error {
	index := relation.indexAmRoutine.index
	primaryIndex := relation.indexAmRoutine.primaryIndex
	return relation.indexAmRoutine.amDelete(index, primaryIndex, name, condition, tr)
}

func (relation *IndexRelation) IndexRelation(oid1 int, oid2 int, result1 interface{}, result2 interface{}) (interface{}, error) {
	return relation.indexAmRoutine.amScanrelation(oid1, oid2, result1, result2)
}

func (relation *IndexRelation) IndexClose() error {
	index := relation.indexAmRoutine.index
	return relation.indexAmRoutine.amClose(index)
}

func (relation *IndexRelation) IndexFlush() {
	index := relation.indexAmRoutine.index
	relation.indexAmRoutine.amFlush(index)
}

type PrimaryIndex interface {
	CreateIndexIfNotExists(mmRows *dictpool.Dict) error
	GetPrimaryKeys(name []byte, opt *query.ProcessorOptions) ([]uint64, error)
	GetDeletePrimaryKeys(name []byte, condition influxql.Expr, tr TimeRange) ([]uint64, error)
	SearchSeriesWithOpts(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, _ interface{}) (GroupSeries, int64, error)
	Path() string
}
