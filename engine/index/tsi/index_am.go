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
)

// indexAm define index access method
type indexAm struct {
	id      uint32
	IdxType IndexType
	IdxName string
	idxFunc func(opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error)
}

var IndexAms = []indexAm{
	{0, MergeSet, "mergeset", MergeSetIndexHandler},
	{1, Text, "text", TextIndexHandler},
	{2, Field, "field", FieldIndexHandler},
}

var (
	IndexNameToID = map[string]uint32{
		"mergeset": 0,
		"text":     1,
		"field":    2,
	}
)

func GetIndexIdByName(name string) (uint32, error) {
	id, ok := IndexNameToID[name]
	if !ok {
		return 0, fmt.Errorf("invalid index type %s", name)
	}
	return id, nil
}

func GetIndexTypeByName(name string) IndexType {
	for _, am := range IndexAms {
		if am.IdxName == name {
			return am.IdxType
		}
	}
	return MergeSet
}

func GetIndexIdByType(idxType IndexType) uint32 {
	for _, am := range IndexAms {
		if am.IdxType == idxType {
			return am.id
		}
	}
	return 0
}

func GetIndexTypeById(id uint32) IndexType {
	for _, am := range IndexAms {
		if am.id == id {
			return am.IdxType
		}
	}
	return MergeSet
}

func GetIndexAmRoutine(id uint32, opt *Options, primaryIndex PrimaryIndex) (*IndexAmRoutine, error) {
	for _, am := range IndexAms {
		if am.id == id {
			return am.idxFunc(opt, primaryIndex)
		}
	}
	return nil, nil
}

type IndexAmRoutine struct {
	amKeyType    IndexType
	index        interface{}
	primaryIndex PrimaryIndex

	amOpen         func(index interface{}) error
	amBuild        func(relation *IndexRelation) error
	amInsert       func(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}) (uint64, error)
	amDelete       func(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error
	amScan         func(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, groups interface{}) (interface{}, error)
	amScanrelation func(oid1 int, oid2 int, result1 interface{}, result2 interface{}) (interface{}, error)
	amClose        func(index interface{}) error
}
