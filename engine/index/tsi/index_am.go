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
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type IndexAmRoutine struct {
	amKeyType    index.IndexType
	index        interface{}
	primaryIndex PrimaryIndex

	amOpen         func(index interface{}) error
	amBuild        func(relation *IndexRelation) error
	amInsert       func(index interface{}, primaryIndex PrimaryIndex, name []byte, row interface{}) (uint64, error)
	amDelete       func(index interface{}, primaryIndex PrimaryIndex, name []byte, condition influxql.Expr, tr TimeRange) error
	amScan         func(index interface{}, primaryIndex PrimaryIndex, span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error, groups interface{}) (interface{}, int64, error)
	amScanrelation func(oid1 int, oid2 int, result1 interface{}, result2 interface{}) (interface{}, error)
	amClose        func(index interface{}) error
	amFlush        func(index interface{})
	amCacheClear   func(index interface{}) error
}
