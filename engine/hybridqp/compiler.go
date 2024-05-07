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

package hybridqp

import (
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type HintType int64

type Options interface {
	GetLimit() int
	GetOffset() int
	HasInterval() bool
	GetCondition() influxql.Expr
	GetLocation() *time.Location
	GetOptDimension() []string
	GetHintType() HintType
	ISChunked() bool
	SetHintType(HintType)
	OptionsName() string
	IsAscending() bool
	SetAscending(bool)
	ChunkSizeNum() int
	GetStartTime() int64
	GetEndTime() int64
	GetMaxParallel() int
	Window(t int64) (start, end int64)
	GetGroupBy() map[string]struct{}
	GetInterval() time.Duration
	IsGroupByAllDims() bool
	GetSourcesNames() []string
	GetMeasurements() []*influxql.Measurement
	HaveOnlyCSStore() bool
	GetDimensions() []string
	SetFill(influxql.FillOption)
	IsTimeSorted() bool
	IsUnifyPlan() bool
	SetSortFields(influxql.SortFields)
	GetSortFields() influxql.SortFields
	FieldWildcard() bool
	GetStmtId() int
	GetLogQueryCurrId() string
	GetIterId() int32
	IsIncQuery() bool
	SetPromQuery(bool)
	IsPromQuery() bool
	IsPromInstantQuery() bool
	IsPromRangeQuery() bool
	GetPromStep() time.Duration
	GetPromRange() time.Duration
	GetPromLookBackDelta() time.Duration
	GetPromQueryOffset() time.Duration
	IsRangeVectorSelector() bool
	IsInstantVectorSelector() bool
}
