// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsreader

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
	"github.com/smartystreets/goconvey/convey"
)

type MockSeriesIDIterator struct {
}

func (m *MockSeriesIDIterator) Next() (index.SeriesIDElem, error) {
	return index.SeriesIDElem{}, nil
}

func (m *MockSeriesIDIterator) Ids() *uint64set.Set {
	return nil
}

func (m *MockSeriesIDIterator) Close() error {
	return nil
}

func TestSeriesIdIterator_Next(t *testing.T) {
	convey.Convey("TestSeriesIdIterator_Next", t, func() {
		sid := uint64(1)
		seriesItr := &MockSeriesIDIterator{}
		patch := gomonkey.ApplyMethod(seriesItr, "Next", func(seriesItr *MockSeriesIDIterator) (index.SeriesIDElem, error) {
			return index.SeriesIDElem{
				SeriesID: sid,
				Expr: &influxql.BinaryExpr{
					Op:  influxql.EQ,
					LHS: &influxql.VarRef{Val: "metric", Type: influxql.Float},
					RHS: &influxql.NumberLiteral{Val: 1.0}},
			}, nil
		})
		defer patch.Reset()
		filterOption := &immutable.BaseFilterOptions{
			CondFunctions: &binaryfilterfunc.ConditionImpl{},
			FieldsIdx:     []int{0},
			RedIdxMap:     map[int]struct{}{},
			FiltersMap:    make(map[string]*influxql.FilterMapValue),
		}
		itr := &SeriesIdIterator{seriesItr: seriesItr, filterOptions: filterOption, schema: []record.Field{{Type: influx.Field_Type_Float, Name: "metric"}}}
		patch1 := gomonkey.ApplyPrivateMethod(itr, "readRecord", func(itr *SeriesIdIterator, sid uint64) (*record.Record, error) {
			return nil, nil
		})
		defer patch1.Reset()
		res, _, err := itr.Next()
		convey.So(res, convey.ShouldEqual, sid)
		convey.So(err, convey.ShouldBeEmpty)
	})
}
