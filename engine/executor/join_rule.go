// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

const TimeSuffix string = ".time"

type JoinTransformCreator struct {
}

func (c *JoinTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	joinCase := plan.Schema().GetJoinCases()[0]
	isSortMergeJoin, err := MatchSortMergeJoin(joinCase, plan.Schema())
	if err != nil {
		return nil, err
	}
	if isSortMergeJoin {
		return NewSortMergeJoinTransform(inRowDataTypes, plan.RowDataType(), joinCase, plan.Schema())
	}
	return NewHashJoinTransform(inRowDataTypes, plan.RowDataType(), joinCase, plan.Schema())
}

var _ = RegistryTransformCreator(&LogicalJoin{}, &JoinTransformCreator{})

// MatchSortMergeJoin used to check whether the sort merge join algorithm is matched.
// Requirements for the sort merge join algorithm:
// 1. Only join on tag is supported. Join on field is not supported.
// 2. The join key belongs to the group by subset.
// 3. Join on join equal value comparison.
func MatchSortMergeJoin(join *influxql.Join, schema hybridqp.Catalog) (bool, error) {
	lSrc, ok := join.LSrc.(*influxql.SubQuery)
	if !ok || lSrc == nil {
		return false, fmt.Errorf("can`t match join type, cause there is no left src or left src is illegal")
	}
	rSrc, ok := join.RSrc.(*influxql.SubQuery)
	if !ok || rSrc == nil {
		return false, fmt.Errorf("can`t match join type, cause there is no right src or right src is illegal")
	}
	joinKeyMap := make(map[string]string)
	err := influxql.GetJoinKeyByCondition(join.Condition, joinKeyMap)
	if err != nil {
		return false, err
	}

	keys, _, err := normalizeJoinKey(lSrc.GetName(), rSrc.GetName(), joinKeyMap)
	if err != nil {
		return false, err
	}

	if !isJoinKeyAllInDim(schema, lSrc.GetName(), rSrc.GetName(), keys) {
		return false, nil
	}

	if !isJoinKeyAsDimPrefix(lSrc.Statement, lSrc.GetName(), keys, true) {
		return false, nil
	}

	if !isJoinKeyAsDimPrefix(rSrc.Statement, rSrc.GetName(), keys, false) {
		return false, nil
	}
	return true, nil
}

func normalizeJoinKey(lName, rName string, joinKeyMap map[string]string) ([]pair, bool, error) {
	keys := make([]pair, 0, len(joinKeyMap))
	timeInJoinKey := false
	lNames := strings.Split(lName, ",")
	rNames := strings.Split(rName, ",")
	for lk, rk := range joinKeyMap {
		if (lk == lName+TimeSuffix && rk == rName+TimeSuffix) ||
			(lk == rName+TimeSuffix && rk == lName+TimeSuffix) {
			timeInJoinKey = true
			continue
		}
		key := pair{}
		if LengthOfPrefixForField(lk, lNames) > 0 {
			key.leftKey = lk
		} else if LengthOfPrefixForField(rk, lNames) > 0 {
			key.leftKey = rk
		} else {
			return nil, false, fmt.Errorf("invalid left join key: %s", lk)
		}

		if LengthOfPrefixForField(rk, rNames) > 0 {
			key.rightKey = rk
		} else if LengthOfPrefixForField(lk, rNames) > 0 {
			key.rightKey = lk
		} else {
			return nil, false, fmt.Errorf("invalid right join key: %s", rk)
		}
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].leftKey < keys[j].leftKey
	})
	return keys, timeInJoinKey, nil
}

func isJoinKeyAllInDim(schema hybridqp.Catalog, lName, rName string, keys []pair) bool {
	dims := schema.Options().GetOptDimension()
	if len(dims) == 0 {
		return false
	}
	keyNum := 0
	lNames := strings.Split(lName, ",")
	rNames := strings.Split(rName, ",")
	for i := range keys {
		lIdx := findKeyIdxInDims(keys[i].leftKey, lNames, dims)
		if lIdx == -1 {
			break
		}
		rIdx := findKeyIdxInDims(keys[i].rightKey, rNames, dims)
		if rIdx == -1 {
			break
		}
		keyNum++
	}
	return keyNum == len(keys)
}

func isJoinKeyAsDimPrefix(stmt *influxql.SelectStatement, name string, keys []pair, isLeft bool) bool {
	if len(stmt.Dimensions) == 0 || len(stmt.Dimensions) < len(keys) {
		return false
	}
	dims := make([]string, 0, len(stmt.Dimensions))
	for i := range stmt.Dimensions {
		if val, ok := stmt.Dimensions[i].Expr.(*influxql.VarRef); ok {
			dims = append(dims, val.Val)
		}
	}
	names := strings.Split(name, ",")
	for i := range keys {
		var idx int
		if isLeft {
			idx = findKeyIdxInDims(keys[i].leftKey, names, dims)
		} else {
			idx = findKeyIdxInDims(keys[i].rightKey, names, dims)
		}
		if idx == -1 || idx > len(keys)-1 {
			return false
		}
	}
	return true
}
