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

package tests

import (
	"fmt"
	"reflect"
	"sort"
)

type Results struct {
	Err     string
	Results []ResultItem
}

type ResultItem struct {
	Error       string
	StatementId int `json:"statement_id"`
	Series      []ResultSeries
}

type ResultSeries struct {
	Name    string
	Tags    map[string]string
	Columns []string
	Values  [][]interface{}
}

func CompareSortedResults(expString, actString string) bool {
	exp, act := &Results{}, &Results{}

	err := json.Unmarshal([]byte(expString), exp)
	if err != nil {
		return false
	}

	err = json.Unmarshal([]byte(actString), act)
	if err != nil {
		return false
	}

	if exp.Err != act.Err || len(exp.Results) != len(act.Results) {
		return false
	}

	for i := range exp.Results {
		if !compareResultItem(&exp.Results[i], &act.Results[i]) {
			return false
		}
	}
	return true
}

func compareResultItem(exp, act *ResultItem) bool {
	if exp.Error != act.Error ||
		exp.StatementId != act.StatementId ||
		len(exp.Series) != len(act.Series) {
		return false
	}

	for i := range exp.Series {
		if !compareResultSeries(&exp.Series[i], &act.Series[i]) {
			return false
		}
	}
	return true
}

func compareResultSeries(exp, act *ResultSeries) bool {
	if exp.Name != act.Name || len(exp.Values) != len(act.Values) {
		return false
	}

	if !reflect.DeepEqual(exp.Tags, act.Tags) || !reflect.DeepEqual(exp.Columns, act.Columns) {
		return false
	}

	var sortValues = func(values [][]interface{}) {
		sort.Slice(values, func(i, j int) bool {
			switch vi := values[i][0].(type) {
			case string:
				vj, okj := values[j][0].(string)
				if !okj {
					return true
				}
				return vi < vj
			case float64:
				vj, okj := values[j][0].(float64)
				if !okj {
					return true
				}
				return vi < vj
			default:
				a := fmt.Sprintf("%v", vi)
				b := fmt.Sprintf("%v", values[j][0])
				return a < b
			}
		})
	}
	sortValues(exp.Values)
	sortValues(act.Values)

	return reflect.DeepEqual(exp.Values, act.Values)
}
