// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package query_test

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestStringFunctionStr(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "str"
	inputArgs := []interface{}{"abc", "bcd", "cde"}
	subStr := "bc"
	expects := []interface{}{true, true, false}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, subStr}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionStrLen(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "strlen"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	expects := []interface{}{int64(0), int64(3), int64(4), int64(5)}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionSubStrOnePara(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "substr"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	start := int64(1)
	expects := []interface{}{"", "bc", "cde", "defg"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, start}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)
}

func TestStringFunctionSubStrTwoPara(t *testing.T) {
	stringValuer := query.StringValuer{}
	inputName := "substr"
	inputArgs := []interface{}{"", "abc", "bcde", "cdefg"}
	start := int64(1)
	subLen := int64(2)
	expects := []interface{}{"", "bc", "cd", "de"}
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := stringValuer.Call(inputName, []interface{}{arg, start, subLen}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, outputs, expects)

}
