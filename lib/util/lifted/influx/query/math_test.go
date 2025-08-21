/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package query_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestTruncateFunc(t *testing.T) {
	urlValuer := query.MathValuer{}
	inputName := "truncate"

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"str"})
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, out)

	// Correct test cases
	inputArgs := make([]interface{}, 3)
	inputArgs[0] = 15.15
	inputArgs[1] = 12.02
	inputArgs[2] = int64(14)
	expects := make([]interface{}, 3)
	expects[0] = 15.00
	expects[1] = 12.00
	expects[2] = 14.00
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}

func TestCastInt64Func(t *testing.T) {
	urlValuer := query.MathValuer{}

	inputName := "cast_int64"
	// Correct test cases
	inputArgs := make([]interface{}, 4)
	inputArgs[0] = 15.15
	inputArgs[1] = int64(12)
	inputArgs[2] = true
	inputArgs[3] = "12"
	expects := make([]interface{}, 4)
	expects[0] = int64(15)
	expects[1] = int64(12)
	expects[2] = int64(1)
	expects[3] = int64(12)
	outputs := make([]interface{}, 0, len(expects))
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)

	// Wrong test case
	out, ok := urlValuer.Call(inputName, []interface{}{"12.3"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	inputName = "cast_float64"
	// Correct test cases
	expects[0] = float64(15.15)
	expects[1] = float64(12)
	expects[2] = float64(1)
	expects[3] = float64(12)
	outputs = outputs[:0]
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)

	// Wrong test case
	out, ok = urlValuer.Call(inputName, []interface{}{"ddd"})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	inputName = "cast_string"
	// Correct test cases
	expects[0] = "15.15"
	expects[1] = "12"
	expects[2] = "true"
	expects[3] = "12"
	outputs = outputs[:0]
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)

	// Wrong test case
	out, ok = urlValuer.Call(inputName, []interface{}{12})
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, out)

	inputName = "cast_bool"
	// Correct test cases
	inputArgs[1] = int64(0)
	inputArgs[2] = true
	inputArgs[3] = "0"
	expects[0] = true
	expects[1] = false
	expects[2] = true
	expects[3] = false
	outputs = outputs[:0]
	for _, arg := range inputArgs {
		if out, ok := urlValuer.Call(inputName, []interface{}{arg}); ok {
			outputs = append(outputs, out)
		}
	}
	assert.Equal(t, expects, outputs)
}
