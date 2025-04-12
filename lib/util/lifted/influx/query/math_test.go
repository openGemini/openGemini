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
