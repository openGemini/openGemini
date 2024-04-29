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

package executor_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/stretchr/testify/assert"
)

func TestPromTimeFuncstions(t *testing.T) {
	valuer := executor.PromTimeValuer{}
	calls := []string{"year_prom", "time_prom", "vector_prom", "month_prom", "day_of_month_prom", "day_of_week_prom", "hour_prom", "minute_prom", "days_in_month_prom"}
	// 1.normal return
	callArgs := []interface{}{1.0}
	expects := []float64{1970, 1, 1, 1, 1, 4, 0, 0, 31}
	for i, call := range calls {
		act, _ := valuer.Call(call, callArgs)
		assert.Equal(t, expects[i], act.(float64))
	}
	callArgs = callArgs[:0]
	for _, call := range calls {
		act, _ := valuer.Call(call, callArgs)
		assert.Equal(t, act, nil)
	}
}
