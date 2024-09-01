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

package engine

import (
	"testing"
)

var times = []int64{1 * 1e9, 2 * 1e9, 3 * 1e9, 4 * 1e9}
var values = []float64{1, 2, 3, 4}

func Test_floatChangesMerger(t *testing.T) {
	changesFn := floatChangesMerger()
	_, _ = changesFn(nil, times, nil, values, 0, 0, nil)
}
