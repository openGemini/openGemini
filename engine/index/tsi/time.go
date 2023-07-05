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
	"math"

	"github.com/openGemini/openGemini/lib/util"
)

// TimeRange is time range.
type TimeRange util.TimeRange

var DefaultTR = TimeRange{Min: 0, Max: math.MaxInt64 - 1}

const nsPerDay = 24 * 3600 * 1e9
