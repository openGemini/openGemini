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

package failpoint

import (
	"sync/atomic"

	"github.com/spf13/cast"
)

type Value struct {
	val *atomic.Value
}

func (v *Value) Bool() bool {
	if v.val == nil {
		return false
	}

	return cast.ToBool(v.val.Load())
}

func (v *Value) Int() int {
	if v.val == nil {
		return 0
	}

	return cast.ToInt(v.val.Load())
}

func (v *Value) Float64() float64 {
	if v.val == nil {
		return 0
	}

	return cast.ToFloat64(v.val.Load())
}

func (v *Value) String() string {
	if v.val == nil {
		return ""
	}
	return cast.ToString(v.val.Load())
}
