// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package atomic_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/atomic"
)

func Test_SetModInt64AndADD(t *testing.T) {
	var a, b int64
	a = 1
	b = 7
	r := atomic.SetModInt64AndADD(&a, b, 4)
	if r != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, r))
	}
	r = atomic.SetModInt64AndADD(&a, b, 10)
	if r != 7 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 7, r))
	}
}

func Test_LoadModInt64AndADD(t *testing.T) {
	var a, b int64
	a = 1
	b = 7
	r := atomic.LoadModInt64AndADD(&a, b, 4)
	if r != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, r))
	}
	r = atomic.LoadModInt64AndADD(&a, b, 10)
	if r != 8 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 8, r))
	}
}

func Test_CompareAndSwapMaxInt64(t *testing.T) {
	var a, b int64
	a = 1
	b = 7
	r := atomic.CompareAndSwapMaxInt64(&a, b)
	if r != 7 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 7, r))
	}
	b = 1
	r = atomic.CompareAndSwapMaxInt64(&a, b)
	if r != 7 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 7, r))
	}
}

func Test_CompareAndSwapMinInt64(t *testing.T) {
	var a, b int64
	a = 1
	b = 7
	r := atomic.CompareAndSwapMinInt64(&a, b)
	if r != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, r))
	}
	b = 0
	r = atomic.CompareAndSwapMinInt64(&a, b)
	if r != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, r))
	}
}
