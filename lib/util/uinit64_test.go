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

package util_test

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/util"
)

func Test_IndexOf(t *testing.T) {
	d := []uint64{1, 2, 3}
	index := util.IndexOf(d, 3)
	if index != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, index))
	}
	index = util.IndexOf(d, 5)
	if index != -1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", -1, index))
	}
}

func Test_Include(t *testing.T) {
	d := []uint64{1, 2, 3}
	exist := util.Include(d, 3)
	if !exist {
		t.Error(fmt.Sprintf("expect %v ,got %v", "exist", "null"))
	}
	exist = util.Include(d, 5)
	if exist {
		t.Error(fmt.Sprintf("expect %v ,got %v", "null", "exist"))
	}
}
