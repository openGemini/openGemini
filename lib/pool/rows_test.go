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

package pool

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestRowsPoolTypeError(t *testing.T) {
	num := 10
	ch := make(chan interface{}, 1)
	RowsPool.Put(ch)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic to occur")
		}
	}()
	GetRows(num)
}

func TestRowsPool(t *testing.T) {
	num := 10
	rows := GetRows(num)
	if len(*rows) != 0 {
		t.Errorf("Expected length of rows to be 0, but got %d", len(*rows))
	}
	if cap(*rows) != num {
		t.Errorf("Expected capacity of rows to be %d, but got %d", num, cap(*rows))
	}

	*rows = append(*rows, influx.Row{})
	PutRows(rows)
	if len(*rows) != 0 {
		t.Errorf("Expected 0 rows, but got %d", len(*rows))
	}

	num = 100
	rows = GetRows(num)
	if len(*rows) != 0 {
		t.Errorf("Expected length of rows to be 0, but got %d", len(*rows))
	}
	if cap(*rows) != num {
		t.Errorf("Expected capacity of rows to be %d, but got %d", num, cap(*rows))
	}
	PutRows(rows)
	if len(*rows) != 0 {
		t.Errorf("Expected 0 rows, but got %d", len(*rows))
	}
}
