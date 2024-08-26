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
	"sync"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var RowsPool sync.Pool

func GetRows(num int) *[]influx.Row {
	rows := RowsPool.Get()
	if rows == nil {
		rs := make([]influx.Row, 0, num)
		return &rs
	}
	rs, ok := rows.(*[]influx.Row)
	if !ok {
		panic("It's not ok for type *[]influx.Row")
	}
	// resize if needed
	if cap(*rs) < num {
		*rs = make([]influx.Row, 0, num)
	}
	return rs
}

func PutRows(rows *[]influx.Row) {
	for i := range *rows {
		(*rows)[i].Reset()
	}
	*rows = (*rows)[:0]
	RowsPool.Put(rows)
}
