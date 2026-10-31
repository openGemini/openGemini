// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var rowsPoolChan = make(chan *[]influx.Row, cpu.GetCpuNum())
var rowsPool = sync.Pool{}

func GetRows(num int) *[]influx.Row {
	var rows *[]influx.Row
	select {
	case rows = <-rowsPoolChan:
	default:
		v := rowsPool.Get()
		if v == nil {
			rs := make([]influx.Row, 0, num)
			return &rs
		}
		rows, _ = v.(*[]influx.Row)
	}

	// resize if needed
	if cap(*rows) < num {
		*rows = make([]influx.Row, 0, num)
	}
	return rows
}

func PutRows(rows *[]influx.Row) {
	for i := range *rows {
		(*rows)[i].ReuseSet()
	}
	*rows = (*rows)[:0]
	select {
	case rowsPoolChan <- rows:
	default:
		rowsPool.Put(rows)
	}
}
