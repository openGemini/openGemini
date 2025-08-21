// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package generate

import (
	"sort"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func GenerateRows(num int, rows []influx.Row) []influx.Row {
	tmpKeys := []string{
		"mst0,tk1=value1,tk2=value2,tk3=value3",
		"mst0,tk1=value11,tk2=value22,tk3=value33",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
		"mst0,tk1=value12,tk2=value23",
	}
	keys := make([]string, num)
	for i := 0; i < num; i++ {
		keys[i] = tmpKeys[i%len(tmpKeys)]
	}
	rows = rows[:cap(rows)]
	for j, key := range keys {
		if len(rows) <= j {
			rows = append(rows, influx.Row{})
		}
		pt := &rows[j]
		strs := strings.Split(key, ",")
		pt.Name = strs[0]
		pt.Tags = pt.Tags[:cap(pt.Tags)]
		for i, str := range strs[1:] {
			if cap(pt.Tags) <= i {
				pt.Tags = append(pt.Tags, influx.Tag{})
			}
			kv := strings.Split(str, "=")
			pt.Tags[i].Key = kv[0]
			pt.Tags[i].Value = kv[1]
		}
		pt.Tags = pt.Tags[:len(strs[1:])]
		sort.Sort(&pt.Tags)
		pt.Timestamp = time.Now().UnixNano()
		pt.UnmarshalIndexKeys(nil)
		pt.ShardKey = pt.IndexKey
		pt.Fields = pt.Fields[:cap(pt.Fields)]
		if cap(pt.Fields) < 1 {
			pt.Fields = append(pt.Fields, influx.Field{}, influx.Field{})
		}
		pt.Fields[0].NumValue = 1
		pt.Fields[0].StrValue = ""
		pt.Fields[0].Type = influx.Field_Type_Float
		pt.Fields[0].Key = "fk1"
		pt.Fields[1].NumValue = 1
		pt.Fields[1].StrValue = ""
		pt.Fields[1].Type = influx.Field_Type_Int
		pt.Fields[1].Key = "fk2"
	}
	return rows[:num]
}
