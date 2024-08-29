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

package record

import (
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

type Field struct {
	Type int
	Name string
}

func (f *Field) equal(other *Field) bool {
	return f.Name == other.Name && f.Type == other.Type
}

func (f *Field) IsString() bool {
	return f.Type == influx.Field_Type_String
}

func (f *Field) String() string {
	var sb strings.Builder
	sb.WriteString(f.Name)
	sb.WriteString(influx.FieldTypeName[f.Type])
	return sb.String()
}

type Schemas []Field

func (sh Schemas) String() string {
	sb := strings.Builder{}
	for _, f := range sh {
		sb.WriteString(f.String() + "\n")
	}
	return sb.String()
}

func (sh Schemas) Copy() Schemas {
	var s Schemas
	for _, v := range sh {
		s = append(s, Field{Name: v.Name, Type: v.Type})
	}
	return s
}

func (sh Schemas) Len() int           { return len(sh) }
func (sh Schemas) Swap(i, j int)      { sh[i], sh[j] = sh[j], sh[i] }
func (sh Schemas) Less(i, j int) bool { return sh[i].Name < sh[j].Name }

func (sh Schemas) FieldIndex(name string) int {
	for i := range sh {
		if sh[i].Name == name {
			return i
		}
	}
	return -1
}

func (sh Schemas) FieldIndexMap() map[string]int {
	fieldMap := make(map[string]int)
	for i := range sh {
		fieldMap[sh[i].Name] = i
	}
	return fieldMap
}

func (sh Schemas) StringFieldIndex() []int {
	fieldIndex := make([]int, 0)
	for i := range sh {
		if sh[i].IsString() {
			fieldIndex = append(fieldIndex, i)
		}
	}
	return fieldIndex
}

func (sh Schemas) Field(i int) *Field {
	if i >= len(sh) || i < 0 {
		return nil
	}
	return &sh[i]
}

func (sh Schemas) Equal(to Schemas) bool {
	if len(sh) != len(to) {
		return false
	}

	// sh and to must be sorted
	for i := range sh {
		if !sh[i].equal(&to[i]) {
			return false
		}
	}

	return true
}
