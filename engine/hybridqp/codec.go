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

package hybridqp

import (
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func ParseFields(s string) (influxql.Fields, error) {
	if s == "" {
		return nil, nil
	}

	p := influxql.NewParser(strings.NewReader("SELECT " + s + " FROM mock"))
	defer p.Release()

	statement, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	stmt, ok := statement.(*influxql.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("invalid fields: %s", s)
	}

	return stmt.Fields, nil
}

type MapConvert struct {
}

// IntToInt64 convert map[string] int to map[string] int64
func (c *MapConvert) IntToInt64(m map[string]int) map[string]int64 {
	if m == nil {
		return nil
	}

	ret := make(map[string]int64, len(m))
	for k, v := range m {
		ret[k] = int64(v)
	}
	return ret
}

// Int64ToInt convert map[string] int64 to map[string] int
func (c *MapConvert) Int64ToInt(m map[string]int64) map[string]int {
	if m == nil {
		return nil
	}

	ret := make(map[string]int, len(m))
	for k, v := range m {
		ret[k] = int(v)
	}
	return ret
}

func (c *MapConvert) BoolToStruct(m map[string]bool) map[string]struct{} {
	if m == nil {
		return nil
	}

	ret := make(map[string]struct{}, len(m))
	for k := range m {
		ret[k] = struct{}{}
	}
	return ret
}

func (c *MapConvert) StructToBool(m map[string]struct{}) map[string]bool {
	if m == nil {
		return nil
	}

	ret := make(map[string]bool, len(m))
	for k := range m {
		ret[k] = true
	}
	return ret
}
