// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this File except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fence

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	GeometrySep = ","
)

func ReloadFence() error {

	mg := ManagerIns()
	return mg.reloadFenceFile()

}

func RewriteRows(rows []influx.Row) {
	mg := ManagerIns()

	for i := range rows {
		row := &rows[i]
		if row.Name == "fence" {
			fence, err := ParseFenceFromRow(row)
			if err != nil {
				logger.GetLogger().Error("failed parse fence from influx row", zap.Error(err))
				continue
			}
			_, ok := mg.fences.Load(fence.ID)
			if ok {
				continue
			}
			mg.Add(fence)

			err = mg.WriteFenceFile(fence, false)
			if err != nil {
				logger.GetLogger().Error("failed write fence File", zap.Error(err))
			}
		}
	}
}

func ParseFenceFromRow(row *influx.Row) (*Fence, error) {
	var id, geometry string

	for i := range row.Tags {
		if row.Tags[i].Key == "id" {
			id = strings.Clone(row.Tags[i].Value)
			break
		}
	}

	if id == "" {
		return nil, fmt.Errorf("invalid geometry row. missing id tag")
	}

	for i := range row.Fields {
		if row.Fields[i].Key == "geometry" {
			geometry = row.Fields[i].StrValue
			break
		}
	}

	if geometry == "" {
		return nil, fmt.Errorf("invalid geometry row. missing geometry field")
	}

	return ParseFence(id, geometry)
}

func ParseFence(id string, geometry string) (*Fence, error) {
	idx := strings.Index(geometry, GeometrySep)
	if idx <= 0 {
		return nil, fmt.Errorf("invalid geometry: %s", geometry)
	}

	typ := geometry[:idx]

	f64s, err := ParseFloats(geometry[idx+1:])
	if err != nil {
		return nil, fmt.Errorf("invalid geometry: %s", geometry)
	}

	fence := &Fence{ID: id, Typ: typ}
	switch typ {
	case GeometryCircle:
		fence.Geometry = &Circle{}
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", typ)
	}

	err = fence.Geometry.Build(f64s)

	return fence, err
}

func ParseFloats(s string) ([]float64, error) {
	var f64s = make([]float64, 0, strings.Count(s, GeometrySep)+1)
	start := 0
	end := 0

	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			end = i
		} else if i == len(s)-1 {
			end = i + 1
		} else {
			continue
		}

		f, err := strconv.ParseFloat(s[start:end], 64)
		if err != nil {
			return nil, err
		}

		f64s = append(f64s, f)
		start = i + 1
	}

	return f64s, nil
}
