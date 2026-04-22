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
	"github.com/golang/geo/s2"
)

const (
	MinLevel        = 1
	MaxLevel        = 14 // https://s2geometry.io/resources/s2cell_statistics.html
	AbsoluteMaxSize = 100
	// The Earth's mean radius in kilometers (according to NASA).
	earthRadiusKm = 6371.01
)

func CoverCell(region s2.Region) []s2.CellID {
	rc := &s2.RegionCoverer{MaxLevel: MaxLevel, MinLevel: MaxLevel, LevelMod: 1, MaxCells: AbsoluteMaxSize}
	return rc.Covering(region)
}
