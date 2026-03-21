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

package util

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/stringinterner"
)

type MeasurementIdent struct {
	DB   string
	RP   string
	Name string
}

func (t *MeasurementIdent) SetSafeName(name string) {
	t.Name = stringinterner.InternSafe(name)
}

func (t *MeasurementIdent) SetName(name string) {
	t.Name = name
}

func (t *MeasurementIdent) String() string {
	return fmt.Sprintf("%s.%s.%s", t.DB, t.RP, t.Name)
}

func NewMeasurementIdent(db, rp string) MeasurementIdent {
	return MeasurementIdent{
		DB: db,
		RP: rp,
	}
}
