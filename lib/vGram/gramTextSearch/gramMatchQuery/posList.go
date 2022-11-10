/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package gramMatchQuery

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type PosList struct {
	sid      utils.SeriesId
	posArray []uint16
}

func (p *PosList) Sid() utils.SeriesId {
	return p.sid
}

func (p *PosList) SetSid(sid utils.SeriesId) {
	p.sid = sid
}

func (p *PosList) PosArray() []uint16 {
	return p.posArray
}

func (p *PosList) SetPosArray(posArray []uint16) {
	p.posArray = posArray
}

func NewPosList(sid utils.SeriesId, posArray []uint16) PosList {
	return PosList{
		sid:      sid,
		posArray: posArray,
	}
}
