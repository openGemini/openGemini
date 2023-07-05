/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package sparseindex

var ConsiderOnlyBeTrue = NewMark(false, true)

// Mark these special constants are used to implement KeyCondition.
// When used as an initial_mask argument in KeyCondition.CheckInRange methods,
// they effectively prevent calculation of discarded Mark component as it is already set to true.
type Mark struct {
	canBeTrue  bool
	canBeFalse bool
}

func NewMark(canBeTrue, canBeFalse bool) *Mark {
	return &Mark{canBeTrue: canBeTrue, canBeFalse: canBeFalse}
}

func (m *Mark) And(mask *Mark) *Mark {
	return NewMark(m.canBeTrue && mask.canBeTrue, m.canBeFalse || mask.canBeFalse)
}

func (m *Mark) Or(mask *Mark) *Mark {
	return NewMark(m.canBeTrue || mask.canBeTrue, m.canBeFalse && mask.canBeFalse)
}

func (m *Mark) Not() *Mark {
	return NewMark(m.canBeFalse, m.canBeTrue)
}

// isComplete If mask is (true, true), then it can no longer change under operation |.
// We use this condition to early-exit.
func (m *Mark) isComplete() bool {
	return m.canBeFalse && m.canBeTrue
}
