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
package gramIndex

type SubGramOffset struct {
	subGram string
	offset  uint16
}

func (s *SubGramOffset) SubGram() string {
	return s.subGram
}

func (s *SubGramOffset) SetSubGram(subGram string) {
	s.subGram = subGram
}

func (s *SubGramOffset) Offset() uint16 {
	return s.offset
}

func (s *SubGramOffset) SetOffset(offset uint16) {
	s.offset = offset
}

func NewSubGramOffset(subGram string, offset uint16) SubGramOffset {
	return SubGramOffset{
		subGram: subGram,
		offset:  offset,
	}
}
