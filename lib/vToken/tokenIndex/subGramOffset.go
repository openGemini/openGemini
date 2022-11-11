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
package tokenIndex

type SubTokenOffset struct {
	subToken []string
	offset   uint16
}

func (s *SubTokenOffset) SubToken() []string {
	return s.subToken
}

func (s *SubTokenOffset) SetSubToken(subToken []string) {
	s.subToken = subToken
}

func (s *SubTokenOffset) Offset() uint16 {
	return s.offset
}

func (s *SubTokenOffset) SetOffset(offset uint16) {
	s.offset = offset
}

func NewSubGramOffset(subToken []string, offset uint16) SubTokenOffset {
	return SubTokenOffset{
		subToken: subToken,
		offset:   offset,
	}
}
