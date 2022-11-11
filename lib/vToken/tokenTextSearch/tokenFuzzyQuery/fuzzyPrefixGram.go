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
package tokenFuzzyQuery

type FuzzyPrefixGram struct {
	gram string
	pos  int
}

func (p *FuzzyPrefixGram) Gram() string {
	return p.gram
}

func (p *FuzzyPrefixGram) SetGram(gram string) {
	p.gram = gram
}

func (p *FuzzyPrefixGram) Pos() int {
	return p.pos
}

func (p *FuzzyPrefixGram) SetPos(pos int) {
	p.pos = pos
}
func NewFuzzyPrefixGram(gram string, pos int) FuzzyPrefixGram {
	return FuzzyPrefixGram{
		gram: gram,
		pos:  pos,
	}
}
