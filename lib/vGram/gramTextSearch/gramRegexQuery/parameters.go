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
package gramRegexQuery

type Parameters struct {
	defaultGramLen int
	alphabet       []int
}

func NewParameters() *Parameters {
	alphabet := make([]int, 127)
	for i := 0; i < 127; i++ {
		alphabet[i] = 1
	}
	return &Parameters{
		defaultGramLen: 3,
		alphabet:       alphabet,
	}
}
