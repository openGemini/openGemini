//go:build logstore
// +build logstore

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

package tokenizer

func NewGramTokenizer(splitChars string, seed uint64, version uint32) Tokenizer {
	return tokenizer.NewSimpleGramTokenizer(splitChars, seed, version)
}

func FreeSimpleGramTokenizer(t Tokenizer) {
	t.FreeSimpleGramTokenizer()
}
