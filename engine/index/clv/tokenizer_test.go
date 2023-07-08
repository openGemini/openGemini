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

package clv

import (
	"testing"
)

type TokenizerTest struct {
	content string
	result  []string
}

func TestTokenizer(t *testing.T) {
	contents := []TokenizerTest{
		{content: "get", result: []string{"get"}},
		{content: "  get iamges 123.png http 1.0  ", result: []string{"get", "iamges", "123.png", "http", "1.0"}},
		{content: "()[]{}?@&<>", result: []string{""}},
		{content: "panda熊猫", result: []string{"panda", "熊", "猫"}},
		{content: "panda 熊猫 panda", result: []string{"panda", "熊", "猫", "panda"}},
		{content: "panda[熊猫]panda", result: []string{"panda", "熊", "猫", "panda"}},
		{content: "熊", result: []string{"熊"}},
		{content: "[熊]", result: []string{"熊"}},
		{content: "panda[熊猫]", result: []string{"panda", "熊", "猫"}},
	}

	tokenizer := NewDefaultSimpleTokenzier()

	for i := 0; i < len(contents); i++ {
		tokenizer.SetData([]byte(contents[i].content))
		j := 0
		for tokenizer.Next() {
			if string(tokenizer.Token()) != contents[i].result[j] {
				t.Fatalf("Tokenizer failed. the %s-%d term expect: %s, get: %s", contents[i].content, j, contents[i].result[j], string(tokenizer.Token()))
			}
			j++
		}
	}
}
