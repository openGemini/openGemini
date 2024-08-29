// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package clv

const (
	DefaultSplitGram = ", '\";=()[]{}?@&<>/:\n\t\r"
)

type SimpleTokenizer struct {
	splitGramTable []byte

	data  []byte
	token []byte
}

func NewSimpleTokenizer() *SimpleTokenizer {
	tokenizer := &SimpleTokenizer{
		splitGramTable: make([]byte, 256),
	}

	return tokenizer
}

func NewDefaultSimpleTokenzier() *SimpleTokenizer {
	tokenizer := NewSimpleTokenizer()
	tokenizer.SetSplitGram(DefaultSplitGram)
	return tokenizer
}

// init the split-gram-table
func (t *SimpleTokenizer) SetSplitGram(splitGram string) {
	for _, c := range splitGram {
		t.splitGramTable[c] = 1
	}
}

func (t *SimpleTokenizer) SetData(data []byte) {
	t.data = data
}

func (t *SimpleTokenizer) isSplit(b byte) bool {
	return (b&0x80 == 0) && (t.splitGramTable[b] == 1)
}

func (t *SimpleTokenizer) isChar(b byte) bool {
	return (b&0x80 == 0) && (t.splitGramTable[b] == 0)
}

func (t *SimpleTokenizer) Next() bool {
	if len(t.data) == 0 {
		return false
	}

	start := 0
	end := 0
	max := len(t.data)
	for end < max {
		c := t.data[end]
		if t.isSplit(c) {
			start++
			end++
			continue
		}

		if t.isChar(c) {
			// 1=byte utf-8
			for end < max && t.isChar(t.data[end]) {
				end++
			}
			break
		} else if c < 0xe0 {
			end += 2 // 2-byte utf-8
			break
		} else if c < 0xf0 {
			end += 3 // 3-byte utf-8
			break
		} else if c < 0xf8 {
			end += 4 // 4-byte utf-8
			break
		}
	}
	if start == end || end > max {
		return false
	}

	t.token = t.data[start:end]
	t.data = t.data[end:]

	return true
}

func (t *SimpleTokenizer) Token() []byte {
	return t.token
}
