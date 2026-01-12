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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	str := "this is a test string"
	assert.Equal(t, uint64(2920730543235385379), Hash([]byte(str)))
	str1 := "this is a test string1"
	str2 := "this is a test string2"
	assert.NotEqual(t, uint64(2920730543235385379), Hash([]byte(str1)))
	assert.NotEqual(t, uint64(2920730543235385379), Hash([]byte(str2)))
}
