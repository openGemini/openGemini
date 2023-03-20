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

package strings_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/strings"
	"github.com/stretchr/testify/assert"
)

func TestUnionSlice(t *testing.T) {
	var data = [][]string{
		{"a", "a", "d", "a", "x", "b", "b", "c", "x", "x"},
		{},
		{"a"},
		{"a", "a", "a"},
		{"a", "c", "d"},
		{"b", "a"},
		{"a", "b", "b", "b"},
	}
	var exp = [][]string{
		{"a", "d", "b", "c", "x"},
		{},
		{"a"},
		{"a"},
		{"a", "c", "d"},
		{"b", "a"},
		{"a", "b"},
	}

	for i, s := range data {
		got := strings.UnionSlice(s)

		sort.Strings(exp[i])
		sort.Strings(got)

		if !reflect.DeepEqual(exp[i], got) {
			t.Fatalf("failed, exp: %+v; got:%+v", exp[i], got)
		}
	}
}

func TestContainsInterface(t *testing.T) {
	var s interface{} = "aaa.bbb.ccc"
	assert.True(t, strings.ContainsInterface(s, "aaa"))

	assert.False(t, strings.ContainsInterface(111, "1"))
}

func TestEqualInterface(t *testing.T) {
	var s interface{} = "aaa"
	assert.True(t, strings.EqualInterface(s, "aaa"))

	assert.False(t, strings.EqualInterface(111, "111"))
}
