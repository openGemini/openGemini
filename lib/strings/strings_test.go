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
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/rand"
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

	for _, s := range data {
		s2 := make([]string, len(s))
		copy(s2, s)
		exp := strings.Union([]string{}, s2)
		got := strings.UnionSlice(s)

		sort.Strings(exp)
		sort.Strings(got)

		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("failed, exp: %+v; got:%+v", exp, got)
		}
	}
}

func TestHashUnionSlice(t *testing.T) {
	var data = [][]string{
		{"a", "a", "d", "a", "x", "b", "b", "c", "x", "x"},
		{},
		{"a"},
		{"a", "a", "a"},
		{"a", "c", "d"},
		{"b", "a"},
		{"a", "b", "b", "b"},
	}

	for _, s := range data {
		s2 := make([]string, len(s))
		copy(s2, s)
		exp := strings.Union([]string{}, s2)
		got := strings.UnionSlice(s)

		sort.Strings(exp)
		sort.Strings(got)

		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("failed, exp: %+v; got:%+v", exp, got)
		}
	}
}

func makeStringSlice() []string {
	n := 5000
	s := make([]string, n)
	for k := 0; k < n; k++ {
		s[k] = fmt.Sprintf("ss_%d", k%(n*3/4))
	}
	sort.Slice(s, func(i, j int) bool {
		return rand.Float64() > 0.5
	})
	return s
}

// BenchmarkUnionSlice-12    	      26	  39023485 ns/op
func BenchmarkUnionSlice(b *testing.B) {
	s := makeStringSlice()

	for i := 0; i < b.N; i++ {
		s2 := make([]string, len(s))
		copy(s2, s)
		strings.Union([]string{}, s2)
	}
}

// BenchmarkHashUnionSlice-12    	    4785	    252293 ns/op
func BenchmarkHashUnionSlice(b *testing.B) {
	s := makeStringSlice()
	for i := 0; i < b.N; i++ {
		s2 := make([]string, len(s))
		copy(s2, s)
		strings.UnionSlice(s2)
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
