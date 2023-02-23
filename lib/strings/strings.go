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

package strings

import (
	"strings"
	"unsafe"

	strings2 "github.com/openGemini/openGemini/open_src/github.com/savsgio/gotils/strings"
)

func Union(s1, s2 []string) []string {
	return strings2.UniqueAppend(s1, s2...)
}

func UnionSlice(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	m := make(map[string]struct{}, len(s))

	for i := 0; i < len(s); i++ {
		m[s[i]] = struct{}{}
	}

	n := 0
	for k := range m {
		s[n] = k
		n++
	}

	return s[:n]
}

func ContainsInterface(s interface{}, sub string) bool {
	ss, ok := s.(string)
	if !ok {
		return false
	}
	return strings.Contains(ss, sub)
}

func EqualInterface(i interface{}, s string) bool {
	ss, ok := i.(string)
	if !ok {
		return false
	}
	return ss == s
}

// TODO: replace it when the go version upgrades 1.18
func Clone(s string) string {
	if len(s) == 0 {
		return ""
	}
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}

// SortIsEqual compares if two sorted strings are the equal
func SortIsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
