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

package atomic

import (
	"fmt"
	"testing"
)

func TestAdd(t *testing.T) {
	var a, b float64
	a = 1.0
	b = 2
	r := AddFloat64(&a, b)
	if r != 3 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 3, r))
	}
}

func TestMin(t *testing.T) {
	var a, b float64
	a = 2
	b = 1
	CompareAndSwapMinFloat64(&a, b)
	if a != 1.0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1.0, a))
	}
}

func TestMax(t *testing.T) {
	var a, b float64
	a = 1.0
	b = 2
	CompareAndSwapMaxFloat64(&a, b)
	if a != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, a))
	}
}

func BenchmarkMin(t *testing.B) {
	var a, b float64
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			a = 2
			b = 1
			CompareAndSwapMinFloat64(&a, b)
		}
	}
}

func BenchmarkAdd(t *testing.B) {
	var a, b float64
	for i := 0; i < t.N; i++ {
		for i := 0; i < 10000000; i++ {
			a = 2
			b = 1
			AddFloat64(&a, b)
		}
	}
}

func TestSetAndSwapPointerFloat64(t *testing.T) {
	a := make([]*float64, 1)
	var tt float64
	tt = 5
	SetAndSwapPointerFloat64(&a[0], &tt)
	if *a[0] != tt {
		t.Error(fmt.Sprintf("expect %v ,got %v", tt, *a[0]))
	}
	tt = 3
	if *a[0] != tt {
		t.Error(fmt.Sprintf("expect %v ,got %v", tt, *a[0]))
	}
	SetAndSwapPointerFloat64(&a[0], nil)
	if a[0] != nil {
		t.Error(fmt.Sprintf("expect %v ,got %v", nil, a[0]))
	}
}
