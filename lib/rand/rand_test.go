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

package rand_test

import (
	"math"
	rand2 "math/rand"
	"testing"

	"github.com/openGemini/openGemini/lib/rand"
)

func TestRand(t *testing.T) {
	var fns = []func(){
		func() {
			v := rand.Int63()
			if v > math.MaxInt64 || v < math.MinInt64 {
				t.Fatalf("invalid random number. exp: [%d, %d]; got: %d",
					math.MinInt64, math.MaxInt64, v)
			}
		},
		func() {
			var n int64 = 123456789
			v := rand.Int63n(n)
			if v >= n {
				t.Fatalf("invalid random number. exp: [0, %d); got: %d",
					n, v)
			}
		},
		func() {
			var n int32 = 123456789
			v := rand.Int31n(n)
			if v >= n {
				t.Fatalf("invalid random number. exp: [0, %d); got: %d",
					n, v)
			}
		},
		func() {
			var n = 123456789
			v := rand.Intn(n)
			if v >= n {
				t.Fatalf("invalid random number. exp: [0, %d); got: %d",
					n, v)
			}
		},
		func() {
			v := rand.Float64()
			if v < 0 || v >= 1 {
				t.Fatalf("invalid random number. exp: [0, 1); got: %v", v)
			}
		},
	}

	for _, f := range fns {
		for i := 0; i < 100; i++ {
			f()
		}
	}
}

func BenchmarkRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = rand.Int63()
	}
}

func BenchmarkMathRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = rand2.Int63()
	}
}
