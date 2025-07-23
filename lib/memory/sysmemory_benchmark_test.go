// Copyright 2025 openGemini Authors.
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

package memory_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/memory"
)

func BenchmarkSysMem(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		memory.GetMemMonitor().SysMem()
	}
}

// go test -bench=BenchmarkSysMem -benchtime=5s
/*
	goos: windows
	goarch: amd64
	pkg: github.com/openGemini/openGemini/lib/memory
	cpu: AMD Ryzen 5 5600H with Radeon Graphics
	BenchmarkSysMem-12      706152351                8.444 ns/op           0 B/op          0 allocs/op
	PASS
	ok      github.com/openGemini/openGemini/lib/memory     6.978s
*/

/*
	goos: linux
	goarch: amd64
	pkg: github.com/openGemini/openGemini/lib/memory
	cpu: AMD Ryzen 5 5600H with Radeon Graphics
	BenchmarkSysMem-12        924720              1281 ns/op               0 B/op          0 allocs/op
	PASS
	ok      github.com/openGemini/openGemini/lib/memory     1.232s
*/
