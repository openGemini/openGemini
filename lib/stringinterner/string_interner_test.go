// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package stringinterner_test

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/stretchr/testify/assert"
)

func TestInternSafe(t *testing.T) {
	s11 := "abc"
	s12 := stringinterner.InternSafe(s11)
	assert.Equal(t, s11, s12)

	s21 := ""
	s22 := stringinterner.InternSafe(s21)
	assert.Equal(t, s21, s22)

	s31 := "abc"
	s32 := stringinterner.InternSafe(s31)
	assert.Equal(t, s32, s12)

	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := keyTemp + strconv.Itoa(i)
				value := stringinterner.InternSafe(key)
				assert.Equal(t, key, value)
			}
		}()
	}
	wg.Wait()
}

var si SingleStringInterner

// Single StringInterner For Inmutable Scenario
type SingleStringInterner struct {
	m     map[string]string
	mutex sync.RWMutex
}

func InternSafe(s string) string {
	si.mutex.RLock()
	interned, ok := si.m[s]
	if ok {
		si.mutex.RUnlock()
		return interned
	}
	si.mutex.RUnlock()
	si.mutex.Lock()
	defer si.mutex.Unlock()

	if si.m == nil {
		si.m = make(map[string]string)
	}

	if interned, ok := si.m[s]; ok {
		return interned
	}

	var sb strings.Builder
	sb.WriteString(s)
	si.m[sb.String()] = sb.String()
	return sb.String()
}

func benchmarkInternSafeSyncMap() {
	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := keyTemp + strconv.Itoa(i)
				value := stringinterner.InternSafe(key)
				if key != value {
					panic("key should be equal to value")
				}
			}
		}()
	}
	wg.Wait()
}

func benchmarkInternSafe() {
	keyTemp := "aaaaaaaaaaaaaaaaaaaaaaaaa_"
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := keyTemp + strconv.Itoa(i)
				value := InternSafe(key)
				if key != value {
					panic("key should be equal to value")
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkInternSafe(t *testing.B) {
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		benchmarkInternSafe()
		t.StartTimer()
	}
}

func BenchmarkInternSafeSyncMap(t *testing.B) {
	t.ReportAllocs()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		t.StartTimer()
		benchmarkInternSafeSyncMap()
		t.StartTimer()
	}
}
