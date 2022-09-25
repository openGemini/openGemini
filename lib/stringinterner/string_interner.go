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

package stringinterner

import (
	"strings"
	"sync"
	"unsafe"
)

func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

var si SingleStringInterner

// Single StringInterner For Inmutable Scenario
type SingleStringInterner struct {
	m     map[string]string
	mutex sync.RWMutex
}

// Warning thread unsafe
func InternUnsafe(s string) string {
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

func InternSafeBytes(b []byte) string {
	si.mutex.Lock()
	defer si.mutex.Unlock()

	s := Bytes2str(b)

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

// Pooled StringInterner For Mutable Scenario
type PooledStringInterner struct {
	pool sync.Pool
}

func (psi *PooledStringInterner) String(s string) string {
	m := psi.pool.Get().(map[string]string)
	if m == nil {
		m = make(map[string]string)
	} else {
		c, ok := m[s]
		if ok {
			psi.pool.Put(m)
			return c
		}
	}

	m[s] = s
	psi.pool.Put(m)
	return s
}

func (psi *PooledStringInterner) Bytes(b []byte) string {
	s := string(b)
	m := psi.pool.Get().(map[string]string)
	if m == nil {
		m = make(map[string]string)
	} else {
		c, ok := m[s]
		if ok {
			psi.pool.Put(m)
			return c
		}
	}

	m[s] = s
	psi.pool.Put(m)
	return s
}
