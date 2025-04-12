// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package stringinterner

import (
	"strings"
	"sync"
)

// split to two struct to store, key and value. avoid map too big to search
var (
	//store key, field key, tag key, measurement name, db name, rp name
	key = SingleStringInterner{m: sync.Map{}}
	//store value, tag values
	value = SingleStringInterner{m: sync.Map{}}
)

// Single StringInterner For Inmutable Scenario
type SingleStringInterner struct {
	m sync.Map // its type is equivalent to map[sting]string
}

// InternSafe store key
func InternSafe(s string) string {
	return loadValue(s, &key)
}

// InternTagValue store value
func InternTagValue(s string) string {
	return loadValue(s, &value)
}

func loadValue(s string, si *SingleStringInterner) string {
	v, ok := si.m.Load(s)
	if !ok {
		k := strings.Clone(s)
		si.m.Store(k, k)
		return k
	}
	return v.(string)
}

const (
	EmptyStr = ""
)

func NewStringDict() *StringDict {
	return &StringDict{
		corpus:        sync.Map{},
		corpusIndexes: []string{EmptyStr},
	}
}

type StringDict struct {
	corpus        sync.Map
	corpusLock    sync.Mutex
	corpusIndex   uint64
	corpusIndexes []string
}

func (s *StringDict) LoadIndex(key string) uint64 {
	vv, ok := s.corpus.Load(key)
	if ok {
		index, _ := vv.(uint64)
		return index
	}
	s.corpusLock.Lock()
	s.corpusIndex = s.corpusIndex + 1
	index := s.corpusIndex
	if uint64(len(s.corpusIndexes)) <= index {
		s.corpusIndexes = append(s.corpusIndexes, EmptyStr)
		s.corpusIndexes = s.corpusIndexes[:cap(s.corpusIndexes)]
	}
	s.corpusLock.Unlock()
	key = strings.Clone(key)
	s.corpusIndexes[index] = key
	s.corpus.Store(key, index)
	return index
}

func (s *StringDict) LoadValue(key int) string {
	if len(s.corpusIndexes) <= key {
		return EmptyStr
	}
	return s.corpusIndexes[key]
}
