// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package consistenthash

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashing(t *testing.T) {
	hash := New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})
	hash.Add("6", "4", "2")
	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}
	for k, v := range testCases {
		require.Equal(t, v, hash.Get(k))
	}
	hash.Add("8")
	testCases["27"] = "8"
	for k, v := range testCases {
		require.Equal(t, v, hash.Get(k))
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)
	hash1.Add("aaa", "bbb", "ccc")
	hash2.Add("bbb", "ccc", "ddd")
	require.Equal(t, hash1.Get("aaa"), hash2.Get("aaa"))
}
