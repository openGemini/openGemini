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

package ski

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testPath = "/tmp/ski/index_test"

func getTestIndex() *ShardKeyIndex {
	lockPath := ""
	index, err := NewShardKeyIndex(testPath, &lockPath)
	if err != nil {
		panic(err)
	}
	return index
}

func clear(idx *ShardKeyIndex) {
	if err := idx.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(testPath); err != nil {
		panic(err)
	}
	idx = nil
}

func createIndex(idx *ShardKeyIndex) {
	keys := []string{
		"mn-1,tk1=value1,tk2=value2,tk3=value3",
		"mn-1,tk1=value11,tk2=value22,tk3=value33",
		"mn-1,tk1=value1,tk2=value22,tk3=value3",
		"mn-1,tk1=value11,tk2=value2,tk3=value33",
		"mn-1,tk1=value11,tk2=value22,tk3=value3",
	}

	name := []byte("mn-1")
	for i := range keys {
		err := idx.CreateIndex(name, []byte(keys[i]), uint64(i))
		if err != nil {
			panic(err)
		}
	}
	if err := idx.Close(); err != nil {
		panic(err)
	}
	if err := idx.Open(); err != nil {
		panic(err)
	}
}

func TestShardKeyIndex_GetShardSeriesCount(t *testing.T) {
	idx := getTestIndex()
	defer clear(idx)
	createIndex(idx)
	count := idx.GetShardSeriesCount()
	assert.Equal(t, count, 5)
}

func TestShardKeyIndex_GetSplitPointsWithSeriesCount(t *testing.T) {
	idx := getTestIndex()
	defer clear(idx)
	createIndex(idx)
	splitKeys, err := idx.GetSplitPointsWithSeriesCount([]int64{2, 4})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, splitKeys, []string{"mn-1,tk1=value1,tk2=value22,tk3=value3", "mn-1,tk1=value11,tk2=value22,tk3=value3"})
}
