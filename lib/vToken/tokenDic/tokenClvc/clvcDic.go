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

package tokenClvc

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type GramDictionary interface {
	GenerateClvcDictionaryTree(logs []utils.LogSeries, qmin int, qmax int, T int)
}

type CLVCDic struct {
	TrieTree *TrieTree
}

func NewCLVCDic(qmin int, qmax int) *CLVCDic {
	return &CLVCDic{
		TrieTree: NewTrieTree(qmin, qmax),
	}
}

/*
	logs are log data, qmin and qmax are the minimum and maximum lengths for dictionary building and dividing the index items,
	and the threshold T is the empirical value parameter for building a dictionary and the return value is a dictionary tree.
*/

func (clvcDic *CLVCDic) GenerateClvcDictionaryTree(logs []utils.LogSeries, qmin int, qmax int, T int) {
	for i := range logs {
		str := logs[i].Log
		tokenArray, _ := utils.DataProcess(str)
		if len(tokenArray) >= qmax {
			for i = 0; i < len(tokenArray)-qmax; i++ {
				var substring = tokenArray[i : i+qmax]
				clvcDic.TrieTree.InsertIntoTrieTree(&substring)
			}
			for i = len(tokenArray) - qmax; i < len(tokenArray)-qmin+1; i++ {
				var substring = tokenArray[i:len(tokenArray)]
				clvcDic.TrieTree.InsertIntoTrieTree(&substring)
			}
		} else {
			clvcDic.TrieTree.InsertIntoTrieTree(&tokenArray)
		}
	}
	clvcDic.TrieTree.PruneTree(T)
	clvcDic.TrieTree.UpdateRootFrequency()
}
