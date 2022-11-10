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
package clvIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)

type CLVIndexNode struct {
	VgramClvcIndexRoot  *gramIndex.IndexTree
	VgramClvlIndexRoot  *gramIndex.IndexTree
	LogTreeRoot         *gramIndex.LogTree
	VtokenClvcIndexRoot *tokenIndex.IndexTree
	VtokenClvlIndexRoot *tokenIndex.IndexTree
}

func NewCLVIndexNode() *CLVIndexNode {
	return &CLVIndexNode{
		VgramClvcIndexRoot:  gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM),
		VgramClvlIndexRoot:  gramIndex.NewIndexTree(QMINGRAM, QMAXGRAM),
		LogTreeRoot:         gramIndex.NewLogTree(QMAXGRAM),
		VtokenClvcIndexRoot: tokenIndex.NewIndexTree(QMINTOKEN, QMAXTOKEN),
		VtokenClvlIndexRoot: tokenIndex.NewIndexTree(QMINTOKEN, QMAXTOKEN),
	}
}

/*
	SHARDBUFFER is the number of data of a SHARD, LogIndex is a counter, and BuffLogStrings is used to store all the data of a SHARD, which is used to build indexes in batches.
*/

const SHARDBUFFER = 500000

var LogIndex = 0
var BuffLogStrings = make([]utils.LogSeries, 0)

func (clvIndexNode *CLVIndexNode) CreateCLVIndexIfNotExists(log string, tsid uint64, timeStamp int64, indexType CLVIndexType, dicType CLVDicType, dictionary CLVDictionary) {
	//fmt.Println(LogIndex)
	if LogIndex < SHARDBUFFER {
		BuffLogStrings = append(BuffLogStrings, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		LogIndex += 1
	}
	if LogIndex == SHARDBUFFER {
		if indexType == VGRAM {
			clvIndexNode.CreateCLVVGramIndexIfNotExists(dicType, BuffLogStrings, dictionary.VgramClvcDicRoot, dictionary.VgramClvlDicRoot)
		}
		if indexType == VTOKEN {
			clvIndexNode.CreateCLVVTokenIndexIfNotExists(dicType, BuffLogStrings, dictionary.VtokenClvcDicRoot, dictionary.VtokenClvlDicRoot)
		}
		BuffLogStrings = make([]utils.LogSeries, 0)
		LogIndex = 0
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVGramIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vgramClvcDicRoot *gramClvc.TrieTree, vgramClvlDicRoot *gramClvc.TrieTree) {
	if dicType == CLVC {
		clvIndexNode.VgramClvcIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, QMAXGRAM, LOGTREEMAX, vgramClvcDicRoot.Root())
	}
	if dicType == CLVL {
		clvIndexNode.VgramClvlIndexRoot, _, clvIndexNode.LogTreeRoot = gramIndex.GenerateIndexTree(buffLogStrings, QMINGRAM, vgramClvlDicRoot.Qmax(), LOGTREEMAX, vgramClvlDicRoot.Root())
	}
}

func (clvIndexNode *CLVIndexNode) CreateCLVVTokenIndexIfNotExists(dicType CLVDicType, buffLogStrings []utils.LogSeries, vtokenClvcDicRoot *tokenClvc.TrieTree, vtokenClvlDicRoot *tokenClvc.TrieTree) {
	if dicType == CLVC {
		clvIndexNode.VtokenClvcIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, QMAXTOKEN, vtokenClvcDicRoot.Root())
	}
	if dicType == CLVL {
		clvIndexNode.VtokenClvlIndexRoot, _ = tokenIndex.GenerateIndexTree(buffLogStrings, QMINTOKEN, vtokenClvlDicRoot.Qmax(), vtokenClvlDicRoot.Root())
	}
}
