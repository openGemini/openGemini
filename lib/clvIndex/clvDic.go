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
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvl"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvl"
)

/*
	QMINGRAM and QMAXGRAM are the minimum and maximum lengths of the tokenizer segmented under the VGRAM index.
	QMINTOKEN and QMAXTOKEN are the minimum and maximum lengths of the tokenizer tokens under the VTOKEN index.
	TGRAM and TTOKEN are the parameter thresholds used by the dictionary (word segmenter) under the VGRAM and VTOKEN indexes respectively,It is only used when constructing the dictionary and is an empirical threshold.
	GRAMWLEN and TOKENWLEN are the number of loads in building a learned dictionary query.
	MAXDICBUFFER is the number of data entries for a SHARD build dictionary, DicIndex is a counter, BuffDicStrings are used to store a batch of log data to build a dictionary.
*/

const QMINGRAM = 2
const QMAXGRAM = 12
const LOGTREEMAX = 12
const QMINTOKEN = 1
const QMAXTOKEN = 2
const TGRAM = 40
const TTOKEN = 40
const GRAMWLEN = 20
const TOKENWLEN = 20

const MAXDICBUFFER = 1

var DicIndex = 0
var BuffDicStrings []utils.LogSeries

const DICOUTPATH = "../../lib/persistence/data/measurement/dicout/"

type CLVDictionary struct {
	DicType       CLVDicType
	VgramDicRoot  *gramClvc.TrieTree
	VtokenDicRoot *tokenClvc.TrieTree
}

func NewCLVDictionary() *CLVDictionary {
	return &CLVDictionary{
		DicType:       CLVC,
		VgramDicRoot:  gramClvc.NewTrieTree(QMINGRAM, QMAXGRAM),
		VtokenDicRoot: tokenClvc.NewTrieTree(QMINTOKEN, QMAXTOKEN),
	}
}

func (clvDic *CLVDictionary) CreateDictionaryIfNotExists(log string, tsid uint64, timeStamp int64, indexType CLVIndexType) {
	if DicIndex < MAXDICBUFFER {
		BuffDicStrings = append(BuffDicStrings, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		DicIndex += 1
	}
	if DicIndex == MAXDICBUFFER {
		if indexType == VGRAM {
			clvDic.CreateCLVVGramDictionaryIfNotExists(BuffDicStrings)
		}
		if indexType == VTOKEN {
			clvDic.CreateCLVVTokenDictionaryIfNotExists(BuffDicStrings)
		}
	}
}

func (clvDic *CLVDictionary) CreateCLVVGramDictionaryIfNotExists(buffDicStrings []utils.LogSeries) {
	if clvDic.DicType == CLVC {
		clvcdic := gramClvc.NewCLVCDic(QMINGRAM, QMAXGRAM)
		clvcdic.GenerateClvcDictionaryTree(buffDicStrings, QMINGRAM, QMAXGRAM, TGRAM)
		clvDic.VgramDicRoot = clvcdic.TrieTree
	}
	if clvDic.DicType == CLVL {
		sampleStrOfWlen := utils.HasSample(buffDicStrings, GRAMWLEN)
		bufflogs := utils.LogSeriesToMap(buffDicStrings)
		clvldic := gramClvl.NewCLVLDic(QMINGRAM, QMAXGRAM)
		clvldic.GenerateClvlDictionaryTree(bufflogs, QMINGRAM, sampleStrOfWlen)
		clvDic.VgramDicRoot = clvldic.TrieTree
	}
}

func (clvDic *CLVDictionary) CreateCLVVTokenDictionaryIfNotExists(buffDicStrings []utils.LogSeries) {
	if clvDic.DicType == CLVC {
		clvcdic := tokenClvc.NewCLVCDic(QMINTOKEN, QMAXTOKEN)
		clvcdic.GenerateClvcDictionaryTree(buffDicStrings, QMINTOKEN, QMAXTOKEN, TTOKEN)
		clvDic.VtokenDicRoot = clvcdic.TrieTree
	}
	if clvDic.DicType == CLVL {
		sampleStrOfWlen := utils.HasSample(buffDicStrings, TOKENWLEN)
		bufflogs := utils.LogSeriesToMap(buffDicStrings)
		clvldic := tokenClvl.NewCLVLDic(QMINTOKEN, QMAXTOKEN)
		clvldic.GenerateClvlDictionaryTree(bufflogs, QMINTOKEN, sampleStrOfWlen)
		clvDic.VtokenDicRoot = clvldic.TrieTree
	}
}
