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

const MAXDICBUFFER = 5000

var DicIndex = 0
var BuffDicStrings []utils.LogSeries

type CLVDictionary struct {
	DicType           CLVDicType
	VgramClvcDicRoot  *gramClvc.TrieTree
	VgramClvc         bool
	VgramClvlDicRoot  *gramClvc.TrieTree
	VgramClvl         bool
	VtokenClvcDicRoot *tokenClvc.TrieTree
	VtokenClvc        bool
	VtokenClvlDicRoot *tokenClvc.TrieTree
	VtokenClvl        bool
}

func NewCLVDictionary() *CLVDictionary {
	return &CLVDictionary{
		DicType:           CLVC,
		VgramClvcDicRoot:  gramClvc.NewTrieTree(QMINGRAM, QMAXGRAM),
		VgramClvc:         false,
		VgramClvlDicRoot:  gramClvc.NewTrieTree(QMINGRAM, QMAXGRAM),
		VgramClvl:         false,
		VtokenClvcDicRoot: tokenClvc.NewTrieTree(QMINTOKEN, QMAXTOKEN),
		VtokenClvc:        false,
		VtokenClvlDicRoot: tokenClvc.NewTrieTree(QMINTOKEN, QMAXTOKEN),
		VtokenClvl:        false,
	}
}

func (clvDic *CLVDictionary) CreateDictionaryIfNotExists(log string, tsid uint64, timeStamp int64, indexType CLVIndexType) {
	if DicIndex < MAXDICBUFFER {
		BuffDicStrings = append(BuffDicStrings, utils.LogSeries{Log: log, Tsid: tsid, TimeStamp: timeStamp})
		DicIndex += 1
	}
	if DicIndex == MAXDICBUFFER {
		if indexType == VGRAM && (clvDic.VgramClvc == false && clvDic.VgramClvl == false) { //!(clvDic.VgramClvc == true && clvDic.VgramClvl == true)
			clvDic.CreateCLVVGramDictionaryIfNotExists(BuffDicStrings) //clvDic.VgramClvcDicRoot, clvDic.VgramClvlDicRoot
			if len(clvDic.VgramClvcDicRoot.Root().Children()) > 0 {
				clvDic.VgramClvc = true
			}
			if len(clvDic.VgramClvlDicRoot.Root().Children()) > 0 {
				clvDic.VgramClvl = true
			}
		}
		if indexType == VTOKEN && (clvDic.VtokenClvc == false && clvDic.VtokenClvl == false) { //!(clvDic.VtokenClvc == true && clvDic.VtokenClvl == true)
			clvDic.CreateCLVVTokenDictionaryIfNotExists(BuffDicStrings)
			if len(clvDic.VtokenClvcDicRoot.Root().Children()) > 0 {
				clvDic.VtokenClvc = true
			}
			if len(clvDic.VtokenClvlDicRoot.Root().Children()) > 0 {
				clvDic.VtokenClvl = true
			}
		}
	}
}

func (clvDic *CLVDictionary) CreateCLVVGramDictionaryIfNotExists(buffDicStrings []utils.LogSeries) {
	if clvDic.DicType == CLVC {
		clvcdic := gramClvc.NewCLVCDic(QMINGRAM, QMAXGRAM)
		clvcdic.GenerateClvcDictionaryTree(buffDicStrings, QMINGRAM, QMAXGRAM, TGRAM)
		clvDic.VgramClvcDicRoot = clvcdic.TrieTree
	}
	if clvDic.DicType == CLVL {
		sampleStrOfWlen := utils.HasSample(buffDicStrings, GRAMWLEN)
		bufflogs := utils.LogSeriesToMap(buffDicStrings)
		clvldic := gramClvl.NewCLVLDic(QMINGRAM, QMAXGRAM)
		clvldic.GenerateClvlDictionaryTree(bufflogs, QMINGRAM, sampleStrOfWlen)
		clvDic.VgramClvlDicRoot = clvldic.TrieTree
	}
}

func (clvDic *CLVDictionary) CreateCLVVTokenDictionaryIfNotExists(buffDicStrings []utils.LogSeries) {
	if clvDic.DicType == CLVC {
		clvcdic := tokenClvc.NewCLVCDic(QMINTOKEN, QMAXTOKEN)
		clvcdic.GenerateClvcDictionaryTree(buffDicStrings, QMINTOKEN, QMAXTOKEN, TTOKEN)
		clvDic.VtokenClvcDicRoot = clvcdic.TrieTree
	}
	if clvDic.DicType == CLVL {
		sampleStrOfWlen := utils.HasSample(buffDicStrings, TOKENWLEN)
		bufflogs := utils.LogSeriesToMap(buffDicStrings)
		clvldic := tokenClvl.NewCLVLDic(QMINTOKEN, QMAXTOKEN)
		clvldic.GenerateClvlDictionaryTree(bufflogs, QMINTOKEN, sampleStrOfWlen)
		clvDic.VtokenClvlDicRoot = clvldic.TrieTree
	}
}
