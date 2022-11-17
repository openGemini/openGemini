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
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramRegexQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenFuzzyQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenRegexQuery"
)

type QuerySearch int32

const (
	MATCHSEARCH QuerySearch = 0
	FUZZYSEARCH QuerySearch = 1
	REGEXSEARCH QuerySearch = 2
)
const ED = 2

type QueryOption struct {
	measurement string
	fieldKey    string
	querySearch QuerySearch
	queryString string
}

func NewQueryOption(measurement string, fieldKey string, search QuerySearch, queryString string) QueryOption {
	return QueryOption{
		measurement: measurement,
		fieldKey:    fieldKey,
		querySearch: search,
		queryString: queryString,
	}
}

func CLVSearchIndex(clvType CLVIndexType, dicType CLVDicType, queryOption QueryOption, dictionary *CLVDictionary, index *CLVIndexNode) []utils.SeriesId {
	var res []utils.SeriesId
	if queryOption.querySearch == MATCHSEARCH {
		if clvType == VGRAM {
			res = MatchSearchVGramIndex(dicType, queryOption.queryString, dictionary, index)
		}
		if clvType == VTOKEN {
			res = MatchSearchVTokenIndex(dicType, queryOption.queryString, dictionary, index)
		}
	}
	if queryOption.querySearch == FUZZYSEARCH {
		if clvType == VGRAM {
			res = FuzzySearchVGramIndex(dicType, queryOption.queryString, dictionary, index)
		}
		if clvType == VTOKEN {
			res = FuzzySearchVTokenIndex(dicType, queryOption.queryString, index)
		}
	}
	if queryOption.querySearch == REGEXSEARCH {
		if clvType == VGRAM {
			res = RegexSearchVGramIndex(dicType, queryOption.queryString, dictionary, index)
		}
		if clvType == VTOKEN {
			res = RegexSearchVTokenIndex(dicType, queryOption.queryString, index)
		}
	}
	return res
}

func MatchSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), index.VgramIndexRoot.Root(), QMINGRAM)
	}
	if dicType == CLVL {
		res = gramMatchQuery.MatchSearch(queryStr, dictionary.VgramDicRoot.Root(), index.VgramIndexRoot.Root(), QMINGRAM)
	}
	return res
}

func MatchSearchVTokenIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), index.VtokenIndexRoot.Root(), QMINTOKEN)
	}
	if dicType == CLVL {
		res = tokenMatchQuery.MatchSearch(queryStr, dictionary.VtokenDicRoot.Root(), index.VtokenIndexRoot.Root(), QMINTOKEN)
	}
	return res
}

func FuzzySearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTrie(index.LogTreeRoot.Root(), queryStr, dictionary.VgramDicRoot.Root(), index.VgramIndexRoot.Root(), QMINGRAM, LOGTREEMAX, ED)
	}
	if dicType == CLVL {
		res = gramFuzzyQuery.FuzzyQueryGramQmaxTrie(index.LogTreeRoot.Root(), queryStr, dictionary.VgramDicRoot.Root(), index.VgramIndexRoot.Root(), QMINGRAM, LOGTREEMAX, ED)
	}
	return res
}

func FuzzySearchVTokenIndex(dicType CLVDicType, queryStr string, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenFuzzyQuery.FuzzySearchComparedWithES(queryStr, index.VtokenIndexRoot.Root(), ED)
	}
	if dicType == CLVL {
		res = tokenFuzzyQuery.FuzzySearchComparedWithES(queryStr, index.VtokenIndexRoot.Root(), ED)
	}
	return res
}

func RegexSearchVGramIndex(dicType CLVDicType, queryStr string, dictionary *CLVDictionary, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, index.VgramIndexRoot)
	}
	if dicType == CLVL {
		res = gramRegexQuery.RegexSearch(queryStr, dictionary.VgramDicRoot, index.VgramIndexRoot)
	}
	return res
}

func RegexSearchVTokenIndex(dicType CLVDicType, queryStr string, index *CLVIndexNode) []utils.SeriesId {
	var res = make([]utils.SeriesId, 0)
	if dicType == CLVC {
		res = tokenRegexQuery.RegexSearch(queryStr, index.VtokenIndexRoot)
	}
	if dicType == CLVL {
		res = tokenRegexQuery.RegexSearch(queryStr, index.VtokenIndexRoot)
	}
	return res
}
