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
package gramFuzzyQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/decode"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
)

type FuzzyEmpty struct{}

var fuzzyEmpty FuzzyEmpty

func MinThree(a, b, c int) int {
	var min int
	if a >= b {
		min = b
	} else {
		min = a
	}
	if min >= c {
		return c
	} else {
		return min
	}
}

func QmaxTrieListPath(root *gramIndex.LogTreeNode, path string, collection map[string]FuzzyEmpty, query string, distance int, qmin int) {
	if len(root.Children()) == 0 {
		path = path + root.Data()
		//fmt.Println(path)
		if len(query) <= len(path)+distance {
			minFuzzyStr := MinimumFuzzySubstring(query, distance, path, qmin)
			if minFuzzyStr == "" {
				return
			}
			JoinCollection(minFuzzyStr, collection)
		} else if len(query) > len(path)+distance {
			return
		}
		return
	} else {
		path = path + root.Data()
		for _, child := range root.Children() {
			QmaxTrieListPath(child, path, collection, query, distance, qmin)
		}
	}
}

func MinimumFuzzySubstring(query string, distance int, path string, qmin int) string {
	l1 := len(query)
	l2 := len(path)

	dp := make([][]int, l1+1)

	dp[0] = make([]int, l2+1)
	for j := 0; j <= l2; j++ {
		dp[0][j] = 0
	}

	for i := 1; i <= l1; i++ {
		dp[i] = make([]int, l2+1)
		dp[i][0] = i
		for j := 1; j <= l2; j++ {
			if query[i-1:i] == path[j-1:j] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = MinThree(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}
	col := 0
	minNum := dp[l1][0]
	for k := 1; k <= l2; k++ {
		if dp[l1][k] < minNum {
			minNum = dp[l1][k]
			col = k
		}
	}
	if dp[l1][col] > distance {
		return ""
	}
	//回溯
	var row int
	var column int
	for row, column = l1, col; row > 0; {
		if row != 0 && dp[row-1][column]+1 == dp[row][column] {
			row--
			continue
		} else if column != 0 && row != 0 && dp[row-1][column-1]+1 == dp[row][column] {
			row--
			column--
			continue
		} else if column != 0 && dp[row][column-1]+1 == dp[row][column] {
			column--
			continue
		}
		row--
		column--
	}
	if len(path[column:col]) < qmin {
		if column <= len(path)-qmin {
			return path[column : column+qmin]
		} else {
			return path[len(path)-qmin:]
		}
	}
	return path[column:col]

}

func JoinCollection(str string, collection map[string]FuzzyEmpty) {
	_, okh := collection[str]
	flag := false
	if okh {
		return
	} else {
		for key, _ := range collection {
			if strings.Contains(str, key) {
				flag = true
				return
			}
			if strings.Contains(key, str) {
				delete(collection, key)
				collection[str] = fuzzyEmpty
				flag = true
			}
		}
	}
	if flag == false {
		collection[str] = fuzzyEmpty
	}
}

func ArrayRemoveDuplicate(array []utils.SeriesId) []utils.SeriesId {
	result := make([]utils.SeriesId, 0)
	var sid uint64
	var time int64 = -1
	for i := 0; i < len(array); i++ {
		if sid == array[i].Id && time == array[i].Time {
			continue
		} else {
			sid = array[i].Id
			time = array[i].Time
			result = append(result, array[i])
		}
	}
	return result
}

func ArraySortAndRemoveDuplicate(array []utils.SeriesId) []utils.SeriesId {
	sort.SliceStable(array, func(i, j int) bool {
		if array[i].Id < array[j].Id && array[i].Time < array[j].Time {
			return true
		}
		return false
	})
	array = ArrayRemoveDuplicate(array)
	return array
}
func FuzzyQueryGramQmaxTrie(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots *decode.SearchTreeNode, qmin int, qmax int, distance int, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	resArrFuzzy := make([]utils.SeriesId, 0)
	if len(searchStr) > qmax+distance {
		fmt.Println("error:查询语句长度大于qmax+distance,无法匹配结果")
		return resArrFuzzy
	}
	collectionMinStr := make(map[string]FuzzyEmpty)
	QmaxTrieListPath(rootFuzzyTrie, "", collectionMinStr, searchStr, distance, qmin)
	for key, _ := range collectionMinStr { //(key, dicRootNode, indexRootNode, qmin)
		arrayNew := gramMatchQuery.MatchSearch(key, dicRootNode, indexRoots, qmin, buffer, addrCache, invertedCache)
		resArrFuzzy = append(resArrFuzzy, arrayNew...)
	}
	resArrFuzzy = ArraySortAndRemoveDuplicate(resArrFuzzy)
	return resArrFuzzy
}

/*func FuzzyQueryGramQmaxTries(rootFuzzyTrie *gramIndex.LogTreeNode, searchStr string, dicRootNode *gramClvc.TrieTreeNode,
	indexRoots []*decode.SearchTreeNode, qmin int, qmax int, distance int,
	buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	var resArr = make([]utils.SeriesId, 0)
	for i := 0; i < len(indexRoots); i++ {
		resArr = append(resArr, FuzzyQueryGramQmaxTrie(rootFuzzyTrie, searchStr, dicRootNode,
			indexRoots, qmin, qmax, distance, buffer, addrCache, invertedCache)...)
	}
	return resArr
}*/
