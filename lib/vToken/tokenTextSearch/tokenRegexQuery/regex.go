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
package tokenRegexQuery

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/decode"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"regexp"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
)

func RegexSearch(re string, root *tokenClvc.TrieTreeNode, indexRoot *decode.SearchTreeNode, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	regex, _ := regexp.Compile(re)
	sidmap := make(map[utils.SeriesId]struct{})
	result := make([]utils.SeriesId, 0)
	childrenlist := root.Children()
	for i, _ := range childrenlist {
		if regex.MatchString(childrenlist[i].Data()) {
			// match
			var invertIndex gramIndex.Inverted_index
			var invertIndexOffset uint64
			var addrOffset uint64
			var indexNode *decode.SearchTreeNode
			var invertIndex1 gramIndex.Inverted_index
			var invertIndex2 gramIndex.Inverted_index
			var invertIndex3 gramIndex.Inverted_index
			invertIndexOffset, addrOffset, indexNode = tokenMatchQuery.SearchNodeAddrFromPersistentIndexTree([]string{childrenlist[i].Data()}, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
			invertIndex1 = utils.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
			invertIndex = utils.DeepCopy(invertIndex1)
			invertIndex2 = utils.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, buffer, addrCache, invertedCache)
			addrOffsets := utils.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
			if indexNode != nil && len(addrOffsets) > 0 {
				invertIndex3 = utils.TurnAddr2InvertLists(addrOffsets, buffer, invertedCache)
			}
			invertIndex = utils.MergeMapsInvertLists(invertIndex2, invertIndex)
			invertIndex = utils.MergeMapsInvertLists(invertIndex3, invertIndex)

			for k, _ := range invertIndex {
				_, isfind := sidmap[k]
				if !isfind {
					sidmap[k] = struct{}{}
				}
			}
		}
	}
	for k, _ := range sidmap {
		sid := utils.NewSeriesId(k.Id, k.Time)
		result = append(result, sid)
	}
	QuickSort(result)
	return result
}

func QuickSort(sidlist []utils.SeriesId) {
	Sort(sidlist, 0, len(sidlist))
}

func Sort(sidlist []utils.SeriesId, left, right int) {
	if left < right {
		pivot := sidlist[left].Id
		j := left
		for i := left; i < right; i++ {
			if sidlist[i].Id < pivot {
				j++
				sidlist[j], sidlist[i] = sidlist[i], sidlist[j]
			}
		}
		sidlist[left], sidlist[j] = sidlist[j], sidlist[left]
		Sort(sidlist, left, j)
		Sort(sidlist, j+1, right)
	}
}
