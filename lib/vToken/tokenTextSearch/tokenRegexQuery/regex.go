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
	"regexp"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"github.com/openGemini/openGemini/lib/vToken/tokenTextSearch/tokenMatchQuery"
)

func RegexSearch(re string, indextree *tokenIndex.IndexTree) []utils.SeriesId {
	regex, _ := regexp.Compile(re)
	sidmap := make(map[utils.SeriesId]struct{})
	result := make([]utils.SeriesId, 0)
	childrenlist := indextree.Root().Children()
	for i, _ := range childrenlist {
		if regex.MatchString(childrenlist[i].Data()) {
			// match
			var invertIndex tokenIndex.Inverted_index
			var indexNode *tokenIndex.IndexTreeNode
			var invertIndex1 tokenIndex.Inverted_index
			var invertIndex2 tokenIndex.Inverted_index
			var invertIndex3 tokenIndex.Inverted_index
			invertIndex1, indexNode = tokenMatchQuery.SearchInvertedListFromCurrentNode([]string{childrenlist[i].Data()}, indextree.Root(), 0, invertIndex1, indexNode)
			invertIndex = tokenMatchQuery.DeepCopy(invertIndex1)
			invertIndex2 = tokenMatchQuery.SearchInvertedListFromChildrensOfCurrentNode(indexNode, nil)
			if indexNode != nil && len(indexNode.AddrOffset()) > 0 {
				invertIndex3 = tokenMatchQuery.TurnAddr2InvertLists(indexNode.AddrOffset(), invertIndex3)
			}
			invertIndex = tokenMatchQuery.MergeMapsInvertLists(invertIndex2, invertIndex)
			invertIndex = tokenMatchQuery.MergeMapsInvertLists(invertIndex3, invertIndex)
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
