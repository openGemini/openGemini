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
package tokenMatchQuery

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/decode"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"sort"
)

func MatchSearch(searchStr string, root *tokenClvc.TrieTreeNode, indexRoots []*decode.SearchTreeNode, qmin int, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	var vgMap = make(map[uint16][]string)
	searchtoken, _ := utils.DataProcess(searchStr)
	tokenIndex.VGCons(root, qmin, searchtoken, vgMap)
	var resArr = make([]utils.SeriesId, 0)
	for i := 0; i < len(indexRoots); i++ {
		resArr = append(resArr, MatchSearch2(vgMap, indexRoots[i], buffer, addrCache, invertedCache)...)
	}
	return resArr
}

func MatchSearch2(vgMap map[uint16][]string, indexRoot *decode.SearchTreeNode, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	var sortSumInvertList = make([]SortKey, 0)
	for x := range vgMap {
		token := vgMap[x]
		if token != nil {
			var invertIndex tokenIndex.Inverted_index
			var invertIndexOffset uint64
			var addrOffset uint64
			var indexNode *decode.SearchTreeNode
			var invertIndex1 tokenIndex.Inverted_index
			var invertIndex2 tokenIndex.Inverted_index
			var invertIndex3 tokenIndex.Inverted_index
			invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(token, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
			invertIndex1 = gramMatchQuery.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
			invertIndex = DeepCopy(invertIndex1)
			invertIndex2 = gramMatchQuery.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, buffer, addrCache, invertedCache)
			addrOffsets := gramMatchQuery.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
			if indexNode != nil && len(addrOffsets) > 0 {
				invertIndex3 = gramMatchQuery.TurnAddr2InvertLists(addrOffsets, buffer, invertedCache)
			}
			invertIndex = MergeMapsInvertLists(invertIndex2, invertIndex)
			invertIndex = MergeMapsInvertLists(invertIndex3, invertIndex)
			sortSumInvertList = append(sortSumInvertList, NewSortKey(x, len(invertIndex), token, invertIndex))
		}
	}
	sort.SliceStable(sortSumInvertList, func(i, j int) bool {
		if sortSumInvertList[i].sizeOfInvertedList < sortSumInvertList[j].sizeOfInvertedList {
			return true
		}
		return false
	})

	var resArr = make([]utils.SeriesId, 0)
	var preSeaPosition uint16 = 0
	var preInverPositionDis = make([]PosList, 0)
	var nowInverPositionDis = make([]PosList, 0)
	for m := 0; m < len(sortSumInvertList); m++ {
		tokenArr := sortSumInvertList[m].tokenArr
		var nowSeaPosition uint16
		if tokenArr != nil {
			nowSeaPosition = sortSumInvertList[m].offset
			var invertIndex tokenIndex.Inverted_index = nil
			invertIndex = sortSumInvertList[m].invertedIndex
			//fmt.Println(len(invertIndex))
			if invertIndex == nil {
				return nil
			}
			if m == 0 {
				for sid := range invertIndex {
					preInverPositionDis = append(preInverPositionDis, NewPosList(sid, make([]uint16, len(invertIndex[sid]), len(invertIndex[sid]))))
					nowInverPositionDis = append(nowInverPositionDis, NewPosList(sid, invertIndex[sid]))
					resArr = append(resArr, sid)
				}
			} else {
				for j := 0; j < len(resArr); j++ { //遍历之前合并好的resArr
					findFlag := false
					sid := resArr[j]
					if _, ok := invertIndex[sid]; ok {
						nowInverPositionDis[j] = NewPosList(sid, invertIndex[sid])
						for z1 := 0; z1 < len(preInverPositionDis[j].posArray); z1++ {
							z1Pos := preInverPositionDis[j].posArray[z1]
							for z2 := 0; z2 < len(nowInverPositionDis[j].posArray); z2++ {
								z2Pos := nowInverPositionDis[j].posArray[z2]
								if nowSeaPosition-preSeaPosition == z2Pos-z1Pos {
									findFlag = true
									break
								}
							}
							if findFlag == true {
								break
							}
						}
					}
					if findFlag == false { //没找到并且候选集的sid比resArr大，删除resArr[j]
						resArr = append(resArr[:j], resArr[j+1:]...)
						preInverPositionDis = append(preInverPositionDis[:j], preInverPositionDis[j+1:]...)
						nowInverPositionDis = append(nowInverPositionDis[:j], nowInverPositionDis[j+1:]...)
						j-- //删除后重新指向，防止丢失元素判断
					}
				}
			}
			preSeaPosition = nowSeaPosition
			copy(preInverPositionDis, nowInverPositionDis)
		}
	}
	sort.SliceStable(resArr, func(i, j int) bool {
		if resArr[i].Id < resArr[j].Id && resArr[i].Time < resArr[j].Time {
			return true
		}
		return false
	})
	return resArr
}

func SearchNodeAddrFromPersistentIndexTree(tokenArr []string, indexRoot *decode.SearchTreeNode, i int, invertIndexOffset uint64, addrOffset uint64, indexNode *decode.SearchTreeNode) (uint64, uint64, *decode.SearchTreeNode) {
	if indexRoot == nil {
		return invertIndexOffset, addrOffset, indexNode
	}
	if i < len(tokenArr)-1 && indexRoot.Children()[utils.StringToHashCode(tokenArr[i])] != nil {
		invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(tokenArr, indexRoot.Children()[utils.StringToHashCode(tokenArr[i])], i+1, invertIndexOffset, addrOffset, indexNode)
	}
	if i == len(tokenArr)-1 && indexRoot.Children()[utils.StringToHashCode(tokenArr[i])] != nil {
		invertIndexOffset = indexRoot.Children()[utils.StringToHashCode(tokenArr[i])].InvtdInfo().IvtdblkOffset()
		addrOffset = indexRoot.Children()[utils.StringToHashCode(tokenArr[i])].AddrInfo().AddrblkOffset()
		indexNode = indexRoot.Children()[utils.StringToHashCode(tokenArr[i])]
	}
	return invertIndexOffset, addrOffset, indexNode
}

func MergeMapsInvertLists(map1 map[utils.SeriesId][]uint16, map2 map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	if len(map2) > 0 {
		for sid1, list1 := range map1 {
			if list2, ok := map2[sid1]; !ok {
				map2[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map2[sid1] = list2
			}
		}
	} else {
		map2 = DeepCopy(map1)
	}
	return map2
}

func UniqueArr(m []uint16) []uint16 {
	d := make([]uint16, 0)
	tempMap := make(map[uint16]bool, len(m))
	for _, v := range m { // 以值作为键名
		if tempMap[v] == false {
			tempMap[v] = true
			d = append(d, v)
		}
	}
	return d
}

func DeepCopy(src map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	dst := make(map[utils.SeriesId][]uint16)
	for key, value := range src {
		list := make([]uint16, 0)
		for i := 0; i < len(value); i++ {
			list = append(list, value[i])
		}
		dst[key] = list
	}
	return dst
}

func MergeMapsTwoInvertLists(map1 map[utils.SeriesId][]uint16, map2 map[utils.SeriesId][]uint16) map[utils.SeriesId][]uint16 {
	if len(map1) == 0 {
		return map2
	} else if len(map2) == 0 {
		return map1
	} else if len(map1) < len(map2) {
		for sid1, list1 := range map1 {
			if list2, ok := map2[sid1]; !ok {
				map2[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map2[sid1] = list2
			}
		}
		return map2
	} else {
		for sid1, list1 := range map2 {
			if list2, ok := map1[sid1]; !ok {
				map1[sid1] = list1
			} else {
				list2 = append(list2, list1...)
				list2 = UniqueArr(list2)
				sort.Slice(list2, func(i, j int) bool { return list2[i] < list2[j] })
				map1[sid1] = list2
			}
		}
		return map1
	}
}
