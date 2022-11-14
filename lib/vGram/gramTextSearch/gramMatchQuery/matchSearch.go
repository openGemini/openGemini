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
package gramMatchQuery

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/decode"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"sort"
)

func MatchSearch(searchStr string, root *gramClvc.TrieTreeNode, indexRoots *decode.SearchTreeNode, qmin int, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	/*var vgMap = make(map[uint16]string)
	gramIndex.VGConsBasicIndex(root, qmin, searchStr, vgMap)
	var resArr = make([]utils.SeriesId, 0) //todo
	for i := 0; i < len(indexRoots); i++ {
		resArr = append(resArr, MatchSearch2(vgMap, indexRoots[i], buffer, addrCache, invertedCache)...)
	}
	return resArr*/
	var vgMap = make(map[uint16]string)
	gramIndex.VGConsBasicIndex(root, qmin, searchStr, vgMap)
	var resArr = make([]utils.SeriesId, 0) //todo
	resArr = append(resArr, MatchSearch2(vgMap, indexRoots, buffer, addrCache, invertedCache)...)
	return resArr
}

func MatchSearch2(vgMap map[uint16]string, indexRoot *decode.SearchTreeNode, buffer []byte,
	addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) []utils.SeriesId {
	//start1 := time.Now().UnixMicro()
	var sortSumInvertList = make([]SortKey, 0)
	for x := range vgMap {
		gram := vgMap[x]
		if gram != "" {
			var invertIndex gramIndex.Inverted_index
			var invertIndexOffset uint64
			var addrOffset uint64
			var indexNode *decode.SearchTreeNode
			var invertIndex1 gramIndex.Inverted_index
			var invertIndex2 gramIndex.Inverted_index
			var invertIndex3 gramIndex.Inverted_index
			invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(gram, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
			invertIndex1 = utils.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
			invertIndex = utils.DeepCopy(invertIndex1)
			invertIndex2 = utils.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, buffer, addrCache, invertedCache)
			addrOffsets := utils.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
			if indexNode != nil && len(addrOffsets) > 0 {
				invertIndex3 = utils.TurnAddr2InvertLists(addrOffsets, buffer, invertedCache)
			}
			invertIndex = utils.MergeMapsInvertLists(invertIndex2, invertIndex)
			invertIndex = utils.MergeMapsInvertLists(invertIndex3, invertIndex)
			sortSumInvertList = append(sortSumInvertList, NewSortKey(x, len(invertIndex), gram, invertIndex))
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
		gramArr := sortSumInvertList[m].gram
		var nowSeaPosition uint16
		if gramArr != "" {
			nowSeaPosition = sortSumInvertList[m].offset
			var invertIndex gramIndex.Inverted_index = nil
			invertIndex = sortSumInvertList[m].invertedIndex
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
				for j := 0; j < len(resArr); j++ {
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
					if findFlag == false {
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
	//end1 := time.Now().UnixMicro()
	//fmt.Println("===============clv")
	//fmt.Println(float64(end1-start1)/1000,"ms")
	return resArr
}

func SearchNodeAddrFromPersistentIndexTree(gramArr string, indexRoot *decode.SearchTreeNode, i int, invertIndexOffset uint64, addrOffset uint64, indexNode *decode.SearchTreeNode) (uint64, uint64, *decode.SearchTreeNode) {
	if indexRoot == nil {
		return invertIndexOffset, addrOffset, indexNode
	}
	if i < len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		invertIndexOffset, addrOffset, indexNode = SearchNodeAddrFromPersistentIndexTree(gramArr, indexRoot.Children()[int(gramArr[i])], i+1, invertIndexOffset, addrOffset, indexNode)
	}
	if i == len(gramArr)-1 && indexRoot.Children()[int(gramArr[i])] != nil {
		invertIndexOffset = indexRoot.Children()[int(gramArr[i])].InvtdInfo().IvtdblkOffset()
		addrOffset = indexRoot.Children()[int(gramArr[i])].AddrInfo().AddrblkOffset()
		indexNode = indexRoot.Children()[int(gramArr[i])]
	}
	return invertIndexOffset, addrOffset, indexNode
}
