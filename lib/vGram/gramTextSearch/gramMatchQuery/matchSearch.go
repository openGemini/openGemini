package gramMatchQuery

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"sort"
)

func MatchSearch(searchStr string, root *gramClvc.TrieTreeNode, indexRoot *gramIndex.IndexTreeNode, qmin int) []utils.SeriesId {

	//start1 := time.Now().UnixMicro()
	var vgMap = make(map[uint16]string)
	gramIndex.VGConsBasicIndex(root, qmin, searchStr, vgMap)
	//fmt.Println(vgMap)

	var sortSumInvertList = make([]SortKey, 0)
	for x := range vgMap {
		gram := vgMap[x]
		if gram != "" {
			var invertIndex gramIndex.Inverted_index
			var indexNode *gramIndex.IndexTreeNode
			var invertIndex1 gramIndex.Inverted_index
			var invertIndex2 gramIndex.Inverted_index
			var invertIndex3 gramIndex.Inverted_index
			invertIndex1, indexNode = SearchInvertedListFromCurrentNode(gram, indexRoot, 0, invertIndex1, indexNode)
			invertIndex = DeepCopy(invertIndex1)
			invertIndex2 = SearchInvertedListFromChildrensOfCurrentNode(indexNode, nil)
			if indexNode != nil && len(indexNode.AddrOffset()) > 0 {
				invertIndex3 = TurnAddr2InvertLists(indexNode.AddrOffset(), invertIndex3)
			}
			invertIndex = MergeMapsInvertLists(invertIndex2, invertIndex)
			invertIndex = MergeMapsInvertLists(invertIndex3, invertIndex)
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

func SearchInvertedListFromCurrentNode(gramArr string, indexRoot *gramIndex.IndexTreeNode, i int, invertIndex1 gramIndex.Inverted_index, indexNode *gramIndex.IndexTreeNode) (gramIndex.Inverted_index, *gramIndex.IndexTreeNode) {
	if indexRoot == nil {
		return invertIndex1, indexNode
	}
	if i < len(gramArr)-1 && indexRoot.Children()[gramArr[i]] != nil {
		invertIndex1, indexNode = SearchInvertedListFromCurrentNode(gramArr, indexRoot.Children()[gramArr[i]], i+1, invertIndex1, indexNode)
	}
	if i == len(gramArr)-1 && indexRoot.Children()[gramArr[i]] != nil {
		invertIndex1 = indexRoot.Children()[gramArr[i]].InvertedIndex()
		indexNode = indexRoot.Children()[gramArr[i]]
	}
	return invertIndex1, indexNode
}

func SearchInvertedListFromChildrensOfCurrentNode(indexNode *gramIndex.IndexTreeNode, invertIndex2 gramIndex.Inverted_index) gramIndex.Inverted_index {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			if len(child.InvertedIndex()) > 0 {
				invertIndex2 = MergeMapsInvertLists(child.InvertedIndex(), invertIndex2)
			}
			if len(child.AddrOffset()) > 0 {
				var invertIndex3 = TurnAddr2InvertLists(child.AddrOffset(), nil)
				invertIndex2 = MergeMapsInvertLists(invertIndex3, invertIndex2)
			}
			invertIndex2 = SearchInvertedListFromChildrensOfCurrentNode(child, invertIndex2)
		}
	}
	return invertIndex2
}

func TurnAddr2InvertLists(addrOffset map[*gramIndex.IndexTreeNode]uint16, invertIndex3 gramIndex.Inverted_index) gramIndex.Inverted_index {
	var res gramIndex.Inverted_index
	for addr, offset := range addrOffset {
		invertIndex3 = make(map[utils.SeriesId][]uint16)
		for key, value := range addr.InvertedIndex() {
			list := make([]uint16, 0)
			for i := 0; i < len(value); i++ {
				list = append(list, value[i]+offset)
			}
			invertIndex3[key] = list
		}
		res = MergeMapsTwoInvertLists(invertIndex3, res)
	}
	return res
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
	for _, v := range m {
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
