package mpTrie

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/utils"
	"sort"
)

func SearchInvertedIndexFromCacheOrDisk(invertIndexOffset uint64, buffer []byte, invertedCache *cache.InvertedCache) map[utils.SeriesId][]uint16 {
	invertedIndex := invertedCache.Get(invertIndexOffset).Mpblk()
	if len(invertedIndex) == 0 {
		invertedIndex = UnserializeInvertedListBlk(invertIndexOffset, buffer).Mpblk()
	}
	return invertedIndex
}

func SearchAddrOffsetsFromCacheOrDisk(addrOffset uint64, buffer []byte, addrCache *cache.AddrCache) map[uint64]uint16 {
	addrOffsets := addrCache.Get(addrOffset).Mpblk()
	if len(addrOffsets) == 0 {
		addrOffsets = UnserializeAddrListBlk(addrOffset, buffer).Mpblk()
	}
	return addrOffsets
}

func SearchInvertedListFromChildrensOfCurrentNode(indexNode *SearchTreeNode, invertIndex2 map[utils.SeriesId][]uint16, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) map[utils.SeriesId][]uint16 {
	if indexNode != nil {
		for _, child := range indexNode.Children() {
			childInvertIndexOffset := child.InvtdInfo().IvtdblkOffset()
			childInvertedIndex := SearchInvertedIndexFromCacheOrDisk(childInvertIndexOffset, buffer, invertedCache)
			if len(childInvertedIndex) > 0 {
				invertIndex2 = MergeMapsInvertLists(childInvertedIndex, invertIndex2)
			}

			childAddrOffset := child.AddrInfo().AddrblkOffset()
			childAddrOffsets := SearchAddrOffsetsFromCacheOrDisk(childAddrOffset, buffer, addrCache)
			if len(childAddrOffsets) > 0 {
				var invertIndex3 = TurnAddr2InvertLists(childAddrOffsets, buffer, invertedCache)
				invertIndex2 = MergeMapsInvertLists(invertIndex3, invertIndex2)
			}
			invertIndex2 = SearchInvertedListFromChildrensOfCurrentNode(child, invertIndex2, buffer, addrCache, invertedCache)
		}
	}
	return invertIndex2
}

func TurnAddr2InvertLists(addrOffsets map[uint64]uint16, buffer []byte, invertedCache *cache.InvertedCache) map[utils.SeriesId][]uint16 {
	var res map[utils.SeriesId][]uint16
	for addr, offset := range addrOffsets {
		invertIndex3 := make(map[utils.SeriesId][]uint16)
		addrInvertedIndex := SearchInvertedIndexFromCacheOrDisk(addr, buffer, invertedCache)
		for key, value := range addrInvertedIndex {
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
