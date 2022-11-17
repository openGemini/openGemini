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
package tokenIndex

import (
	"fmt"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
)

func GenerateIndexTree(logs []utils.LogSeries, qmin int, qmax int, root *tokenClvc.TrieTreeNode) (*IndexTree, *IndexTreeNode) {
	indexTree := NewIndexTree(qmin, qmax)
	var vgMaps = make(map[string]Inverted_index)
	for i := range logs {
		tsid := logs[i].Tsid
		timeStamp := logs[i].TimeStamp
		log := logs[i].Log
		tokenarr, _ := utils.DataProcess(log)
		var vgMap map[uint16][]string
		vgMap = make(map[uint16][]string)
		sid := utils.NewSeriesId(tsid, timeStamp)
		VGCons(root, qmin, tokenarr, vgMap)
		vgMaps = WriteToVgMaps(vgMap, sid, vgMaps)
	}

	var addr *IndexTreeNode
	for tokenStr := range vgMaps {
		token := strings.Split(tokenStr, " ")
		tokenArr := token[0 : len(token)-1]
		invert_index := vgMaps[tokenStr]
		addr = indexTree.InsertIntoIndexTree(tokenArr, invert_index)
		if len(tokenArr) > qmin && len(tokenArr) <= qmax {
			TokenSubs = make([]SubTokenOffset, 0)
			GenerateQmin2QmaxTokens(tokenArr, qmin)
			indexTree.InsertOnlyTokenIntoIndexTree(TokenSubs, addr)
		}
	}
	indexTree.cout = len(logs)
	//indexTree.PrintIndexTree()
	fmt.Println(indexTree.cout)
	return indexTree, indexTree.root
}

func WriteToVgMaps(vgMap map[uint16][]string, sid utils.SeriesId, vgMaps VgMaps) VgMaps {
	var keys = make([]uint16, 0)
	for key := range vgMap {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for i := 0; i < len(keys); i++ {
		position := keys[i]
		token := vgMap[position]
		tokenStr := ""
		for k := 0; k < len(token); k++ {
			tokenStr += (token[k] + " ")
		}
		if _, ok := vgMaps[tokenStr]; ok {
			invert_index := vgMaps[tokenStr]
			if _, ok := invert_index[sid]; ok {
				invert_index[sid] = append(invert_index[sid], position)
			} else {
				posArray := []uint16{}
				posArray = append(posArray, position)
				invert_index[sid] = posArray
			}
		} else {
			posArray := make([]uint16, 0)
			posArray = append(posArray, position)
			var invert_index Inverted_index = make(map[utils.SeriesId][]uint16)
			invert_index[sid] = posArray
			vgMaps[tokenStr] = invert_index
		}
	}
	return vgMaps
}

var TokenSubs []SubTokenOffset

func GenerateQmin2QmaxTokens(tokenArr []string, qmin int) {
	length := len(tokenArr)
	var i uint16
	for i = 1; int(i) <= length-qmin; i++ {
		tokenSub := tokenArr[i:length]
		TokenSubs = append(TokenSubs, NewSubGramOffset(tokenSub, i))
	}
}

func VGCons(root *tokenClvc.TrieTreeNode, qmin int, tokenArray []string, vgMap map[uint16][]string) {
	len1 := len(tokenArray)
	var lastPosition uint16 = 0
	var p uint16
	for p = 0; int(p) < len1-qmin+1; p++ {
		tSub = tSub[0:0]
		FindLongestTokenFromDic(root, tokenArray, p)
		t := tSub
		if len(t) == 0 || len(t) < qmin {
			t = tokenArray[int(p) : int(p)+qmin]
		}
		if lastPosition == 0 || int(p)+len(t) > int(lastPosition) {
			for i := 0; i < len(t); i++ {
				vgMap[p] = append(vgMap[p], t[i])
			}
			lastPosition = p + uint16(len(t))
		}
	}
}

func TokenVGCons(trie *IndexTree, qmin int, tokenArray []string, vgMap map[uint16][]string) {
	len1 := len(tokenArray)
	var lastPosition uint16 = 0
	var p uint16
	for p = 0; int(p) < len1-qmin+1; p++ {
		t := FindLongestTokenFromDic08(trie, tokenArray, p)
		if len(t) == 0 || len(t) < qmin {
			t = tokenArray[int(p) : int(p)+qmin]
		}
		if lastPosition == 0 || int(p)+len(t) > int(lastPosition) {
			for i := 0; i < len(t); i++ {
				vgMap[p] = append(vgMap[p], t[i])
			}
			lastPosition = p + uint16(len(t))
		}
	}
}

var tSub []string

func FindLongestTokenFromDic(root *tokenClvc.TrieTreeNode, str []string, p uint16) {
	if int(p) < len(str) {
		c := str[p : p+1]
		s := strings.Join(c, "")
		if root.Children()[utils.StringToHashCode(s)] != nil {
			tSub = append(tSub, s)
			FindLongestTokenFromDic(root.Children()[utils.StringToHashCode(s)], str, p+1)
		} else {
			return
		}
	}
}

func FindLongestTokenFromDic08(trie *IndexTree, str []string, p uint16) []string {
	var strSub []string
	var findLongestGramFromDic08 func(root *IndexTreeNode, str []string, p uint16)
	findLongestGramFromDic08 = func(root *IndexTreeNode, str []string, p uint16) {
		if int(p) < len(str) {
			c := str[p : p+1]
			s := strings.Join(c, "")
			if root.Children()[utils.StringToHashCode(s)] != nil {
				strSub = append(strSub, s)
				findLongestGramFromDic08(root.Children()[utils.StringToHashCode(s)], str, p+1)
			} else {
				return
			}
		}
	}
	findLongestGramFromDic08(trie.Root(), str, p)
	return strSub
}
