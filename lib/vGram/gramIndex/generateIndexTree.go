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
package gramIndex

import (
	"fmt"
	"sort"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
)

// According to a batch of log data, the VG is divided by the gramClvc tree, and the gramIndex item set is constructed.
func GenerateIndexTree(logs []utils.LogSeries, qmin int, qmax int, logTreeMax int, root *gramClvc.TrieTreeNode) (*IndexTree, *IndexTreeNode, *LogTree) {
	indexTree := NewIndexTree(qmin, qmax)
	var vgMaps = make(map[string]Inverted_index)
	logTree := NewLogTree(logTreeMax)
	for i := range logs {
		tsid := logs[i].Tsid
		timeStamp := logs[i].TimeStamp
		log := logs[i].Log
		if len(log) <= logTreeMax {
			logTree.InsertIntoTrieTreeLogTree(log)
		} else {
			for k := 0; k <= len(log)-logTreeMax; k++ {
				substring := log[k : k+logTreeMax]
				logTree.InsertIntoTrieTreeLogTree(substring)
			}
		}
		var vgMap map[uint16]string
		vgMap = make(map[uint16]string)
		sid := utils.NewSeriesId(tsid, timeStamp)
		VGConsBasicIndex(root, qmin, log, vgMap)
		vgMaps = WriteToVgMaps(vgMap, sid, vgMaps) //写入到vgMaps
	}
	var addr *IndexTreeNode
	for gram := range vgMaps {
		invert_index := vgMaps[gram]
		addr = indexTree.InsertIntoIndexTree(gram, invert_index)
		if len(gram) > qmin && len(gram) <= qmax { //Generate all gramIndex entries between qmin+1 - len(gram)
			GramSubs = make([]SubGramOffset, 0)
			GenerateQmin2QmaxGrams(gram, qmin)
			indexTree.InsertOnlyGramIntoIndexTree(GramSubs, addr)
		}
	}
	indexTree.cout = len(logs)
	//indexTree.PrintIndexTree()
	fmt.Println(indexTree.cout)
	return indexTree, indexTree.root, logTree
}

func WriteToVgMaps(vgMap map[uint16]string, sid utils.SeriesId, vgMaps VgMaps) VgMaps {
	var keys = []uint16{}
	for key := range vgMap {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for i := 0; i < len(keys); i++ {
		position := keys[i]
		gram := vgMap[position]
		if _, ok := vgMaps[gram]; ok {
			invert_index := vgMaps[gram]
			if _, ok := invert_index[sid]; ok {
				invert_index[sid] = append(invert_index[sid], position)
			} else {
				posArray := []uint16{}
				posArray = append(posArray, position)
				invert_index[sid] = posArray
			}
		} else {
			posArray := []uint16{}
			posArray = append(posArray, position)
			var invert_index Inverted_index = make(map[utils.SeriesId][]uint16)
			invert_index[sid] = posArray
			vgMaps[gram] = invert_index
		}
	}
	return vgMaps
}

var GramSubs []SubGramOffset

func GenerateQmin2QmaxGrams(gram string, qmin int) {
	length := len(gram)
	var i uint16
	for i = 1; int(i) <= length-qmin; i++ {
		gramSub := gram[i:length]
		GramSubs = append(GramSubs, NewSubGramOffset(gramSub, i))
	}
}

func VGConsBasicIndex(root *gramClvc.TrieTreeNode, qmin int, str string, vgMap map[uint16]string) { //分词
	len1 := len(str)
	var lastPosition uint16 = 0
	var p uint16
	for p = 0; int(p) < len1-qmin+1; p++ {
		tSub = ""
		FindLongestGramFromDic(root, str, p)
		t := tSub
		if t == "" || len(t) < qmin {
			t = str[int(p) : int(p)+qmin]
		}
		if lastPosition == 0 || int(p)+len(t) > int(lastPosition) {
			vgMap[p] = t
			lastPosition = p + uint16(len(t))
		}
	}
}

var tSub string

func FindLongestGramFromDic(root *gramClvc.TrieTreeNode, str string, p uint16) {
	if int(p) < len(str) {
		c := str[p : p+1]
		if root.Children()[c[0]] != nil {
			tSub += c
			FindLongestGramFromDic(root.Children()[c[0]], str, p+1)
		} else {
			return
		}
	}
}

func VGConsBasicIndex08(root *IndexTreeNode, qmin int, str string, vgMap map[uint16]string) {
	len1 := len(str)
	var lastPosition uint16 = 0
	var p uint16
	for p = 0; int(p) < len1-qmin+1; p++ {
		tSub08 = ""
		FindLongestGramFromDic08(root, str, p)
		t := tSub08
		if t == "" || len(t) < qmin { //字典D中 qmin - qmax 之间都是叶子节点（索引项中不一定是）也就是说FindLongestGramFromDic找到的只要是长度大于qmin就都是VG的gram
			t = str[int(p) : int(p)+qmin]
		}
		if lastPosition == 0 || int(p)+len(t) > int(lastPosition) {
			vgMap[p] = t
			lastPosition = p + uint16(len(t))
		}
	}
}

var tSub08 string

func FindLongestGramFromDic08(root *IndexTreeNode, str string, p uint16) {
	if int(p) < len(str) {
		c := str[p : p+1]
		if root.Children()[c[0]] != nil {
			tSub08 += c
			FindLongestGramFromDic08(root.Children()[c[0]], str, p+1)
		} else {
			return
		}
	}
}
