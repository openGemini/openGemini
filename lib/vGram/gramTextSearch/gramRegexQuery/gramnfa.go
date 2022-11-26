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
package gramRegexQuery

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/decode"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/vGram/gramTextSearch/gramMatchQuery"
)

type GnfaNode struct {
	nexts    []*GnfaEdge
	prevs    []*GnfaEdge
	path     []*ParseNfaNode
	pathstr  string
	hashpath string
	id       int
	repeated bool
	offset   int
}

type GnfaEdge struct {
	label    string
	epsilon  bool
	skipable bool
	checked  bool
	isloop   bool
	start    *GnfaNode
	end      *GnfaNode
	index    *gramIndex.Inverted_index
}

type Gnfa struct {
	nodes   []*GnfaNode
	edges   []*GnfaEdge
	inode   *GnfaNode
	fnode   *GnfaNode
	nodeset map[string]*GnfaNode
}

func NewGnfa() *Gnfa {
	return &Gnfa{
		nodes:   make([]*GnfaNode, 0),
		edges:   make([]*GnfaEdge, 0),
		inode:   nil,
		fnode:   nil,
		nodeset: make(map[string]*GnfaNode),
	}
}

func NewGnfaNode() *GnfaNode {
	return &GnfaNode{
		nexts:    make([]*GnfaEdge, 0),
		prevs:    make([]*GnfaEdge, 0),
		path:     make([]*ParseNfaNode, 0),
		pathstr:  "",
		hashpath: "",
		id:       0,
		repeated: false,
		offset:   1,
	}
}

func NewGnfaEdge(label string, start *GnfaNode, end *GnfaNode) *GnfaEdge {
	edge := GnfaEdge{
		label:    label,
		epsilon:  false,
		skipable: false,
		checked:  false,
		isloop:   false,
		start:    start,
		end:      end,
		index:    nil,
	}
	if label == "" {
		edge.epsilon = true
	}
	return &edge
}

func NewGnfaNodeWithNodePath(path_ []*ParseNfaNode) *GnfaNode {
	node := NewGnfaNode()
	for i := 0; i < len(path_); i++ {
		node.path = append(node.path, path_[i])
	}
	node.hashpath = GetHash(node.path)
	return node
}

func GetHash(path []*ParseNfaNode) string {
	var str string
	for i := 0; i < len(path); i++ {
		address := fmt.Sprintf("%p", path[i])
		str = str + address
	}
	return str
}

func GenerateGnfa(parseNfa *Nfa, trietree *gramClvc.TrieTree) *Gnfa {
	gnfa := NewGnfa()
	// create initial node for Gg, and record it.
	gnfa.inode = NewGnfaNode()
	gnfa.AddNode(gnfa.inode)
	// intialize a stack for the nodes of Gg.
	S := InitializeGnfaStack()
	l := make([]*ParseNfaNode, 1)
	l[0] = parseNfa.inode
	_, subpaths := parseNfa.FindSubPath(l, trietree, "")
	for i := 0; i < len(subpaths); i++ {
		pathstr := subpaths[i].str
		pathnodes := subpaths[i].nodes
		// find the set of node of the q-1 gram
		suffixpathnodes := GetSuffixPath(pathnodes)
		var V *GnfaNode
		if pathnodes[len(pathnodes)-1] == parseNfa.fnode {
			if gnfa.fnode == nil {
				gnfa.fnode = NewGnfaNode()
				gnfa.AddNode(gnfa.fnode)
			}
			V = gnfa.fnode
		} else {
			V = NewGnfaNodeWithNodePath(suffixpathnodes)
			V.pathstr = pathstr[1:]
			gnfa.AddNode(V)
			gnfa.nodeset[V.hashpath] = V
			S.Push(V)
		}
		gnfa.AddEdge(pathstr, gnfa.inode, V)
	}
	for len(S.list) != 0 {
		gnfanodepop := S.Pop()
		if gnfanodepop != gnfa.fnode {
			pathnodes := gnfanodepop.path
			tolast, subpaths := parseNfa.FindSubPath(pathnodes, trietree, gnfanodepop.pathstr)
			if tolast {
				if gnfa.fnode == nil {
					gnfa.fnode = NewGnfaNode()
					gnfa.AddNode(gnfa.fnode)
				}
				gnfa.AddEdge("", gnfanodepop, gnfa.fnode)
			}
			for i := 0; i < len(subpaths); i++ {
				subpath := subpaths[i].nodes
				subpathstr := subpaths[i].str
				// to the FNode of the NFA
				var V *GnfaNode
				if subpath[len(subpath)-1] == parseNfa.fnode {
					if gnfa.fnode == nil {
						gnfa.fnode = NewGnfaNode()
						gnfa.AddNode(gnfa.fnode)
					}
					V = gnfa.fnode
				} else {
					subpath = GetSuffixPath(subpath)
					n, find := gnfa.nodeset[GetHash(subpath)]
					if find {
						V = n
					} else {
						V = NewGnfaNodeWithNodePath(subpath)
						V.pathstr = subpathstr[1:]
						gnfa.AddNode(V)
						gnfa.nodeset[V.hashpath] = V
						S.Push(V)
					}
				}
				gnfa.AddEdge(subpathstr, gnfanodepop, V)
			}
		}
	}
	MarkWildcard(gnfa.inode)
	for i := 0; i < len(gnfa.edges); i++ {
		edge := gnfa.edges[i]
		edge.end.prevs = append(edge.end.prevs, edge)
	}
	return gnfa
}

func MarkWildcard(node *GnfaNode) {
	for i := 0; i < len(node.nexts); i++ {
		edge := node.nexts[i]
		if edge.checked == false {
			if IsWildcard(edge) {
				edge.skipable = true
				if edge.end == node {
					node.repeated = true
				}
			}
			edge.checked = true
			MarkWildcard(edge.end)
		}
	}
}

func IsWildcard(edge *GnfaEdge) bool {
	for i := 0; i < len(edge.label); i++ {
		if int(edge.label[i]) == 127 {
			return true
		}
	}
	return false
}

func (g *Gnfa) GetAllgrams() []*string {
	gramset := make([]*string, 0)
	for i := 0; i < len(g.edges); i++ {
		if !g.edges[i].epsilon && !g.edges[i].skipable {
			gramset = append(gramset, &g.edges[i].label)
		}
	}
	return gramset
}

func (g *Gnfa) AddNode(node *GnfaNode) {
	g.nodes = append(g.nodes, node)
	node.id = len(g.nodes) - 1
}

func (g *Gnfa) AddEdge(label string, start *GnfaNode, end *GnfaNode) {
	edge := NewGnfaEdge(label, start, end)
	start.nexts = append(start.nexts, edge)
	g.edges = append(g.edges, edge)

}

func (g *Gnfa) LoadInvertedIndex(index *decode.SearchTreeNode, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) {
	for i := 0; i < len(g.edges); i++ {
		edge := g.edges[i]
		label := edge.label
		invertedindex := SearchString(index, label, buffer, addrCache, invertedCache)
		edge.index = invertedindex
	}
}

func (g *Gnfa) Match() []*SeriesIdWithPosition {
	startnode := g.inode
	//sidmap := make(map[*SeriesIdWithPosition]struct{})
	result_map_s := make(map[utils.SeriesId][]uint16)
	result_map_e := make(map[utils.SeriesId][]uint16)
	for i := 0; i < len(startnode.nexts); i++ {
		edge := startnode.nexts[i]
		indexlist := make([]*SeriesIdWithPosition, 0)
		if edge.index != nil {
			for key := range *edge.index {
				sid := utils.NewSeriesId(key.Id, key.Time)
				// avoid to change the index
				endposition := (*edge.index)[key]
				copyofendposition := make([]uint16, len(endposition))
				copy(copyofendposition, endposition)
				copyofstartposition := make([]uint16, len(endposition))
				copy(copyofstartposition, endposition)
				sidwithposition := NewSeriesIdWithPosition(sid, copyofstartposition, copyofendposition)
				indexlist = append(indexlist, sidwithposition)
			}
			partofresult := g.RecursionMatch(edge, indexlist)
			for j := 0; j < len(partofresult); j++ {
				_, ok := result_map_s[partofresult[j].sid]
				if ok {
					for k := 0; k < len(partofresult[j].startposition); k++ {
						result_map_s[partofresult[j].sid] = AddToInOrderList(result_map_s[partofresult[j].sid], partofresult[j].startposition[k])
					}
					for k := 0; k < len(partofresult[j].endposition); k++ {
						result_map_e[partofresult[j].sid] = AddToInOrderList(result_map_e[partofresult[j].sid], partofresult[j].endposition[k])
					}
				} else {
					result_map_s[partofresult[j].sid] = partofresult[j].startposition
					result_map_e[partofresult[j].sid] = partofresult[j].endposition
				}

			}
		}
	}
	result := make([]*SeriesIdWithPosition, len(result_map_s))
	index := 0
	for k, startposition := range result_map_s {
		result[index] = NewSeriesIdWithPosition(k, startposition, result_map_e[k])
		index++
	}
	return result
}

func (g *Gnfa) RecursionMatch(edge *GnfaEdge, sidlist []*SeriesIdWithPosition) []*SeriesIdWithPosition {
	if len(sidlist) == 0 {
		return make([]*SeriesIdWithPosition, 0)
	}
	// to the end
	if g.Isfinal(edge.end) {
		// update final position
		for i := 0; i < len(sidlist); i++ {
			for j := 0; j < len(sidlist[i].endposition); j++ {
				sidlist[i].endposition[j] += uint16(len(edge.label))
			}
		}
		return sidlist
	}
	result := make([]*SeriesIdWithPosition, 0)
	for i := 0; i < len(edge.end.nexts); i++ {
		nextedge := edge.end.nexts[i]
		if nextedge.index != nil {
			// have this gram
			canmergelist := make([]*SeriesIdWithPosition, 0)
			for j := 0; j < len(sidlist); j++ {
				sp := sidlist[j].startposition
				ep := sidlist[j].endposition
				nextedgepositionlist, isfind := (*nextedge.index)[sidlist[j].sid]
				canmerge, startposition, endposition := CanMerge(sp, ep, nextedgepositionlist)
				if isfind && canmerge {
					canmergelist = append(canmergelist, NewSeriesIdWithPosition(sidlist[j].sid, startposition, endposition))
				}
			}
			partofresult := g.RecursionMatch(nextedge, canmergelist)

			result = append(result, partofresult...)
		}

	}
	return result
}

func (g *Gnfa) Isfinal(node *GnfaNode) bool {
	if node == g.fnode {
		return true
	}
	S := InitializeGnfaStack()
	S.Push(node)
	for len(S.list) != 0 {
		n := S.Pop()
		for i := 0; i < len(n.nexts); i++ {
			if n.nexts[i].epsilon {
				if n.nexts[i].end == g.fnode {
					return true
				} else {
					S.Push(n.nexts[i].end)
				}
			}
		}
	}
	return false
}

func SearchString(indexRoot *decode.SearchTreeNode, label string, buffer []byte, addrCache *cache.AddrCache, invertedCache *cache.InvertedCache) *gramIndex.Inverted_index {
	var invertIndex gramIndex.Inverted_index
	var invertIndexOffset uint64
	var addrOffset uint64
	var indexNode *decode.SearchTreeNode
	var invertIndex1 gramIndex.Inverted_index
	var invertIndex2 gramIndex.Inverted_index
	var invertIndex3 gramIndex.Inverted_index
	invertIndexOffset, addrOffset, indexNode = gramMatchQuery.SearchNodeAddrFromPersistentIndexTree(label, indexRoot, 0, invertIndexOffset, addrOffset, indexNode)
	invertIndex1 = utils.SearchInvertedIndexFromCacheOrDisk(invertIndexOffset, buffer, invertedCache)
	invertIndex = utils.DeepCopy(invertIndex1)
	invertIndex2 = utils.SearchInvertedListFromChildrensOfCurrentNode(indexNode, invertIndex2, buffer, addrCache, invertedCache)
	addrOffsets := utils.SearchAddrOffsetsFromCacheOrDisk(addrOffset, buffer, addrCache)
	if indexNode != nil && len(addrOffsets) > 0 {
		invertIndex3 = utils.TurnAddr2InvertLists(addrOffsets, buffer, invertedCache)
	}
	invertIndex = utils.MergeMapsInvertLists(invertIndex2, invertIndex)
	invertIndex = utils.MergeMapsInvertLists(invertIndex3, invertIndex)

	return &invertIndex
}

type SeriesIdWithPosition struct {
	sid           utils.SeriesId
	startposition []uint16
	endposition   []uint16
}

func (sidwp *SeriesIdWithPosition) Print() {
	fmt.Print("id : ")
	fmt.Print(sidwp.sid.Id)
	fmt.Print(" time : ")
	fmt.Print(sidwp.sid.Time)
	fmt.Print(" startposition : ")
	fmt.Print(sidwp.startposition)
	fmt.Print(" endposition : ")
	fmt.Println(sidwp.endposition)
}

func NewSeriesIdWithPosition(sid utils.SeriesId, startposition []uint16, endposition []uint16) *SeriesIdWithPosition {
	return &SeriesIdWithPosition{sid: sid, startposition: startposition, endposition: endposition}
}

//func QuickSort(sidlist []*SeriesIdWithPosition) {
//	Sort(sidlist, 0, len(sidlist))
//}

//func Sort(sidlist []*SeriesIdWithPosition, left, right int) {
//	if left < right {
//		pivot := sidlist[left].sid.Id
//		j := left
//		for i := left; i < right; i++ {
//			if sidlist[i].sid.Id < pivot {
//				j++
//				sidlist[j], sidlist[i] = sidlist[i], sidlist[j]
//			}
//		}
//		sidlist[left], sidlist[j] = sidlist[j], sidlist[left]
//		Sort(sidlist, left, j)
//		Sort(sidlist, j+1, right)
//	}
//}

//func QuickSort2(sidlist []utils.SeriesId) {
//	Sort2(sidlist, 0, len(sidlist))
//}

//func Sort2(sidlist []utils.SeriesId, left, right int) {
//	if left < right {
//		pivot := sidlist[left].Id
//		j := left
//		for i := left; i < right; i++ {
//			if sidlist[i].Id < pivot {
//				j++
//				sidlist[j], sidlist[i] = sidlist[i], sidlist[j]
//			}
//		}
//		sidlist[left], sidlist[j] = sidlist[j], sidlist[left]
//		Sort2(sidlist, left, j)
//		Sort2(sidlist, j+1, right)
//	}
//}

func RemoveDuplicate(sidlist []*SeriesIdWithPosition) []*SeriesIdWithPosition {
	sidmap := make(map[utils.SeriesId]*SeriesIdWithPosition)
	for i := 0; i < len(sidlist); i++ {
		_, ok := sidmap[sidlist[i].sid]
		if !ok {
			sidmap[sidlist[i].sid] = sidlist[i]
		}
	}
	result := make([]*SeriesIdWithPosition, 0)
	for _, v := range sidmap {
		result = append(result, v)
	}
	return result
}
