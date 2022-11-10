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
package gramClvl

import (
	"math"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
)

func HasIndexNode(children map[uint8]*gramIndex.IndexTreeNode, data string) int8 {
	char := data[0]
	if children[char] != nil {
		return int8(char)
	}
	return -1
}

func InsertIndexNode(array map[uint8]*gramIndex.IndexTreeNode, node *gramIndex.IndexTreeNode) {
	index := HasIndexNode(array, node.Data())
	if index == -1 {
		array[node.Data()[0]] = node
	} else {
		treeNode := array[uint8(index)]
		for sidkey, posvalue := range node.InvertedIndex() {
			if _, ok := treeNode.InvertedIndex()[sidkey]; !ok {
				treeNode.InvertedIndex()[sidkey] = posvalue
			} else {
				if len(posvalue) == 0 {
					continue
				} else {
					for _, pos := range posvalue {
						ifsidhavePos := IfsidhavePos(treeNode.InvertedIndex()[sidkey], sidkey, pos)
						if !ifsidhavePos {
							treeNode.InvertedIndex()[sidkey] = append(treeNode.InvertedIndex()[sidkey], pos)
						}
					}
				}
			}
		}
	}
}

func GenerateQminTree(logs map[utils.SeriesId]string, qmin int) *gramIndex.IndexTree {
	indexTrie := gramIndex.NewIndexTrie(qmin)
	var vgMaps = make(map[string]gramIndex.Inverted_index)
	for key := range logs {
		gramMap := make(map[uint16]string, 0)
		tsid := key.Id
		timeStamp := key.Time
		log := logs[key]
		var i uint16
		for i = 0; int(i) <= len(log)-qmin; i++ {
			str := log[int(i) : int(i)+qmin]
			gramMap[i] = str
		}
		seriesId := utils.NewSeriesId(tsid, timeStamp)
		vgMaps = gramIndex.WriteToVgMaps(gramMap, seriesId, vgMaps)
	}
	for gram, invert_index := range vgMaps {
		indexTrie.InsertIntoIndexTree(gram, invert_index)
	}
	indexTrie.SetCout(len(logs))
	return indexTrie
}

func LeafToQueue(tree *gramIndex.IndexTree) []*gramIndex.IndexTreeNode {
	var queue []*gramIndex.IndexTreeNode
	var dfsTree func(treeNode *gramIndex.IndexTreeNode)
	dfsTree = func(treeNode *gramIndex.IndexTreeNode) {
		if treeNode == nil {
			return
		}
		if treeNode.Isleaf() {
			queue = append(queue, treeNode)
		}
		for _, child := range treeNode.Children() {
			dfsTree(child)
		}
	}
	dfsTree(tree.Root())
	return queue
}

func Evaluate(sampleStringOfWLength int, g1 *gramIndex.IndexTreeNode, g1str string, cnode *gramIndex.IndexTreeNode, cstr string, g2 *gramIndex.IndexTreeNode, g2str string, trieW *gramIndex.IndexTree, sampleStringOfW map[utils.SeriesId]string) bool {
	h := float64(CalculateListLengh(trieW)) / float64(sampleStringOfWLength)
	g1Array := StrToStrArray(g1str)
	g1nodeW := FindGInTreeW(trieW, g1Array)
	g2Array := StrToStrArray(g2str)
	g2nodeW := FindGInTreeW(trieW, g2Array)
	p24 := 0
	p3 := 0
	cnodeTWList := make(map[utils.SeriesId][]uint16)
	if g1nodeW == nil && g2nodeW == nil {
		return false
	} else {
		if g1nodeW != nil {
			g1indexlistW := g1nodeW.InvertedIndex()
			if g1indexlistW != nil {
				for g1sid, g1posArray := range g1indexlistW {
					stringOfW := sampleStringOfW[g1sid]
					for j, _ := range g1posArray {
						if (int(g1posArray[j])+len(g1str)+1 <= len(stringOfW)) && (stringOfW[int(g1posArray[j]):int(g1posArray[j])+len(g1str)+1] == cstr) {
							p3++
							cnodeTWList[g1sid] = append(cnodeTWList[g1sid], g1posArray[j])
						} else {
							p24++
						}
					}
				}
			}
		}
		if g2nodeW != nil {
			g2indexlistW := g2nodeW.InvertedIndex()
			if g2indexlistW != nil {
				for g2sid, g2posArray := range g2indexlistW {
					stringOfW := sampleStringOfW[g2sid]
					for j, _ := range g2posArray {
						posg2 := g2posArray[j]
						if !((int(posg2)-(len(cstr)-len(g2str))+len(cstr) <= len(stringOfW)) && (int(posg2)-(len(cstr)-len(g2str)) >= 0) && (stringOfW[int(posg2)-(len(cstr)-len(g2str)):int(posg2)-(len(cstr)-len(g2str))+len(cstr)]) == cstr) {
							p24++
						}
					}
				}
			}
		}
	}
	lGd := CalculateNodeListLength(cnode)
	lG1d := CalculateNodeListLength(g1)
	lG2d := CalculateNodeListLength(g2)
	T := float64(p24)*float64(lGd)*math.Log10(float64(h)) + float64(p3)*(float64(lG1d)+float64(lG2d)-float64(lGd))*math.Log10(float64(h))
	if T <= 0 {
		return false
	}
	if g1nodeW != nil {
		suffixstr := cstr[1:len(cstr)]
		var suffixNodew *gramIndex.IndexTreeNode
		suffixstr, suffixNodew = GetloggestSuffixNode(trieW, suffixstr, trieW.Qmin())
		var cNodew *gramIndex.IndexTreeNode
		cNodew = gramIndex.NewIndexTreeNode(cstr[len(cstr)-1 : len(cstr)])
		cNodew.SetInvertedIndex(cnodeTWList)
		cNodew.SetIsleaf(true)
		for cdelsid, cdelposarry := range cNodew.InvertedIndex() {
			if len(g1nodeW.InvertedIndex()) > 0 {
				RemoveInvertedIndex(g1nodeW.InvertedIndex(), cdelsid, cdelposarry)
			}
			if suffixNodew != nil {
				if len(suffixNodew.InvertedIndex()) > 0 {
					SuffixRemoveInvertedIndex(suffixNodew.InvertedIndex(), cdelsid, cdelposarry)
				}
			}
		}
		InsertIndexNode(g1nodeW.Children(), cNodew)
	}
	return true
}

func GetloggestSuffixNode(trie *gramIndex.IndexTree, suffixstr string, qmin int) (string, *gramIndex.IndexTreeNode) {
	var returnNode *gramIndex.IndexTreeNode
	var returnstr string
	for i := 0; i <= len(suffixstr)-qmin; {
		tempnode := GetSuffixNode(trie, suffixstr[i:])
		if tempnode != nil {
			returnNode = tempnode
			returnstr = suffixstr[i:]
			break
		} else {
			i++
		}
	}
	return returnstr, returnNode
}
func GetSuffixNode(trie *gramIndex.IndexTree, suffixstr string) *gramIndex.IndexTreeNode {
	curnode := trie.Root()
	var returnNode *gramIndex.IndexTreeNode
	for _, suffixchar := range suffixstr {
		nodeindex := HasIndexNode(curnode.Children(), string(suffixchar))
		if nodeindex == -1 {
			returnNode = nil
			break
		} else {
			returnNode = curnode.Children()[uint8(nodeindex)]
			curnode = curnode.Children()[uint8(nodeindex)]
		}
	}
	return returnNode
}

func MaxDepth(tree *gramIndex.IndexTree) int {
	depth := MaxDepthNode(tree.Root())
	return depth - 1
}

func MaxDepthNode(root *gramIndex.IndexTreeNode) int {
	if root == nil {
		return 0
	}
	var max int = 0
	for i, _ := range root.Children() {
		depth := MaxDepthNode(root.Children()[i])
		if max < depth {
			max = depth
		}
	}
	return max + 1
}
func traverseTree(indexRoot *gramIndex.IndexTreeNode) *gramClvc.TrieTreeNode {
	if indexRoot == nil {
		return nil
	}
	newDicNode := gramClvc.NewTrieTreeNode(indexRoot.Data())
	newDicNode.SetIsleaf(indexRoot.Isleaf())
	for key, _ := range indexRoot.Children() {
		childNode := traverseTree(indexRoot.Children()[key])
		newDicNode.Children()[key] = childNode
	}
	return newDicNode
}

func TrieNodeTrans(indexTrie *gramIndex.IndexTree) *gramClvc.TrieTree {
	dicTrie := gramClvc.NewTrieTree(indexTrie.Qmin(), indexTrie.Qmax())
	dicTrie.SetRoot(traverseTree(indexTrie.Root()))
	return dicTrie
}
