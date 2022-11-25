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

package tokenClvl

import (
	"math"
	"strings"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
)

func TokenCalculateIndexTrieInverstlist(indextree *tokenIndex.IndexTree) *[]int {
	leaflist := new([]int)
	var calculatEachInverst func(node *tokenIndex.IndexTreeNode, list *[]int)
	calculatEachInverst = func(node *tokenIndex.IndexTreeNode, list *[]int) {
		if node.Isleaf() && node.InvertedIndex() != nil && len(node.InvertedIndex()) != 0 {
			*list = append(*list, CalculateNodeListLength(node))
		}
		for _, childnode := range node.Children() {
			calculatEachInverst(childnode, list)
		}
	}
	calculatEachInverst(indextree.Root(), leaflist)
	return leaflist
}

func LeafToQueue(tree *tokenIndex.IndexTree) []*tokenIndex.IndexTreeNode {
	var queue []*tokenIndex.IndexTreeNode
	var dfsTree func(treeNode *tokenIndex.IndexTreeNode)
	dfsTree = func(treeNode *tokenIndex.IndexTreeNode) {
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

func GenerateQminIndexTree(logs map[utils.SeriesId]string, qmin int) *tokenIndex.IndexTree {
	indexTrie := tokenIndex.NewIndexTrie08(qmin)
	for seriesid, _ := range logs {
		keys := make([]uint16, 0)
		log := logs[seriesid]
		tsid := seriesid.Id
		timeStamp := seriesid.Time
		tokens, _ := utils.DataProcess(log)
		tokenMap := make(map[uint16][]string, 0)
		var i uint16
		for i = 0; int(i) <= len(tokens)-qmin; i++ {
			for j := 0; j < qmin; j++ {
				tokenMap[i] = append(tokenMap[i], tokens[int(i)+j])
			}
			keys = append(keys, i)
		}
		for m := 0; m < len(keys); m++ {
			token := tokenMap[keys[m]]
			seriesId := utils.NewSeriesId(tsid, timeStamp)
			indexTrie.InsertTokensIntoIndexTree08(&token, seriesId, keys[m])
		}
	}
	indexTrie.SetCout(len(logs))
	return indexTrie
}

func GenTokenTw(trie *tokenIndex.IndexTree, sampleStringOfW map[utils.SeriesId]string, qmin int) (*tokenIndex.IndexTree, map[utils.SeriesId]string) {
	logMap := make(map[utils.SeriesId]string)
	treeW := tokenIndex.NewIndexTrie08(trie.Qmin())
	var tokenVGmaps = make(map[string]tokenIndex.Inverted_index)
	for key, value := range sampleStringOfW {
		vgMap := make(map[uint16][]string)
		tsid := key.Id
		timeStamp := key.Time
		sid := utils.NewSeriesId(tsid, timeStamp)
		log := value
		tokenArray, queryString := utils.DataProcess(log)
		logMap[sid] = queryString
		tokenIndex.TokenVGCons(trie, trie.Qmin(), tokenArray, vgMap)
		tokenVGmaps = tokenIndex.WriteToVgMaps(vgMap, sid, tokenVGmaps)
	}
	for tokenStr := range tokenVGmaps {
		token := strings.Split(tokenStr, " ")
		tokenArr := token[0 : len(token)-1]
		invert_index := tokenVGmaps[tokenStr]
		addr := treeW.InsertIntoIndexTree(tokenArr, invert_index)
		if len(tokenArr) > qmin { //Generate all index entries between qmin+1 - len(gram)
			tokenIndex.TokenSubs = make([]tokenIndex.SubTokenOffset, 0)
			tokenIndex.GenerateQmin2QmaxTokens(tokenArr, qmin)
			treeW.InsertOnlyTokenIntoIndexTree(tokenIndex.TokenSubs, addr)
		}
	}
	treeW.SetCout(len(sampleStringOfW))
	return treeW, logMap
}

func EvaluateToken(sampleStringOfWLength int, g1 *tokenIndex.IndexTreeNode, g1Array []string, cnode *tokenIndex.IndexTreeNode, cstr []string, g2 *tokenIndex.IndexTreeNode, g2Array []string, trieW *tokenIndex.IndexTree, sampleStringOfW map[utils.SeriesId]string) bool {
	h := float64(CalculateListLengh(trieW)) / float64(sampleStringOfWLength)
	g1nodeW := FindGInTreeW(trieW, &g1Array)
	g2nodeW := FindGInTreeW(trieW, &g2Array)
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
					strWFields, _ := utils.DataProcess(stringOfW)
					for j, _ := range g1posArray {
						if (int(g1posArray[j])+len(g1Array)+1 <= len(strWFields)) && IfEquals(strWFields, int(g1posArray[j]), int(g1posArray[j])+len(g1Array), cstr) {
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
					strWFields, _ := utils.DataProcess(stringOfW)
					for j, _ := range g2posArray {
						posg2 := g2posArray[j]
						if !(int(posg2)+len(g2Array) <= len(strWFields)) && (int(posg2)+len(g2Array)-len(cstr) >= 0) && IfEquals(strWFields, int(posg2)-len(cstr)+len(g2Array), int(posg2)+len(g2Array), cstr) {
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
		var suffixNodew *tokenIndex.IndexTreeNode
		suffixstr, suffixNodew = GetloggestSuffixTokenNode(trieW, suffixstr, trieW.Qmin())
		var cNodew *tokenIndex.IndexTreeNode
		cNodew = tokenIndex.NewIndexTreeNode(cstr[len(cstr)-1])
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
		InsertIndexNode(g1nodeW, cNodew) //
	}
	return true
}

func IfEquals(strfields []string, start int, end int, cstr []string) bool {
	if len(strfields) == 0 || len(cstr) == 0 {
		return false
	}
	var matchFields = make([]string, 0)
	if start >= 0 && end <= len(strfields) {
		for i := start; i < end; i++ {
			matchFields = append(matchFields, strfields[i])
		}
	} else {
		return false
	}

	if len(matchFields) == len(cstr) && len(matchFields) != 0 {
		for i := 0; i < len(matchFields); i++ {
			if matchFields[i] != cstr[i] {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

func GetloggestSuffixTokenNode(trie *tokenIndex.IndexTree, suffixstr []string, qmin int) ([]string, *tokenIndex.IndexTreeNode) {
	var returnNode *tokenIndex.IndexTreeNode
	var returnstr []string
	for i := 0; i <= len(suffixstr)-qmin; {
		tempnode := GetSuffixTokenNode(trie, suffixstr[i:])
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

func GetSuffixTokenNode(trie *tokenIndex.IndexTree, suffixstr []string) *tokenIndex.IndexTreeNode {
	curnode := trie.Root()
	var returnNode *tokenIndex.IndexTreeNode
	for _, suffixtoken := range suffixstr {
		nodeindex := tokenIndex.GetIndexNode(curnode.Children(), string(suffixtoken))
		if nodeindex == -1 {
			returnNode = nil
			break
		} else {
			returnNode = curnode.Children()[nodeindex]
			curnode = curnode.Children()[nodeindex]
		}
	}
	return returnNode
}

func InsertIndexNode(trie *tokenIndex.IndexTreeNode, node *tokenIndex.IndexTreeNode) {
	array := trie.Children()
	index := tokenIndex.GetIndexNode(array, node.Data())
	if index == -1 {
		array[utils.StringToHashCode(node.Data())] = node
	} else {
		treeNode := (array)[index]
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
	trie.SetChildren(array)
}

func MaxDepth(tree *tokenIndex.IndexTree) int {
	depth := MaxDepthNode(tree.Root())
	return depth - 1
}

func MaxDepthNode(root *tokenIndex.IndexTreeNode) int {
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
func traverseTree(indexRoot *tokenIndex.IndexTreeNode) *tokenClvc.TrieTreeNode {
	if indexRoot == nil {
		return nil
	}
	newDicNode := tokenClvc.NewTrieTreeNode(indexRoot.Data())
	newDicNode.SetIsleaf(indexRoot.Isleaf())
	for key, _ := range indexRoot.Children() {
		childNode := traverseTree(indexRoot.Children()[key])
		newDicNode.Children()[key] = childNode
	}
	return newDicNode
}

func TrieNodeTrans(indexTrie *tokenIndex.IndexTree) *tokenClvc.TrieTree {
	dicTrie := tokenClvc.NewTrieTree(indexTrie.Qmin(), indexTrie.Qmax())
	dicTrie.SetRoot(traverseTree(indexTrie.Root()))
	return dicTrie
}
