package gramClvl

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"sort"
)

type GramDictionary interface {
	GenerateClvlDictionaryTree(logs map[utils.SeriesId]string, qmin int, sampleStringOfW map[utils.SeriesId]string)
}

type CLVLDic struct {
	TrieTree *gramClvc.TrieTree
}

func NewCLVLDic(qmin int, qmax int) *CLVLDic {
	return &CLVLDic{
		TrieTree: gramClvc.NewTrieTree(qmin, qmax),
	}
}

/*
	logs are log data, qmin are the minimum lengths for dictionary building and dividing the index items,
	and sampleStringOfW is the extracted query payload, the return value is a dictionary tree.
*/

func (clvlDic *CLVLDic) GenerateClvlDictionaryTree(logs map[utils.SeriesId]string, qmin int, sampleStringOfW map[utils.SeriesId]string) {
	trie := GenerateQminTree(logs, qmin)
	trieW := GeneratorTw(sampleStringOfW, qmin, trie)
	queuelist := LeafToQueue(trie)
	queue := make([]*gramIndex.IndexTreeNode, 0)
	for _, onenode := range queuelist {
		node := gramIndex.NewIndexTreeNode(onenode.Data())
		node.SetIsleaf(true)
		node.SetInvertedIndex(onenode.InvertedIndex())
		queue = append(queue, node)
	}
	floorNum := qmin
	for len(queue) != 0 {
		var newqueue []*gramIndex.IndexTreeNode
		for _, data := range queue {
			indexlist := data.InvertedIndex()
			var sidSort []utils.SeriesId
			for oneSid, _ := range indexlist {
				sidSort = append(sidSort, oneSid)
			}
			sort.SliceStable(sidSort, func(i, j int) bool {
				if sidSort[i].Id < sidSort[j].Id {
					return true
				}
				return false
			})
			for sidindex := 0; sidindex < len(sidSort); sidindex++ {
				oneSid := sidSort[sidindex]
				oneArray := indexlist[oneSid]
				nextgram := make(map[string]gramIndex.Inverted_index, 0)
				keys := make([]string, 0)
				var evaluateResult bool
				var cNode, suffixNode *gramIndex.IndexTreeNode
				var pathsuffix string
				var commomsuffix string
				curstr := logs[oneSid]
				poslist := oneArray
				//获取扩展gram
				if len(poslist) == 0 && poslist == nil {
					fmt.Errorf("倒排列表错误，有seriesid，但positionlist为空！")
					break
				} else {
					if len(poslist) >= 1 {
						for i := 0; i < len(poslist); i++ {
							pos := int(poslist[i]) + floorNum
							if pos < len(curstr) {
								pathsuffix = curstr[poslist[i]:pos]
								commomsuffix = curstr[poslist[i]+1 : pos]
								curNextgram := string(curstr[pos])
								if nextgram[curNextgram] == nil {
									nextgram[curNextgram] = make(map[utils.SeriesId][]uint16)
								}
								nextgram[curNextgram][oneSid] = append(nextgram[curNextgram][oneSid], poslist[i])
								keys = append(keys, curNextgram)
							}
						}
					}
					for key := 0; key < len(keys); key++ {
						gramchar := keys[key]
						trieNode := GetSuffixNode(trie, pathsuffix)
						if trieNode == nil || len(trieNode.InvertedIndex()) == 0 {
							continue
						}
						suffixstr := commomsuffix + gramchar
						suffixstr, suffixNode = GetloggestSuffixNode(trie, suffixstr, qmin)
						if suffixNode == nil || len(suffixstr) == 0 {
							fmt.Errorf("没有最长后缀！")
							suffixNode = gramIndex.NewIndexTreeNode("")
						}
						cNode = gramIndex.NewIndexTreeNode(gramchar)
						cNode.SetInvertedIndex(nextgram[gramchar])
						cNode.SetIsleaf(true)

						evaluateResult = Evaluate(len(sampleStringOfW), trieNode, pathsuffix, cNode, pathsuffix+gramchar, suffixNode, suffixstr, trieW, sampleStringOfW)
						if evaluateResult {
							for cdelSid, cdelPosArray := range cNode.InvertedIndex() {
								if len(trieNode.InvertedIndex()) > 0 {
									RemoveInvertedIndex(trieNode.InvertedIndex(), cdelSid, cdelPosArray)
								}
								if len(suffixNode.InvertedIndex()) > 0 {
									SuffixRemoveInvertedIndex(suffixNode.InvertedIndex(), cdelSid, cdelPosArray)
								}
							}
							InsertIndexNode(trieNode.Children(), cNode)
							newqueue = append(newqueue, cNode)
						} else {
							continue
						}
					}
				}
			}
		}
		floorNum++
		queue = newqueue
	}
	trie.SetQmax(MaxDepth(trie))
	clvlDic.TrieTree = TrieNodeTrans(trie)
	clvlDic.TrieTree.PrintTree()
}
