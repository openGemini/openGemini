package tokenIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"strings"
)

func AddIndex(logs []utils.LogSeries, qmin int, qmax int, root *tokenClvc.TrieTreeNode, indexTree *IndexTree) (*IndexTree, *IndexTreeNode) {
	cout := indexTree.Cout()
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
	indexTree.SetCout(cout + len(logs))
	//indexTree.PrintIndexTree()
	return indexTree, indexTree.root
}
