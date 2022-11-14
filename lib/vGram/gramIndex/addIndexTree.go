package gramIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
)

func AddIndex(logs []utils.LogSeries, qmin int, qmax int, logTreeMax int, root *gramClvc.TrieTreeNode, logTree *LogTree, indexTree *IndexTree) (*IndexTree, *IndexTreeNode, *LogTree) {
	cout := indexTree.Cout()
	var vgMaps = make(map[string]Inverted_index)
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
		vgMaps = WriteToVgMaps(vgMap, sid, vgMaps)
	}
	var addr *IndexTreeNode
	for gram := range vgMaps {
		invert_index := vgMaps[gram]
		addr = indexTree.InsertIntoIndexTree(gram, invert_index)
		if len(gram) > qmin && len(gram) <= qmax {
			GramSubs = make([]SubGramOffset, 0)
			GenerateQmin2QmaxGrams(gram, qmin)
			indexTree.InsertOnlyGramIntoIndexTree(GramSubs, addr)
		}
	}
	indexTree.SetCout(cout + len(logs))
	//indexTree.PrintIndexTree()
	return indexTree, indexTree.root, logTree
}
