package gramClvc

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type GramDictionary interface {
	GenerateClvcDictionaryTree(logs []utils.LogSeries, qmin int, qmax int, T int)
}

type CLVCDic struct {
	TrieTree *TrieTree
}

func NewCLVCDic(qmin int, qmax int) *CLVCDic {
	return &CLVCDic{
		TrieTree: NewTrieTree(qmin, qmax),
	}
}

/*
	logs are log data, qmin and qmax are the minimum and maximum lengths for dictionary building and dividing the index items,
	and the threshold T is the empirical value parameter for building a dictionary and the return value is a dictionary tree.
*/

func (clvcDic *CLVCDic) GenerateClvcDictionaryTree(logs []utils.LogSeries, qmin int, qmax int, T int) {
	for i := range logs {
		str := logs[i].Log
		if len(str) >= qmax {
			for i = 0; i < len(str)-qmax; i++ {
				substring := str[i : i+qmax]
				clvcDic.TrieTree.InsertIntoTrieTree(substring)
			}
			for i = len(str) - qmax; i < len(str)-qmin+1; i++ {
				substring := str[i:len(str)]
				clvcDic.TrieTree.InsertIntoTrieTree(substring)
			}
		} else {
			clvcDic.TrieTree.InsertIntoTrieTree(str)
		}
	}
	clvcDic.TrieTree.PruneTree(T)
	clvcDic.TrieTree.UpdateRootFrequency()
}
