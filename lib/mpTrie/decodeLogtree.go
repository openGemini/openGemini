package mpTrie

import (
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"strings"
)

func UnserializeLogTreeFromFile(filename string) *gramIndex.LogTree {
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	grams := strings.Split(bufstr, SPLITFLAG)
	grams = grams[:len(grams)-1]
	qmax := 0
	for _, gram := range grams {
		if qmax < len(gram) {
			qmax = len(gram)
		}
	}
	logtree := gramIndex.NewLogTree(qmax)
	for _, gram := range grams {
		logtree.InsertIntoTrieTreeLogTree(gram)
	}
	return logtree
}
