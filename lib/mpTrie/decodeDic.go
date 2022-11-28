package mpTrie

import (
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"strings"
)

func UnserializeGramDicFromFile(filename string) *gramClvc.TrieTree {
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	grams := strings.Split(bufstr, SPLITFLAG)
	grams = grams[:len(grams)-1]
	qmin, qmax := 0, 0
	for _, gram := range grams {
		if qmin > len(gram) {
			qmin = len(gram)
		}
		if qmax < len(gram) {
			qmax = len(gram)
		}
	}
	dictrie := gramClvc.NewTrieTree(qmin, qmax)
	for _, gram := range grams {
		dictrie.InsertIntoTrieTree(gram)
	}
	return dictrie
}

func UnserializeTokenDicFromFile(filename string) *tokenClvc.TrieTree {
	buffer, _ := GetBytesFromFile(filename)
	bufstr := string(buffer)
	res := strings.Split(bufstr, SPLITFLAG)
	res = res[:len(res)-1]
	qmin, qmax := 0, 0
	for _, tokens := range res {
		if qmin > len(tokens) {
			qmin = len(tokens)
		}
		if qmax < len(tokens) {
			qmax = len(tokens)
		}
	}
	dictrie := tokenClvc.NewTrieTree(qmin, qmax)
	for _, tokens := range res {
		token := strings.Split(tokens, " ")
		dictrie.InsertIntoTrieTree(&token)
	}
	return dictrie
}
