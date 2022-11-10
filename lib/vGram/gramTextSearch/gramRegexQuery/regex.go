package gramRegexQuery

import (
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"strings"
	"time"
)

type Regex struct {
	re   string
	gnfa *Gnfa
}

func NewRegex(re string, trietree *gramClvc.TrieTree) *Regex {
	parseTree := GenerateParseTree(re)
	nfa := GenerateNfa(parseTree)
	gnfa := GenerateGnfa(nfa, trietree)
	return &Regex{
		re:   re,
		gnfa: gnfa,
	}
}

type RegexPlus struct {
	re      string
	gnfa    *Gnfa
	front   uint8
	sidlist []*SeriesIdWithPosition
}

func NewRegexPlus(re string, front uint8) *RegexPlus {
	return &RegexPlus{re: re, front: front}
}

func (rp *RegexPlus) GenerateGNFA(trietree *gramClvc.TrieTree) {
	parseTree := GenerateParseTree(rp.re)
	nfa := GenerateNfa(parseTree)
	rp.gnfa = GenerateGnfa(nfa, trietree)
}

func GenerateRegexPlusList(re string, qmin int) (bool, []*RegexPlus) {
	result := make([]*RegexPlus, 0)
	relist := strings.Split(re, ".")
	if relist[len(relist)-1] == "" || relist[len(relist)-1] == "+" || relist[len(relist)-1] == "?" || relist[len(relist)-1] == "*" {
		relist = relist[:len(relist)-1]
	}
	i := 0
	if re[0] != '.' {
		regex := relist[i]
		if len(regex) < qmin {
			return false, make([]*RegexPlus, 0)
		}
		rp := NewRegexPlus(relist[i], ' ')
		result = append(result, rp)
		i = 1
	} else {
		relist = relist[1:]
	}
	for ; i < len(relist); i++ {
		ch := relist[i][0]
		if ch == '?' || ch == '+' || ch == '*' {
			regex := relist[i][1:]
			if len(regex) < qmin {
				return false, make([]*RegexPlus, 0)
			}
			rp := NewRegexPlus(regex, ch)
			result = append(result, rp)
		} else {
			regex := relist[i]
			if len(regex) < qmin {
				return false, make([]*RegexPlus, 0)
			}
			rp := NewRegexPlus(relist[i], '.')
			result = append(result, rp)
		}
	}
	return true, result
}

func RegexStandardization(re string) string {
	length := len(re)
	for i := 0; i < length; i++ {
		if (re[i] == '+' || re[i] == '?' || re[i] == '*') && re[i-1] != ')' {
			re = re[:i-1] + "(" + re[i-1:i] + ")" + re[i:]
			length += 2
			i += 2
		}
	}
	return re
}

func RegexSearch(re string, trietree *gramClvc.TrieTree, indextree *gramIndex.IndexTree) []utils.SeriesId {
	//fmt.Println(re + " : ")
	//start := time.Now().UnixMicro()
	split := strings.Contains(re, ".")
	result := make([]utils.SeriesId, 0)
	if !split {
		sidlist := MatchRegex(re, trietree, indextree)
		result = make([]utils.SeriesId, len(sidlist))
		for i := 0; i < len(sidlist); i++ {
			result[i] = sidlist[i].sid
		}
	} else {
		isnoterror, rplist := GenerateRegexPlusList(re, indextree.Qmin())
		if !isnoterror {
			//fmt.Println("syntax error !")
			return nil
		} else {
			resultmap := MatchRegexPlusList(rplist, trietree, indextree)
			result = make([]utils.SeriesId, len(resultmap))
			index := 0
			for key := range resultmap {
				result[index] = key
				index++
			}
			//QuickSort2(result)
		}
	}
	//end := time.Now().UnixMicro()
	//fmt.Println("总计花费时间：", end-start)
	return result
}

func MatchRegex(re string, trietree *gramClvc.TrieTree, indextree *gramIndex.IndexTree) []*SeriesIdWithPosition {
	//gramnum := 0
	//ctime := int64(0)
	//mtime := int64(0)
	//srtime := int64(0)
	//ltime := int64(0)
	//constart := time.Now().UnixMicro()
	re = RegexStandardization(re)
	regex := NewRegex(re, trietree)
	//conend := time.Now().UnixMicro()
	//ctime = conend - constart

	//lstart := time.Now().UnixMicro()
	regex.LoadInvertedIndex(indextree)
	//	lend := time.Now().UnixMicro()
	//ltime = lend - lstart

	//matchstart := time.Now().UnixMicro()
	sidlist := regex.Match()
	//matchend := time.Now().UnixMicro()
	//mtime = matchend - matchstart

	//sortstart := time.Now().UnixMicro()
	sidlist = SortAndRemoveDuplicate(sidlist)
	//sortend := time.Now().UnixMicro()
	//srtime = sortend - sortstart
	//fmt.Print("count : ")
	//fmt.Println(len(sidlist))

	//for i := 0; i < len(sidlist); i++ {
	//	sidlist[i].Print()
	//}

	//gramnum = len(regex.gnfa.edges)
	//fmt.Println("构建自动机用时：", ctime)
	//fmt.Println("读取索引用时：", ltime)
	//fmt.Println("匹配用时：", mtime)
	//fmt.Println("排序去重用时：", srtime)
	//fmt.Println("总计grams数量：", gramnum)
	return sidlist
}

func MatchRegexPlusList(regexpluslist []*RegexPlus, trietree *gramClvc.TrieTree, indextree *gramIndex.IndexTree) map[utils.SeriesId][]uint16 {
	gramnum := 0
	//ctime := int64(0)
	mtime := int64(0)
	srtime := int64(0)
	//mergetime := int64(0)
	ltime := int64(0)
	for i := 0; i < len(regexpluslist); i++ {
		//constart := time.Now().UnixMicro()
		regexpluslist[i].re = RegexStandardization(regexpluslist[i].re)
		regexpluslist[i].GenerateGNFA(trietree)
		//conend := time.Now().UnixMicro()
		//ctime = conend - constart

		lstart := time.Now().UnixMicro()
		regexpluslist[i].gnfa.LoadInvertedIndex(indextree)
		lend := time.Now().UnixMicro()
		ltime += lend - lstart

		matchstart := time.Now().UnixMicro()
		sidlist := regexpluslist[i].gnfa.Match()
		matchend := time.Now().UnixMicro()
		mtime += matchend - matchstart

		sortstart := time.Now().UnixMicro()
		sidlist = SortAndRemoveDuplicate(sidlist)
		sortend := time.Now().UnixMicro()
		srtime += sortend - sortstart

		regexpluslist[i].sidlist = sidlist
		gramnum += len(regexpluslist[i].gnfa.edges)
	}

	//mergestart := time.Now().UnixMicro()
	resultlist := MergeRegexPlus(regexpluslist)
	//resultlist := MergeSidList(sidlist)
	//mergeend := time.Now().UnixMicro()
	//mergetime = mergeend - mergestart
	//fmt.Println("-----------------------------------")
	//
	//fmt.Println("-----------------------------------")
	//fmt.Print("count : ")
	//fmt.Println(len(resultlist))
	//fmt.Println("构建自动机用时：", ctime)
	//fmt.Println("读取索引用时：", ltime)
	//fmt.Println("匹配用时：", mtime)
	//fmt.Println("排序去重用时：", srtime)
	//fmt.Println("归并用时：", mergetime)
	//fmt.Println("总计grams数量：", gramnum)
	return resultlist

}

func MergeRegexPlus(rplist []*RegexPlus) map[utils.SeriesId][]uint16 {
	// get start list
	mergemap := make(map[utils.SeriesId][]uint16)
	if rplist[0].front == '.' {
		for i := 0; i < len(rplist[0].sidlist); i++ {
			startpos := rplist[0].sidlist[i].startposition
			if startpos[0] == 0 {
				startpos = startpos[1:]
				if len(startpos) != 0 {
					mergemap[rplist[0].sidlist[i].sid] = rplist[0].sidlist[i].endposition
				}
			}

		}
	} else {
		for i := 0; i < len(rplist[0].sidlist); i++ {
			mergemap[rplist[0].sidlist[i].sid] = rplist[0].sidlist[i].endposition
		}
	}
	// recursion
	for i := 1; i < len(rplist); i++ {
		nextmergemap := make(map[utils.SeriesId][]uint16)
		for j := 0; j < len(rplist[i].sidlist); j++ {
			sid := rplist[i].sidlist[j].sid
			endlist, find := mergemap[sid]
			if find {
				canmerge, list := MergeWithOp(endlist, rplist[i].sidlist[j].startposition, rplist[i].sidlist[j].endposition, rplist[i].front)
				if canmerge {
					nextmergemap[sid] = list
				}
			}
		}
		mergemap = nextmergemap
	}
	return mergemap

}

func MergeWithOp(lastendposition []uint16, nextstartposition []uint16, nextendposition []uint16, op uint8) (bool, []uint16) {
	endposisiton := make([]uint16, 0)
	for i := 0; i < len(nextstartposition); i++ {
		for j := 0; j < len(lastendposition); j++ {
			if CanMergeWithOp(lastendposition[j], nextstartposition[i], op) {
				endposisiton = append(endposisiton, nextendposition[i])
				break
			}
		}
	}
	if len(endposisiton) == 0 {
		return false, endposisiton
	}
	return true, endposisiton
}

func CanMergeWithOp(lastend uint16, nextstart uint16, op uint8) bool {
	if op == '?' && nextstart-lastend <= 1 && nextstart-lastend >= 0 {
		return true
	} else if op == '+' && nextstart-lastend >= 1 {
		return true
	} else if op == '*' && nextstart-lastend >= 0 {
		return true
	} else if op == '.' && nextstart-lastend == 1 {
		return true
	} else {
		return false
	}
}

func (re *Regex) LoadInvertedIndex(indextree *gramIndex.IndexTree) {
	re.gnfa.LoadInvertedIndex(indextree)
}

func (re *Regex) Match() []*SeriesIdWithPosition {
	sidlist := re.gnfa.Match()
	return sidlist

}

func SortAndRemoveDuplicate(sidlist []*SeriesIdWithPosition) []*SeriesIdWithPosition {
	//QuickSort(sidlist)
	sidlist = RemoveDuplicate(sidlist)
	return sidlist
}
