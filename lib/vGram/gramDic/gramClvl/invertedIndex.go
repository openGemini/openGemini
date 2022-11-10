package gramClvl

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
)

func IfsidhavePos(posarray []uint16, sid utils.SeriesId, pos uint16) bool {
	for _, onepos := range posarray {
		if onepos == pos {
			fmt.Errorf("此sid: ", sid, "倒排列表中已存在该位置信息！添加失败")
			return true
		}
	}
	return false
}

func PrintInverted_index(node *gramIndex.IndexTreeNode) {
	for sid, posArray := range node.InvertedIndex() {
		fmt.Print("  /  sid : ", sid, " positionList : ", posArray, "\n")
	}
}

func RemoveInvertedIndex(indexlist gramIndex.Inverted_index, delsid utils.SeriesId, delpos []uint16) {
	if indexlist == nil {
		fmt.Errorf("map为nil!")
	}
	if _, ok := indexlist[delsid]; !ok {
		fmt.Errorf("删除失败，原倒排列表中无此项！")
		return
	} else {
		if len(indexlist[delsid]) == 0 {
			if len(indexlist) > 0 {
				delete(indexlist, delsid)
			} else {
				indexlist = make(gramIndex.Inverted_index, 0)
			}
		} else {
			for _, delposition := range delpos {
				for j, pos := range indexlist[delsid] {
					if delposition == pos {
						indexlist[delsid] = append(indexlist[delsid][:j], indexlist[delsid][j+1:]...)
					}
				}
			}
			if len(indexlist[delsid]) == 0 {
				if len(indexlist) > 0 {
					delete(indexlist, delsid)
				} else {
					indexlist = make(gramIndex.Inverted_index, 0)
				}
			}
		}
	}
}

func SuffixRemoveInvertedIndex(indexlist gramIndex.Inverted_index, delsid utils.SeriesId, delpos []uint16) {
	if indexlist == nil {
		fmt.Errorf("map为nil!")
	}
	if _, ok := indexlist[delsid]; !ok {
		fmt.Errorf("删除失败，原倒排列表中无此项！")
		return
	} else {
		if len(indexlist[delsid]) == 0 {
			if len(indexlist) > 0 {
				delete(indexlist, delsid)
			} else {
				indexlist = make(gramIndex.Inverted_index, 0)
			}
		} else {
			for _, delposition := range delpos {
				for j, pos := range indexlist[delsid] {
					if delposition+1 == pos {
						indexlist[delsid] = append(indexlist[delsid][:j], indexlist[delsid][j+1:]...)
					}
				}
			}
			if len(indexlist[delsid]) == 0 {
				if len(indexlist) > 0 {
					delete(indexlist, delsid)
				} else {
					indexlist = make(gramIndex.Inverted_index, 0)
				}
			}
		}
	}
}
