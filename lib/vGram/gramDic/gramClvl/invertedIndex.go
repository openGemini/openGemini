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
	"fmt"

	"github.com/openGemini/openGemini/lib/utils"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
)

func IfsidhavePos(posarray []uint16, sid utils.SeriesId, pos uint16) bool {
	for _, onepos := range posarray {
		if onepos == pos {
			// sid has existed in the inverted index
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
		// map is nil
		return
	}
	if _, ok := indexlist[delsid]; !ok {
		return
	} else {
		if len(indexlist[delsid]) == 0 {
			if len(indexlist) > 0 {
				delete(indexlist, delsid)
			}
			// else {
			//	indexlist = make(gramIndex.Inverted_index, 0)
			// }
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
				}
				// else {
				//	indexlist = make(gramIndex.Inverted_index, 0)
				//}
			}
		}
	}
}

func SuffixRemoveInvertedIndex(indexlist gramIndex.Inverted_index, delsid utils.SeriesId, delpos []uint16) {
	if indexlist == nil {
		// map is nil
		return
	}
	if _, ok := indexlist[delsid]; !ok {
		// Delete failed.
		return
	} else {
		if len(indexlist[delsid]) == 0 {
			if len(indexlist) > 0 {
				delete(indexlist, delsid)
			}
			// else {
			//	indexlist = make(gramIndex.Inverted_index, 0)
			//}
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
				}
				// else {
				//	indexlist = make(gramIndex.Inverted_index, 0)
				//}
			}
		}
	}
}
