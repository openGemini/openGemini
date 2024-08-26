// Copyright Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mergeindex

import (
	"bytes"
	"log"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
)

const (
	MaxItemSize = 64 * 1024
)

func MergeItemsForColumnStore(data []byte, items []mergeset.Item,
	nsPrefix byte, rowsMerger ItemMergerForLabelStore, isArrowFlight bool) ([]byte, []mergeset.Item) {
	if len(items) <= 2 {
		return data, items
	}
	head := items[0].Bytes(data)
	tail := items[len(items)-1].Bytes(data)
	if (len(head) > 0 && head[0] > nsPrefix) || (len(tail) > 0 && tail[0] < nsPrefix) {
		return data, items
	}

	// items contain at least one row starting from NsPrefix. Merge rows with common tag.
	mp := rowsMerger.GetMergeParser()
	mpPrev := rowsMerger.GetPreMergeParser()
	dstData := data[:0]
	dstItems := items[:0]
	prevItem := head

	for i, it := range items {
		item := it.Bytes(data)
		if len(item) == 0 || item[0] != nsPrefix || i == 0 || i == len(items)-1 {
			// Write rows not starting with NsPrefix as-is.
			// Additionally write the first and the last row as-is in order to preserve
			// sort order for adjacent blocks.
			dstData, dstItems = flush(dstData, item, dstItems, mpPrev, rowsMerger)
			continue
		}

		if bytes.Equal(prevItem, item) {
			continue
		} else {
			prevItem = item
		}

		if isArrowFlight {
			if err := mp.Init(item, nsPrefix); err != nil {
				log.Panicf("FATAL: cannot parse row starting with NsPrefix %d during merge: %s", nsPrefix, err)
			}
			if mp.GetValuesSize() >= MaxItemSize {
				dstData, dstItems = flush(dstData, item, dstItems, mpPrev, rowsMerger)
				mp.ResetValueSize()
				continue
			}
			if !mp.EqualPrefix(mpPrev) {
				dstData, dstItems = rowsMerger.FlushPendingValues(dstData, dstItems, mpPrev)
			}

			if rowsMerger.GetPendingValuesSize(mp.GetValues()) >= MaxItemSize {
				dstData, dstItems = rowsMerger.FlushPendingValues(dstData, dstItems, mpPrev)
			}
			rowsMerger.SetPrifixSize(mp.GetPrifixSize())
			rowsMerger.AppendValues(mp.GetValues())
			mp.ResetValueSize()
			mpPrev, mp = mp, mpPrev
		} else {
			dstData, dstItems = flush(dstData, item, dstItems, mpPrev, rowsMerger)
		}
	}

	if rowsMerger.GetPendingValues() > 0 {
		logger.Panicf("BUG: tagValues must be empty at this point")
	}

	return dstData, dstItems
}

func flush(dstData, item []byte, dstItems []mergeset.Item, mpPrev ItemParserForLabelStore, rowsMerger ItemMergerForLabelStore) ([]byte, []mergeset.Item) {
	dstData, dstItems = rowsMerger.FlushPendingValues(dstData, dstItems, mpPrev)
	dstData = append(dstData, item...)
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(len(dstData) - len(item)),
		End:   uint32(len(dstData)),
	})
	return dstData, dstItems
}

type ItemMergerForLabelStore interface {
	AppendValues(values [][]byte)
	SetPrifixSize(size int)
	GetMergeParser() ItemParserForLabelStore
	GetPreMergeParser() ItemParserForLabelStore
	GetPendingValuesSize(values [][]byte) int
	GetPendingValues() int
	FlushPendingValues(dstData []byte, dstItems []mergeset.Item, mp ItemParserForLabelStore) ([]byte, []mergeset.Item)
}
