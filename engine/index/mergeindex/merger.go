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

package mergeindex

import (
	"log"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

const MaxTSIDsPerRow = 64

func MergeItems(data []byte, items []mergeset.Item, nsPrefix byte, rowsMerger ItemMerger) ([]byte, []mergeset.Item) {
	// Perform quick checks whether items contain rows starting from NsPrefix
	// based on the fact that items are sorted.
	if len(items) <= 2 {
		// The first and the last row must remain unchanged.
		return data, items
	}
	firstItem := items[0].Bytes(data)
	if len(firstItem) > 0 && firstItem[0] > nsPrefix {
		return data, items
	}
	lastItem := items[len(items)-1].Bytes(data)
	if len(lastItem) > 0 && lastItem[0] < nsPrefix {
		return data, items
	}

	// items contain at least one row starting from NsPrefix. Merge rows with common tag.
	rowsMerger.AppendData(data)
	rowsMerger.AppendItems(items)
	mp := rowsMerger.GetMergeParser()
	mpPrev := rowsMerger.GetPreMergeParser()
	dstData := data[:0]
	dstItems := items[:0]
	for i, it := range items {
		item := it.Bytes(data)
		if len(item) == 0 || item[0] != nsPrefix || i == 0 || i == len(items)-1 {
			// Write rows not starting with NsPrefix as-is.
			// Additionally write the first and the last row as-is in order to preserve
			// sort order for adjacent blocks.
			dstData, dstItems = rowsMerger.FlushPendingTSIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if err := mp.Init(item, nsPrefix); err != nil {
			log.Panicf("FATAL: cannot parse row starting with NsPrefix %d during merge: %s", nsPrefix, err)
		}
		if mp.TSIDsLen() >= MaxTSIDsPerRow {
			dstData, dstItems = rowsMerger.FlushPendingTSIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if !mp.EqualPrefix(mpPrev) {
			dstData, dstItems = rowsMerger.FlushPendingTSIDs(dstData, dstItems, mpPrev)
		}
		mp.ParseTSIDs()
		rowsMerger.AppendTSIDs(mp.GetTSIDs())
		mpPrev, mp = mp, mpPrev
		if len(rowsMerger.GetPendingTSIDs()) >= MaxTSIDsPerRow {
			dstData, dstItems = rowsMerger.FlushPendingTSIDs(dstData, dstItems, mpPrev)
		}
	}
	if len(rowsMerger.GetPendingTSIDs()) > 0 {
		logger.Panicf("BUG: tmm.pendingTSIDs must be empty at this point; got %d items: %d", len(rowsMerger.GetPendingTSIDs()), rowsMerger.GetPendingTSIDs())
	}

	atomic.AddUint64(&indexBlocksWithTSIDsProcessed, 1)
	return dstData, dstItems
}

var indexBlocksWithTSIDsProcessed uint64

type ItemMerger interface {
	FlushPendingTSIDs(dstData []byte, dstItems []mergeset.Item, mp ItemParser) ([]byte, []mergeset.Item)
	AppendTSIDs(tsids []uint64)
	AppendData(data []byte)
	AppendItems(items []mergeset.Item)
	GetMergeParser() ItemParser
	GetPreMergeParser() ItemParser
	GetPendingTSIDs() []uint64
}
