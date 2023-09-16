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

package immutable

import (
	"github.com/openGemini/openGemini/engine/index/clv"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

type Location struct {
	ctx     *ReadContext
	r       TSSPFile
	meta    *ChunkMeta
	segPos  int
	fragPos int // Indicates the sequence number of a fragment range.
	fragRgs []*fragment.FragmentRange
}

func NewLocation(r TSSPFile, ctx *ReadContext) *Location {
	return &Location{
		r:       r,
		ctx:     ctx,
		meta:    nil,
		segPos:  0,
		fragPos: 0,
		fragRgs: nil,
	}
}

func NewLocationCursor(n int) *LocationCursor {
	return &LocationCursor{
		pos: 0,
		lcs: make([]*Location, 0, n),
	}
}

func (l *Location) SetFragmentRanges(frs []*fragment.FragmentRange) {
	if len(frs) == 0 {
		return
	}
	l.fragRgs = frs
	// suppose frs is ascending
	if l.ctx.Ascending {
		l.fragPos = 0
		l.segPos = int(frs[0].Start)
		return
	}
	// frs is descending
	l.fragPos = len(l.fragRgs) - 1
	l.segPos = int(l.fragRgs[l.fragPos].End - 1)
}

func (l *Location) readChunkMeta(id uint64, tr util.TimeRange, buffer *[]byte) error {
	idx, m, err := l.r.MetaIndex(id, tr)
	if err != nil {
		return err
	}

	if m == nil {
		return nil
	}

	meta, err := l.r.ChunkMeta(id, m.offset, m.size, m.count, idx, l.meta, buffer, fileops.IO_PRIORITY_ULTRA_HIGH)
	if err != nil {
		return err
	}

	if meta == nil {
		return nil
	}

	if !tr.Overlaps(meta.MinMaxTime()) {
		return nil
	}

	l.meta = meta
	// init a new FragmentRange as [0, meta.segCount) if not SetFragmentRanges.
	if len(l.fragRgs) == 0 {
		if cap(l.fragRgs) <= 0 {
			l.fragRgs = []*fragment.FragmentRange{{Start: 0, End: meta.segCount}}
		} else {
			l.fragRgs = l.fragRgs[:1]
			l.fragRgs[0].Start, l.fragRgs[0].End = 0, meta.segCount
		}
		l.fragPos = 0
	}
	if !l.ctx.Ascending {
		l.fragPos = len(l.fragRgs) - 1
		l.segPos = int(l.fragRgs[l.fragPos].End - 1)
	}

	return nil
}

func (l *Location) GetChunkMeta() *ChunkMeta {
	return l.meta
}

func (l *Location) hasNext() bool {
	if l.meta == nil {
		return false
	}

	if l.ctx.Ascending {
		return l.segPos < int(l.fragRgs[len(l.fragRgs)-1].End)
	}
	return l.segPos >= int(l.fragRgs[0].Start)

}

func (l *Location) AscendingDone() {
	l.segPos = int(l.fragRgs[len(l.fragRgs)-1].End)
}

func (l *Location) DescendingDone() {
	l.segPos = int(l.fragRgs[0].Start) - 1
}

func (l *Location) Contains(sid uint64, tr util.TimeRange, buffer *[]byte) (bool, error) {
	// use bloom filter and file time range to filter generally
	contains, err := l.r.ContainsValue(sid, tr)
	if err != nil {
		return false, err
	}
	if !contains {
		return false, nil
	}

	// read file meta to judge whether file has data, chunk meta will also init
	err = l.readChunkMeta(sid, tr, buffer)
	if err != nil {
		return false, err
	}

	if l.meta == nil {
		return false, nil
	}

	if l.ctx.Ascending {
		return l.segPos < int(l.fragRgs[len(l.fragRgs)-1].End), nil
	}
	return l.segPos >= int(l.fragRgs[0].Start), nil

}

func (l *Location) isPreAggRead() bool {
	return len(l.ctx.ops) > 0
}

func (l *Location) nextSegment(toLast bool) {
	if l.ctx.Ascending {
		if toLast {
			l.AscendingDone()
		} else {
			if (l.fragPos == len(l.fragRgs)-1 && int(l.fragRgs[l.fragPos].Start) <= l.segPos && l.segPos < int(l.fragRgs[l.fragPos].End)) ||
				(l.fragPos < len(l.fragRgs)-1 && int(l.fragRgs[l.fragPos].Start) <= l.segPos && l.segPos < int(l.fragRgs[l.fragPos].End)-1) {
				l.segPos++
			} else {
				l.fragPos++
				l.segPos = int(l.fragRgs[l.fragPos].Start)
			}
		}
	} else {
		if toLast {
			l.DescendingDone()
		} else {
			if (l.fragPos == 0 && int(l.fragRgs[l.fragPos].Start) <= l.segPos && l.segPos < int(l.fragRgs[l.fragPos].End)) ||
				(l.fragPos > 0 && int(l.fragRgs[l.fragPos].Start) < l.segPos && l.segPos < int(l.fragRgs[l.fragPos].End)) {
				l.segPos--
			} else {
				l.fragPos--
				l.segPos = int(l.fragRgs[l.fragPos].End - 1)
			}
		}
	}
}

func (l *Location) getCurSegMinMax() (int64, int64) {
	minMaxSeg := l.meta.timeRange[l.segPos]
	min, max := minMaxSeg.minTime(), minMaxSeg.maxTime()
	return min, max
}

func (l *Location) overlapsForRowFilter(rowFilters *[]clv.RowFilter) bool {
	if rowFilters == nil || len(*rowFilters) == 0 {
		return true
	}

	min, max := l.getCurSegMinMax()
	if (max < (*rowFilters)[0].RowId) ||
		(min > (*rowFilters)[len(*rowFilters)-1].RowId) {
		return false
	}

	return true
}

func (l *Location) ReadData(filterOpts *FilterOptions, dst *record.Record) (*record.Record, error) {
	rec, _, err := l.readData(filterOpts, dst, nil, nil)
	return rec, err
}

func (l *Location) readData(filterOpts *FilterOptions, dst, filterRec *record.Record, filterBitmap *bitmap.FilterBitmap) (*record.Record, int, error) {
	var rec *record.Record
	var err error
	var oriRowCount int

	if !l.ctx.tr.Overlaps(l.meta.MinMaxTime()) {
		l.nextSegment(true)
		return nil, 0, nil
	}

	for rec == nil && l.hasNext() {
		if (!l.ctx.tr.Overlaps(l.getCurSegMinMax())) ||
			(!l.overlapsForRowFilter(filterOpts.rowFilters)) {
			l.nextSegment(false)
			continue
		}

		tracing.StartPP(l.ctx.readSpan)
		rec, err = l.r.ReadAt(l.meta, l.segPos, dst, l.ctx, fileops.IO_PRIORITY_ULTRA_HIGH)
		if err != nil {
			return nil, 0, err
		}
		l.nextSegment(false)

		if l.isPreAggRead() {
			return rec, 0, nil
		}
		tracing.EndPP(l.ctx.readSpan)

		tracing.SpanElapsed(l.ctx.filterSpan, func() {
			if rec != nil {
				oriRowCount += rec.RowNums()
				if l.ctx.Ascending {
					rec = FilterByTime(rec, l.ctx.tr)
				} else {
					rec = FilterByTimeDescend(rec, l.ctx.tr)
				}
			}

			// filter by field
			if rec != nil {
				rec = FilterByField(rec, filterRec, filterOpts.options, filterOpts.cond, filterOpts.rowFilters, filterOpts.pointTags, filterBitmap)
			}
		})
	}

	return rec, oriRowCount, nil
}

func (l *Location) readMeta(filterOpts *FilterOptions, dst *record.Record, filterBitmap *bitmap.FilterBitmap) (*record.Record, error) {
	if l.ctx.preAggBuilders == nil {
		l.ctx.preAggBuilders = newPreAggBuilders()
	}

	rec, _, err := l.readData(filterOpts, dst, nil, filterBitmap)
	return rec, err
}

func (l *Location) ResetMeta() {
	l.segPos = 0
	l.meta = nil
	l.fragPos = 0
	if len(l.fragRgs) > 0 {
		l.fragRgs = l.fragRgs[:0]
	}
}
