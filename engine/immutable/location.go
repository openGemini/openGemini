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
	"github.com/openGemini/openGemini/lib/record"
)

type Location struct {
	ctx    *ReadContext
	r      TSSPFile
	meta   *ChunkMeta
	segPos int
}

func NewLocation(r TSSPFile, ctx *ReadContext) *Location {
	return &Location{r: r, ctx: ctx}
}

func NewLocationCursor(n int) *LocationCursor {
	return &LocationCursor{
		pos: 0,
		lcs: make([]*Location, 0, n),
	}
}

func (l *Location) readChunkMeta(id uint64, tr record.TimeRange, buffer *[]byte) error {
	idx, m, err := l.r.MetaIndex(id, tr)
	if err != nil {
		return err
	}

	if m == nil {
		return nil
	}

	meta, err := l.r.ChunkMeta(id, m.offset, m.size, m.count, idx, l.meta, buffer)
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
	if !l.ctx.Ascending {
		l.segPos = int(meta.segCount) - 1
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
		return l.segPos < int(l.meta.segCount)
	}
	return l.segPos >= 0
}

func (l *Location) ReadDone() {
	if l.ctx.Ascending {
		l.segPos = -1
	} else {
		l.segPos = int(l.meta.segCount)
	}
}

func (l *Location) Contains(sid uint64, tr record.TimeRange, buffer *[]byte) (bool, error) {
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
		return l.segPos < int(l.meta.segCount), nil
	}

	return l.segPos >= 0, nil
}

func (l *Location) isPreAggRead() bool {
	return len(l.ctx.ops) > 0
}

func (l *Location) nextSegment() {
	if l.ctx.Ascending {
		if l.isPreAggRead() {
			l.segPos = int(l.meta.segCount)
		} else {
			l.segPos++
		}
	} else {
		if l.isPreAggRead() {
			l.segPos = -1
		} else {
			l.segPos--
		}
	}
}

func (l *Location) getCurSegMinMax() (int64, int64) {
	minMaxSeg := l.meta.timeRange[l.segPos]
	min, max := minMaxSeg.minTime(), minMaxSeg.maxTime()
	return min, max
}

func (l *Location) overlapsForRowFilter(rowFilters []clv.RowFilter) bool {
	if len(rowFilters) == 0 {
		return true
	}

	min, max := l.getCurSegMinMax()
	if (max < rowFilters[0].RowId) ||
		(min > rowFilters[len(rowFilters)-1].RowId) {
		return false
	}

	return true
}

func (l *Location) ReadData(filterOpts *FilterOptions, dst *record.Record) (*record.Record, error) {
	return l.readData(filterOpts, dst)
}

func (l *Location) readData(filterOpts *FilterOptions, dst *record.Record) (*record.Record, error) {
	var rec *record.Record
	var err error
	for rec == nil && l.hasNext() {
		if (!l.ctx.tr.Overlaps(l.meta.MinMaxTime())) ||
			(!l.overlapsForRowFilter(filterOpts.rowFilters)) {
			l.nextSegment()
			continue
		}

		rec, err = l.r.ReadAt(l.meta, l.segPos, dst, l.ctx)
		if err != nil {
			return nil, err
		}
		l.nextSegment()

		if l.isPreAggRead() {
			return rec, nil
		}

		if rec != nil {
			if l.ctx.Ascending {
				rec = FilterByTime(rec, l.ctx.tr)
			} else {
				rec = FilterByTimeDescend(rec, l.ctx.tr)
			}
		}
		// filter by field
		if rec != nil {
			rec = FilterByfilterOpts(rec, filterOpts)
		}
	}

	return rec, nil
}

func (l *Location) readMeta(filterOpts *FilterOptions, dst *record.Record) (*record.Record, error) {
	if l.ctx.preAggBuilders == nil {
		l.ctx.preAggBuilders = newPreAggBuilders()
	}

	return l.readData(filterOpts, dst)
}

func (l *Location) ResetMeta() {
	l.segPos = 0
	l.meta = nil
}
