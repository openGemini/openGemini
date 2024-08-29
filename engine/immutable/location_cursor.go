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

package immutable

import (
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/record"
)

type LocationCursor struct {
	rowNum        int
	pos           int
	lcs           []*Location
	filterRecPool *record.CircularRecordPool
}

func (l *LocationCursor) AddFilterRecPool(pool *record.CircularRecordPool) {
	l.filterRecPool = pool
}

func (l *LocationCursor) FragmentCount() int {
	var count int
	for i := range l.lcs {
		for _, rg := range l.lcs[i].fragRgs {
			count += int(rg.End - rg.Start)
		}
	}
	return count
}

func (l *LocationCursor) RowCount() int {
	return l.rowNum
}

func (l *LocationCursor) Reset() {
	l.Unref()
	l.rowNum = 0
	l.pos = 0
	l.lcs = l.lcs[:0]
}

func (l *LocationCursor) AddLocation(loc *Location) {
	l.lcs = append(l.lcs, loc)
}

func (l *LocationCursor) Len() int {
	return len(l.lcs)
}

func (l *LocationCursor) Swap(i, j int) {
	l.lcs[i], l.lcs[j] = l.lcs[j], l.lcs[i]
}

func (l *LocationCursor) Less(i, j int) bool {
	if l.lcs[i].r.IsOrder() {
		ti, _ := l.lcs[i].meta.MinMaxTime()
		tj, _ := l.lcs[j].meta.MinMaxTime()
		return ti < tj
	}
	_, seqI := l.lcs[i].r.LevelAndSequence()
	_, seqJ := l.lcs[j].r.LevelAndSequence()
	return seqI < seqJ
}

func (l *LocationCursor) Reverse() {
	left, right := 0, len(l.lcs)-1
	for left < right {
		// we can't reverse segment meta because it's just a references, should not change it
		l.lcs[left], l.lcs[right] = l.lcs[right], l.lcs[left]
		left++
		right--
	}
}

func (l *LocationCursor) Close() {
	l.rowNum = 0
	l.pos = 0
	l.lcs = l.lcs[:0]
	if l.filterRecPool != nil {
		l.filterRecPool.Put()
	}
}

func (l *LocationCursor) AddRef() {
	for i := range l.lcs {
		l.lcs[i].r.Ref()
	}
}

func (l *LocationCursor) Unref() {
	if fileQueryCache != nil && len(l.lcs) <= int(fileQueryCache.GetCap()) {
		for i := range l.lcs {
			fileQueryCache.Put(l.lcs[i].r)
			l.lcs[i].r.Unref()
		}
	} else {
		for i := range l.lcs {
			l.lcs[i].r.UnrefFileReader()
			l.lcs[i].r.Unref()
		}
	}
}

func (l *LocationCursor) ReadMeta(filterOpts *FilterOptions, dst *record.Record, filterBitmap *bitmap.FilterBitmap) (*record.Record, error) {
	var err error
	var rec *record.Record
	var readCxt = l.lcs[0].ctx

	if readCxt.onlyFirstOrLast && l.lcs[0].ctx.Ascending && l.lcs[0].ctx.ops[0].Call.Name == "last" {
		// tmp code for ascending locations. Delete when support descending locations.
		for {
			pos := len(l.lcs) - l.pos - 1
			if pos < 0 {
				return nil, nil
			}
			loc := l.lcs[pos]

			for loc.hasNext() {
				var tmpRec *record.Record
				tmpRec, err = loc.readMeta(filterOpts, dst, filterBitmap)
				if err != nil {
					return nil, err
				}
				if tmpRec != nil {
					rec = tmpRec
				}
			}
			l.pos++

			if rec != nil {
				if readCxt.onlyFirstOrLast {
					l.pos = len(l.lcs)
				}
				return rec, nil
			}
		}
	}

	for {
		if l.pos >= len(l.lcs) {
			return nil, nil
		}
		loc := l.lcs[l.pos]
		if !loc.hasNext() {
			l.pos++
			continue
		}
		rec, err = loc.readMeta(filterOpts, dst, filterBitmap)
		if err != nil {
			return nil, err
		}
		l.pos++

		if readCxt.onlyFirstOrLast {
			l.pos = len(l.lcs)
		}
		return rec, nil
	}
}

func (l *LocationCursor) ReadOutOfOrderMeta(filterOpts *FilterOptions, dst *record.Record) (*record.Record, error) {
	var err error
	var rec *record.Record

	for {
		var midRec *record.Record
		if l.pos >= len(l.lcs) {
			break
		}
		loc := l.lcs[l.pos]
		if !loc.hasNext() {
			l.pos++
			continue
		}
		midRec, err = loc.readMeta(filterOpts, dst, nil)
		if err != nil {
			return nil, err
		}
		if midRec == nil {
			break
		}
		l.pos++
		if rec == nil {
			rec = midRec.Copy(true, nil, midRec.Schema)
			continue
		}
		AggregateData(rec, midRec, loc.ctx.ops)
	}
	return rec, nil
}

func (l *LocationCursor) ReadData(filterOpts *FilterOptions, dst *record.Record, filterBitmap *bitmap.FilterBitmap,
	unnestOperator UnnestOperator) (*record.Record, error) {
	if len(l.lcs) == 0 {
		return nil, nil
	}

	var err error
	var rowNum int
	var rec *record.Record
	var filterRec *record.Record
	var readCtx = l.lcs[0].ctx

	if len(readCtx.ops) > 0 {
		return l.ReadMeta(filterOpts, dst, filterBitmap)
	}

	if l.filterRecPool != nil {
		filterRec = l.filterRecPool.Get()
	}
	for {
		if l.pos >= len(l.lcs) {
			return nil, nil
		}
		loc := l.lcs[l.pos]
		if !loc.hasNext() {
			l.pos++
			continue
		}
		if loc.ctx.IsAborted() {
			return nil, nil
		}
		rec, rowNum, err = loc.readData(filterOpts, dst, filterRec, filterBitmap, unnestOperator)
		if err != nil {
			return nil, err
		}
		l.rowNum += rowNum

		if rec != nil {
			return rec, nil
		}
		dst.Reuse()
		if filterRec != nil {
			filterRec.Reuse()
		}
		l.pos++
	}
}
