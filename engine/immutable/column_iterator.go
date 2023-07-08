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
	"errors"
	"sync"

	"github.com/openGemini/openGemini/lib/record"
)

var errClosed = errors.New("column iterator closed")

type ColumnIteratorPerformer interface {
	Handle(col *record.ColVal, times []int64, lastSeg bool) error
	HasSeries(uint64) bool
	ColumnChanged(*record.Field) error
	SeriesChanged(uint64, []int64) error
	WriteOriginal(fi *FileIterator) error
	Finish() error
}

type segWalkHandler func(col *record.ColVal, lastSeg bool) error

type ColumnIterator struct {
	fi *FileIterator
	sr *SegmentReader

	col   *record.ColVal
	times []int64

	mu     sync.RWMutex
	closed bool
	signal chan struct{}
	field  record.Field
}

func NewColumnIterator(fi *FileIterator) *ColumnIterator {
	return &ColumnIterator{
		fi:     fi,
		sr:     NewSegmentReader(fi),
		signal: make(chan struct{}),
	}
}

func (itr *ColumnIterator) initTimeColumn() error {
	itr.times = itr.times[:0]

	colIdx := len(itr.fi.curtChunkMeta.colMeta) - 1
	return itr.walkSegment(&timeField, colIdx, func(col *record.ColVal, lastSeg bool) error {
		itr.times = append(itr.times, col.IntegerValues()...)
		return nil
	})
}

func (itr *ColumnIterator) NextChunkMeta() bool {
	itr.fi.curtChunkMeta = nil
	return itr.fi.NextChunkMeta()
}

func (itr *ColumnIterator) IncrChunkUsed() {
	itr.fi.chunkUsed++
}

func (itr *ColumnIterator) Error() error {
	return itr.fi.err
}

func (itr *ColumnIterator) NextColumn(colIdx int) (*record.Field, bool) {
	// exclude time column
	if colIdx >= len(itr.fi.curtChunkMeta.colMeta)-1 {
		return nil, false
	}

	itr.field.Name = itr.fi.curtChunkMeta.colMeta[colIdx].name
	itr.field.Type = int(itr.fi.curtChunkMeta.colMeta[colIdx].ty)

	return &itr.field, true
}

func (itr *ColumnIterator) Run(p ColumnIteratorPerformer) error {
	defer itr.Close()

	var sid uint64
	for {
		if itr.isClosed() {
			return errClosed
		}

		if !itr.NextChunkMeta() {
			if itr.Error() != nil {
				return itr.Error()
			}

			return p.Finish()
		}
		itr.IncrChunkUsed()

		sid = itr.fi.curtChunkMeta.sid
		if !p.HasSeries(sid) {
			if err := p.SeriesChanged(sid, nil); err != nil {
				return err
			}

			if err := p.WriteOriginal(itr.fi); err != nil {
				return err
			}
			continue
		}

		if err := itr.initTimeColumn(); err != nil {
			return err
		}

		if err := p.SeriesChanged(sid, itr.times); err != nil {
			return err
		}

		if err := itr.walkColumn(p); err != nil {
			return err
		}
	}
}

func (itr *ColumnIterator) walkColumn(p ColumnIteratorPerformer) error {
	colIdx, timeIdx := 0, 0

	handle := func(col *record.ColVal, lastSeg bool) error {
		begin := timeIdx
		timeIdx += col.Len

		return p.Handle(col, itr.times[begin:timeIdx], lastSeg)
	}

	for {
		if itr.isClosed() {
			return errClosed
		}

		ref, ok := itr.NextColumn(colIdx)
		if !ok {
			return nil
		}

		if err := p.ColumnChanged(ref); err != nil {
			return err
		}

		timeIdx = 0
		if err := itr.walkSegment(ref, colIdx, handle); err != nil {
			return err
		}

		colIdx++
	}
}

func (itr *ColumnIterator) walkSegment(ref *record.Field, colIdx int, handle segWalkHandler) error {
	segIdx := 0

	for {
		if itr.isClosed() {
			return errClosed
		}

		col, err := itr.read(colIdx, segIdx, ref)
		if err != nil {
			return err
		}

		lastSeg := segIdx >= itr.fi.curtChunkMeta.segmentCount()-1
		if err := handle(col, lastSeg); err != nil {
			return err
		}

		if lastSeg {
			return nil
		}

		segIdx++
	}
}

func (itr *ColumnIterator) PutCol(col *record.ColVal) {
	itr.col = col
}

func (itr *ColumnIterator) isClosed() bool {
	return itr.closed
}

func (itr *ColumnIterator) Close() {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.closed {
		return
	}
	itr.closed = true
	close(itr.signal)
}

func (itr *ColumnIterator) resetCol() {
	if itr.col == nil {
		itr.col = &record.ColVal{}
	}
	itr.col.Init()
}

func (itr *ColumnIterator) read(colIdx, segIdx int, ref *record.Field) (*record.ColVal, error) {
	itr.resetCol()
	meta := itr.fi.curtChunkMeta
	seg := meta.colMeta[colIdx]

	err := itr.sr.Read(seg.entries[segIdx], ref, itr.col)
	if err != nil {
		return nil, err
	}

	return itr.col, nil
}
