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
	"container/heap"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"go.uber.org/zap"
)

type ChunkIterator struct {
	*FileIterator
	ctx    *ReadContext
	id     uint64
	fields record.Schemas
	rec    *record.Record
	merge  *record.Record
	log    *Log.Logger
}

type ChunkIterators struct {
	closed       chan struct{}
	dropping     *int64
	name         string // measurement name with version
	itrs         []*ChunkIterator
	id           uint64
	merged       *record.Record
	estimateSize int
	maxN         int

	log *Log.Logger
}

func (c *ChunkIterators) WithLog(log *Log.Logger) {
	c.log = log
	for i := range c.itrs {
		c.itrs[i].WithLog(log)
	}
}

func (c *ChunkIterators) Len() int      { return len(c.itrs) }
func (c *ChunkIterators) Swap(i, j int) { c.itrs[i], c.itrs[j] = c.itrs[j], c.itrs[i] }
func (c *ChunkIterators) Less(i, j int) bool {
	iID := c.itrs[i].id
	jID := c.itrs[j].id
	if iID != jID {
		return iID < jID
	}

	return c.itrs[i].merge.MinTime(true) < c.itrs[j].merge.MinTime(true)
}

func (c *ChunkIterators) Push(v interface{}) {
	c.itrs = append(c.itrs, v.(*ChunkIterator))
}

func (c *ChunkIterators) Pop() interface{} {
	l := len(c.itrs)
	v := c.itrs[l-1]
	c.itrs = c.itrs[:l-1]
	return v
}

func (c *ChunkIterators) Close() {
	for _, itr := range c.itrs {
		itr.Close()
	}
}

func (c *ChunkIterators) stopCompact() bool {
	if atomic.LoadInt64(c.dropping) > 0 {
		return true
	}

	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *ChunkIterators) Next() (uint64, *record.Record, error) {
	defer func() { c.id = 0 }()

	if c.Len() == 0 {
		return 0, nil, nil
	}

	if c.stopCompact() {
		c.log.Error("compact stopped")
		return 0, nil, ErrCompStopped
	}

	itr, _ := heap.Pop(c).(*ChunkIterator)
	preId, rec := itr.id, itr.merge

	c.id = preId
	c.merged.Reset()
	c.merged.SetSchema(rec.Schema)
	c.merged.ReserveColVal(len(rec.Schema))
	c.merged.ReserveColumnRows(rec.RowNums())
	c.merged.Merge(rec)

	if !itr.Next() {
		itr.Close()
		if itr.err != nil {
			return 0, nil, itr.err
		}
	} else {
		heap.Push(c, itr)
	}

	for c.Len() > 0 {
		itr, _ = heap.Pop(c).(*ChunkIterator)
		if c.id == itr.id {
			c.merged.Merge(itr.merge)
			itr.id = 0
		} else {
			heap.Push(c, itr)
			return c.id, c.merged, nil
		}

		if c.stopCompact() {
			return 0, nil, ErrCompStopped
		}

		if !itr.Next() {
			itr.Close()
			if itr.err != nil {
				return 0, nil, itr.err
			}
			continue
		}

		heap.Push(c, itr)
	}

	return c.id, c.merged, nil
}

func NewChunkIterator(r *FileIterator) *ChunkIterator {
	itr := &ChunkIterator{
		FileIterator: r,
		ctx:          NewReadContext(true),
		merge:        allocRecord(),
		rec:          allocRecord(),
	}

	return itr
}

func (c *ChunkIterator) WithLog(log *Log.Logger) {
	c.log = log
}

func (c *ChunkIterator) Close() {
	c.FileIterator.Close()
	freeRecord(c.rec)
	freeRecord(c.merge)
	c.ctx.Release()
	c.ctx = nil
}

func (c *ChunkIterator) Next() bool {
	if c.err != nil {
		return false
	}

	if c.chunkUsed >= c.chunkN || c.mIndexPos > c.mIndexN {
		return false
	}

	if !c.NextChunkMeta() {
		return false
	}

	if cap(c.fields) < int(c.curtChunkMeta.columnCount) {
		delta := int(c.curtChunkMeta.columnCount) - cap(c.fields)
		c.fields = c.fields[:cap(c.fields)]
		c.fields = append(c.fields, make([]record.Field, delta)...)
	}
	c.fields = c.fields[:c.curtChunkMeta.columnCount]
	for i := range c.curtChunkMeta.colMeta {
		cm := c.curtChunkMeta.colMeta[i]
		c.fields[i].Name = cm.Name()
		c.fields[i].Type = int(cm.ty)
	}

	if c.err = c.read(); c.err != nil {
		return false
	}

	return true
}

func (c *ChunkIterator) read() error {
	var err error
	c.id = c.curtChunkMeta.sid
	cMeta := c.curtChunkMeta

	c.merge.Reset()
	c.merge.SetSchema(c.fields)
	c.merge.ReserveColVal(len(c.fields))
	timeMeta := cMeta.timeMeta()
	for i := range timeMeta.entries {
		c.rec.Reset()
		c.rec.SetSchema(c.fields)
		c.rec.ReserveColVal(len(c.fields))
		c.rec.ReserveColumnRows(8)
		record.CheckRecord(c.rec)

		c.rec, err = c.r.ReadAt(cMeta, i, c.rec, c.ctx, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			c.log.Error("read segment error", zap.String("file", c.r.Path()), zap.Error(err))
			return err
		}

		c.segPos++

		record.CheckRecord(c.rec)
		c.merge.Merge(c.rec)
		record.CheckRecord(c.merge)
	}

	if c.segPos >= len(timeMeta.entries) {
		c.curtChunkMeta = nil
		c.chunkUsed++
	}

	return nil
}

func (c *ChunkIterator) GetSeriesID() uint64 {
	return c.id
}

func (c *ChunkIterator) GetRecord() *record.Record {
	return c.merge
}
