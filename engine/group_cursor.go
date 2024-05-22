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

package engine

import (
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type groupCursor struct {
	preAgg        bool
	lazyInit      bool
	pos           int
	id            int
	ctx           *idKeyCursorContext
	span          *tracing.Span
	querySchema   *executor.QuerySchema
	recordPool    *record.CircularRecordPool
	name          string
	closeOnce     sync.Once
	tagSetCursors comm.KeyCursors
	seriesTagFunc func(sinfo comm.SeriesInfoIntf, pt *influx.PointTags, tmpSeriesKey []byte) ([]byte, error)
	limitBound    int64
	rowCount      int64
}

func (c *groupCursor) SetOps(ops []*comm.CallOption) {
	if len(ops) > 0 {
		c.preAgg = true
	}
	c.tagSetCursors.SetOps(ops)
}

func (c *groupCursor) SinkPlan(plan hybridqp.QueryNode) {
	c.tagSetCursors.SinkPlan(plan)
}

func (c *groupCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.preAgg || hasMultipleColumnsWithFirst(c.querySchema) {
		return c.next()
	}
	return c.nextWithReuse()
}

func (c *groupCursor) CloseSubCursor(pos int) error {
	if c.lazyInit {
		return nil
	}
	if err := c.tagSetCursors[pos].Close(); err != nil {
		log.Error("close tagSet cursor failed, ", zap.Error(err))
		return err
	}
	return nil
}

func (c *groupCursor) nextWithReuse() (*record.Record, comm.SeriesInfoIntf, error) {
	if c.recordPool == nil {
		c.recordPool = record.NewCircularRecordPool(c.ctx.aggPool, groupCursorRecordNum, c.GetSchema(), true)
	}

	re := c.recordPool.Get()
	var sameTag bool
	currTags := &influx.PointTags{}
	var tmpSeriesKey []byte
	var e error
	for {
		if abortNow := c.limitBound > 0 && c.rowCount >= c.limitBound; c.pos >= len(c.tagSetCursors) || abortNow {
			if abortNow && c.pos < len(c.tagSetCursors) {
				c.pos++
				if err := c.CloseSubCursor(c.pos - 1); err != nil {
					return nil, nil, err
				}
			}
			if re.RowNums() == 0 {
				return nil, nil, nil
			}
			return re, nil, nil
		}

		rec, info, err := c.tagSetCursors[c.pos].Next()
		if err != nil {
			return nil, nil, err
		}
		if rec == nil {
			// This variable must be incremented by 1 to avoid repeated close
			c.pos++
			if err := c.CloseSubCursor(c.pos - 1); err != nil {
				return nil, nil, err
			}
			sameTag = false
			continue
		}
		c.rowCount += int64(rec.RowNums())
		if !sameTag {
			tmpSeriesKey, e = c.seriesTagFunc(info, currTags, tmpSeriesKey)
			if e != nil {
				return nil, nil, e
			}
			var tag []byte
			if c.querySchema.Options().IsPromGroupAllOrWithout() {
				tag = executor.NewChunkTagsWithoutDims(*currTags, c.querySchema.Options().GetOptDimension()).GetTag()
			} else {
				tag = executor.NewChunkTags(*currTags, c.querySchema.Options().GetOptDimension()).GetTag()
			}
			re.AddTagIndexAndKey(&tag, re.RowNums())
			sameTag = true
		}
		re.AppendRec(rec, 0, rec.RowNums())
		if re.RowNums() >= c.querySchema.Options().ChunkSizeNum() {
			return re, nil, nil
		}
	}
}

func (c *groupCursor) getSeriesTags(sinfo comm.SeriesInfoIntf, pt *influx.PointTags, tmpSeriesKey []byte) ([]byte, error) {
	*pt = *sinfo.GetSeriesTags()
	return nil, nil
}

// next preAgg or ops will use colmeta in record, can not merge data to, just return record
func (c *groupCursor) next() (*record.Record, comm.SeriesInfoIntf, error) {
	for {
		if c.pos >= len(c.tagSetCursors) {
			return nil, nil, nil
		}
		rec, info, err := c.tagSetCursors[c.pos].Next()
		if err != nil {
			return nil, nil, err
		}
		if rec != nil {
			return rec, info, nil
		}

		c.pos++
		if err := c.CloseSubCursor(c.pos - 1); err != nil {
			return nil, nil, err
		}
	}
}

func (c *groupCursor) GetSchema() record.Schemas {
	if len(c.tagSetCursors) > 0 {
		return c.tagSetCursors[0].GetSchema()
	}
	return nil
}

func (c *groupCursor) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.ctx.decs.Release()
		if (executor.GetEnableFileCursor() && c.querySchema.HasOptimizeAgg()) || c.lazyInit {
			c.ctx.UnRef()
		}
		startPos := c.pos
		if c.lazyInit {
			startPos = 0
		}
		// some cursors may have been closed during iterate, so we start from c.pos
		for i := startPos; i < len(c.tagSetCursors); i++ {
			itr := c.tagSetCursors[i]
			if itr != nil {
				err = itr.Close()
			}
		}
	})
	if c.recordPool != nil {
		c.recordPool.Put()
		c.recordPool = nil
	}

	return err
}

func (c *groupCursor) Name() string {
	return c.name
}

func (c *groupCursor) StartSpan(span *tracing.Span) {
	if span != nil {
		if c.lazyInit {
			c.span = span.StartSpan(createTagSetCursorDuration)
		} else {
			c.span = span.StartSpan(fmt.Sprintf("group_cursor_%d", c.id))
		}
		c.span.CreateCounter(unorderRowCount, "")
		c.span.CreateCounter(unorderDuration, "ns")
		enableFileCursor := executor.GetEnableFileCursor() && c.querySchema.HasOptimizeAgg()
		if enableFileCursor {
			c.span.CreateCounter(fileCursorDurationSpan, "ns")
		} else {
			c.span.CreateCounter(tsmIterCount, "")
			c.span.CreateCounter(tsmIterDuration, "ns")
		}
		for _, cursor := range c.tagSetCursors {
			cursor.StartSpan(c.span)
		}
	}
}

func (c *groupCursor) EndSpan() {
	if c.span != nil {
		c.span.Finish()
	}
}

func (c *groupCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}
