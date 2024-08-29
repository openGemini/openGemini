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
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
)

type ReadContext struct {
	coderCtx       *encoding.CoderContext
	preAggBuilders *PreAggBuilders
	decBuf         []byte
	offset         []uint32
	col            record.ColVal

	ops             []*comm.CallOption
	tr              util.TimeRange
	Ascending       bool
	onlyFirstOrLast bool
	origData        []byte

	readBuf []byte

	readSpan     *tracing.Span
	filterSpan   *tracing.Span
	closedSignal *bool
}

func NewReadContext(ascending bool) *ReadContext {
	var readBuf []byte
	if !fileops.MmapEn {
		readBuf = bufferpool.Get()
	}
	b := bufferpool.Get()
	return &ReadContext{
		coderCtx:       encoding.NewCoderContext(),
		preAggBuilders: newPreAggBuilders(),
		decBuf:         b[:0],
		Ascending:      ascending,
		tr:             record.MinMaxTimeRange,
		readBuf:        readBuf,
	}
}

func (d *ReadContext) SetClosedSignal(s *bool) {
	d.closedSignal = s
}

func (d *ReadContext) IsAborted() bool {
	return d.closedSignal != nil && *d.closedSignal
}

func (d *ReadContext) SetSpan(readSpan, filterSpan *tracing.Span) {
	d.readSpan = readSpan
	d.filterSpan = filterSpan
}

func (d *ReadContext) GetOps() []*comm.CallOption {
	return d.ops
}

func (d *ReadContext) SetOps(c []*comm.CallOption) {
	d.ops = c
}

func (d *ReadContext) MatchPreAgg() bool {
	return len(d.ops) > 0
}

func (d *ReadContext) Set(ascending bool, tr util.TimeRange, onlyFirstOrLast bool, ops []*comm.CallOption) {
	d.Ascending = ascending
	d.tr = tr
	d.onlyFirstOrLast = onlyFirstOrLast
	d.ops = ops
}

func (d *ReadContext) Reset() {}

func (d *ReadContext) Release() {
	if d.coderCtx != nil {
		d.coderCtx.Release()
		d.coderCtx = nil
	}

	if d.decBuf != nil {
		d.decBuf = d.decBuf[:0]
		bufferpool.Put(d.decBuf)
		d.decBuf = nil
	}

	if d.preAggBuilders != nil {
		d.preAggBuilders.Release()
		d.preAggBuilders = nil
	}

	if d.readBuf != nil {
		bufferpool.Put(d.readBuf)
		d.readBuf = nil
	}
}

func (d *ReadContext) SetTr(tr util.TimeRange) {
	d.tr = tr
}

func (d *ReadContext) InitPreAggBuilder() {
	d.preAggBuilders = newPreAggBuilders()
}

func (d *ReadContext) GetReadBuff() []byte {
	return d.readBuf
}

func (d *ReadContext) GetCoder() *encoding.CoderContext {
	return d.coderCtx
}
