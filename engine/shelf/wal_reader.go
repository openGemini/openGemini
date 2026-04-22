// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package shelf

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type WalCtx struct {
	buf     pool.Buffer
	header  WalBlockHeader
	rec     record.Record
	offsets []int64
}

func (ctx *WalCtx) MemSize() int {
	return cap(ctx.buf.B) + cap(ctx.buf.Swap)
}

func (ctx *WalCtx) Instance() *WalCtx {
	return ctx
}

var walCtxPool *pool.UnionPool[WalCtx]

func initWalCtxPool() {
	walCtxPool = pool.NewDefaultUnionPool(func() *WalCtx {
		return &WalCtx{}
	})
}

func NewWalCtx() (*WalCtx, func()) {
	ctx := walCtxPool.Get()
	return ctx, func() {
		walCtxPool.Put(ctx)
	}
}

func (wal *Wal) ReadRecord(ctx *WalCtx, sid uint64, dst *record.Record, keepSchema bool) error {
	ctx.offsets = wal.seriesOffsets.Get(sid, ctx.offsets[:0])

	if len(ctx.offsets) == 0 {
		return nil
	}

	if keepSchema {
		return wal.readRecordWithSchema(ctx, ctx.offsets, dst)
	}

	dst.Reset()
	return wal.readRecord(ctx, ctx.offsets, func(rec *record.Record) error {
		if dst.Len() == 0 {
			dst.Schema = append(dst.Schema[:0], rec.Schema...)
			dst.ReserveColVal(rec.Len())
			dst.AppendRec(rec, 0, rec.RowNums())
		} else {
			dst.Merge(rec)
		}
		return nil
	})
}

func (wal *Wal) readRecordWithSchema(ctx *WalCtx, offsets []int64, dst *record.Record) error {
	return wal.readRecord(ctx, offsets, func(rec *record.Record) error {
		times := rec.Times()
		if len(times) == 0 {
			return nil
		}

		err := util.FindIntersectionIndex(rec.Schema[:rec.Len()-1], dst.Schema[:dst.Len()-1],
			func(f1 record.Field, f2 record.Field) int {
				return strings.Compare(f1.Name, f2.Name)
			},
			func(i, j int) error {
				if rec.Schema[i].Type == dst.Schema[j].Type {
					dst.ColVals[j].AppendColVal(&rec.ColVals[i], rec.Schema[i].Type, 0, len(times))
				}
				return nil
			})

		dst.AppendTime(times...)
		dst.TryPadColumn()
		return err
	})
}

func (wal *Wal) readRecord(ctx *WalCtx, offsets []int64, callback func(rec *record.Record) error) error {
	decoder := &WalRecordDecoder{}
	rec := &ctx.rec
	for _, offset := range offsets {
		buf, err := wal.ReadBlock(ctx, offset)
		if err != nil {
			return err
		}

		rec.Reset()
		err = decoder.Decode(rec, buf)
		if err != nil {
			return err
		}

		err = callback(rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wal *Wal) ReadBlock(ctx *WalCtx, ofs int64) ([]byte, error) {
	header := &ctx.header
	var err error

	buf := &ctx.buf
	buf.B = bufferpool.Resize(buf.B, walBlockHeaderSize)

	buf.B, err = wal.readAt(buf.B, ofs)
	if err != nil {
		return nil, err
	}

	err = header.Unmarshal(buf.B)
	if err != nil {
		return nil, err
	}
	if header.size > maxWalBlockSize {
		return nil, fmt.Errorf("too big block size: %d > %d", header.size, maxWalBlockSize)
	}

	size := int(header.size)
	ofs += int64(walBlockHeaderSize) + int64(header.mstLen) // skip header and measurement name

	buf.B = slices.Grow(buf.B[:0], size)[:size]
	buf.B, err = wal.readAt(buf.B, ofs)
	if err != nil {
		return nil, err
	}

	switch header.compressFlag {
	case walCompressLz4:
		buf.Swap, err = LZ4DecompressBlock(buf.B, buf.Swap)
		return buf.Swap, err
	case walCompressSnappy:
		buf.Swap, err = SnappyDecompressBlock(buf.B, buf.Swap)
		return buf.Swap, err
	default:
		break
	}

	return buf.B, nil
}

func (wal *Wal) readAt(dst []byte, ofs int64) ([]byte, error) {
	n, err := wal.file.ReadAt(dst, ofs)
	if err != nil {
		return nil, err
	}
	if n != len(dst) {
		return nil, errno.NewError(errno.ShortRead, n, len(dst))
	}

	return dst, nil
}

type WalRecordIterator struct {
	sids []uint64
	wal  *Wal
	ctx  WalCtx
}

func NewWalRecordIterator(wal *Wal) *WalRecordIterator {
	return &WalRecordIterator{
		wal: wal,
	}
}

func (itr *WalRecordIterator) WalMeasurements(fn func(mst string)) {
	itr.wal.seriesMap.Walk(func(mst string, series map[uint64]struct{}) {
		itr.sids = itr.sids[:0]
		for id := range series {
			itr.sids = append(itr.sids, id)
		}
		sort.Slice(itr.sids, func(i, j int) bool { return itr.sids[i] > itr.sids[j] })

		fn(mst)
	})
}

func (itr *WalRecordIterator) Next(dst *record.Record) (uint64, error) {
	idx := len(itr.sids) - 1
	if idx < 0 {
		return 0, io.EOF
	}
	sid := itr.sids[idx]
	itr.sids = itr.sids[:idx]

	err := itr.wal.ReadRecord(&itr.ctx, sid, dst, false)
	return sid, err
}
