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
	"sort"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
)

type WalCtx struct {
	buf    pool.Buffer
	header WalBlockHeader
	rec    record.Record
}

func (ctx *WalCtx) MemSize() int {
	return cap(ctx.buf.B) + cap(ctx.buf.Swap)
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

func (wal *Wal) LoadIntoMemory() {
	wal.file.LoadIntoMemory()
}

func (wal *Wal) ReadRecord(ctx *WalCtx, sid uint64, dst *record.Record) error {
	ofs := wal.seriesOffsets.Get(sid)

	if len(ofs) == 0 {
		return nil
	}

	return wal.readRecord(ctx, ofs, dst)
}

func (wal *Wal) readRecord(ctx *WalCtx, offsets []int64, dst *record.Record) error {
	rec := &ctx.rec
	for i, offset := range offsets {
		buf, err := wal.ReadBlock(ctx, offset)
		if err != nil {
			return err
		}

		rec.ResetDeep()
		err = wal.codec.Decode(rec, buf)
		if err != nil {
			return err
		}

		if i == 0 {
			dst.Schema = append(dst.Schema[:0], rec.Schema...)
			dst.ReserveColVal(rec.Len())
			dst.AppendRec(rec, 0, rec.RowNums())
		} else {
			dst.Merge(rec)
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

	buf.B = bufferpool.Resize(buf.B, int(header.size))
	buf.B, err = wal.readAt(buf.B[:header.size], ofs+int64(walBlockHeaderSize))
	if err != nil {
		return nil, err
	}

	switch header.flag {
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

func (itr *WalRecordIterator) Init(wal *Wal) {
	wal.LoadIntoMemory()
	itr.wal = wal
	itr.sids = wal.GetAllSid(itr.sids[:0])

	// reverse order
	sort.Slice(itr.sids, func(i, j int) bool { return itr.sids[i] > itr.sids[j] })
}

func (itr *WalRecordIterator) Next(dst *record.Record) (uint64, error) {
	idx := len(itr.sids) - 1
	if idx < 0 {
		return 0, io.EOF
	}
	sid := itr.sids[idx]
	itr.sids = itr.sids[:idx]

	err := itr.wal.ReadRecord(&itr.ctx, sid, dst)
	return sid, err
}

type MemWalReader struct {
	data []byte
	size int64
}

func (r *MemWalReader) Load(reader io.ReaderAt, size int) error {
	if cap(r.data) < size {
		r.data = make([]byte, size)
	}

	n, err := reader.ReadAt(r.data[:size], 0)
	if n != size {
		return errno.NewError(errno.ShortRead, n, size)
	}
	r.size = int64(size)
	return err
}

func (r *MemWalReader) ReadAt(dst []byte, ofs int64) (int, error) {
	n := len(dst)
	end := ofs + int64(n)
	if end > r.size {
		return 0, io.ErrUnexpectedEOF
	}
	copy(dst, r.data[ofs:end])
	return n, nil
}
