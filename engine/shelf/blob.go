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
	"encoding/binary"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type Blob struct {
	tm      time.Time
	err     error
	done    func()
	shardID uint64
	hash    uint64

	// | 4-byte blob size | series key | record |
	data []byte
	tr   util.TimeRange
}

func (b *Blob) ResetTime() {
	b.tm = time.Now()
}

func (b *Blob) TimeRange() *util.TimeRange {
	return &b.tr
}

func (b *Blob) MicroSince() int64 {
	return time.Since(b.tm).Microseconds()
}

func (b *Blob) WriteRecordRow(seriesKey []byte, rec *record.Record, rowIndex int) {
	tm := rec.Time(rowIndex)
	b.tr.Min = min(b.tr.Min, tm)
	b.tr.Max = max(b.tr.Max, tm)

	blobSizeOfs := len(b.data)
	b.data = codec.AppendUint32(b.data, 0)
	b.data = append(b.data, seriesKey...)
	b.data = EncodeRecordRow(b.data, rec, rowIndex)
	binary.BigEndian.PutUint32(b.data[blobSizeOfs:], uint32(len(b.data)-blobSizeOfs-util.Uint32SizeBytes))
}

func (b *Blob) Reset() {
	b.err = nil
	b.done = nil
	b.hash = 0
	b.shardID = 0
	b.data = b.data[:0]
	b.tr.Min = math.MaxInt64
	b.tr.Max = math.MinInt64
}

func (b *Blob) SetShardID(id uint64) {
	b.shardID = id
}

func (b *Blob) ShardID() uint64 {
	return b.shardID
}

func (b *Blob) Hash() uint64 {
	return b.hash
}

func (b *Blob) Done(err error) {
	b.err = err
	if b.done != nil {
		b.done()
	}
}

func (b *Blob) Error() error {
	return b.err
}

func (b *Blob) IsEmpty() bool {
	return len(b.data) == 0
}

func (b *Blob) Iterator() *BlobIterator {
	return NewBlobIterator(b.data)
}

type BlobIterator struct {
	dec codec.BinaryDecoder
}

func NewBlobIterator(data []byte) *BlobIterator {
	itr := &BlobIterator{}
	itr.dec.Reset(data)
	return itr
}

func (itr *BlobIterator) Next() ([]byte, []byte, error) {
	dec := &itr.dec
	if dec.RemainSize() <= 0 {
		return nil, nil, io.EOF
	}

	buf := dec.BytesNoCopy()
	seriesKeySize := binary.BigEndian.Uint32(buf)
	return buf[:seriesKeySize], buf[seriesKeySize:], nil
}

var blobGroupPool = pool.NewDefaultUnionPool(func() *BlobGroup {
	return &BlobGroup{}
})

type BlobGroup struct {
	wg    sync.WaitGroup
	tm    time.Time
	size  uint64
	blobs []Blob
}

func NewBlobGroup(size int) (*BlobGroup, func()) {
	group := blobGroupPool.Get()
	group.Init(size)
	return group, func() {
		blobGroupPool.PutWithMemSize(group, 0)
	}
}

func (bg *BlobGroup) Init(size int) {
	bg.blobs = slices.Grow(bg.blobs, size)[:size]
	bg.size = uint64(size)
	for i := range bg.blobs {
		bg.blobs[i].Reset()
	}
}

func (bg *BlobGroup) Walk(fn func(blob *Blob)) {
	for i := range bg.blobs {
		fn(&bg.blobs[i])
	}
}

func (bg *BlobGroup) GroupingRow(mst string, seriesKey []byte, rec *record.Record, rowIndex int) {
	idx := xxhash.Sum64String(mst) % bg.size
	factor := uint64(conf.SeriesHashFactor)
	if factor > 1 {
		idx = (idx + xxhash.Sum64(seriesKey)%factor) % bg.size
	}
	bg.blobs[idx].hash = idx
	bg.blobs[idx].WriteRecordRow(seriesKey, rec, rowIndex)
}

func (bg *BlobGroup) ResetTime() {
	bg.tm = time.Now()
}

func (bg *BlobGroup) Wait() {
	bg.wg.Wait()
}

func (bg *BlobGroup) Error() error {
	for _, blob := range bg.blobs {
		if err := blob.Error(); err != nil {
			return err
		}
	}
	return nil
}
