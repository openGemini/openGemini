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

package writer

import (
	"errors"
	"fmt"
	"slices"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/openGemini/openGemini/lib/compress"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	pb "github.com/openGemini/opengemini-client-go/proto"
	"go.uber.org/zap"
)

type MstRecord struct {
	Mst string
	Rec record.Record
}

type RecordDecoder struct {
	errs []error
	swap []byte

	allocator *pool.Allocator[int, MstRecord]
}

var recordDecoderPool = pool.NewDefaultUnionPool(func() *RecordDecoder {
	return &RecordDecoder{
		allocator: pool.NewAllocator[int, MstRecord](),
	}
})

func NewRecordDecoder() (*RecordDecoder, func()) {
	dec := recordDecoderPool.Get()
	return dec, func() {
		dec.Reset()
		recordDecoderPool.Put(dec)
	}
}

func (d *RecordDecoder) MemSize() int {
	return len(d.swap)
}

func (d *RecordDecoder) Instance() *RecordDecoder {
	return d
}

func (d *RecordDecoder) Reset() {
	d.allocator.Reset()
	clear(d.errs)
}

func (d *RecordDecoder) Decode(blocks []*pb.Record) ([]*MstRecord, []error) {
	d.errs = slices.Grow(d.errs, len(blocks))[:len(blocks)]

	for i, row := range blocks {
		if !meta.ValidMeasurementName(row.Measurement) {
			d.errs[i] = errors.New("invalid measurement name")
			continue
		}

		data, err := d.decompress(row.CompressMethod, row.Block)
		if err != nil {
			d.errs[i] = err
			continue
		}

		mr := d.allocator.Alloc()
		mr.Mst = row.Measurement
		err = d.unmarshalRecord(&mr.Rec, data)
		if err != nil {
			d.allocator.Rollback()
			logger.GetLogger().Error("invalid record", zap.String("record", mr.Rec.String()))
			d.errs[i] = err
			continue
		}
	}

	return d.allocator.Values(), d.errs
}

func (d *RecordDecoder) decompress(algo pb.CompressMethod, data []byte) ([]byte, error) {
	var err error
	switch algo {
	case pb.CompressMethod_UNCOMPRESSED:
		return data, nil
	case pb.CompressMethod_LZ4_FAST:
		panic("please implement me")
	case pb.CompressMethod_ZSTD_FAST:
		d.swap, err = zstd.Decompress(d.swap[:0], data)
		return d.swap, err
	case pb.CompressMethod_SNAPPY:
		d.swap, err = compress.SnappyDecoding(data, d.swap[:0])
		return d.swap, err
	default:
		return nil, fmt.Errorf("invalid compress algorithm")
	}
}

func (d *RecordDecoder) unmarshalRecord(dst *record.Record, data []byte) error {
	dst.Unmarshal(data)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("invalid record:%v", r)
			}
		}()
		record.CheckRecord(dst)
	}()

	return err
}
