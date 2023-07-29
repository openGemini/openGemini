/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package colstore

import (
	"errors"
	"hash/crc32"

	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type ChunkBuilder struct {
	chunk      []byte
	log        *Log.Logger
	colBuilder *ColumnBuilder
}

func NewChunkBuilder() *ChunkBuilder {
	return &ChunkBuilder{
		colBuilder: NewColumnBuilder(),
	}
}

func (b *ChunkBuilder) reset(dst []byte) {
	b.chunk = dst
}

func (b *ChunkBuilder) EncodeChunk(rec *record.Record, dst []byte, offset []byte, offsetStart uint32) ([]byte, error) {
	b.reset(dst)

	var err error
	var ref *record.Field
	var col *record.ColVal
	for i := range rec.Schema[:len(rec.Schema)] {
		ref = &rec.Schema[i]
		col = rec.Column(i)
		if col.NilCount != 0 {
			return nil, errors.New("not support for column with nil value")
		}
		pos := len(b.chunk)
		numberenc.MarshalUint32Copy(offset[i*util.Uint32SizeBytes:(i+1)*util.Uint32SizeBytes], offsetStart+uint32(pos))
		b.chunk = numberenc.MarshalUint32Append(b.chunk, crc32.ChecksumIEEE(col.Val))
		b.colBuilder.set(b.chunk)
		if b.chunk, err = b.colBuilder.EncodeColumn(ref, col); err != nil {
			b.log.Error("encode column fail", zap.Error(err))
			return nil, err
		}
	}

	return b.chunk, nil
}
