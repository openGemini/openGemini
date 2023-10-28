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
	"io"
	"os"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type IndexBuilder struct {
	inited       bool
	encodeChunk  []byte
	meta         []byte
	log          *Log.Logger
	chunkBuilder *ChunkBuilder
	fd           fileops.File
	writer       fileops.FileWriter
}

func NewIndexBuilder(lockPath *string, filePath string) *IndexBuilder {
	indexBuilder := &IndexBuilder{}
	var err error
	lock := fileops.FileLockOption(*lockPath)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	indexBuilder.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		panic(err)
	}
	indexBuilder.writer = newPrimaryKeyWriter(indexBuilder.fd, lockPath)
	indexBuilder.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	indexBuilder.chunkBuilder = NewChunkBuilder()

	return indexBuilder

}

func (b *IndexBuilder) WriteData(rec *record.Record, tcLocation int8) error {
	b.encodeChunk = b.encodeChunk[:0]
	if !b.inited {
		b.inited = true
		b.encodeChunk = append(b.encodeChunk, primaryKeyMagic...)
		b.encodeChunk = numberenc.MarshalUint32Append(b.encodeChunk, version)
		b.meta = marshalMeta(&rec.Schema, rec.RowNums(), tcLocation)
	}
	var err error
	schemaByteSize := len(rec.Schemas()) * util.Uint32SizeBytes
	/*
		offset is encoding after
		- total length of meta(uint32), length of schema(uint32), row number(uint32)
		- time cluster location(int8)
		- field name size details(schemaByteSize), field type details(schemaByteSize)
		and length of offset is schemaByteSize, so the location of offset is:
		  [util.Uint32SizeBytes*3+schemaByteSize*2 : util.Uint32SizeBytes*3+schemaByteSize*3]
		see marshalMeta for more detail.
	*/
	offset := b.meta[util.Uint32SizeBytes*3+util.Int8SizeBytes+schemaByteSize*2 : util.Uint32SizeBytes*3+util.Int8SizeBytes+schemaByteSize*3]

	data := make([]byte, 0)
	data, err = b.chunkBuilder.EncodeChunk(rec, data, offset, uint32(len(b.meta)+headerSize))
	if err != nil {
		b.log.Error("encode chunk fail", zap.Error(err))
		return err
	}
	b.encodeChunk = append(b.encodeChunk, b.meta...)
	b.encodeChunk = append(b.encodeChunk, data...)

	var num int
	if num, err = b.writer.WriteData(b.encodeChunk); err != nil {
		return err
	}

	if err != nil {
		err = errno.NewError(errno.WriteFileFailed, err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}
	if num != len(b.encodeChunk) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), num}))
		return io.ErrShortWrite
	}

	return nil
}

func (b *IndexBuilder) Reset() {
	b.encodeChunk = b.encodeChunk[:0]
	b.meta = b.meta[:0]
	b.inited = false

	if b.writer != nil {
		_ = b.writer.Close()
		b.writer = nil
	}
	if b.fd != nil {
		_ = b.fd.Close()
		b.fd = nil
	}
}
