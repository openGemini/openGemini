// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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
	colsOffset   []byte //cols offset for meta
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
	indexBuilder.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0600, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		panic(err)
	}
	indexBuilder.writer = newIndexWriter(indexBuilder.fd, lockPath)
	indexBuilder.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	indexBuilder.chunkBuilder = NewChunkBuilder()

	return indexBuilder
}

func NewIndexBuilderByFd(lockPath *string, fd fileops.File, firstFlush bool) *IndexBuilder {
	indexBuilder := &IndexBuilder{}
	indexBuilder.fd = fd
	indexBuilder.writer = newIndexWriter(indexBuilder.fd, lockPath)
	indexBuilder.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	indexBuilder.chunkBuilder = NewChunkBuilder()
	if !firstFlush {
		indexBuilder.inited = true
	}
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

	return b.writeData()
}

func (b *IndexBuilder) firstFlush(schemas record.Schemas, tcLocation int8) {
	//init encodeChunk
	b.encodeChunk = append(b.encodeChunk, primaryKeyMagic...)
	b.encodeChunk = numberenc.MarshalUint32Append(b.encodeChunk, version)

	//init meta
	schemaByteSize := len(schemas) * util.Uint32SizeBytes
	b.meta = marshalDetachedMetaHeader(schemas, tcLocation, schemaByteSize)
}

func (b *IndexBuilder) WriteDetachedData(rec *record.Record, tcLocation int8) error {
	b.encodeChunk = b.encodeChunk[:0]
	schemas := rec.Schemas()
	schemaByteSize := len(schemas) * util.Uint32SizeBytes
	// colsOffset is pre-allocated with 0, need to update with data encoding.
	b.colsOffset = make([]byte, schemaByteSize)

	if !b.inited {
		b.inited = true
		b.firstFlush(schemas, tcLocation)
	}
	var err error
	data := make([]byte, 0)
	data, err = b.chunkBuilder.EncodeChunk(rec, data, b.colsOffset, 0)
	if err != nil {
		b.log.Error("encode pkIndex data fail", zap.Error(err))
		return err
	}
	b.encodeChunk = append(b.encodeChunk, data...)
	return b.writeData()
}

func (b *IndexBuilder) WriteDetachedMeta(startId, endId uint64, offset, size uint32, fd fileops.File) error {
	b.reserveMeta()
	// marshal primary key metablock
	b.meta = MarshalPkMetaBlock(startId, endId, offset, size, b.meta, b.colsOffset)
	//init encodeChunk writer
	if b.writer != nil {
		_ = b.writer.Close()
	}
	//write meta
	lockPath := ""
	b.writer = newIndexWriter(fd, &lockPath)
	var num int
	var err error
	if num, err = b.writer.WriteData(b.meta); err != nil {
		err = errno.NewError(errno.WriteFileFailed, err)
		b.log.Error("write pkIndex meta fail", zap.Error(err))
		return err
	}

	if num != len(b.meta) {
		b.log.Error("write pkIndex meta fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), num}))
		return io.ErrShortWrite
	}
	return nil
}

func (b *IndexBuilder) reserveMeta() {
	length := len(b.meta)
	if cap(b.meta) < length+accMetaSize {
		delta := length + accMetaSize - cap(b.meta)
		b.meta = b.meta[:cap(b.meta)]
		b.meta = append(b.meta, make([]byte, delta)...)
	}
	b.meta = b.meta[:length]
}

func (b *IndexBuilder) writeData() error {
	num, err := b.writer.WriteData(b.encodeChunk)
	if err != nil {
		err = errno.NewError(errno.WriteFileFailed, err)
		b.log.Error("write pkIndex data fail", zap.Error(err))
		return err
	}
	if num != len(b.encodeChunk) {
		b.log.Error("write pkIndex data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), num}))
		return io.ErrShortWrite
	}

	return b.fd.Sync()
}

func (b *IndexBuilder) GetEncodeChunkSize() uint32 {
	return uint32(len(b.encodeChunk))
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

type schemaInfo struct {
	fields        string
	fieldNameSize []byte
	fieldType     []byte
}

func getSchemaInfo(schemas record.Schemas) *schemaInfo {
	schemaByteSize := len(schemas) * util.Uint32SizeBytes
	sInfo := &schemaInfo{
		fields:        "",
		fieldNameSize: make([]byte, 0, schemaByteSize),
		fieldType:     make([]byte, 0, schemaByteSize),
	}
	for _, schema := range schemas {
		fieldName := schema.Name
		sInfo.fieldNameSize = numberenc.MarshalUint32Append(sInfo.fieldNameSize, uint32(len(fieldName)))
		sInfo.fieldType = numberenc.MarshalUint32Append(sInfo.fieldType, uint32(schema.Type))
		sInfo.fields += fieldName
	}
	return sInfo
}
