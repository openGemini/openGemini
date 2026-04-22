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
	"go.uber.org/zap"
)

var snapshotWriteLimiter = fileops.NewLimiter(48*1024*1024, 64*1024*1024)
var defaultWriterBufferSize = 1024 * 1024
var log = Log.GetLogger()

/*
layout for primaryKey file:

header:
  - magic number: "COLX" (4 bytes).
  - version: version of layout (4 bytes).
  - meta: total length, field name, field type, field offset, etc.

data:
  - data: col1 data, col2 data, ... colN data
  - col data: data encoded as chunk@tssp file.
*/

type indexWriter struct {
	num        int64 // byte written
	fd         fileops.File
	fileWriter fileops.BasicFileWriter // disk writer
}

func newIndexWriter(fd fileops.File, lockPath *string) fileops.FileWriter {
	lw := fileops.NewLimitWriter(fd, snapshotWriteLimiter)
	w := &indexWriter{
		fd:         fd,
		fileWriter: fileops.NewFileWriter(lw, defaultWriterBufferSize, lockPath),
	}

	return w
}

func (c *indexWriter) WriteData(b []byte) (int, error) {
	n, err := c.fileWriter.Write(b)
	if err != nil {
		return 0, err
	}
	c.num += int64(n)

	return n, nil
}

func (c *indexWriter) WriteChunkMeta(b []byte) (int, error) {
	panic("WriteChunkMeta not implement for indexWriter") // no chunk meta for colStore
}

func (c *indexWriter) Close() error {
	if c.fileWriter != nil {
		if err := c.fileWriter.Close(); err != nil {
			log.Error("close data writer fail", zap.Error(err))
			return err
		}
		c.fileWriter = nil
	}

	if err := c.fd.Sync(); err != nil {
		log.Error("sync file fail", zap.String("file", c.fd.Name()), zap.Error(err))
		return err
	}
	c.fd = nil

	return nil
}

func (c *indexWriter) DataSize() int64 {
	return c.num
}

func (c *indexWriter) ChunkMetaSize() int64 {
	panic("ChunkMetaSize not implement for indexWriter")
}

func (c *indexWriter) ChunkMetaBlockSize() int64 {
	return 0
}

func (c *indexWriter) GetFileWriter() fileops.BasicFileWriter {
	return c.fileWriter
}

func (c *indexWriter) AppendChunkMetaToData() error {
	panic("AppendChunkMetaToData not implement for indexWriter")
}

func (c *indexWriter) SwitchMetaBuffer() (int, error) {
	panic("SwitchMetaBuffer not implement for indexWriter")
}

func (c *indexWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	panic("MetaDataBlocks not implement for indexWriter")
}

func (c *indexWriter) Name() string {
	return c.fd.Name()
}

type IndexWriter struct {
	fd     fileops.File
	writer fileops.FileWriter
	log    *Log.Logger
}

func NewIndexWriter(lockPath *string, filePath string) (*IndexWriter, error) {
	indexWriter := &IndexWriter{}
	var err error
	lock := fileops.FileLockOption(*lockPath)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	indexWriter.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		return nil, err
	}
	indexWriter.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	indexWriter.writer = newIndexWriter(indexWriter.fd, lockPath)
	return indexWriter, nil
}

func (b *IndexWriter) WriteData(data []byte) error {
	var num int
	var err error
	if num, err = b.writer.WriteData(data); err != nil {
		err = errno.NewError(errno.WriteFileFailed, err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}

	if num != len(data) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Int("size", num))
		return io.ErrShortWrite
	}
	return nil
}

func (b *IndexWriter) Reset() {
	if b.writer != nil {
		_ = b.writer.Close()
		b.writer = nil
	}
	if b.fd != nil {
		_ = b.fd.Close()
		b.fd = nil
	}
}
