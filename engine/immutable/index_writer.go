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

package immutable

import (
	"bufio"
	"bytes"
	"io"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding/lz4"
)

type IndexWriter interface {
	Init(name string, lock *string, cacheMeta bool, limitCompact bool)
	Write(p []byte) (int, error)
	Size() int
	BlockSize() int
	CopyTo(to io.Writer) (int, error)
	SwitchMetaBuffer() (int, error)
	MetaDataBlocks(dst [][]byte) [][]byte
	allInBuffer() bool
	Close() error
}

func newIndexWriter() IndexWriter {
	if GetChunkMetaCompressMode() == ChunkMetaCompressNone {
		return &indexWriter{}
	}

	return newIndexCompressWriter()
}

func NewPKIndexWriter(indexName string, cacheMeta bool, limitCompact bool, lockPath *string) IndexWriter {
	w, ok := indexWriterPool.Get().(IndexWriter)
	if !ok || IsInterfaceNil(w) {
		w = newIndexWriter()
	}
	w.Init(indexName, lockPath, cacheMeta, limitCompact)

	return w
}

type IndexCompressWriter struct {
	name         string
	lock         *string
	limitCompact bool

	size int
	buf  []byte
	swap []byte

	swapper   *FileSwapper
	writer    *bufio.Writer
	memWriter memoryWriteCloser
}

func newIndexCompressWriter() *IndexCompressWriter {
	return &IndexCompressWriter{}
}

func (w *IndexCompressWriter) Init(name string, lock *string, cacheMeta bool, limitCompact bool) {
	w.limitCompact = limitCompact
	w.name = name
	w.lock = lock
	if w.writer == nil {
		w.writer = bufio.NewWriterSize(nil, fileops.DefaultBufferSize)
	}
	w.buf = w.buf[:0]
	w.size = 0
	w.swapper = nil
	w.memWriter.Reset()
}

func (w *IndexCompressWriter) GetWriter() *bufio.Writer {
	return w.writer
}

func (w *IndexCompressWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *IndexCompressWriter) Close() error {
	if w.swapper != nil {
		w.swapper.MustClose()
		w.swapper = nil
	}
	indexWriterPool.Put(w)
	return fileops.Remove(w.name, fileops.FileLockOption(*w.lock))
}

func (w *IndexCompressWriter) Size() int {
	return w.size
}

func (w *IndexCompressWriter) BlockSize() int {
	return 0
}

func (w *IndexCompressWriter) CopyTo(to io.Writer) (int, error) {
	if w.allInBuffer() {
		// All the data is in memory
		w.swapper.MustClose()
		w.swapper.SetWriter(&w.memWriter)
		err := w.writer.Flush()
		if err != nil {
			return 0, err
		}
		w.writer.Reset(nil)
		w.swapper = nil
		util.MustClose(&w.memWriter)

		return to.Write(w.memWriter.Bytes())
	}

	err := w.writer.Flush()
	if err != nil {
		return 0, err
	}
	w.writer.Reset(nil)

	w.buf = bufferpool.Resize(w.buf, fileops.DefaultBufferSize)
	n, err := w.swapper.CopyTo(to, w.buf)
	w.swapper = nil
	return int(n), err
}

func (w *IndexCompressWriter) SwitchMetaBuffer() (int, error) {
	if len(w.buf) == 0 {
		return 0, nil
	}

	err := w.openSwapper()
	if err != nil {
		return 0, err
	}

	w.swap = w.compress(w.swap[:0], w.buf)
	wn, err := w.writer.Write(w.swap)
	if err != nil {
		return 0, err
	}

	w.size += wn
	w.buf = w.buf[:0]
	return wn, nil
}

func (w *IndexCompressWriter) openSwapper() error {
	if w.swapper != nil {
		return nil
	}

	var err error
	w.swapper, err = NewFileSwapper(w.name, *w.lock, w.limitCompact, SwapperCompressNone)
	if err != nil {
		return err
	}

	w.writer.Reset(w.swapper)
	return nil
}

func tryExpand(dst []byte, estimateLen int) []byte {
	if cap(dst) < estimateLen {
		dst = make([]byte, 0, estimateLen)
	}
	return dst
}

func (w *IndexCompressWriter) compress(dst []byte, src []byte) []byte {
	switch GetChunkMetaCompressMode() {
	case ChunkMetaCompressSnappy:
		dst = snappy.Encode(dst[:cap(dst)], src)
	case ChunkMetaCompressLZ4:
		// estimate the size of compressed data
		maxLen := util.Uint32SizeBytes + lz4.CompressBlockBound(len(src))
		dst = tryExpand(dst, maxLen)

		// encoded data includes 4 bytes of data size before compression and compressed data size
		dst = numberenc.MarshalUint32Append(dst, uint32(len(src)))
		n, err := lz4.CompressBlock(src, dst[util.Uint32SizeBytes:maxLen])
		if err != nil {
			dst = append(dst[:util.Uint32SizeBytes], src...)
		}
		dst = dst[:n+util.Uint32SizeBytes]
	default:
		dst = append(dst[:0], src...)
	}
	return dst
}

func (w *IndexCompressWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	// Not supported in compress mode
	return dst[:0]
}

func (w *IndexCompressWriter) allInBuffer() bool {
	return w.size < fileops.DefaultWriterBufferSize && w.size == w.writer.Buffered()
}

type memoryWriteCloser struct {
	bytes.Buffer
}

func (w *memoryWriteCloser) Close() error {
	return nil
}
