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

package fileops

import (
	"bufio"
	"fmt"
	"io"

	"github.com/openGemini/openGemini/lib/bufferpool"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"go.uber.org/zap"
)

var log = Log.GetLogger()

const (
	DefaultWriterBufferSize = 1024 * 1024
	DefaultBufferSize       = 256 * 1024
	maxBufferSize           = 1024 * 1024
	minBufferSize           = 4 * 1024
)

var diskWriterPool pool.FixedPool

type BasicFileWriter interface {
	Write(b []byte) (int, error)
	Close() error
	Size() int
	Reset(lw NameReadWriterCloser)
	Bytes() []byte
	CopyTo(w io.Writer) (int, error)
	SwitchMetaBuffer()
	MetaDataBlocks(dst [][]byte) [][]byte
	GetWriter() *bufio.Writer
}

type fileWriter struct {
	lw   NameReadWriterCloser
	w    *bufio.Writer
	n    int
	lock *string
}

func NewFileWriter(lw NameReadWriterCloser, bufferSize int, lockPath *string) *fileWriter {
	if bufferSize == 0 {
		bufferSize = DefaultBufferSize
	} else if bufferSize < minBufferSize {
		bufferSize = minBufferSize
	} else if bufferSize > maxBufferSize {
		bufferSize = maxBufferSize
	}

	dw, ok := diskWriterPool.Get().(*fileWriter)
	if !ok || dw == nil {
		dw = &fileWriter{
			w: bufio.NewWriterSize(lw, bufferSize),
		}
	}

	dw.Reset(lw)
	dw.n = 0
	dw.lock = lockPath

	return dw
}

func (w *fileWriter) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	if err != nil || n != len(b) {
		log.Error("write file fail", zap.String("file", w.lw.Name()),
			zap.Error(err), zap.Ints("write size", []int{n, len(b)}))
		return 0, err
	}
	w.n += n

	return n, nil
}

func (w *fileWriter) Close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}

	w.lw = nil
	w.n = 0
	diskWriterPool.Put(w)

	return nil
}

func (w *fileWriter) Size() int {
	return w.n
}

func (w *fileWriter) Reset(lw NameReadWriterCloser) {
	w.lw = lw
	w.n = 0
	w.w.Reset(lw)
}

func (w *fileWriter) Bytes() []byte {
	return nil
}

func (w *fileWriter) CopyTo(to io.Writer) (int, error) {
	if err := w.w.Flush(); err != nil {
		log.Error("flush fail", zap.String("name", w.lw.Name()), zap.Error(err))
		return 0, err
	}

	name := w.lw.Name()
	if err := w.lw.Close(); err != nil {
		log.Error("close file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	lock := FileLockOption("")
	fd, err := Open(name, lock)
	if err != nil {
		log.Error("open file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	defer func(fn string) {
		if err = fd.Close(); err != nil {
			log.Error("close file fail", zap.String("file", fn), zap.Error(err))
		}
		if err = Remove(fn, FileLockOption(*w.lock)); err != nil {
			log.Error("remove file fail", zap.String("file", fn), zap.Error(err))
		}
	}(name)

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)
	buf = bufferpool.Resize(buf, DefaultBufferSize)

	var wn int64
	wn, err = io.CopyBuffer(to, fd, buf)
	if err != nil {
		log.Error("copy file fail", zap.String("file", name), zap.Error(err))
		return 0, err
	}

	if wn != int64(w.n) {
		err = fmt.Errorf("copy file(%v) fail, file size:%v, copy size: %v", name, w.n, wn)
		return 0, err
	}

	return int(wn), err
}

func (w *fileWriter) SwitchMetaBuffer()                {}
func (w *fileWriter) MetaDataBlocks([][]byte) [][]byte { return nil }

func (w *fileWriter) GetWriter() *bufio.Writer {
	return w.w
}

type FileWriter interface {
	WriteData(b []byte) (int, error)
	WriteChunkMeta(b []byte) (int, error)
	Close() error
	DataSize() int64
	ChunkMetaSize() int64
	GetFileWriter() BasicFileWriter
	AppendChunkMetaToData() error
	SwitchMetaBuffer()
	MetaDataBlocks(dst [][]byte) [][]byte
	Name() string
}
