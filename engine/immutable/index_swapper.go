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

package immutable

import (
	"bufio"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	SwapperCompressNone   = 0
	SwapperCompressSnappy = 1
	SwapperCompressZSTD   = 2
)

var indexCompressMode = SwapperCompressNone

func SetIndexCompressMode(mode int) {
	if mode < SwapperCompressNone || mode > SwapperCompressZSTD {
		return
	}

	indexCompressMode = mode
}

type FileSwapper struct {
	lock         string
	file         string
	fd           fileops.File
	lw           fileops.NameReadWriterCloser
	bw           *bufio.Writer
	writer       io.WriteCloser
	compressMode int
}

func NewFileSwapper(file string, lock string, limitCompact bool, compressMode int) (*FileSwapper, error) {
	s := &FileSwapper{compressMode: compressMode, file: file, lock: lock}
	err := s.open(limitCompact)
	return s, err
}

func (s *FileSwapper) open(limitCompact bool) error {
	fd, err := fileops.OpenFile(s.file, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0640, fileops.FileLockOption(s.lock))
	if err != nil {
		log.Error("create file fail", zap.String("file", s.file), zap.Error(err))
		return err
	}

	s.fd = fd
	s.lw = newWriteLimiter(fd, limitCompact)

	s.writer = s.createWriter(s.lw)
	return nil
}

func (s *FileSwapper) CopyTo(to io.Writer, buf []byte) (int64, error) {
	if err := s.flush(); err != nil {
		return 0, err
	}

	fd, err := fileops.OpenFile(s.file, os.O_RDONLY, 0640, fileops.FileLockOption(s.lock))
	if err != nil {
		return 0, err
	}
	defer util.MustClose(fd)

	return io.CopyBuffer(to, s.createReader(fd), buf)
}

func (s *FileSwapper) flush() error {
	if s.compressMode != SwapperCompressNone {
		if err := s.writer.Close(); err != nil {
			return err
		}
	}

	if s.bw != nil {
		err := s.bw.Flush()
		if err != nil {
			return err
		}
	}

	err := s.fd.Close()
	s.fd = nil
	return err
}

func (s *FileSwapper) createWriter(w io.WriteCloser) io.WriteCloser {
	switch s.compressMode {
	case SwapperCompressSnappy:
		s.bw = bufio.NewWriterSize(w, fileops.DefaultWriterBufferSize)
		return snappy.NewBufferedWriter(s.bw)
	case SwapperCompressZSTD:
		s.bw = bufio.NewWriterSize(w, fileops.DefaultWriterBufferSize)
		zw, err := zstd.NewWriter(s.bw)
		if err != nil {
			return w
		}
		return zw
	default:
		return w
	}
}

func (s *FileSwapper) createReader(r io.Reader) io.Reader {
	switch s.compressMode {
	case SwapperCompressSnappy:
		return snappy.NewReader(bufio.NewReaderSize(r, fileops.DefaultBufferSize))
	case SwapperCompressZSTD:
		zr, err := zstd.NewReader(bufio.NewReaderSize(r, fileops.DefaultBufferSize))
		if err != nil {
			return r
		}
		return zr
	default:
		return r
	}
}

func (s *FileSwapper) SetWriter(w io.WriteCloser) {
	s.writer = w
}

func (s *FileSwapper) Write(b []byte) (int, error) {
	return s.writer.Write(b)
}

func (s *FileSwapper) MustClose() {
	if s.fd != nil {
		util.MustClose(s.fd)
	}
}
