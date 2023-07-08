/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package pusher

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	maxBlockSize  = 64 * 1024 * 1024 // 64M
	retryInternal = 200 * time.Millisecond
	maxRetry      = 10
)

var pool = bufferpool.NewByteBufferPool(0)

func GetBuffer(size int) []byte {
	buf := pool.Get()
	if size > 0 {
		buf = bufferpool.Resize(buf, size)
	}
	return buf
}

func PutBuffer(b []byte) {
	pool.Put(b)
}

type SnappyWriter struct {
	compress bool
	block    []byte
	f        *os.File
	sizeBuf  [4]byte
}

func NewSnappyWriter() *SnappyWriter {
	return &SnappyWriter{
		block:   []byte{},
		sizeBuf: [4]byte{},
	}
}

func (w *SnappyWriter) EnableCompress() {
	w.compress = true
}

func (w *SnappyWriter) File() *os.File {
	return w.f
}

func (w *SnappyWriter) OpenFile(file string) error {
	fd, err := os.OpenFile(path.Clean(file), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	w.f = fd
	if err != nil {
		panic(err)
	}
	return err
}

// WriteBlock consists: |4-byte data size|data|
// 1. write 4-byte data length after compression
// 2. write data
// 3. write size=0, indicating EOF
func (w *SnappyWriter) WriteBlock(b []byte) error {
	if len(b) == 0 || w.f == nil {
		return nil
	}

	if w.compress {
		w.block = snappy.Encode(w.block, b)
	} else {
		w.block = b
	}

	binary.BigEndian.PutUint32(w.sizeBuf[:], uint32(len(w.block)))
	if err := w.write(w.f, w.sizeBuf[:]); err != nil {
		return err
	}

	if err := w.write(w.f, w.block); err != nil {
		return err
	}

	return w.f.Sync()
}

func (w *SnappyWriter) write(writer io.Writer, b []byte) error {
	n, err := writer.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return errno.NewError(errno.ShortWrite, n, len(b))
	}
	return nil
}

func (w *SnappyWriter) Close() error {
	defer func() {
		_ = w.f.Close()
	}()
	binary.BigEndian.PutUint32(w.sizeBuf[:], 0)
	return w.write(w.f, w.sizeBuf[:])
}

type SnappyReader struct {
	compress bool
	f        *os.File
	sizeBuf  [util.Uint32SizeBytes]byte
	location int64
	eof      bool
}

func NewSnappyReader() *SnappyReader {
	return &SnappyReader{}
}

func (r *SnappyReader) EnableCompress() {
	r.compress = true
}

func (r *SnappyReader) OpenFile(file string) error {
	fd, err := os.OpenFile(path.Clean(file), os.O_RDONLY, 0400)
	r.f = fd
	return err
}

func (r *SnappyReader) Stat() (os.FileInfo, error) {
	return r.f.Stat()
}

func (r *SnappyReader) ReadBlock() ([]byte, error) {
	block, err := r.readBlock()
	if err != nil {
		return nil, err
	}

	if !r.compress || len(block) == 0 {
		return block, nil
	}

	decoded, err := snappy.Decode(GetBuffer(0), block)
	if err != nil {
		return nil, err
	}
	PutBuffer(block)

	return decoded, nil
}

func (r *SnappyReader) readBlock() ([]byte, error) {
	if err := r.read(r.sizeBuf[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(r.sizeBuf[:])
	if size == 0 {
		r.eof = true
		r.location += int64(util.Uint32SizeBytes)
		return nil, io.EOF
	}

	if size > maxBlockSize {
		return nil, fmt.Errorf("invalid block size. max: %d; current: %d", maxBlockSize, size)
	}

	block := GetBuffer(int(size))
	if err := r.retryRead(block); err != nil {
		if err == io.EOF {
			return nil, r.SeekStart(r.location)
		}
		return nil, err
	}

	r.location += int64(size) + int64(util.Uint32SizeBytes)
	return block, nil
}

func (r *SnappyReader) read(b []byte) error {
	n, err := r.f.Read(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return errno.NewError(errno.ShortRead, n, len(b))
	}

	return nil
}

func (r *SnappyReader) retryRead(b []byte) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errno.NewError(errno.RecoverPanic, e).SetModule(errno.ModuleStat)
		}
	}()

	retryCount := 0
	var wait = func() bool {
		if retryCount >= maxRetry {
			return false
		}
		retryCount++
		time.Sleep(retryInternal)
		return true
	}

	tmp := b
	readN := 0

	for {
		n, err := r.f.Read(tmp)
		if err == io.EOF {
			if !wait() {
				return errno.NewError(errno.ShortRead, readN, len(b))
			}
			continue
		}

		if err != nil {
			return err
		}

		readN += n
		if readN != len(b) {
			tmp = tmp[n:]
			if !wait() {
				return errno.NewError(errno.ShortRead, readN, len(b))
			}
			continue
		}

		return nil
	}
}

func (r *SnappyReader) SeekStart(location int64) error {
	_, err := r.f.Seek(location, io.SeekStart)
	return err
}

func (r *SnappyReader) Location() int64 {
	return r.location
}

func (r *SnappyReader) isEOF() bool {
	return r.eof
}

func (r *SnappyReader) Close() error {
	return r.f.Close()
}

type SnappyTail struct {
	reader  *SnappyReader
	timeout time.Duration

	stat   os.FileInfo
	close  chan struct{}
	closed bool
	mu     sync.Mutex

	compress bool
}

const (
	DefaultTailTimeout = time.Hour
)

func NewSnappyTail(timeout time.Duration, compress bool) *SnappyTail {
	if timeout == 0 {
		timeout = DefaultTailTimeout
	}

	return &SnappyTail{
		timeout:  timeout,
		close:    make(chan struct{}),
		compress: compress,
		closed:   true,
	}
}

func (t *SnappyTail) Tail(file string, handle func(data []byte)) error {
	t.reader = NewSnappyReader()
	if err := t.reader.OpenFile(file); err != nil {
		return err
	}
	t.closed = false

	if t.compress {
		t.reader.EnableCompress()
	}

	stat, err := t.reader.Stat()
	if err != nil {
		return err
	}
	t.stat = stat

	for {
		err := t.readBlock(handle)
		if err == io.EOF && !t.closed {
			changed, err := t.watch()
			if changed {
				continue
			}
			return err
		}

		if err != nil {
			return err
		}
	}
}

func (t *SnappyTail) readBlock(handle func(data []byte)) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return io.EOF
	}

	data, err := t.reader.ReadBlock()
	if err != nil {
		return err
	}

	if handle != nil && len(data) > 0 {
		handle(data)
	}
	return nil
}

func (t *SnappyTail) watch() (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || t.reader.isEOF() {
		return false, io.EOF
	}

	ticker := time.NewTicker(time.Second)
	defer func() {
		ticker.Stop()
	}()
	timer := time.NewTimer(t.timeout)

	for {
		select {
		case <-ticker.C:
			stat, err := t.reader.Stat()
			if err != nil {
				return false, err
			}

			if stat.ModTime() != t.stat.ModTime() ||
				stat.Size() != t.stat.Size() {
				t.stat = stat
				return true, nil
			}
		case <-timer.C:
			return false, fmt.Errorf("watch file timeout")
		case <-t.close:
			return false, io.EOF
		}
	}
}

func (t *SnappyTail) Close() {
	close(t.close)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}

	t.closed = true
	_ = t.reader.Close()
}
