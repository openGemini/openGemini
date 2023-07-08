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

package immutable

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type tsspFileWriter struct {
	fd         fileops.File
	fileWriter fileops.BasicFileWriter // data chunk writer
	dataN      int64

	cmw *indexWriter // chunkmeta writer
	cmN int64
}

func newWriteLimiter(fd fileops.File, limitCompact bool) fileops.NameReadWriterCloser {
	var lw fileops.NameReadWriterCloser
	if limitCompact {
		lw = fileops.NewLimitWriter(fd, compWriteLimiter)
	} else {
		lw = fd
		if SnapshotLimit() {
			lw = fileops.NewLimitWriter(fd, snapshotWriteLimiter)
		}
	}
	return lw
}

func newTsspFileWriter(fd fileops.File, cacheMeta bool, limitCompact bool, lockPath *string) fileops.FileWriter {
	name := fd.Name()
	idxName := name[:len(name)-len(tmpTsspFileSuffix)] + ".index.init"

	lw := newWriteLimiter(fd, limitCompact)
	w := &tsspFileWriter{
		fd:         fd,
		fileWriter: fileops.NewFileWriter(lw, fileops.DefaultWriterBufferSize, lockPath),
	}

	w.cmw = NewIndexWriter(idxName, cacheMeta, limitCompact, lockPath)

	return w
}

func (w *tsspFileWriter) GetFileWriter() fileops.BasicFileWriter {
	return w.fileWriter
}

func (w *tsspFileWriter) DataSize() int64 {
	return w.dataN
}

func (w *tsspFileWriter) ChunkMetaSize() int64 {
	return w.cmN
}

func (w *tsspFileWriter) WriteData(b []byte) (int, error) {
	n, err := w.fileWriter.Write(b)
	if err != nil {
		return 0, err
	}
	w.dataN += int64(n)

	return n, nil
}

func (w *tsspFileWriter) Name() string {
	return w.fd.Name()
}

func (w *tsspFileWriter) WriteChunkMeta(b []byte) (int, error) {
	n, err := w.cmw.Write(b)
	if err != nil {
		return 0, err
	}
	w.cmN += int64(n)

	return n, nil
}

func (w *tsspFileWriter) Close() error {
	if w.fileWriter != nil {
		if err := w.fileWriter.Close(); err != nil {
			log.Error("close data writer fail", zap.Error(err))
			return err
		}
		w.fileWriter = nil
	}

	if w.cmw != nil {
		if err := w.cmw.Close(); err != nil {
			log.Error("close chunk meta writer fail", zap.Error(err))
			return err
		}
		w.cmw = nil
	}

	if err := w.fd.Sync(); err != nil {
		log.Error("sync file fail", zap.String("file", w.fd.Name()), zap.Error(err))
		return err
	}
	w.fd = nil

	return nil
}

func (w *tsspFileWriter) AppendChunkMetaToData() error {
	n, err := w.cmw.CopyTo(w.fileWriter)
	if err != nil {
		log.Error("copy chunk meta fail", zap.Error(err))
		return err
	}

	if n != w.cmw.Size() {
		err = fmt.Errorf("copy chunkmeta to data fail, metasize:%v, copysize:%v", w.cmw.Size(), n)
		log.Error(err.Error())
		return err
	}

	w.dataN += int64(n)

	return err
}

func (w *tsspFileWriter) SwitchMetaBuffer() {
	w.cmw.SwitchMetaBuffer()
}

func (w *tsspFileWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	return w.cmw.MetaDataBlocks(dst)
}

var (
	indexWriterPool pool.FixedPool
	diskWriterPool  pool.FixedPool
	metaBlkPool     = sync.Pool{}
)

func InitWriterPool(size int) {
	stat := statistics.NewHitRatioStatistics()
	indexWriterPool.Reset(size, func() interface{} {
		return &indexWriter{}
	}, pool.NewHitRatioHook(stat.AddIndexWriterGetTotal, stat.AddIndexWriterHitTotal))

	diskWriterPool.Reset(size, nil, nil)
}

func getMetaBlockBuffer(size int) []byte {
	v := metaBlkPool.Get()
	if v == nil {
		return make([]byte, 0, size)
	}
	buf, ok := v.([]byte)
	if !ok {
		return make([]byte, 0, size)
	}
	return buf[:0]
}

// nolint
func freeMetaBlockBuffer(b []byte) {
	metaBlkPool.Put(b[:0])
}

func freeMetaBlocks(buffers [][]byte) int {
	n := 0
	for i := range buffers {
		n += cap(buffers[i])
		freeMetaBlockBuffer(buffers[i])
	}
	return n
}

type indexWriter struct {
	name string
	lock *string
	buf  []byte
	n    int
	wn   int
	lw   fileops.NameReadWriterCloser
	tmp  []byte

	blockSize    int
	cacheMeta    bool
	limitCompact bool
	metas        [][]byte
}

func NewIndexWriter(indexName string, cacheMeta bool, limitCompact bool, lockPath *string) *indexWriter {
	w, ok := indexWriterPool.Get().(*indexWriter)
	if !ok || w == nil {
		w = &indexWriter{}
	}

	w.limitCompact = limitCompact
	w.lock = lockPath
	w.reset(indexName, cacheMeta)
	if w.cacheMeta {
		w.buf = getMetaBlockBuffer(w.blockSize)
	} else {
		if cap(w.buf) >= w.blockSize {
			w.buf = w.buf[:w.blockSize]
		} else {
			w.buf = make([]byte, w.blockSize)
		}
	}

	return w
}

func (w *indexWriter) reset(name string, cacheMeta bool) {
	if w.lw != nil {
		_ = w.lw.Close()
	}

	w.cacheMeta = cacheMeta
	w.blockSize = fileops.DefaultWriterBufferSize
	w.name = name
	w.metas = w.metas[:0]
	w.buf = w.buf[:0]
	w.n = 0
	w.wn = 0
	w.lw = nil
}

func (w *indexWriter) flush() error {
	if w.n == 0 {
		return nil
	}

	n, err := w.lw.Write(w.buf[0:w.n])
	if err != nil || n < w.n {
		return io.ErrShortWrite
	}
	w.n = 0

	return nil
}

func (w *indexWriter) available() int { return len(w.buf) - w.n }
func (w *indexWriter) buffered() int  { return w.n }

func (w *indexWriter) writeBuffer(p []byte) (int, error) {
	var wn int
	var err error
	for len(p) > w.available() && err == nil {
		if w.lw == nil {
			lock := fileops.FileLockOption(*w.lock)
			fd, err := fileops.OpenFile(w.name, os.O_CREATE|os.O_RDWR, 0640, lock)
			if err != nil {
				log.Error("create file fail", zap.String("file", w.name), zap.Error(err))
				return 0, err
			}
			w.lw = newWriteLimiter(fd, w.limitCompact)
		}

		var n int
		if w.buffered() == 0 {
			// Big data block, write directly from p to avoid copy
			n, err = w.lw.Write(p)
		} else {
			n = copy(w.buf[w.n:], p)
			w.n += n
			err = w.flush()
		}

		wn += n
		p = p[n:]
	}
	if err != nil {
		return wn, err
	}

	n := copy(w.buf[w.n:], p)
	w.n += n

	wn += n
	w.wn += wn

	return wn, nil
}

func (w *indexWriter) writeCacheBlocks(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	w.wn += len(p)
	return len(p), nil
}

func (w *indexWriter) Write(p []byte) (nn int, err error) {
	if w.cacheMeta {
		return w.writeCacheBlocks(p)
	}

	return w.writeBuffer(p)
}

func (w *indexWriter) Close() error {
	if w.lw != nil {
		_ = w.lw.Close()
		lock := fileops.FileLockOption(*w.lock)
		_ = fileops.Remove(w.name, lock)
		w.lw = nil
	}

	if w.cacheMeta {
		for _, b := range w.metas {
			freeMetaBlockBuffer(b)
		}
		if w.buf != nil {
			freeMetaBlockBuffer(w.buf)
		}
		w.buf = nil
	}

	w.reset("", false)
	indexWriterPool.Put(w)

	return nil
}

func (w *indexWriter) Size() int {
	return w.wn
}

func (w *indexWriter) bytes() []byte {
	return w.buf[:w.n]
}

func (w *indexWriter) copyFromCacheTo(to io.Writer) (int, error) {
	n := 0

	if len(w.buf) > 0 {
		w.metas = append(w.metas, w.buf)
		w.buf = nil
	}

	for i := range w.metas {
		if len(w.metas[i]) == 0 {
			continue
		}
		s, err := to.Write(w.metas[i])
		if err != nil {
			return n, err
		}
		n += s
	}

	return n, nil
}

func (w *indexWriter) allInBuffer() bool {
	return w.lw == nil && w.n <= w.wn
}

func (w *indexWriter) seekStart() error {
	err := w.lw.Close()
	if err != nil {
		return err
	}
	if w.lw, err = fileops.OpenFile(w.name, os.O_RDONLY, 0640); err != nil {
		return err
	}

	return nil
}

func (w *indexWriter) CopyTo(to io.Writer) (int, error) {
	if w.cacheMeta {
		return w.copyFromCacheTo(to)
	}

	if w.allInBuffer() {
		buf := w.bytes()
		return to.Write(buf)
	}

	if err := w.flush(); err != nil {
		return 0, err
	}

	// Seek file pos to start
	if err := w.seekStart(); err != nil {
		return 0, err
	}

	if len(w.tmp) <= 0 {
		w.tmp = bufferpool.Resize(w.tmp, fileops.DefaultBufferSize)
	}
	n, err := io.CopyBuffer(to, w.lw, w.tmp)
	if err != nil {
		return 0, err
	}

	return int(n), nil
}

func (w *indexWriter) SwitchMetaBuffer() {
	if w.cacheMeta {
		w.metas = append(w.metas, w.buf)
		w.buf = getMetaBlockBuffer(w.blockSize)
	}
}

func (w *indexWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	dst = dst[:0]
	if w.cacheMeta {
		if len(w.buf) > 0 {
			w.metas = append(w.metas, w.buf)
			w.buf = nil
		}
		for i := range w.metas {
			if len(w.metas[i]) > 0 {
				dst = append(dst, w.metas[i])
			}
		}
		w.metas = w.metas[:0]
	}
	return dst
}
