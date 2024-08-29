// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"io"
	"sync"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type tsspFileWriter struct {
	fd         fileops.File
	fileWriter fileops.BasicFileWriter // data chunk writer
	dataN      int64

	cmw IndexWriter // chunkmeta writer
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
	idxName := name[:len(name)-len(tmpFileSuffix)] + ".index.init"

	lw := newWriteLimiter(fd, limitCompact)
	w := &tsspFileWriter{
		fd:         fd,
		fileWriter: fileops.NewFileWriter(lw, fileops.DefaultWriterBufferSize, lockPath),
	}

	w.cmw = NewPKIndexWriter(idxName, cacheMeta, limitCompact, lockPath)

	return w
}

func (w *tsspFileWriter) GetFileWriter() fileops.BasicFileWriter {
	return w.fileWriter
}

func (w *tsspFileWriter) DataSize() int64 {
	return w.dataN
}

func (w *tsspFileWriter) ChunkMetaSize() int64 {
	return int64(w.cmw.Size())
}

func (w *tsspFileWriter) ChunkMetaBlockSize() int64 {
	return int64(w.cmw.BlockSize())
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
	return w.cmw.Write(b)
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

func (w *tsspFileWriter) SwitchMetaBuffer() (int, error) {
	return w.cmw.SwitchMetaBuffer()
}

func (w *tsspFileWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	return w.cmw.MetaDataBlocks(dst)
}

var (
	indexWriterPool pool.FixedPool
	metaBlkPool     = sync.Pool{}
)

func InitWriterPool(size int) {
	stat := statistics.NewHitRatioStatistics()
	indexWriterPool.Reset(size, func() interface{} {
		return newIndexWriter()
	}, pool.NewHitRatioHook(stat.AddIndexWriterGetTotal, stat.AddIndexWriterHitTotal))

	fileops.InitWriterPool(size)
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
	//lint:ignore SA6002 argument should be pointer-like to avoid allocations
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

	swapper *FileSwapper
	tmp     []byte

	blockSize    int
	cacheMeta    bool
	limitCompact bool
	metas        [][]byte

	indexBlockSize int
}

func (w *indexWriter) Init(name string, lock *string, cacheMeta bool, limitCompact bool) {
	w.limitCompact = limitCompact
	w.name = name
	w.lock = lock
	w.cacheMeta = cacheMeta
	w.reset()

	if w.cacheMeta {
		w.buf = getMetaBlockBuffer(w.blockSize)
	} else {
		if cap(w.buf) >= w.blockSize {
			w.buf = w.buf[:w.blockSize]
		} else {
			w.buf = make([]byte, w.blockSize)
		}
	}
}

func (w *indexWriter) reset() {
	if w.swapper != nil {
		w.swapper.MustClose()
		w.swapper = nil
	}

	w.blockSize = fileops.DefaultWriterBufferSize
	w.metas = w.metas[:0]
	w.buf = w.buf[:0]
	w.n = 0
	w.wn = 0
	w.indexBlockSize = 0
}

func (w *indexWriter) flush() error {
	if w.n == 0 {
		return nil
	}

	n, err := w.swapper.Write(w.buf[0:w.n])
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
		if w.swapper == nil {
			w.swapper, err = NewFileSwapper(w.name, *w.lock, w.limitCompact, indexCompressMode)
			if err != nil {
				return 0, err
			}
		}

		var n int
		if w.buffered() == 0 {
			// Big data block, write directly from p to avoid copy
			n, err = w.swapper.Write(p)
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
	w.indexBlockSize += wn

	return wn, nil
}

func (w *indexWriter) writeCacheBlocks(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	w.wn += len(p)
	w.indexBlockSize += len(p)
	return len(p), nil
}

func (w *indexWriter) Write(p []byte) (nn int, err error) {
	if w.cacheMeta {
		return w.writeCacheBlocks(p)
	}

	return w.writeBuffer(p)
}

func (w *indexWriter) Close() error {
	if w.swapper != nil {
		w.swapper.MustClose()
		lock := fileops.FileLockOption(*w.lock)
		_ = fileops.Remove(w.name, lock)
		w.swapper = nil
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

	w.reset()
	indexWriterPool.Put(w)

	return nil
}

func (w *indexWriter) Size() int {
	return w.wn
}

func (w *indexWriter) BlockSize() int {
	return w.indexBlockSize
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
	return w.swapper == nil && w.n <= w.wn
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

	if len(w.tmp) <= 0 {
		w.tmp = bufferpool.Resize(w.tmp, fileops.DefaultBufferSize)
	}

	n, err := w.swapper.CopyTo(to, w.tmp)

	return int(n), err
}

func (w *indexWriter) SwitchMetaBuffer() (int, error) {
	if w.cacheMeta {
		w.metas = append(w.metas, w.buf)
		w.buf = getMetaBlockBuffer(len(w.buf))
	}
	size := w.indexBlockSize
	w.indexBlockSize = 0
	return size, nil
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

const (
	DataFile        = "segment.bin"
	ChunkMetaFile   = "segment.meta"
	MetaIndexFile   = "segment.idx"
	PrimaryKeyFile  = "primary.idx"
	PrimaryMetaFile = "primary.meta"
)

const (
	FD_OUTSIDE uint32 = 0x00001
)

type obsWriter struct {
	fd         fileops.File
	fileWriter fileops.BasicFileWriter // meta index writer with buffer
	fileSize   int64
	flag       uint32
}

func NewObsWriter(path, fileName string, obsOpts *obs.ObsOptions) (*obsWriter, error) {
	w := &obsWriter{}
	fd, err := fileops.OpenObsFile(path, fileName, obsOpts, false)
	if err != nil {
		return nil, err
	}
	lockPath := ""
	w.fd = fd
	w.fileWriter = fileops.NewFileWriter(w.fd, fileops.DefaultWriterBufferSize, &lockPath)

	fileFize, err := w.fd.Size()
	if err != nil {
		log.Error("execute stat() failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}
	w.fileSize = fileFize
	return w, nil
}

func NewObsWriterByFd(fd fileops.File, obsOpts *obs.ObsOptions) (*obsWriter, error) {
	w := &obsWriter{}
	lockPath := ""
	w.fd = fd
	w.fileWriter = fileops.NewFileWriter(w.fd, fileops.DefaultWriterBufferSize, &lockPath)

	fileFize, err := w.fd.Size()
	if err != nil {
		log.Error("execute stat() failed", zap.Error(err))
		return nil, err
	}
	w.fileSize = fileFize
	w.flag = w.flag | FD_OUTSIDE

	return w, nil
}

func (w *obsWriter) Write(b []byte) (int, error) {
	n, err := w.fileWriter.Write(b)
	if err != nil {
		return 0, err
	}
	w.fileSize += int64(n)

	return n, nil
}

func (w *obsWriter) GetFd() fileops.File {
	return w.fd
}

func (w *obsWriter) GetFileWriter() fileops.BasicFileWriter {
	return w.fileWriter
}

func (w *obsWriter) Size() int64 {
	return w.fileSize
}

func (w *obsWriter) Name() string {
	return w.fd.Name()
}

func (w *obsWriter) Close() error {
	if w.fileWriter != nil {
		if err := w.fileWriter.Close(); err != nil {
			log.Error("close data writer fail", zap.Error(err))
			return err
		}
		w.fileWriter = nil
		if w.flag&FD_OUTSIDE > 0 {
			return nil
		}
		return w.fd.Close()
	}
	return nil
}

// data and trunkdata in obs file
type obsFileWriter struct {
	dataWriter *obsWriter
	metaWriter *obsWriter
}

func newObsFileWriter(fd fileops.File, path string, obsOpts *obs.ObsOptions) (fileops.FileWriter, error) {
	dataWriter, err := NewObsWriterByFd(fd, obsOpts)
	if err != nil {
		log.Error("create obs writer for data file failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}
	metaWriter, err := NewObsWriter(path, ChunkMetaFile, obsOpts)
	if err != nil {
		log.Error("create obs writer for chunk data file failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	w := &obsFileWriter{
		dataWriter: dataWriter,
		metaWriter: metaWriter,
	}

	return w, nil
}

func (w *obsFileWriter) GetFileWriter() fileops.BasicFileWriter {
	return w.dataWriter.GetFileWriter()
}

func (w *obsFileWriter) DataSize() int64 {
	return w.dataWriter.Size()
}

func (w *obsFileWriter) ChunkMetaSize() int64 {
	return w.metaWriter.Size()
}

func (w *obsFileWriter) ChunkMetaBlockSize() int64 {
	return 0
}

func (w *obsFileWriter) WriteData(b []byte) (int, error) {
	return w.dataWriter.Write(b)
}

func (w *obsFileWriter) WriteChunkMeta(b []byte) (int, error) {
	return w.metaWriter.Write(b)
}

func (w *obsFileWriter) Close() error {
	if w.dataWriter != nil {
		err := w.dataWriter.Close()
		if err != nil {
			log.Error("close metaIndexWriter fail", zap.Error(err))
			return err
		}
		w.dataWriter = nil
	}

	if w.metaWriter != nil {
		err := w.metaWriter.Close()
		if err != nil {
			log.Error("close metaIndexWriter fail", zap.Error(err))
			return err
		}
		w.metaWriter = nil
	}
	return nil
}

func (w *obsFileWriter) AppendChunkMetaToData() error {
	return nil
}

func (w *obsFileWriter) SwitchMetaBuffer() (int, error) {
	return 0, nil
}

func (w *obsFileWriter) MetaDataBlocks(dst [][]byte) [][]byte {
	return nil
}

func (w *obsFileWriter) Name() string {
	return w.dataWriter.Name()
}

type obsIndexWriter struct {
	metaIndexWriter      *obsWriter
	primaryKeyWriter     *obsWriter
	PrimaryKeyMetaWriter *obsWriter
	bloomfilterWriters   []*obsWriter
}

func newObsIndexFileWriter(path string, obsOpts *obs.ObsOptions, bfCols []string, fullTextIdx bool) (fileops.MetaWriter, error) {
	w := newObsIndexWriter(bfCols, fullTextIdx)

	var err error
	// metaindex
	w.metaIndexWriter, err = NewObsWriter(path, MetaIndexFile, obsOpts)
	if err != nil {
		log.Error("NewObsWriter for metaIndex failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	// primary key
	w.primaryKeyWriter, err = NewObsWriter(path, PrimaryKeyFile, obsOpts)
	if err != nil {
		log.Error("NewObsWriter for primary key failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	// primary key meta
	w.PrimaryKeyMetaWriter, err = NewObsWriter(path, PrimaryMetaFile, obsOpts)
	if err != nil {
		log.Error("NewObsWriter for primary key meta failed", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	// new bloomfilter writer
	return w.newBloomFilterWriter(path, bfCols, fullTextIdx, obsOpts)
}

func newObsIndexWriter(bfCols []string, fullTextIdx bool) *obsIndexWriter {
	if fullTextIdx {
		return &obsIndexWriter{
			bloomfilterWriters: make([]*obsWriter, sparseindex.FullTextIdxColumnCnt),
		}
	}
	return &obsIndexWriter{
		bloomfilterWriters: make([]*obsWriter, len(bfCols)),
	}
}

func (w *obsIndexWriter) newBloomFilterWriter(path string, bfCols []string, fullTextIdx bool, obsOpts *obs.ObsOptions) (fileops.MetaWriter, error) {
	var err error
	var fileName string
	for i := 0; i < len(w.bloomfilterWriters); i++ {
		if fullTextIdx {
			fileName = sparseindex.BloomFilterFilePrefix + sparseindex.FullTextIndex + sparseindex.BloomFilterFileSuffix
		} else {
			fileName = sparseindex.BloomFilterFilePrefix + bfCols[i] + sparseindex.BloomFilterFileSuffix
		}
		w.bloomfilterWriters[i], err = NewObsWriter(path, fileName, obsOpts)
		if err != nil {
			log.Error("NewObsWriter for bloomfilter failed", zap.String("path", path), zap.Bool("fullTextIndex", fullTextIdx), zap.String("path", bfCols[i]), zap.Error(err))
			return nil, err
		}
	}
	return w, nil
}

func (w *obsIndexWriter) WriteMetaIndex(b []byte) (int, error) {
	return w.metaIndexWriter.Write(b)
}

func (w *obsIndexWriter) WritePrimaryKey(b []byte) (int, error) {
	return w.primaryKeyWriter.Write(b)
}

func (w *obsIndexWriter) WritePrimaryKeyMeta(b []byte) (int, error) {
	return w.PrimaryKeyMetaWriter.Write(b)
}

func (w *obsIndexWriter) WriteBloomFilter(bfIdx int, b []byte) (int, error) {
	return w.bloomfilterWriters[bfIdx].Write(b)
}

func (w *obsIndexWriter) GetMetaIndexSize() int64 {
	return w.metaIndexWriter.Size()
}

func (w *obsIndexWriter) GetPrimaryKeySize() int64 {
	return w.primaryKeyWriter.Size()
}

func (w *obsIndexWriter) GetPrimaryKeyMetaSize() int64 {
	return w.PrimaryKeyMetaWriter.Size()
}

func (w *obsIndexWriter) GetBloomFilterSize(bfIdx int) int64 {
	return w.bloomfilterWriters[bfIdx].Size()
}

func (w *obsIndexWriter) GetPrimaryKeyHandler() fileops.File {
	return w.primaryKeyWriter.GetFd()
}

func (w *obsIndexWriter) GetPrimaryKeyMetaHandler() fileops.File {
	return w.PrimaryKeyMetaWriter.GetFd()
}

func (w *obsIndexWriter) Close() error {
	if w.metaIndexWriter != nil {
		err := w.metaIndexWriter.Close()
		if err != nil {
			log.Error("close metaIndexWriter fail", zap.Error(err))
			return err
		}
		w.metaIndexWriter = nil
	}

	if w.PrimaryKeyMetaWriter != nil {
		err := w.PrimaryKeyMetaWriter.Close()
		if err != nil {
			log.Error("close PrimaryKeyMetaWriter fail", zap.Error(err))
			return err
		}
		w.PrimaryKeyMetaWriter = nil
	}

	for i := range w.bloomfilterWriters {
		if w.bloomfilterWriters[i] != nil {
			err := w.bloomfilterWriters[i].Close()
			if err != nil {
				log.Error("close PrimaryKeyMetaWriter fail", zap.Error(err))
				return err
			}
			w.bloomfilterWriters[i] = nil
		}
	}
	w.bloomfilterWriters = w.bloomfilterWriters[:0]
	return nil
}
