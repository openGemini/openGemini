package filestream

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/openGemini/openGemini/lib/fileops"
)

const dontNeedBlockSize = 16 * 1024 * 1024

// ReadCloser is a standard interface for filestream Reader.
type ReadCloser interface {
	Read(p []byte) (int, error)
	MustClose()
}

// WriteCloser is a standard interface for filestream Writer.
type WriteCloser interface {
	Write(p []byte) (int, error)
	MustClose()
}

func getBufferSize() int {
	bufferSizeOnce.Do(func() {
		n := memory.Allowed() / 1024 / 8
		if n < 4*1024 {
			n = 4 * 1024
		}
		if n > 512*1024 {
			n = 512 * 1024
		}
		bufferSize = n
	})
	return bufferSize
}

var (
	bufferSize     int
	bufferSizeOnce sync.Once
)

// Reader implements buffered file reader.
type Reader struct {
	f  fileops.File
	br *bufio.Reader
	st streamTracker
}

// OpenReaderAt opens the file at the given path in nocache mode at the given offset.
//
// If nocache is set, then the reader doesn't pollute OS page cache.
func OpenReaderAt(path string, offset int64, nocache bool) (*Reader, error) {
	r, err := Open(path, nocache)
	if err != nil {
		return nil, err
	}
	n, err := r.f.Seek(offset, io.SeekStart)
	if err != nil {
		r.MustClose()
		return nil, fmt.Errorf("cannot seek to offset=%d for %q: %w", offset, path, err)
	}
	if n != offset {
		r.MustClose()
		return nil, fmt.Errorf("invalid seek offset for %q; got %d; want %d", path, n, offset)
	}
	return r, nil
}

// Open opens the file from the given path in nocache mode.
//
// If nocache is set, then the reader doesn't pollute OS page cache.
func Open(path string, nocache bool) (*Reader, error) {
	f, err := fileops.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %q: %w", path, err)
	}
	r := &Reader{
		f:  f,
		br: getBufioReader(f),
	}
	if nocache && fileops.GetFsType(path) != fileops.Obs {
		r.st.fd = f.Fd()
		r.st.file = f
	}
	return r, nil
}

// MustClose closes the underlying file passed to Open.
func (r *Reader) MustClose() {
	if err := r.st.close(); err != nil {
		logger.Panicf("FATAL: cannot close streamTracker for file %q: %s", r.f.Name(), err)
	}
	if err := r.f.Close(); err != nil {
		logger.Panicf("FATAL: cannot close file %q: %s", r.f.Name(), err)
	}
	r.f = nil

	putBufioReader(r.br)
	r.br = nil
}

// Read reads file contents to p.
func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.br.Read(p)
	if err != nil {
		return n, err
	}
	if err := r.st.adviseDontNeed(n, false); err != nil {
		return n, fmt.Errorf("advise error for %q: %w", r.f.Name(), err)
	}
	return n, nil
}

type statReader struct {
	fileops.File
}

func (sr *statReader) Read(p []byte) (int, error) {
	n, err := sr.File.Read(p)
	return n, err
}

func getBufioReader(f fileops.File) *bufio.Reader {
	sr := &statReader{f}
	v := brPool.Get()
	if v == nil {
		return bufio.NewReaderSize(sr, getBufferSize())
	}
	br := v.(*bufio.Reader)
	br.Reset(sr)
	return br
}

func putBufioReader(br *bufio.Reader) {
	brPool.Put(br)
}

var brPool sync.Pool

// Writer implements buffered file writer.
type Writer struct {
	f  fileops.File
	bw *bufio.Writer
	st streamTracker
}

// OpenWriterAt opens the file at path in nocache mode for writing at the given offset.
//
// The file at path is created if it is missing.
//
// If nocache is set, the writer doesn't pollute OS page cache.
func OpenWriterAt(path string, lockPath *string, offset int64, nocache bool) (*Writer, error) {
	lock := fileops.FileLockOption(*lockPath)
	f, err := fileops.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600, lock)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", path, err)
	}
	n, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cannot seek to offset=%d in %q: %w", offset, path, err)
	}
	if n != offset {
		_ = f.Close()
		return nil, fmt.Errorf("invalid seek offset for %q; got %d; want %d", path, n, offset)
	}
	return newWriter(f, nocache), nil
}

// Create creates the file for the given path in nocache mode.
//
// If nocache is set, the writer doesn't pollute OS page cache.
func Create(path string, lockPath *string, nocache bool) (*Writer, error) {
	lock := fileops.FileLockOption(*lockPath)
	f, err := fileops.CreateV2(path, lock)
	if err != nil {
		return nil, fmt.Errorf("cannot create file %q: %w", path, err)
	}
	return newWriter(f, nocache), nil
}

func newWriter(f fileops.File, nocache bool) *Writer {
	w := &Writer{
		f:  f,
		bw: getBufioWriter(f),
	}
	if nocache {
		w.st.fd = f.Fd()
		w.st.file = f
	}
	return w
}

// MustClose syncs the underlying file to storage and then closes it.
func (w *Writer) MustClose() {
	if err := w.bw.Flush(); err != nil {
		logger.Panicf("FATAL: cannot flush buffered data to file %q: %s", w.f.Name(), err)
	}
	putBufioWriter(w.bw)
	w.bw = nil

	if err := w.f.Sync(); err != nil {
		logger.Panicf("FATAL: cannot sync file %q: %d", w.f.Name(), err)
	}
	if err := w.st.close(); err != nil {
		logger.Panicf("FATAL: cannot close streamTracker for file %q: %s", w.f.Name(), err)
	}
	if err := w.f.Close(); err != nil {
		logger.Panicf("FATAL: cannot close file %q: %s", w.f.Name(), err)
	}
	w.f = nil
}

// Write writes p to the underlying file.
func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.bw.Write(p)
	if err != nil {
		return n, err
	}
	if err := w.st.adviseDontNeed(n, true); err != nil {
		return n, fmt.Errorf("advise error for %q: %w", w.f.Name(), err)
	}
	return n, nil
}

// MustFlush flushes all the buffered data to file.
//
// if isSync is true, then the flushed data is fsynced to the underlying storage.
func (w *Writer) MustFlush(isSync bool) {
	if err := w.bw.Flush(); err != nil {
		logger.Panicf("FATAL: cannot flush buffered data to file %q: %s", w.f.Name(), err)
	}
	if isSync {
		if err := w.f.Sync(); err != nil {
			logger.Panicf("FATAL: cannot fsync data to the underlying storage for file %q: %s", w.f.Name(), err)
		}
	}
}

type statWriter struct {
	fileops.File
}

func (sw *statWriter) Write(p []byte) (int, error) {
	n, err := sw.File.Write(p)
	return n, err
}

func getBufioWriter(f fileops.File) *bufio.Writer {
	sw := &statWriter{f}
	v := bwPool.Get()
	if v == nil {
		return bufio.NewWriterSize(sw, getBufferSize())
	}
	bw := v.(*bufio.Writer)
	bw.Reset(sw)
	return bw
}

func putBufioWriter(bw *bufio.Writer) {
	bwPool.Put(bw)
}

var bwPool sync.Pool

type streamTracker struct {
	file   fileops.File
	fd     uintptr
	offset uint64
	length uint64
}
