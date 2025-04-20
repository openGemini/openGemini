/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package raftlog

import (
	"bufio"
	"encoding/binary"
	"io"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"go.uber.org/zap"
)

const unit32Size = 4 // the byte size of unit32
const unit64Size = 8 // the byte size of unit64

var NewFile = errors.New("Create a new file")

var cache *FileWrapCache = NewCache() // cache files not Updated of limit size

var fileWrapPool *pool.UnionPool[FileWrap]

func init() {
	fileWrapPool = pool.NewUnionPool[FileWrap](config.FileWrapSize, 0, 0, NewNilFileWrap)
}

type FileWrapper interface {
	Name() string
	Size() int
	GetEntryData(start, end int) []byte
	Write(dat []byte) (int, error)
	WriteAt(offset int64, dat []byte) (int, error)
	WriteSlice(offset int64, dat []byte) error
	ReadSlice(offset int64) []byte
	SliceSize(offset int) int
	Truncate(size int64) error
	TrySync() error
	Delete() error
	Close() error
	setCurrent()
	rotateCurrent()
}

// FileWrap represents a file and includes both the buffer to the data
// and the file descriptor.
type FileWrap struct {
	fd      fileops.File
	data    []byte
	current bool
	mu      sync.Mutex
}

func NewNilFileWrap() *FileWrap {
	return nil
}

func NewFileWrap(size int) *FileWrap {
	return &FileWrap{data: make([]byte, size)}
}

func (fw *FileWrap) reset() {
	fw.data = fw.data[:0]
	fw.current = false
	fw.fd = nil
}

func (fw *FileWrap) reSize(size int) {
	if cap(fw.data) < size {
		fw.data = append(fw.data[:cap(fw.data)], make([]byte, size-cap(fw.data))...)
	}
	fw.data = fw.data[:size]
}

func (fw *FileWrap) setCurrent() {
	fw.current = false
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("setCurrent get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return
	}
	fw.current = true
	fw.data = content
}

func (fw *FileWrap) rotateCurrent() {
	fw.current = false
	cache.cache.Add(fw.Name(), fw.data)
}

// OpenFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In case the file is created, z.NewFile is
// returned.
func OpenFile(fpath string, flag int, maxSz int) (FileWrapper, error) {
	fd, err := fileops.OpenFile(fpath, flag, 0600, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", fpath)
	}

	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var fw *FileWrap
	if strings.HasSuffix(fpath, metaName) {
		fw = &FileWrap{
			fd: fd,
		}
	} else {
		fw = fileWrapPool.Get()
		if fw == nil {
			fw = NewFileWrap(int(fi.Size()))
		}
		fw.reSize(int(fi.Size()))
		fw.fd = fd
	}

	var rerr error
	fileSize := fi.Size()
	if maxSz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.
		var buff = make([]byte, maxSz)
		if _, err = fw.Write(buff); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		if err = fw.TrySync(); err != nil {
			return nil, errors.Wrapf(err, "error while trySync")
		}
		rerr = NewFile
	} else {
		reader := bufio.NewReader(fd)
		if strings.HasSuffix(fpath, metaName) {
			content, err := io.ReadAll(reader)
			if err != nil {
				return nil, errors.Wrapf(err, "error while reading meta file:%s", fw.Name())
			}
			fw.data = content
		} else {
			n, err := io.ReadFull(reader, fw.data)
			if err != nil || n != len(fw.data) {
				return nil, errors.Wrapf(err, "error while reading raftlog file:%s, n:%d len(fw.data):%d", fw.Name(), n, len(fw.data))
			}
			cache.cache.Add(fw.Name(), fw.data)
		}
	}
	return fw, rerr
}

func (fw *FileWrap) getContent() ([]byte, error) {
	// first judge is current log file
	if fw.current || fw.isMetaFile() {
		return fw.data, nil
	}
	// get content from cache
	content, ok := cache.cache.Get(fw.Name())
	if ok && len(content) != 0 {
		return content, nil
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	// read from file begin
	_, err := fw.fd.Seek(0, io.SeekStart)
	if err != nil {
		logger.GetLogger().Error("file seek error ", zap.String("fw name", fw.Name()), zap.Error(err))
	}
	// read file to get content
	reader := bufio.NewReader(fw.fd)
	size, err := fw.fd.Size()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get size of file: %s", fw.fd.Name())
	}
	fw.reSize(int(size))
	n, err := io.ReadFull(reader, fw.data)
	if err != nil || n != len(fw.data) {
		return nil, errors.Wrapf(err, "error while reading raftlog file:%s, n:%d len(fw.data):%d", fw.Name(), n, len(fw.data))
	}
	cache.cache.Add(fw.Name(), fw.data)
	return fw.data, nil
}

func (fw *FileWrap) Name() string {
	if fw.fd == nil {
		return ""
	}
	return fw.fd.Name()
}

func (fw *FileWrap) isMetaFile() bool {
	return strings.HasSuffix(fw.Name(), metaName)
}

// todo: don't need getContent, clear by size will be slow when fileNum is big, which read from disk mostly
func (fw *FileWrap) Size() int {
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("fw.Size() get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
	}
	return len(content)
}

// GetEntryData returns entry data
func (fw *FileWrap) GetEntryData(start, end int) []byte {
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("GetEntryData get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return nil
	}
	return content[start:end]
}

func (fw *FileWrap) Write(dat []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed:%s", fw.Name())
	}

	if fw.current || fw.isMetaFile() {
		fw.data = append(fw.data, dat...)
	} else {
		// try to clean cache
		cache.cache.Remove(fw.Name())
	}
	return n, err
}

func (fw *FileWrap) WriteAt(offset int64, dat []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return 0, errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
	}
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	if fw.current || fw.isMetaFile() {
		copy(fw.data[offset:], dat)
	} else {
		// try to clean cache
		cache.cache.Remove(fw.Name())
	}
	_, err = fw.fd.Seek(0, io.SeekEnd)
	return n, errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrap) WriteSlice(offset int64, dat []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
	}
	var buff = make([]byte, 0, unit32Size)
	buff = encoding.MarshalUint32(buff, uint32(len(dat))) // size
	_, err = fw.fd.Write(buff)
	if err != nil {
		return errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	_, err = fw.fd.Write(dat) // data
	if err != nil {
		return errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	_, err = fw.fd.Seek(0, io.SeekEnd)
	if fw.current || fw.isMetaFile() {
		if int(offset)+unit32Size+len(dat) >= len(fw.data) {
			fw.reSize(int(offset) + unit32Size + len(dat))
		}
		dst := fw.data[offset:]
		binary.BigEndian.PutUint32(dst[:unit32Size], uint32(len(dat))) // size
		copy(dst[unit32Size:], dat)                                    // data
	} else {
		// try to clean cache
		cache.cache.Remove(fw.Name())
	}

	return errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrap) ReadSlice(offset int64) []byte {
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("ReadSlice get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return []byte{}
	}

	sz := encoding.UnmarshalUint32(content[offset:])
	start := offset + unit32Size
	next := int(start) + int(sz)
	if next > len(content) {
		return []byte{}
	}
	return content[start:next]
}

func (fw *FileWrap) SliceSize(offset int) int {
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("SliceSize get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return 0
	}
	sz := encoding.UnmarshalUint32(content[offset:])
	return unit32Size + int(sz)
}

func (fw *FileWrap) Truncate(size int64) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.fd.Truncate(size)
}

func (fw *FileWrap) Delete() error {
	cache.cache.Remove(fw.Name())
	// Badger can set the m.Data directly, without setting any Fd. In that case, this should be a
	// NOOP.
	if fw.fd == nil {
		return nil
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.fd.Close(); err != nil {
		return errors.Wrapf(err, "while close file:%s", fw.Name())
	}
	if err := fileops.Remove(fw.fd.Name()); err != nil {
		return errors.Wrapf(err, "while remove file:%s", fw.Name())
	}
	fw.reset()
	fileWrapPool.Put(fw)
	return nil
}

func (fw *FileWrap) Close() error {
	if err := fw.TrySync(); err != nil {
		return errors.Wrapf(err, "sync error:%s", fw.Name())
	}
	cache.cache.Remove(fw.Name())
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.fd.Close(); err != nil {
		return errors.Wrapf(err, "fw close error:%s", fw.Name())
	}
	fw.reset()
	fileWrapPool.Put(fw)
	return nil
}

func (fw *FileWrap) TrySync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.fd == nil {
		return nil
	}
	return fw.fd.Sync()
}
