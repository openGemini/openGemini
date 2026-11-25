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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding"
	"go.uber.org/zap"
)

const unit32Size = 4 // the byte size of unit32
const unit64Size = 8 // the byte size of unit64

var ErrNewFile = errors.New("create a new file")

var cache *FileWrapCache = NewCache() // cache files not Updated of limit size
// todo: don't read from fileStart when getContent() of noCache file which only supports append updates

var fileWrapPool *pool.UnionPool[FileWrap]

func init() {
	fileWrapPool = pool.NewUnionPool[FileWrap](config.FileWrapSize, 0, 0, NewNilFileWrap)
}

type FileWrapper interface {
	Name() string
	Size() int
	GetEntryData(slotIdx int, start, end int, isMeta bool) []byte
	Write(dat []byte) (int, error)
	WriteAt(slotIdx int, offset int64, dat []byte, isMeta bool) (int, error)
	WriteSlice(slotIdx int, endSlotIdx int, offset int64, dat []byte, isMeta bool, clearSlots bool) error
	ReadSlice(slotIdx int, offset int64, isMeta bool) []byte
	SliceSize(slotIdx int, offset int) int
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
		return nil, fmt.Errorf("unable to open: %s: %w", fpath, err)
	}

	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot stat file: %s: %w", filename, err)
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
			return nil, fmt.Errorf("error while truncation: %w", err)
		}
		if err = fw.TrySync(); err != nil {
			return nil, fmt.Errorf("error while trySync: %w", err)
		}
		rerr = ErrNewFile
	} else {
		reader := bufio.NewReader(fd)
		if strings.HasSuffix(fpath, metaName) {
			content, err := io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("error while reading meta file:%s: %w", fw.Name(), err)
			}
			fw.data = content
		} else {
			n, err := io.ReadFull(reader, fw.data)
			if err != nil || n != len(fw.data) {
				return nil, fmt.Errorf("error while reading raftlog file:%s, n:%d len(fw.data):%d: %w", fw.Name(), n, len(fw.data), err)
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
		return nil, fmt.Errorf("cannot get size of file: %s: %w", fw.fd.Name(), err)
	}
	fw.reSize(int(size))
	n, err := io.ReadFull(reader, fw.data)
	if err != nil || n != len(fw.data) {
		return nil, fmt.Errorf("error while reading raftlog file:%s, n:%d len(fw.data):%d: %w", fw.Name(), n, len(fw.data), err)
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
		return 0
	}
	return len(content)
}

// GetEntryData returns entry data
func (fw *FileWrap) GetEntryData(slotIdx int, start, end int, isMeta bool) []byte {
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
		return 0, fmt.Errorf("write failed:%s: %w", fw.Name(), err)
	}

	if fw.current || fw.isMetaFile() {
		fw.data = append(fw.data, dat...)
	} else {
		// try to clean cache
		cache.cache.Remove(fw.Name())
	}
	return n, err
}

func (fw *FileWrap) WriteAt(slotIdx int, offset int64, dat []byte, isMeta bool) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return 0, fmt.Errorf("seek unreachable file:%s: %w", fw.Name(), err)
	}
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, fmt.Errorf("write failed for file:%s: %w", fw.Name(), err)
	}
	if fw.current || fw.isMetaFile() {
		copy(fw.data[offset:], dat)
	} else {
		// try to clean cache
		cache.cache.Remove(fw.Name())
	}
	_, err = fw.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return n, fmt.Errorf("seek unreachable file:%s: %w", fw.Name(), err)
	}
	return n, nil
}

func (fw *FileWrap) WriteSlice(slotIdx int, endSlotIdx int, offset int64, dat []byte, isMeta bool, clearSlots bool) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("seek unreachable file:%s: %w", fw.Name(), err)
	}
	var buff = make([]byte, 0, unit32Size)
	buff = encoding.MarshalUint32(buff, uint32(len(dat))) // size
	_, err = fw.fd.Write(buff)
	if err != nil {
		return fmt.Errorf("write failed for file:%s: %w", fw.Name(), err)
	}
	_, err = fw.fd.Write(dat) // data
	if err != nil {
		return fmt.Errorf("write failed for file:%s: %w", fw.Name(), err)
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

	if err != nil {
		return fmt.Errorf("seek unreachable file:%s: %w", fw.Name(), err)
	}
	return nil
}

func (fw *FileWrap) ReadSlice(slotIdx int, offset int64, isMeta bool) []byte {
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

func (fw *FileWrap) SliceSize(slotIdx int, offset int) int {
	content, err := fw.getContent()
	if err != nil {
		logger.GetLogger().Error("SliceSize get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return 0
	}
	if len(content) <= offset {
		logger.GetLogger().Error("SliceSize get content len(content) <= offset ", zap.String("fw name", fw.Name()),
			zap.Int("len(content)", len(content)), zap.Int("offset", offset))
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
		return fmt.Errorf("while close file:%s: %w", fw.Name(), err)
	}
	if err := fileops.Remove(fw.fd.Name()); err != nil {
		return fmt.Errorf("while remove file:%s: %w", fw.Name(), err)
	}
	fw.reset()
	fileWrapPool.Put(fw)
	return nil
}

func (fw *FileWrap) Close() error {
	if err := fw.TrySync(); err != nil {
		return fmt.Errorf("sync error:%s: %w", fw.Name(), err)
	}
	cache.cache.Remove(fw.Name())
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.fd.Close(); err != nil {
		return fmt.Errorf("fw close error:%s: %w", fw.Name(), err)
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

func (fw *FileWrap) MemSize() int {
	return cap(fw.data)
}

func (fw *FileWrap) Instance() *FileWrap {
	return fw
}
