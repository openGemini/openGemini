/*
Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.

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
	"io"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type fileSlotCache struct {
	data       []byte
	slot       []byte
	sz         uint32
	slotCached bool
	szCached   bool
}

// FileWrapV2 represents a file and includes fileSlotCaches, current data cache, metaCache
// and the file descriptor.
type FileWrapV2 struct {
	fd        fileops.File
	mu        sync.RWMutex
	cache     []fileSlotCache
	metaCache []byte
	current   bool
}

func NewFileWrapV2(fd fileops.File) *FileWrapV2 {
	return &FileWrapV2{fd: fd}
}

func (fw *FileWrapV2) setCurrent() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.current = true
}

func (fw *FileWrapV2) rotateCurrent() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.current = false
	for i := range fw.cache {
		fw.cache[i].data = nil
	}
}

// OpenFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In case the file is created, z.NewFile is
// returned.
func OpenFileV2(fpath string, flag int, maxSz int) (FileWrapper, error) {
	fd, err := fileops.OpenFile(fpath, flag, 0600, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", fpath)
	}

	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	fw := NewFileWrapV2(fd)

	var rerr error
	fileSize := fi.Size()
	if maxSz > 0 && fileSize == 0 {
		var buff = make([]byte, maxSz)
		if _, err = fw.Write(buff); err != nil {
			fd.Close()
			return nil, errors.Wrapf(err, "error while truncation")
		}
		if err = fw.TrySync(); err != nil {
			fd.Close()
			return nil, errors.Wrapf(err, "error while trySync")
		}
		rerr = NewFile
	}
	if strings.HasSuffix(fpath, metaName) {
		_, err = fw.fd.Seek(0, io.SeekStart)
		if err != nil {
			logger.GetLogger().Error("file seek error ", zap.String("fw name", fw.Name()), zap.Error(err))
		}
		reader := bufio.NewReader(fd)
		fw.metaCache = make([]byte, maxSz)
		n, err := io.ReadFull(reader, fw.metaCache)
		if err != nil || n != len(fw.metaCache) {
			fd.Close()
			return nil, errors.Wrapf(err, "error while reading meta file:%s n:%d, len:%d", fw.Name(), n, len(fw.metaCache))
		}
	}
	return fw, rerr
}

func (fw *FileWrapV2) Name() string {
	if fw.fd == nil {
		return ""
	}
	return fw.fd.Name()
}

func (fw *FileWrapV2) Size() int {
	fw.mu.RLock()
	defer fw.mu.RUnlock()
	size, err := fw.fd.Size()
	if err != nil {
		logger.GetLogger().Error("fw.Size() get content error ", zap.String("fw name", fw.Name()), zap.Error(err))
		return 0
	}
	return int(size)
}

// GetEntryData returns entry data
func (fw *FileWrapV2) GetEntryData(slotIdx int, start, end int, isMeta bool) []byte {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if isMeta {
		return fw.metaCache[start:end]
	}
	if len(fw.cache) <= slotIdx || !fw.cache[slotIdx].slotCached {
		data := make([]byte, end-start)
		n, err := fw.fd.ReadAt(data, int64(start))
		if err != nil || n != len(data) {
			logger.GetLogger().Error("GetEntryData get content error ", zap.String("fw name", fw.Name()), zap.Int("n", n), zap.Int("start", start),
				zap.Int("end", end), zap.Error(err))
			return nil
		}
		if len(fw.cache) <= slotIdx {
			fw.cache = append(fw.cache, make([]fileSlotCache, slotIdx-len(fw.cache)+1)...)
		}
		fw.cache[slotIdx].slotCached = true
		fw.cache[slotIdx].slot = data
		return data
	}
	return fw.cache[slotIdx].slot
}

func (fw *FileWrapV2) Write(dat []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed:%s", fw.Name())
	}
	return n, err
}

func (fw *FileWrapV2) WriteAt(slotIdx int, offset int64, dat []byte, isMeta bool) (int, error) {
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
	if !isMeta {
		if len(fw.cache) <= slotIdx {
			fw.cache = append(fw.cache, make([]fileSlotCache, slotIdx-len(fw.cache)+1)...)
		}
		fw.cache[slotIdx].slot = dat
		fw.cache[slotIdx].slotCached = true
	} else {
		copy(fw.metaCache[offset:], dat)
	}

	return n, errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrapV2) WriteSlice(slotIdx int, endSlotIdx int, offset int64, dat []byte, isMeta bool, clearSlots bool) error {
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

	if clearSlots {
		for i := slotIdx; i < endSlotIdx && i < len(fw.cache); i++ {
			fw.cache[i].slotCached = false
			fw.cache[i].slot = nil
			fw.cache[i].szCached = false
			fw.cache[i].sz = 0
			fw.cache[i].data = nil
		}
	}

	if !isMeta && len(fw.cache) <= slotIdx {
		if len(fw.cache) <= slotIdx {
			fw.cache = append(fw.cache, make([]fileSlotCache, slotIdx-len(fw.cache)+1)...)
		}
		fw.cache[slotIdx].szCached = true
		fw.cache[slotIdx].sz = uint32(len(dat))
		if fw.current && len(fw.cache[slotIdx].data) == 0 {
			fw.cache[slotIdx].data = append(fw.cache[slotIdx].data, dat...)
		}
	}
	if isMeta {
		copy(fw.metaCache[offset:], buff)
		copy(fw.metaCache[offset+int64(len(buff)):], dat)
	}

	return errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrapV2) ReadSlice(slotIdx int, offset int64, isMeta bool) []byte {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if isMeta {
		sz := encoding.UnmarshalUint32(fw.metaCache[offset : offset+unit32Size])
		start := offset + unit32Size
		end := start + int64(sz)
		return fw.metaCache[start:end]
	}
	var sz uint32
	if len(fw.cache) > slotIdx && fw.cache[slotIdx].szCached {
		sz = fw.cache[slotIdx].sz
		if fw.current && len(fw.cache[slotIdx].data) > 0 {
			return fw.cache[slotIdx].data
		}
	} else {
		data1 := make([]byte, unit32Size)
		n, err := fw.fd.ReadAt(data1, offset)
		if err != nil || n != len(data1) {
			logger.GetLogger().Error("ReadSlice get size error ", zap.String("fw name", fw.Name()), zap.Int("n", n), zap.Error(err))
			return nil
		}
		sz = encoding.UnmarshalUint32(data1)
	}

	start := offset + unit32Size
	end := start + int64(sz)

	data2 := make([]byte, end-start)
	n, err := fw.fd.ReadAt(data2, start)
	if err != nil || n != len(data2) {
		logger.GetLogger().Error("ReadSlice get content error ", zap.String("fw name", fw.Name()), zap.Int("n", n), zap.Int64("start", start),
			zap.Int64("end", end), zap.Error(err))
		return nil
	}
	if !isMeta {
		if len(fw.cache) <= slotIdx {
			fw.cache = append(fw.cache, make([]fileSlotCache, slotIdx-len(fw.cache)+1)...)
		}
		fw.cache[slotIdx].szCached = true
		fw.cache[slotIdx].sz = sz
		if fw.current && len(fw.cache[slotIdx].data) == 0 {
			fw.cache[slotIdx].data = append(fw.cache[slotIdx].data, data2...)
		}
	}

	return data2
}

func (fw *FileWrapV2) SliceSize(slotIdx int, offset int) int {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.cache) <= slotIdx || !fw.cache[slotIdx].szCached {
		data1 := make([]byte, unit32Size)
		n, err := fw.fd.ReadAt(data1, int64(offset))
		if err != nil || n != len(data1) {
			logger.GetLogger().Error("SliceSize get size error ", zap.String("fw name", fw.Name()), zap.Int("n", n), zap.Error(err))
			return 0
		}
		sz := encoding.UnmarshalUint32(data1)
		if len(fw.cache) <= slotIdx {
			fw.cache = append(fw.cache, make([]fileSlotCache, slotIdx-len(fw.cache)+1)...)
		}
		fw.cache[slotIdx].szCached = true
		fw.cache[slotIdx].sz = sz
		return unit32Size + int(sz)
	}
	return int(fw.cache[slotIdx].sz) + unit32Size
}

func (fw *FileWrapV2) Truncate(size int64) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.fd.Truncate(size)
}

func (fw *FileWrapV2) Delete() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.fd == nil {
		return nil
	}
	if err := fw.fd.Close(); err != nil {
		return errors.Wrapf(err, "while close file:%s", fw.Name())
	}
	if err := fileops.Remove(fw.fd.Name()); err != nil {
		return errors.Wrapf(err, "while remove file:%s", fw.Name())
	}
	return nil
}

func (fw *FileWrapV2) Close() error {
	if err := fw.TrySync(); err != nil {
		return errors.Wrapf(err, "sync error:%s", fw.Name())
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.fd.Close(); err != nil {
		return errors.Wrapf(err, "fw close error:%s", fw.Name())
	}
	return nil
}

func (fw *FileWrapV2) TrySync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.fd == nil {
		return nil
	}
	return fw.fd.Sync()
}
