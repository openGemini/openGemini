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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/readcache"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

var hitRatioStat = statistics.NewHitRatioStatistics()

var MmapEn = false
var ReadMetaCacheEn = false
var ReadDataCacheEn = false

var (
	errMapFail    = func(v ...interface{}) error { return errno.NewError(errno.MapFileFailed, v...) }
	errReadFail   = func(v ...interface{}) error { return errno.NewError(errno.ReadFileFailed, v...) }
	errOpenFail   = func(v ...interface{}) error { return errno.NewError(errno.OpenFileFailed, v...) }
	errCloseFail  = func(v ...interface{}) error { return errno.NewError(errno.MapFileFailed, v...) }
	errRenameFail = func(v ...interface{}) error { return errno.NewError(errno.RenameFileFailed, v...) }
)

func EnableMmapRead(en bool) {
	MmapEn = en
}

func SetPageSize(confPageSize string) {
	readcache.SetPageSizeByConf(confPageSize)
}

func SetMetaPageList(confMetaPageList []string) {
	readcache.SetMataPageListByConf(confMetaPageList)
}

func EnableReadMetaCache(en uint64) {
	if en > 0 {
		ReadMetaCacheEn = true
		readcache.SetReadMetaCacheLimitSize(en)
	} else {
		ReadMetaCacheEn = false
	}
}

func EnableReadDataCache(en uint64) {
	if en > 0 {
		ReadDataCacheEn = true
		readcache.SetReadDataCacheLimitSize(en)
	} else {
		ReadDataCacheEn = false
	}
}

type BasicFileReader interface {
	Name() string
	Size() (int64, error)
	ReadAt(off int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error)
	StreamReadBatch(off, length []int64, c chan *request.StreamReader, limit int, isStat bool)
	Rename(newName string) error
	RenameOnObs(newName string, tmp bool, obsOpt *obs.ObsOptions) error
	ReOpen() error
	IsMmapRead() bool
	IsOpen() bool
	FreeFileHandle() error
	Close() error
	ReadAll(dst []byte) ([]byte, error)
}

type fileReader struct {
	fd       File
	name     string
	fileSize int64
	lock     *string
	mmapData []byte
	once     *sync.Once
}

func NewFileReader(f File, lock *string) *fileReader {
	fName := f.Name()
	fi, err := f.Stat()
	if err != nil {
		log.Error("stat file fail", zap.String("file", fName), zap.Error(err))
		panic(err)
	}

	fileSize := fi.Size()
	r := &fileReader{fd: f, fileSize: fileSize, lock: lock, name: fName, once: new(sync.Once)}

	if MmapEn {
		r.mmapData, err = Mmap(int(f.Fd()), 0, int(fileSize))
		if err != nil {
			err = errMapFail(fName, err)
			log.Error("mmap file fail", zap.Error(err))
		}
	}

	return r
}

func (r *fileReader) IsMmapRead() bool {
	return len(r.mmapData) > 0
}

func (r *fileReader) ReadAll(dst []byte) ([]byte, error) {
	if cap(dst) < int(r.fileSize) {
		dst = make([]byte, r.fileSize)
	}
	n, err := io.ReadFull(r.fd, dst[:r.fileSize])
	if err != nil {
		return nil, err
	}
	if n < int(r.fileSize) {
		err = errno.NewError(errno.ShortRead, n, r.fileSize)
	}
	return dst, err
}

func (r *fileReader) Size() (int64, error) {
	stat, err := r.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (r *fileReader) IsOpen() bool {
	return r.fd != nil
}

func (r *fileReader) StreamReadBatch(off, length []int64, c chan *request.StreamReader, limitNum int, isStat bool) {
	go r.fd.StreamReadBatch(off, length, -1, c, limitNum, isStat)
}

func (r *fileReader) ReadAt(off int64, size uint32, dstPtr *[]byte, ioPriority int) ([]byte, error) {
	if size < 1 {
		return nil, nil
	}

	if off < 0 || off > r.fileSize {
		err := fmt.Errorf("invalid read offset %v, filesize %v", off, r.fileSize)
		err = errReadFail(r.Name(), err)
		log.Error(err.Error())
		return nil, err
	}

	if len(r.mmapData) > 0 {
		return r.mmapReadAt(off, size, dstPtr)
	}

	*dstPtr = bufferpool.Resize(*dstPtr, int(size))
	dst := *dstPtr

	start := time.Now()
	n, err := r.fd.ReadAt(dst, off)
	if err != nil && err != io.EOF {
		err = errReadFail(r.Name(), err)
		log.Error(err.Error())
		return nil, err
	}

	if n < int(size) {
		return nil, errno.NewError(errno.ShortRead, n, size).SetModule(errno.ModuleTssp)
	}

	if ioPriority == IO_PRIORITY_LOW_READ {
		err = BackGroundReaderWait(int(size))
		if err != nil {
			log.Error("read meta block wait error", zap.Error(err))
			return nil, err
		}
		atomic.AddInt64(&statistics.IOStat.IOBackReadDuration, time.Since(start).Nanoseconds())
		atomic.AddInt64(&statistics.IOStat.IOBackReadOkBytes, int64(size))
		atomic.AddInt64(&statistics.IOStat.IOBackReadOkCount, 1)
	} else {
		atomic.AddInt64(&statistics.IOStat.IOFrontReadDuration, time.Since(start).Nanoseconds())
		atomic.AddInt64(&statistics.IOStat.IOFrontReadOkBytes, int64(size))
		atomic.AddInt64(&statistics.IOStat.IOFrontReadOkCount, 1)
	}
	return dst[:n], nil
}

func (r *fileReader) mmapReadAt(off int64, size uint32, dstPtr *[]byte) ([]byte, error) {
	if off > int64(len(r.mmapData)) {
		err := fmt.Errorf("off=%d, size=%v is out of allowed len=%d", off, size, len(r.mmapData))
		err = errReadFail(r.Name(), err)
		log.Error(err.Error())
		return nil, err
	}

	end := off + int64(size)
	rb := r.mmapData[off:end]

	if dstPtr != nil && len(*dstPtr) > 0 {
		*dstPtr = bufferpool.Resize(*dstPtr, int(size))
		n := copy(*dstPtr, rb)
		return (*dstPtr)[:n], nil
	}

	return rb, nil
}

func (r *fileReader) FreeFileHandle() error {
	if err := r.close(); err != nil {
		return err
	}
	r.fd = nil

	return nil
}

func (r *fileReader) Name() string {
	return r.name
}

// rename r.name -> newName
func (r *fileReader) Rename(newName string) error {
	if r.mmapData != nil {
		_ = MUnmap(r.mmapData)
		r.mmapData = nil
	}
	return r.rename(r.name, newName)
}

// RenameOnObs rename tssp.obs -> tssp
func (r *fileReader) RenameOnObs(oldName string, renameTmp bool, obsOpt *obs.ObsOptions) error {
	if r.mmapData != nil {
		_ = MUnmap(r.mmapData)
		r.mmapData = nil
	}

	newName := r.name
	if !renameTmp {
		newName, oldName = oldName, newName
	} else {
		if obsOpt != nil {
			newName = oldName[:len(oldName)-len(obs.ObsFileTmpSuffix)]
		}
	}

	err := r.rename(oldName, newName)
	if err != nil {
		isOpen := r.IsOpen()
		if !isOpen {
			// first try to reopen tssp file, then try to reopen tssp.obs file
			if err := r.ReOpen(); err != nil {
				r.name = oldName
				return r.ReOpen()
			}
		}
	}
	return err
}

func (r *fileReader) rename(oldPath, newPath string) error {
	isOpen := r.IsOpen() //if target fd is in use, we will reopen it after rename.

	if isOpen {
		if err := r.fd.Close(); err != nil {
			log.Error("close file fail", zap.String("file", oldPath), zap.Error(err))
			err = errCloseFail(oldPath, err)
			return err
		}
		r.fd = nil
	}

	log.Debug("rename file", zap.String("old", oldPath), zap.String("new", newPath), zap.Int64("size", r.fileSize))
	lock := FileLockOption(*r.lock)
	if err := RenameFile(oldPath, newPath, lock); err != nil {
		err = errRenameFail(zap.String("old", oldPath), zap.String("new", newPath), err)
		log.Error("rename file fail", zap.Error(err))
		return err
	}
	r.name = newPath

	if isOpen {
		if err := r.ReOpen(); err != nil {
			return err
		}
	}

	return nil
}

func (r *fileReader) ReOpen() error {
	defer util.TimeCost("open file")()
	var err error
	lock := FileLockOption("")
	pri := FilePriorityOption(IO_PRIORITY_NORMAL)
	r.fd, err = Open(r.name, lock, pri)
	hitRatioStat.AddQueryFileUnHitTotal(1)
	if err != nil {
		err = errOpenFail(r.name, err)
		log.Error("open file fail", zap.Error(err))
		return err
	}

	if MmapEn {
		r.mmapData, err = Mmap(int(r.fd.Fd()), 0, int(r.fileSize))
		if err != nil {
			err = errMapFail(r.name, err)
			log.Error("mmap file fail", zap.Error(err))
		}
	}
	r.once = new(sync.Once)

	return nil
}

func (r *fileReader) Close() error {
	if ReadMetaCacheEn {
		cacheIns := readcache.GetReadMetaCacheIns()
		cacheIns.Remove(r.Name())
	}
	if ReadDataCacheEn {
		dataCacheIns := readcache.GetReadDataCacheIns()
		dataCacheIns.RemovePageCache(r.Name())
	}

	err := r.close()
	if err != nil {
		return err
	}
	return nil
}

func (r *fileReader) close() error {
	var err error
	r.once.Do(func() {
		name := r.Name()
		if r.mmapData != nil {
			if err = MUnmap(r.mmapData); err != nil {
				log.Error("munmap file fail", zap.String("name", name), zap.Error(err))
				return
			}
		}
		if r.fd != nil {
			err = r.fd.Close()
		}

	})

	return err
}
