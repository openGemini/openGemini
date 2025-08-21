//go:build streamfs

// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package shelf

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type WalFile struct {
	WalFileHot

	mu   sync.RWMutex
	name string
	lock *string

	fdRead  fileops.File
	fdWrite fileops.File

	reader      io.ReaderAt
	writer      *bufio.Writer
	writtenSize int64
	syncedSize  int64
}

func NewWalFile(name string, lock *string) *WalFile {
	return &WalFile{
		name:        name,
		lock:        lock,
		writtenSize: 0,
		syncedSize:  0,
	}
}

func (wf *WalFile) setWalObsOptions(option *obs.ObsOptions) {

}

func (wf *WalFile) OpenReadonly() error {
	if wf.reader != nil {
		return nil
	}

	wf.mu.Lock()
	defer wf.mu.Unlock()

	return wf.openReader()
}

func (wf *WalFile) Open() error {
	fd, err := wf.open(os.O_CREATE | os.O_RDWR | os.O_APPEND | os.O_TRUNC)
	if err != nil {
		return err
	}

	wf.fdWrite = fd
	wf.writer = bufio.NewWriterSize(fd, walBufferSize)
	return nil
}

func (wf *WalFile) ReOpenReader() error {
	if wf.reader == nil {
		return nil
	}

	wf.mu.Lock()
	defer wf.mu.Unlock()

	util.MustClose(wf.fdRead)
	wf.reader = nil
	wf.fdRead = nil
	return wf.openReader()
}

func (wf *WalFile) openReader() error {
	fd, err := wf.open(os.O_RDONLY)
	if err != nil {
		return err
	}

	wf.fdRead = fd
	wf.reader = fd
	return nil
}

func (wf *WalFile) open(flag int) (fileops.File, error) {
	lockOpt := fileops.FileLockOption(*wf.lock)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)

	return fileops.OpenFile(wf.name, flag, 0600, lockOpt, pri)
}

func (wf *WalFile) LoadIntoMemory() {
	if wf.writtenSize == 0 {
		return
	}

	if mf := wf.memFile; mf != nil {
		wf.reader = mf
		return
	}

	mr := &fileops.MemFile{}
	err := mr.Load(wf, int(wf.writtenSize))
	if err != nil {
		logger.GetLogger().Error("failed to load wal into memory", zap.Error(err))
		return
	}
	wf.reader = mr
}

func (wf *WalFile) Name() string {
	return wf.name
}

func (wf *WalFile) WrittenSize() int64 {
	return wf.writtenSize
}

func (wf *WalFile) Write(b []byte) (int, error) {
	if mf := wf.memFile; mf != nil {
		mf.Write(b)
	}

	n, err := wf.writer.Write(b)
	wf.writtenSize += int64(n)
	return n, err
}

func (wf *WalFile) Flush() error {
	if wf.writer == nil || wf.writer.Size() == 0 {
		return nil
	}
	defer statistics.MicroTimeUse(stat.DiskFlushCount, stat.DiskFlushDurSum)()

	return wf.writer.Flush()
}

func (wf *WalFile) Close() error {
	util.MustClose(wf.fdRead)
	util.MustClose(wf.fdWrite)
	return nil
}

func (wf *WalFile) Sync() error {
	if wf.writtenSize == wf.syncedSize {
		return nil
	}

	defer statistics.MicroTimeUse(stat.DiskSyncCount, stat.DiskSyncDurSum)()

	err := wf.fdWrite.Sync()
	if err == nil {
		wf.syncedSize = wf.writtenSize

		err = wf.ReOpenReader()
	}

	return err
}

func (wf *WalFile) ReadAt(dst []byte, ofs int64) (int, error) {
	if mf := wf.memFile; mf != nil {
		n, err := mf.ReadAt(dst, ofs)
		if err == nil {
			return n, nil
		}
	}

	end := int64(len(dst)) + ofs
	if wf.syncedSize > 0 && wf.syncedSize < end {
		return 0, io.EOF
	}

	return wf.readOperate(func() (int, error) {
		return wf.reader.ReadAt(dst, ofs)
	})
}

func (wf *WalFile) Read(dst []byte) (int, error) {
	return wf.readOperate(func() (int, error) {
		return wf.fdRead.Read(dst)
	})
}

func (wf *WalFile) ReadFull(dst []byte) (int, error) {
	return wf.readOperate(func() (int, error) {
		return io.ReadFull(wf.fdRead, dst)
	})
}

func (wf *WalFile) readOperate(op func() (int, error)) (int, error) {
	err := wf.OpenReadonly()
	if err != nil {
		return 0, err
	}

	wf.mu.RLock()
	defer wf.mu.RUnlock()

	return op()
}

func (wf *WalFile) Seek(offset int64, whence int) (int64, error) {
	wf.mu.RLock()
	defer wf.mu.RUnlock()

	return wf.fdRead.Seek(offset, whence)
}
