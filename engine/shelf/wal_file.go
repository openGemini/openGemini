//go:build !streamfs

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

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type WalFile struct {
	WalFileHot

	name string
	lock *string

	fd     fileops.File
	reader io.ReaderAt
	writer *bufio.Writer

	writtenSize int64
	syncedSize  int64

	option *obs.ObsOptions
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
	wf.option = option
}

func (wf *WalFile) OpenReadonly() error {
	return wf.open(os.O_RDONLY)
}

func (wf *WalFile) Open() error {
	return wf.open(os.O_CREATE | os.O_RDWR | os.O_APPEND | os.O_TRUNC)
}

func (wf *WalFile) open(flag int) error {
	lockOpt := fileops.FileLockOption(*wf.lock)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)

	// Force modify wf.name,wal files flush to OBS.
	obsOptions := wf.option
	if obsOptions != nil {
		if obsOptions.Enabled {
			wf.name = fileops.GetRemoteDataPath(obsOptions, wf.name)
		}
	}

	fd, err := fileops.OpenFile(wf.name, flag, 0600, lockOpt, pri)
	if err != nil {
		return err
	}

	wf.fd = fd
	wf.reader = fd
	if flag != os.O_RDONLY {
		wf.writer = bufio.NewWriterSize(fd, walBufferSize)
	}
	return nil
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
	if wf.fd == nil {
		return nil
	}
	return wf.fd.Close()
}

func (wf *WalFile) Sync() error {
	if wf.writtenSize == wf.syncedSize {
		return nil
	}

	defer statistics.MicroTimeUse(stat.DiskSyncCount, stat.DiskSyncDurSum)()

	err := wf.fd.Sync()
	if err == nil {
		wf.syncedSize = wf.writtenSize
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

	return wf.reader.ReadAt(dst, ofs)
}

func (wf *WalFile) Read(dst []byte) (int, error) {
	return wf.fd.Read(dst)
}

func (wf *WalFile) ReadFull(dst []byte) (int, error) {
	return io.ReadFull(wf.fd, dst)
}

func (wf *WalFile) Seek(offset int64, whence int) (int64, error) {
	return wf.fd.Seek(offset, whence)
}
