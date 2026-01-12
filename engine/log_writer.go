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

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/fileops"
)

type LogWriters []LogWriter

type LogWriter struct {
	writeMu         sync.Mutex
	syncMu          sync.Mutex
	logPath         string
	lock            *string
	syncTaskCount   int32
	SyncInterval    time.Duration
	closed          chan struct{}
	fileSeq         int
	fileNames       []string
	currentFd       fileops.File
	currentFileSize int
}

type LogReplays []LogReplay

type LogReplay struct {
	fileNames []string
}

func (w *LogWriter) closeCurrentFile() error {
	if w.currentFd == nil || w.currentFileSize == 0 {
		return nil
	}
	if err := w.currentFd.Sync(); err != nil {
		return err
	}
	if err := w.currentFd.Close(); err != nil {
		return err
	}
	w.currentFileSize = 0
	w.currentFd = nil
	return nil
}

func (w *LogWriter) Switch() ([]string, error) {
	w.syncMu.Lock()
	err := w.closeCurrentFile()
	w.syncMu.Unlock()
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0, len(w.fileNames))
	fileNames = append(fileNames, w.fileNames...)

	// reset
	w.fileNames = w.fileNames[:0]

	return fileNames, nil
}

func (w *LogWriter) sync() error {
	var err error
	if w.SyncInterval == 0 {
		w.syncMu.Lock()
		if w.currentFd != nil {
			err = w.currentFd.Sync()
		}
		w.syncMu.Unlock()
		return err
	}

	t := time.NewTicker(w.SyncInterval)
	defer t.Stop()

	for {
		select {
		case <-w.closed:
			atomic.StoreInt32(&w.syncTaskCount, 0)
			return nil
		case <-t.C:
			w.syncMu.Lock()
			if w.currentFd != nil {
				err = w.currentFd.Sync()
			}
			atomic.StoreInt32(&w.syncTaskCount, 0)
			w.syncMu.Unlock()

			return err
		}
	}
}

func (w *LogWriter) trySync() error {
	if w.SyncInterval == 0 {
		return w.sync()
	}

	if !atomic.CompareAndSwapInt32(&w.syncTaskCount, 0, 1) {
		return nil
	}

	go func() {
		_ = w.sync()
	}()
	return nil
}

func (w *LogWriter) Write(compBuf []byte) error {
	select {
	case <-w.closed:
		return fmt.Errorf("WAL log writer closed")
	default:
		w.writeMu.Lock()
		defer w.writeMu.Unlock()

		w.syncMu.Lock()
		err := w.trySwitchFile(w.logPath)
		if err != nil {
			w.syncMu.Unlock()
			return err
		}
		w.syncMu.Unlock()

		if _, err = w.currentFd.Write(compBuf); err != nil {
			return err
		}

		if err = w.trySync(); err != nil {
			return err
		}

		w.currentFileSize += len(compBuf)
		return nil
	}
}

func (w *LogWriter) close() error {
	close(w.closed)

	w.syncMu.Lock()
	err := w.closeCurrentFile()
	w.syncMu.Unlock()
	if err != nil {
		return err
	}
	w.fileNames = w.fileNames[:0]
	return nil
}

func (w *LogWriter) trySwitchFile(logPath string) error {
	if w.currentFd == nil || w.currentFileSize > DefaultFileSize {
		w.fileSeq++
		// close current file
		err := w.closeCurrentFile()
		if err != nil {
			return err
		}

		// new file
		fileName := filepath.Join(logPath, fmt.Sprintf("%d.%s", w.fileSeq, WALFileSuffixes))
		lock := fileops.FileLockOption(*w.lock)
		pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_ULTRA_HIGH)
		fd, err := fileops.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0600, lock, pri)
		if err != nil {
			return err
		}

		w.fileNames = append(w.fileNames, fileName)
		w.currentFd = fd
		w.currentFileSize = 0
	}

	return nil
}
