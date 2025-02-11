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

package engine

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

const unixNanoLen = 19

var streamWalManager *StreamWalManager

type StreamHandler func(rows influx.Rows, fileNames []string) error

type StreamWalManager struct {
	mu            sync.RWMutex
	files         []*WalFiles
	loadFiles     [][]*WalFiles
	streamHandler StreamHandler
}

func init() {
	streamWalManager = &StreamWalManager{loadFiles: [][]*WalFiles{}}
}

func NewStreamWalManager() *StreamWalManager {
	return streamWalManager
}

func (m *StreamWalManager) InitStreamHandler(f StreamHandler) {
	m.streamHandler = f
}

func (m *StreamWalManager) Load(dir string, lock *string) error {
	streamDir := path.Join(dir, StreamWalDir)
	files, err := fileops.ReadDir(streamDir)
	if err != nil {
		return err
	}

	fileGroups := make(map[int64]*WalFiles)

	for i := range files {
		if files[i].IsDir() {
			continue
		}
		filename := files[i].Name()
		maxTime, sec := ParseSteamWalFilename(filename)
		if maxTime == 0 || sec == 0 {
			logger.NewLogger(errno.ModuleWal).Error("invalid wal filename", zap.String("filename", filename))
			continue
		}

		walFiles, ok := fileGroups[maxTime]
		if !ok {
			walFiles = newWalFiles(maxTime, lock, dir)
			fileGroups[maxTime] = walFiles
		}
		walFiles.Add(path.Join(streamDir, filename))
	}

	walFiles := make([]*WalFiles, len(fileGroups))
	index := 0
	for _, walFile := range fileGroups {
		logger.GetLogger().Info("add walFiles")
		walFiles[index] = walFile
		index++
	}
	m.loadFiles = append(m.loadFiles, walFiles)
	return nil
}

func (m *StreamWalManager) Replay(ctx context.Context, tm int64, ptID uint32, index int) (int, error) {
	if index >= len(m.loadFiles) {
		return index, nil
	}

	loadFiles := m.loadFiles[index]
	index++

	path := strconv.FormatUint(uint64(ptID), 10)
	logger.GetLogger().Info("replay", zap.String("path", path))
	conf := config.GetStoreConfig().Wal
	wal := &WAL{}
	wal.walEnabled = true
	wal.log = logger.NewLogger(errno.ModuleWal)
	wal.replayParallel = conf.WalReplayParallel

	wal.logReplay = make(LogReplays, len(loadFiles))
	for i, walFiles := range loadFiles {
		wal.logReplay[i].fileNames = walFiles.files
	}
	logger.GetLogger().Info("replay", zap.Int("loadFiles", len(loadFiles)))

	_, err := wal.Replay(ctx, func(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType, logReplay LogReplay) error {
		return m.streamHandler(rowsCtx.rows, logReplay.fileNames)
	})

	for i := range loadFiles {
		util.MustRun(func() error {
			return removeWalFiles(loadFiles[i])
		})
	}
	return index, err
}

func (m *StreamWalManager) Add(files *WalFiles) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.files = append(m.files, files)
}

func (m *StreamWalManager) Free(tm int64) {
	files := m.getRemoveList(tm)
	for i := range files {
		util.MustRun(func() error {
			return removeWalFiles(files[i])
		})
	}
}

func (m *StreamWalManager) CleanLoadFiles() {
	m.loadFiles = m.loadFiles[:0]
}

func (m *StreamWalManager) getRemoveList(tm int64) []*WalFiles {
	m.mu.Lock()
	defer m.mu.Unlock()
	matched, not := util.SliceSplitFunc(m.files, func(files **WalFiles) bool {
		return (*files).maxTime < tm
	})
	m.files = not
	return matched
}

type WalFiles struct {
	mu      sync.Mutex
	files   []string
	maxTime int64
	lock    *string
	dir     string
}

func newWalFiles(maxTime int64, lock *string, dir string) *WalFiles {
	return &WalFiles{maxTime: maxTime, lock: lock, dir: dir}
}

func (w *WalFiles) Add(files ...string) {
	w.mu.Lock()
	w.files = append(w.files, files...)
	w.mu.Unlock()
}

func RemoveWalFiles(files *WalFiles) error {
	if files == nil || len(files.files) == 0 {
		return nil
	}

	if config.GetStoreConfig().Wal.WalUsedForStream {
		return moveToStream(files)
	}

	return removeWalFiles(files)
}

func removeWalFiles(files *WalFiles) error {
	if files == nil {
		return nil
	}

	var err error
	lock := fileops.FileLockOption(*files.lock)
	for _, f := range files.files {
		e := fileops.Remove(f, lock)
		if e != nil {
			err = e
			logger.NewLogger(errno.ModuleWal).Error("failed to remove wal file", zap.String("file", f), zap.Error(err))
		}
	}
	return err
}

func moveToStream(files *WalFiles) error {
	dir := path.Join(files.dir, StreamWalDir)
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return err
	}

	sec := time.Now().UnixNano()
	lock := fileops.FileLockOption(*files.lock)

	for i := range files.files {
		name := fmt.Sprintf("%d%s%d.%s", files.maxTime, pathSeparator, sec, WALFileSuffixes)
		sec++

		oldFile := files.files[i]
		newFile := path.Join(dir, name)

		err := fileops.RenameFile(oldFile, newFile, lock)
		if err != nil {
			logger.NewLogger(errno.ModuleWal).Error("failed to move wal to stream dir", zap.String("file", oldFile), zap.Error(err))
			return err
		}

		files.files[i] = newFile
	}

	NewStreamWalManager().Add(files)
	return nil
}

func ParseSteamWalFilename(name string) (int64, int64) {
	suffixSize := len(WALFileSuffixes) + 1
	size := len(name)
	if size <= suffixSize {
		return 0, 0
	}

	tmp := strings.Split(name[:len(name)-suffixSize], pathSeparator)
	if len(tmp) < 2 {
		return 0, 0
	}

	if len(tmp[0]) < unixNanoLen || len(tmp[1]) < unixNanoLen {
		return 0, 0
	}

	maxTime, err := strconv.ParseInt(tmp[0], 10, 64)
	if err != nil {
		return 0, 0
	}

	sec, err := strconv.ParseInt(tmp[1], 10, 64)
	if err != nil {
		return 0, 0
	}

	return maxTime, sec
}
