/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/pingcap/failpoint"
)

const (
	DefaultFileSize   = 10 * 1024 * 1024
	WALFileSuffixes   = "wal"
	WalRecordHeadSize = 1 + 4
	WalCompBufSize    = 256 * 1024
	WalCompMaxBufSize = 2 * 1024 * 1024
)

type WalRecordType byte

const (
	WriteWALRecord WalRecordType = 0x01
)

var (
	walCompBufPool = bufferpool.NewByteBufferPool(WalCompBufSize)
)

type WAL struct {
	log            *logger.Logger
	mu             sync.RWMutex
	logPath        string
	partitionNum   int
	writeReq       uint64
	logWriter      []LogWriter
	walEnabled     bool
	replayParallel bool
}

func NewWAL(path string, walSyncInterval time.Duration, walEnabled, replayParallel bool, partitionNum int) *WAL {
	wal := &WAL{
		logPath:        path,
		partitionNum:   partitionNum,
		logWriter:      make([]LogWriter, partitionNum),
		walEnabled:     walEnabled,
		replayParallel: replayParallel,
		log:            logger.NewLogger(errno.ModuleWal),
	}

	lock := fileops.FileLockOption("")
	for i := 0; i < partitionNum; i++ {
		wal.logWriter[i] = LogWriter{
			closed:       make(chan struct{}),
			logPath:      filepath.Join(path, strconv.Itoa(i)),
			SyncInterval: walSyncInterval,
		}

		err := fileops.MkdirAll(wal.logWriter[i].logPath, 0750, lock)
		if err != nil {
			panic(err)
		}
	}

	return wal
}

func (l *WAL) writeBinary(binaryData []byte) error {
	// prepare for compress memory
	compBuf := walCompBufPool.Get()
	maxEncodeLen := snappy.MaxEncodedLen(len(binaryData))
	compBuf = bufferpool.Resize(compBuf, WalRecordHeadSize+maxEncodeLen)
	defer func() {
		if len(compBuf) <= WalCompMaxBufSize {
			walCompBufPool.Put(compBuf)
		}
	}()

	// compress data
	compData := snappy.Encode(compBuf[WalRecordHeadSize:], binaryData)

	// encode record header
	compBuf[0] = byte(WriteWALRecord)
	binary.BigEndian.PutUint32(compBuf[1:WalRecordHeadSize], uint32(len(compData)))
	compBuf = compBuf[:WalRecordHeadSize+len(compData)]

	// write data, switch to new file if needed
	l.mu.RLock()
	err := l.logWriter[(atomic.AddUint64(&l.writeReq, 1)-1)%uint64(l.partitionNum)].Write(compBuf)
	l.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("writing WAL entry failed: %v", err)
	}
	return nil
}

func (l *WAL) Write(binary []byte) error {
	if !l.walEnabled {
		return nil
	}
	if len(binary) == 0 {
		return nil
	}
	return l.writeBinary(binary)
}

func (l *WAL) Switch() ([]string, error) {
	if !l.walEnabled {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var mu = sync.Mutex{}
	var errs []error
	var wg sync.WaitGroup
	var totalFiles []string

	wg.Add(l.partitionNum)
	for i := 0; i < l.partitionNum; i++ {
		go func(lw *LogWriter) {
			files, err := lw.Switch()

			mu.Lock()
			if err != nil {
				errs = append(errs, err)
			}
			totalFiles = append(totalFiles, files...)
			mu.Unlock()

			wg.Done()
		}(&l.logWriter[i])
	}
	wg.Wait()

	for _, err := range errs {
		return nil, err
	}

	return totalFiles, nil
}

func (l *WAL) Remove(files []string) error {
	if !l.walEnabled {
		return nil
	}
	lock := fileops.FileLockOption("")
	for _, fn := range files {
		err := fileops.Remove(fn, lock)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *WAL) replayPhysicRecord(fd fileops.File, offset, fileSize int64, recordCompBuff []byte,
	callBack func(binary []byte) error) (int64, []byte, error) {
	// check file is all replayed
	if offset >= fileSize {
		return offset, recordCompBuff, io.EOF
	}

	// read record header
	var recordHeader [WalRecordHeadSize]byte
	n, err := fd.ReadAt(recordHeader[:], offset)
	if err != nil {
		l.log.Warn(errno.NewError(errno.ReadWalFileFailed, fd.Name(), offset, "record header").Error())
		return offset, recordCompBuff, io.EOF
	}
	if n != WalRecordHeadSize {
		l.log.Warn(errno.NewError(errno.WalRecordHeaderCorrupted, fd.Name(), offset).Error())
		return offset, recordCompBuff, io.EOF
	}
	offset += int64(len(recordHeader))

	// prepare record memory
	compBinaryLen := binary.BigEndian.Uint32(recordHeader[1:WalRecordHeadSize])
	recordCompBuff = bufferpool.Resize(recordCompBuff, int(compBinaryLen))

	// read record body
	var recordBuff []byte
	n, err = fd.ReadAt(recordCompBuff, offset)
	if err == nil || err == io.EOF {
		offset += int64(n)
		var innerErr error
		recordBuff, innerErr = snappy.Decode(recordBuff, recordCompBuff)
		if innerErr != nil {
			l.log.Warn(errno.NewError(errno.DecompressWalRecordFailed, fd.Name(), offset, innerErr.Error()).Error())
			return offset, recordCompBuff, io.EOF
		}

		innerErr = callBack(recordBuff)
		if innerErr == nil {
			return offset, recordCompBuff, err
		}

		return offset, recordCompBuff, innerErr
	}
	l.log.Warn(errno.NewError(errno.ReadWalFileFailed, fd.Name(), offset, "record body").Error())
	return offset, recordCompBuff, io.EOF
}

func (l *WAL) replayWalFile(walFileName string, callBack func(binary []byte) error) error {
	failpoint.Inject("mock-replay-wal-error", func(val failpoint.Value) {
		msg := val.(string)
		if strings.Contains(walFileName, msg) {
			failpoint.Return(fmt.Errorf(msg))
		}
	})
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(walFileName, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		return err
	}

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return nil
	}

	recordCompBuff := walCompBufPool.Get()
	defer func() {
		if len(recordCompBuff) <= WalCompMaxBufSize {
			walCompBufPool.Put(recordCompBuff)
		}
	}()

	offset := int64(0)
	for {
		offset, recordCompBuff, err = l.replayPhysicRecord(fd, offset, fileSize, recordCompBuff, callBack)
		if err != nil {
			util.MustClose(fd)
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (l *WAL) replayOnePartition(logPath string, callBack func(binary []byte) error) ([]string, error) {
	// read wal files
	dirs, err := fileops.ReadDir(logPath)
	if err != nil {
		return nil, err
	}

	// sort file ordered by fileSeq
	sort.Slice(dirs, func(i, j int) bool {
		iLen := len(dirs[i].Name())
		jLen := len(dirs[j].Name())
		if iLen == jLen {
			return dirs[i].Name() < dirs[j].Name()
		}
		return iLen < jLen
	})

	walFileNames := make([]string, 0, len(dirs))
	for i := range dirs {
		fileName := filepath.Join(logPath, dirs[i].Name())
		err = l.replayWalFile(fileName, callBack)
		if err != nil {
			return nil, err
		}
		walFileNames = append(walFileNames, fileName)
	}
	return walFileNames, nil

}

func consumeRecordSerial(mu *sync.Mutex, ptChs []chan []byte, errs *[]error, finish chan struct{}, callBack func(binary []byte) error) {
	// consume wal data according to partition order from channel
	ptFinish := make([]bool, len(ptChs))
	ptFinishNum := 0
	for {
		for i := range ptChs {
			if ptFinish[i] {
				continue
			}
			binary := <-ptChs[i]
			if binary == nil {
				ptFinishNum++
				ptFinish[i] = true
				if ptFinishNum == len(ptChs) {
					finish <- struct{}{}
					return
				}
				continue
			}
			err := callBack(binary)
			if err != nil {
				mu.Lock()
				*errs = append(*errs, err)
				mu.Unlock()
			}
		}
	}
}

func consumeRecordParallel(mu *sync.Mutex, ptChs []chan []byte, errs *[]error, finish chan struct{}, callBack func(binary []byte) error) {
	var wg sync.WaitGroup
	wg.Add(len(ptChs))
	for i := 0; i < len(ptChs); i++ {
		go func(idx int) {
			for {
				binary := <-ptChs[idx]
				if binary == nil {
					wg.Done()
					return
				}
				err := callBack(binary)
				if err != nil {
					mu.Lock()
					*errs = append(*errs, err)
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	finish <- struct{}{}
}

func (l *WAL) productRecordParallel(mu *sync.Mutex, ptChs []chan []byte, errs *[]error, dirs []os.FileInfo) []string {
	var wg sync.WaitGroup
	var walFileNames []string
	wg.Add(len(dirs))
	for i := range dirs {
		go func(logPath string, idx int) {
			fileName, err := l.replayOnePartition(logPath, func(binary []byte) error {
				ptChs[idx] <- binary
				return nil
			})
			ptChs[idx] <- nil
			mu.Lock()
			if err != nil {
				*errs = append(*errs, err)
			}
			walFileNames = append(walFileNames, fileName...)
			mu.Unlock()
			wg.Done()
		}(filepath.Join(l.logPath, dirs[i].Name()), i)
	}
	wg.Wait()
	return walFileNames
}

func (l *WAL) Replay(callBack func(binary []byte) error) ([]string, error) {
	if !l.walEnabled {
		return nil, nil
	}

	// read wal partition dirs
	dirs, err := fileops.ReadDir(l.logPath)
	if err != nil {
		return nil, err
	}

	// replay wal files
	var mu = sync.Mutex{}
	var errs []error
	var ptChs []chan []byte
	consumeFinish := make(chan struct{})
	for i := 0; i < len(dirs); i++ {
		ptChs = append(ptChs, make(chan []byte, 4))
	}

	if l.replayParallel {
		// multi partition wal files replayed parallel, the update of the same time at a certain series is not guaranteed
		go consumeRecordParallel(&mu, ptChs, &errs, consumeFinish, callBack)
	} else {
		// multi partition wal files replayed serial, the update of the same time at a certain series is guaranteed
		go consumeRecordSerial(&mu, ptChs, &errs, consumeFinish, callBack)
	}

	// production wal data to channel
	walFileNames := l.productRecordParallel(&mu, ptChs, &errs, dirs)
	<-consumeFinish
	close(consumeFinish)
	for i := range ptChs {
		close(ptChs[i])
	}

	for _, err := range errs {
		return nil, err
	}
	return walFileNames, nil
}

func (l *WAL) Close() error {
	if !l.walEnabled {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	var mu = sync.Mutex{}
	var errs []error
	var wg sync.WaitGroup

	wg.Add(l.partitionNum)
	for i := 0; i < l.partitionNum; i++ {
		go func(lw *LogWriter) {
			err := lw.close()
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
			wg.Done()
		}(&l.logWriter[i])
	}
	wg.Wait()

	for _, err := range errs {
		return err
	}
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
		lock := fileops.FileLockOption("")
		pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_ULTRA_HIGH)
		fd, err := fileops.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
		if err != nil {
			return err
		}

		w.fileNames = append(w.fileNames, fileName)
		w.currentFd = fd
		w.currentFileSize = 0
	}

	return nil
}

type LogWriter struct {
	writeMu         sync.Mutex
	syncMu          sync.Mutex
	logPath         string
	syncTaskCount   int32
	SyncInterval    time.Duration
	closed          chan struct{}
	fileSeq         int
	fileNames       []string
	currentFd       fileops.File
	currentFileSize int
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
