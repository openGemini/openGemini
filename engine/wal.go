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
	"bufio"
	"context"
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

	"github.com/docker/go-units"
	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
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
	WriteWalUnKnownType = iota
	WriteWalLineProtocol
	WriteWalArrowFlight
	WriteWalEnd
)

var (
	walCompBufPool = bufferpool.NewByteBufferPool(WalCompBufSize, cpu.GetCpuNum(), bufferpool.MaxLocalCacheLen)
)

var walRowsObjectsPool sync.Pool

type walRowsObjects struct {
	rowsDataBuff []byte // the snappy decoded data for unmarshal Rows

	rows   influx.Rows         // the unmarshal rows
	tags   influx.PointTags    // the unmarshal tags
	fields influx.Fields       // the unmarshal fields
	opts   influx.IndexOptions // the unmarshal IndexOptions
	keys   []byte              // the unmarshal IndexKeys
}

func getWalRowsObjects() *walRowsObjects {
	v := walRowsObjectsPool.Get()
	if v == nil {
		return &walRowsObjects{}
	}
	return v.(*walRowsObjects)
}

// putWalRowsObjects puts the *walRowsObjects to sync.Pool. The reuse object MUST be a point-like param. DO NOT CHANGE ME!
func putWalRowsObjects(objs *walRowsObjects) {
	if len(objs.rowsDataBuff) > 0 {
		objs.rowsDataBuff = objs.rowsDataBuff[:0]
	}

	if len(objs.rows) > 0 {
		objs.rows = objs.rows[:0]
	}
	if len(objs.tags) > 0 {
		objs.tags = objs.tags[:0]
	}
	if len(objs.fields) > 0 {
		objs.fields = objs.fields[:0]
	}
	if len(objs.opts) > 0 {
		objs.opts = objs.opts[:0]
	}
	if len(objs.keys) > 0 {
		objs.keys = objs.keys[:0]
	}
	walRowsObjectsPool.Put(objs)
}

type WAL struct {
	log             *logger.Logger
	mu              sync.RWMutex
	replayWG        sync.WaitGroup // wait for replay wal finish
	shardID         uint64
	logPath         string
	lock            *string
	partitionNum    int
	writeReq        uint64
	logWriter       LogWriters
	logReplay       LogReplays
	walEnabled      bool
	replayParallel  bool
	replayBatchSize int
}

func NewWAL(path string, lockPath *string, shardID uint64, walSyncInterval time.Duration, walEnabled, replayParallel bool, partitionNum int, walReplayBatchSize int) *WAL {
	if walReplayBatchSize < 256*units.KiB {
		walReplayBatchSize = 256 * units.KiB // at least 256 KiB
	}

	wal := &WAL{
		logPath:         path,
		partitionNum:    partitionNum,
		shardID:         shardID,
		logWriter:       make(LogWriters, partitionNum),
		logReplay:       make(LogReplays, partitionNum),
		walEnabled:      walEnabled,
		replayParallel:  replayParallel,
		replayBatchSize: walReplayBatchSize,
		log:             logger.NewLogger(errno.ModuleWal),
		lock:            lockPath,
	}

	lock := fileops.FileLockOption(*lockPath)
	for i := 0; i < partitionNum; i++ {
		wal.logWriter[i] = LogWriter{
			closed:       make(chan struct{}),
			logPath:      filepath.Join(path, strconv.Itoa(i)),
			SyncInterval: walSyncInterval,
			lock:         lockPath,
		}
		_, err := fileops.Stat(wal.logWriter[i].logPath)
		if err != nil && os.IsNotExist(err) {
			err = fileops.MkdirAll(wal.logWriter[i].logPath, 0750, lock)
			if err != nil {
				panic(err)
			}
		}
	}
	return wal
}

// restoreLogs reloads the wal file on the disk
func (l *WAL) restoreLogs() {
	for i := range l.logWriter {
		writer := &l.logWriter[i]
		replay := &l.logReplay[i]
		l.restoreLog(writer, replay)
	}
}

// restoreLog sets logWriter max file seq and logReplay the fileNames on the disk
func (l *WAL) restoreLog(writer *LogWriter, replay *LogReplay) {
	logPath := writer.logPath
	// read logPath
	dirs, err := fileops.ReadDir(logPath)
	if err != nil {
		panic(err)
	}
	// sort file ordered by fileSeq. sort from new to old
	sort.Slice(dirs, func(i, j int) bool {
		iLen := len(dirs[i].Name())
		jLen := len(dirs[j].Name())
		if iLen == jLen {
			return dirs[i].Name() > dirs[j].Name()
		}
		return iLen > jLen
	})
	if len(dirs) == 0 {
		return
	}
	maxSeq, err := strconv.Atoi(dirs[0].Name()[:len(dirs[0].Name())-len(WALFileSuffixes)-1])
	if err != nil {
		l.log.Error("parse wal file failed", zap.String("path", logPath), zap.Error(err))
		return
	}
	writer.fileSeq = maxSeq
	// traverse files from old to new
	for n := len(dirs) - 1; n >= 0; n-- {
		walFile := filepath.Join(logPath, dirs[n].Name())
		replay.fileNames = append(replay.fileNames, walFile)
	}
}

func (l *WAL) writeBinary(walRecord *walRecord) error {
	// prepare for compress memory
	compBuf := walCompBufPool.Get()
	maxEncodeLen := snappy.MaxEncodedLen(len(walRecord.binary))
	compBuf = bufferpool.Resize(compBuf, WalRecordHeadSize+maxEncodeLen)
	defer func() {
		if len(compBuf) <= WalCompMaxBufSize {
			walCompBufPool.Put(compBuf)
		}
	}()

	// compress data
	compData := snappy.Encode(compBuf[WalRecordHeadSize:], walRecord.binary)

	// encode record header
	compBuf[0] = byte(walRecord.writeWalType)

	binary.BigEndian.PutUint32(compBuf[1:WalRecordHeadSize], uint32(len(compData)))
	compBuf = compBuf[:WalRecordHeadSize+len(compData)]

	// write data, switch to new file if needed
	l.mu.RLock()
	err := l.logWriter[(atomic.AddUint64(&l.writeReq, 1)-1)%uint64(l.partitionNum)].Write(compBuf)
	l.mu.RUnlock()
	if err != nil {
		panic(fmt.Errorf("writing WAL entry failed: %v", err))
	}
	return nil
}

func (l *WAL) Write(walRecord *walRecord) error {
	if !l.walEnabled {
		return nil
	}
	if len(walRecord.binary) == 0 {
		return nil
	}
	return l.writeBinary(walRecord)
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
	lock := fileops.FileLockOption(*l.lock)
	for _, fn := range files {
		err := fileops.Remove(fn, lock)
		if err != nil {
			l.log.Error("failed to remove wal file", zap.String("file", fn))
			return err
		}
	}

	return nil
}

func (l *WAL) replayPhysicRecord(fr *bufio.Reader, walFileName string, recordCompBuff []byte, callBack func(pc *walRecord) error) ([]byte, error) {
	// read record header
	var recordHeader [WalRecordHeadSize]byte
	n, err := io.ReadFull(fr, recordHeader[:])
	if err != nil {
		l.log.Error(errno.NewError(errno.ReadWalFileFailed).Error(), zap.Error(err))
		return recordCompBuff, io.EOF
	}
	if n != WalRecordHeadSize {
		l.log.Warn(errno.NewError(errno.WalRecordHeaderCorrupted).Error(), zap.Error(err))
		return recordCompBuff, io.EOF
	}

	writeWalType := WalRecordType(recordHeader[0])
	if writeWalType <= WriteWalUnKnownType || writeWalType >= WriteWalEnd {
		l.log.Error("unKnown write wal type", zap.Int("writeWalType", int(writeWalType)))
		return recordCompBuff, io.EOF
	}

	// prepare record memory
	compBinaryLen := binary.BigEndian.Uint32(recordHeader[1:WalRecordHeadSize])
	recordCompBuff = bufferpool.Resize(recordCompBuff, int(compBinaryLen))

	// read wal binary body
	var rowsObjects = getWalRowsObjects()
	var binaryBuff = rowsObjects.rowsDataBuff[:cap(rowsObjects.rowsDataBuff)]

	wr := &walRecord{
		writeWalType: writeWalType,
	}
	_, err = io.ReadFull(fr, recordCompBuff)
	if err == nil || err == io.EOF {
		var innerErr error
		binaryBuff, innerErr = snappy.Decode(binaryBuff, recordCompBuff)
		if innerErr != nil {
			l.log.Error(errno.NewError(errno.DecompressWalRecordFailed, walFileName, innerErr.Error()).Error())
			return recordCompBuff, io.EOF
		}
		if writeWalType == WriteWalLineProtocol {
			rowsObjects, err = l.unmarshalRows(binaryBuff, rowsObjects)
			if err != nil {
				err = io.EOF
				return recordCompBuff, err
			}
			rowsObjects.rowsDataBuff = binaryBuff
			wr.rowsObjs = rowsObjects
		} else {
			wr.binary = binaryBuff
		}

		innerErr = callBack(wr)
		return recordCompBuff, innerErr
	}
	l.log.Error(errno.NewError(errno.ReadWalFileFailed).Error())
	return recordCompBuff, io.EOF
}

func (l *WAL) unmarshalRows(binary []byte, ctx *walRowsObjects) (*walRowsObjects, error) {
	rows := ctx.rows
	tagPools := ctx.tags
	fieldPools := ctx.fields
	indexOptionPools := ctx.opts
	indexKeyPools := ctx.keys

	var err error

	rows, tagPools, fieldPools, indexOptionPools, indexKeyPools, err = influx.FastUnmarshalMultiRows(binary, rows, tagPools, fieldPools, indexOptionPools, indexKeyPools)
	if err != nil {
		err = errno.NewError(errno.WalRecordUnmarshalFailed, l.shardID, err.Error())
		l.log.Error("wal unmarshal rows fail", zap.Error(err))
	}

	// set the reuse objects to ctx
	ctx.rows = rows
	ctx.tags = tagPools
	ctx.fields = fieldPools
	ctx.opts = indexOptionPools
	ctx.keys = indexKeyPools
	return ctx, err
}

func (l *WAL) replayWalFile(ctx context.Context, walFileName string, callBack func(pc *walRecord) error) error {
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
	logger.GetLogger().Info("start to replay wal file", zap.Uint64("shardID", l.shardID), zap.String("filename", walFileName), zap.String("file size", units.HumanSize(float64(fileSize))))
	recordCompBuff := walCompBufPool.Get()
	defer func() {
		if len(recordCompBuff) <= WalCompMaxBufSize {
			walCompBufPool.Put(recordCompBuff)
		}
		util.MustClose(fd)
	}()

	fr := bufio.NewReaderSize(fd, l.replayBatchSize)
	for {
		select {
		case <-ctx.Done():
			l.log.Info("cancel replay wal", zap.String("filename", walFileName))
			return nil
		default:
		}
		recordCompBuff, err = l.replayPhysicRecord(fr, walFileName, recordCompBuff, callBack)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (l *WAL) replayOnePartition(ctx context.Context, idx int, callBack func(pc *walRecord) error) error {
	for _, fileName := range l.logReplay[idx].fileNames {
		select {
		case <-ctx.Done():
			l.log.Info("cancel replay wal", zap.String("filename", fileName))
			return nil
		default:
		}
		err := l.replayWalFile(ctx, fileName, callBack)
		if err != nil {
			return err
		}
	}
	return nil
}

func consumeRecordSerial(ctx context.Context, mu *sync.Mutex, ptChs []chan *walRecord, errs *[]error, finish chan<- struct{},
	callBack func(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType) error) {
	// consume wal data according to partition order from channel
	ptFinish := make([]bool, len(ptChs))
	ptFinishNum := 0
	for {
		for i := range ptChs {
			if ptFinish[i] {
				continue
			}
			var pc *walRecord
			select {
			case <-ctx.Done():
				finish <- struct{}{}
				return
			case pc = <-ptChs[i]:
			}
			if len(pc.binary) == 0 && (pc.rowsObjs == nil || len(pc.rowsObjs.rows) == 0) {
				ptFinishNum++
				ptFinish[i] = true
				if ptFinishNum == len(ptChs) {
					finish <- struct{}{}
					return
				}
				continue
			}

			err := callBack(pc.binary, pc.rowsObjs, pc.writeWalType)
			if err != nil {
				mu.Lock()
				*errs = append(*errs, err)
				mu.Unlock()
			}
		}
	}
}

func consumeRecordParallel(ctx context.Context, mu *sync.Mutex, ptChs []chan *walRecord, errs *[]error, finish chan<- struct{},
	callBack func(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType) error) {
	var wg sync.WaitGroup
	wg.Add(len(ptChs))
	for i := 0; i < len(ptChs); i++ {
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case pc := <-ptChs[idx]:
					if len(pc.binary) == 0 && (pc.rowsObjs == nil || len(pc.rowsObjs.rows) == 0) {
						return
					}
					err := callBack(pc.binary, pc.rowsObjs, pc.writeWalType)
					if err != nil {
						mu.Lock()
						*errs = append(*errs, err)
						mu.Unlock()
					}
				}
			}
		}(i)
	}
	wg.Wait()
	finish <- struct{}{}
}

// sendWal2ptChs sends the wal binary to the idx ptChs
func (l *WAL) sendWal2ptChs(ctx context.Context, ptChs []chan *walRecord, idx int, pc *walRecord) {
	select {
	case <-ctx.Done():
		l.log.Info("cancel replay wal", zap.Int("log writer", idx))
	case ptChs[idx] <- pc:
	}
}

func (l *WAL) productRecordParallel(ctx context.Context, mu *sync.Mutex, ptChs []chan *walRecord, errs *[]error) []string {
	var wg sync.WaitGroup
	var walFileNames []string
	wg.Add(len(l.logReplay))
	for i := range l.logReplay {
		go func(idx int) {
			err := l.replayOnePartition(ctx, idx, func(pc *walRecord) error {
				l.sendWal2ptChs(ctx, ptChs, idx, pc)
				return nil
			})
			l.sendWal2ptChs(ctx, ptChs, idx, &walRecord{})
			mu.Lock()
			if err != nil {
				*errs = append(*errs, err)
			}
			walFileNames = append(walFileNames, l.logReplay[idx].fileNames...)
			mu.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	return walFileNames
}

func (l *WAL) ref() {
	l.replayWG.Add(1)
}

func (l *WAL) unref() {
	l.replayWG.Done()
}

func (l *WAL) wait() {
	l.replayWG.Wait()
}

type walRecord struct {
	binary       []byte          // write wal for both type and replay wal for arrowFlight type
	rowsObjs     *walRowsObjects // replay wal for lineProtocol type for Rows unmarshalled
	writeWalType WalRecordType
}

func (l *WAL) Replay(ctx context.Context, callBack func(binary []byte, rowsCtx *walRowsObjects, writeWalType WalRecordType) error) ([]string, error) {
	if !l.walEnabled {
		return nil, nil
	}

	// replay wal files
	var mu = sync.Mutex{}
	var errs []error
	var ptChs []chan *walRecord
	consumeFinish := make(chan struct{})
	for i := 0; i < len(l.logReplay); i++ {
		ptChs = append(ptChs, make(chan *walRecord, 4))
	}

	if l.replayParallel {
		// multi partition wal files replayed parallel, the update of the same time at a certain series is not guaranteed
		l.ref()
		go func() {
			defer l.unref()
			consumeRecordParallel(ctx, &mu, ptChs, &errs, consumeFinish, callBack)
		}()
	} else {
		l.ref()
		// multi partition wal files replayed serial, the update of the same time at a certain series is guaranteed
		go func() {
			defer l.unref()
			consumeRecordSerial(ctx, &mu, ptChs, &errs, consumeFinish, callBack)
		}()
	}

	// production wal data to channel
	walFileNames := l.productRecordParallel(ctx, &mu, ptChs, &errs)
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
