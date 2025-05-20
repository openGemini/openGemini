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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding/lz4"
	"go.uber.org/zap"
)

const (
	walFileSuffixes    = "wal"
	walExtensionSuffix = ".ext"
	maxWalBlockSize    = 256 * config.MB
	flagSeriesKey      = 1
	flagDictBlock      = 2

	walCompressNone   = 0
	walCompressLz4    = 1
	walCompressSnappy = 2

	walBufferSize = 4 * config.MB
)

var walBlockHeaderSize = int(unsafe.Sizeof(WalBlockHeader{}))
var zeroBlockHeader = make([]byte, walBlockHeaderSize)

type WriterFlusher interface {
	io.Writer
	Flush() error
}

type Wal struct {
	util.Reference

	mu        sync.RWMutex
	closeOnce sync.Once

	// for store record data
	file *WalFile
	// for store extension data: dict, series key
	fileExt *WalFile

	// record the offset of each series data in the wal file
	seriesOffsets *SeriesOffsets

	// Index not created or loaded into cache.
	// Record the series key, which is used by the background thread to asynchronously create indexes.
	seriesKeyOffsets *SeriesKeyOffsets

	timeRange util.TimeRange
	codec     *WalRecordDecoder
	ctx       *WalCtx

	expireAt        uint64
	lock            *string
	dir             string
	mst             string
	opened          bool
	backgroundSync  bool
	backgroundFlush bool
	converted       bool // successfully converted to TSSP file
	writing         bool
}

func NewWal(dir string, lock *string, mst string) *Wal {
	wal := &Wal{
		lock:             lock,
		seriesOffsets:    NewSeriesOffsets(),
		seriesKeyOffsets: NewSeriesKeyOffsets(),
		dir:              filepath.Join(dir, mst),
		mst:              mst,
		timeRange: util.TimeRange{
			Min: math.MaxInt64,
			Max: math.MinInt64,
		},
		ctx:             &WalCtx{},
		backgroundSync:  conf.ReliabilityLevel < config.ReliabilityLevelHigh,
		backgroundFlush: conf.ReliabilityLevel == config.ReliabilityLevelLow,
		expireAt:        fasttime.UnixTimestamp() + uint64(time.Duration(conf.MaxWalDuration)/time.Second),
	}

	return wal
}

func (wal *Wal) SizeLimited() bool {
	return wal.file.WrittenSize() >= int64(conf.MaxWalFileSize)
}

func (wal *Wal) WrittenSize() int64 {
	return wal.file.WrittenSize()
}

func (wal *Wal) NeedCreateIndex() bool {
	return wal.seriesKeyOffsets.Len() > 0
}

func (wal *Wal) Name() string {
	return wal.file.Name()
}

func (wal *Wal) EndWrite() {
	wal.writing = false
}

func (wal *Wal) open() error {
	if wal.opened {
		return nil
	}

	err := fileops.MkdirAll(wal.dir, 0700)
	if err != nil {
		return err
	}

	file := filepath.Join(wal.dir, fmt.Sprintf("%d.%s", AllocWalSeq(), walFileSuffixes))
	wal.file = NewWalFile(file, wal.lock)
	wal.fileExt = NewWalFile(file+walExtensionSuffix, wal.lock)

	flag := os.O_CREATE | os.O_RDWR | os.O_APPEND | os.O_TRUNC
	if err = wal.file.Open(flag); err != nil {
		return err
	}
	if err = wal.fileExt.Open(flag); err != nil {
		return err
	}

	wal.codec = NewWalRecordDecoder()
	wal.opened = true
	return nil
}

func (wal *Wal) Expired() bool {
	return !wal.writing && fasttime.UnixTimestamp() >= wal.expireAt
}

func (wal *Wal) BackgroundSync() {
	if !wal.opened {
		return
	}

	err := wal.sync()
	if err != nil {
		logger.GetLogger().Error("failed to sync wal",
			zap.Error(err),
			zap.String("file", wal.file.Name()))
	}
}

func (wal *Wal) UpdateTimeRange(tr *util.TimeRange) {
	if wal.timeRange.Min > tr.Min {
		wal.timeRange.Min = tr.Min
	}
	if wal.timeRange.Max < tr.Max {
		wal.timeRange.Max = tr.Max
	}
}

func (wal *Wal) WriteRecord(sid uint64, seriesKey []byte, rec []byte) error {
	err := wal.open()
	if err != nil {
		return err
	}

	ofs := wal.WrittenSize()
	err = wal.writeRecord(sid, seriesKey, rec)
	if err != nil {
		return err
	}

	if sid > 0 {
		wal.AddSeriesOffsets(sid, ofs)
		return nil
	}
	wal.addSeriesKeyOffset(seriesKey, ofs)
	return nil
}

func (wal *Wal) writeRecord(sid uint64, seriesKey []byte, rec []byte) error {
	block := wal.compress(sid, rec)

	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if sid == 0 {
		// indexes need to be created asynchronously
		err := writeExtBlock(wal.fileExt, seriesKey, flagSeriesKey)
		if err != nil {
			return err
		}
	}

	_, err := wal.file.Write(block)
	return err
}

func (wal *Wal) compress(sid uint64, rec []byte) []byte {
	buf := &wal.ctx.buf
	header := &wal.ctx.header
	var err error

	switch conf.WalCompressMode {
	case walCompressLz4:
		buf.Swap = slices.Grow(buf.Swap, walBlockHeaderSize)
		buf.Swap, err = LZ4CompressBlock(rec, buf.Swap[:walBlockHeaderSize])
		if err != nil {
			// fault-tolerant processing without compressing data
			logger.GetLogger().Error("failed to lz4 compress block", zap.Error(err))
			break
		}

		header.Set(len(buf.Swap)-walBlockHeaderSize, walCompressLz4, sid)
		header.Put(buf.Swap)
		return buf.Swap
	case walCompressSnappy:
		buf.Swap = slices.Grow(buf.Swap, walBlockHeaderSize)
		buf.Swap = SnappyCompressBlock(rec, buf.Swap[:walBlockHeaderSize])
		header.Set(len(buf.Swap)-walBlockHeaderSize, walCompressSnappy, sid)
		header.Put(buf.Swap)
		return buf.Swap
	default:
		break
	}

	buf.Swap = append(buf.Swap[:0], zeroBlockHeader[:]...)
	buf.Swap = append(buf.Swap, rec...)
	header.Set(len(buf.Swap)-walBlockHeaderSize, walCompressNone, sid)
	header.Put(buf.Swap)
	return buf.Swap
}

func (wal *Wal) Opened() bool {
	return wal.opened
}

func (wal *Wal) Converted() bool {
	return wal.converted
}

func (wal *Wal) Flush() error {
	if wal.backgroundFlush {
		return nil
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	util.MustRun(wal.fileExt.Flush)
	return wal.file.Flush()
}

func (wal *Wal) Sync() error {
	if wal.backgroundSync {
		return nil
	}
	return wal.sync()
}

func (wal *Wal) sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	util.MustRun(wal.fileExt.Flush)
	util.MustRun(wal.fileExt.Sync)

	err := wal.file.Flush()
	if err != nil {
		return err
	}

	return wal.file.Sync()
}

func (wal *Wal) PopSeriesKey() ([]byte, int64) {
	return wal.seriesKeyOffsets.Pop()
}

func (wal *Wal) GetAllSid(dst []uint64) []uint64 {
	return wal.seriesOffsets.GetAllKeyNoLock(dst)
}

func (wal *Wal) MustClose() {
	util.MustRun(wal.sync)
	wal.closeOnce.Do(func() {
		util.MustClose(wal.file)
		util.MustClose(wal.fileExt)
	})
}

func (wal *Wal) Clean() {
	lockOpt := fileops.FileLockOption(*wal.lock)
	util.MustRun(func() error {
		return fileops.RemoveAll(wal.file.Name(), lockOpt)
	})
	util.MustRun(func() error {
		return fileops.RemoveAll(wal.fileExt.Name(), lockOpt)
	})
}

func (wal *Wal) Load(name string) error {
	wal.file = NewWalFile(name, wal.lock)
	wal.fileExt = NewWalFile(name+walExtensionSuffix, wal.lock)

	if err := wal.file.Open(os.O_RDONLY); err != nil {
		return err
	}
	if err := wal.fileExt.Open(os.O_RDONLY); err != nil {
		return err
	}

	wal.codec = NewWalRecordDecoder()
	wal.opened = true

	seriesKeys := wal.loadExt()
	wal.loadWal(seriesKeys)

	return nil
}

func (wal *Wal) loadWal(seriesKeys [][]byte) {
	header := &wal.ctx.header
	buf := make([]byte, walBlockHeaderSize)
	var err error

	fd := wal.file.fd
	var offset int64 = 0
	var n = 0

	for {
		_, err = io.ReadFull(wal.file.fd, buf[:])
		if err != nil {
			logger.GetLogger().Error("failed to read ext block", zap.Error(err))
			break
		}

		err = header.Unmarshal(buf)
		if err != nil {
			logger.GetLogger().Error("failed to unmarshal ext block", zap.Error(err))
			break
		}

		if header.sid > 0 {
			wal.AddSeriesOffsets(header.sid, offset)
		} else if len(seriesKeys) > n {
			wal.addSeriesKeyOffset(seriesKeys[n], offset)
			n++
		}

		offset += int64(walBlockHeaderSize) + int64(header.size)

		_, err = fd.Seek(offset, io.SeekStart)
		if err != nil {
			logger.GetLogger().Error("failed to seek offset", zap.Error(err))
			break
		}
	}
}

func (wal *Wal) loadExt() [][]byte {
	var header [8]byte
	var err error
	var buf []byte
	var flag uint8
	var seriesKeys [][]byte
	reader := bufio.NewReaderSize(wal.fileExt.fd, walBufferSize)

	for {
		flag, buf, err = readExtBlock(reader, header[:], buf)
		if err != nil {
			logger.GetLogger().Error("failed to read ext block", zap.Error(err))
			break
		}

		switch flag {
		case flagSeriesKey:
			seriesKeys = append(seriesKeys, buf)
			buf = nil
		}
	}
	return seriesKeys
}

func (wal *Wal) AddSeriesOffsets(sid uint64, offsets ...int64) {
	wal.seriesOffsets.Add(sid, offsets...)
}

func (wal *Wal) addSeriesKeyOffset(seriesKey []byte, offset int64) {
	wal.seriesKeyOffsets.Add(seriesKey, offset)
}

type WalFile struct {
	name string
	lock *string

	fd          fileops.File
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

func (wf *WalFile) Open(flag int) error {
	lockOpt := fileops.FileLockOption(*wf.lock)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)

	fd, err := fileops.OpenFile(wf.name, flag, 0600, lockOpt, pri)
	if err != nil {
		return err
	}

	wf.fd = fd
	wf.reader = fd
	wf.writer = bufio.NewWriterSize(fd, walBufferSize)
	return nil
}

func (wf *WalFile) LoadIntoMemory() {
	if wf.writtenSize == 0 {
		return
	}

	mr := &MemWalReader{}
	err := mr.Load(wf.fd, int(wf.writtenSize))
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
	n, err := wf.writer.Write(b)
	wf.writtenSize += int64(n)
	return n, err
}

func (wf *WalFile) Flush() error {
	if wf.writer.Size() == 0 {
		return nil
	}
	defer statistics.MicroTimeUse(stat.DiskFlushCount, stat.DiskFlushDurSum)()

	return wf.writer.Flush()
}

func (wf *WalFile) Close() error {
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
	end := int64(len(dst)) + ofs
	if wf.syncedSize > 0 && wf.syncedSize < end {
		return 0, io.EOF
	}

	return wf.reader.ReadAt(dst, ofs)
}

type WalBlockHeader struct {
	size uint32
	flag uint32
	sid  uint64
}

func (h *WalBlockHeader) Set(size int, flag uint32, sid uint64) {
	h.size = uint32(size)
	h.flag = flag
	h.sid = sid
}

func (h *WalBlockHeader) Put(dst []byte) {
	if cap(dst) < walBlockHeaderSize {
		return
	}

	dst = dst[:walBlockHeaderSize]
	binary.BigEndian.PutUint32(dst, h.size)
	binary.BigEndian.PutUint32(dst[util.Uint32SizeBytes:], h.flag)
	binary.BigEndian.PutUint64(dst[util.Uint32SizeBytes*2:], h.sid)
}

func (h *WalBlockHeader) Unmarshal(src []byte) error {
	if len(src) < walBlockHeaderSize {
		return fmt.Errorf("too few bytes for WalBlockHeader")
	}

	h.size = binary.BigEndian.Uint32(src)
	h.flag = binary.BigEndian.Uint32(src[util.Uint32SizeBytes:])
	h.sid = binary.BigEndian.Uint64(src[util.Uint32SizeBytes*2:])
	return nil
}

type Offsets struct {
	ofs []int64
}

func (o *Offsets) Add(v ...int64) {
	o.ofs = append(o.ofs, v...)
}

type SeriesKeyOffsets struct {
	mu      sync.RWMutex
	keys    [][]byte
	offsets []int64
}

func NewSeriesKeyOffsets() *SeriesKeyOffsets {
	return &SeriesKeyOffsets{}
}

func (o *SeriesKeyOffsets) Add(key []byte, ofs int64) {
	k := slices.Clone(key)
	o.mu.Lock()
	o.keys = append(o.keys, k)
	o.offsets = append(o.offsets, ofs)
	o.mu.Unlock()
}

func (o *SeriesKeyOffsets) Pop() ([]byte, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	size := len(o.keys)
	if size == 0 {
		return nil, 0
	}

	key, ofs := o.keys[size-1], o.offsets[size-1]
	o.keys = o.keys[:size-1]
	o.offsets = o.offsets[:size-1]
	return key, ofs
}

func (o *SeriesKeyOffsets) Len() int {
	o.mu.RLock()
	n := len(o.keys)
	o.mu.RUnlock()
	return n
}

type SeriesOffsets struct {
	mu   sync.RWMutex
	data map[uint64]*Offsets
}

func NewSeriesOffsets() *SeriesOffsets {
	return &SeriesOffsets{data: make(map[uint64]*Offsets)}
}

func (o *SeriesOffsets) Add(key uint64, ofs ...int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	offsets, ok := o.data[key]
	if !ok {
		offsets = &Offsets{}
		o.data[key] = offsets
	}
	offsets.Add(ofs...)
}

func (o *SeriesOffsets) Get(sid uint64) []int64 {
	o.mu.RLock()
	defer o.mu.RUnlock()

	offsets, ok := o.data[sid]
	if !ok {
		return nil
	}
	n := len(offsets.ofs)
	return offsets.ofs[:n]
}

func (o *SeriesOffsets) GetAllKeyNoLock(dst []uint64) []uint64 {
	for sid := range o.data {
		dst = append(dst, sid)
	}
	return dst
}

func LZ4CompressBlock(src, dst []byte) ([]byte, error) {
	bound := lz4.CompressBlockBound(len(src)) +
		util.Uint32SizeBytes // extra 4 bytes record the length of the original data

	size := len(dst)
	dst = slices.Grow(dst, bound)

	n, err := lz4.CompressBlock(src, dst[size+util.Uint32SizeBytes:size+bound])
	if err == nil {
		binary.BigEndian.PutUint32(dst[size:size+util.Uint32SizeBytes], uint32(len(src)))
		return dst[:n+size+util.Uint32SizeBytes], nil
	}

	return nil, err
}

func LZ4DecompressBlock(src, dst []byte) ([]byte, error) {
	size := int(binary.BigEndian.Uint32(src))
	dst = slices.Grow(dst[:0], size)
	_, err := lz4.DecompressSafe(src[util.Uint32SizeBytes:], dst[:size])
	if err != nil {
		return nil, err
	}
	return dst[:size], nil
}

func SnappyCompressBlock(src, dst []byte) []byte {
	bound := snappy.MaxEncodedLen(len(src))

	size := len(dst)
	dst = slices.Grow(dst, bound)

	enc := snappy.Encode(dst[size:size+bound], src)
	return dst[:len(enc)+size]
}

func SnappyDecompressBlock(src, dst []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func writeExtBlock(w io.Writer, data []byte, flag uint8) error {
	header := uint64(len(data))<<32 | uint64(flag)
	err := binary.Write(w, binary.BigEndian, header)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

func readExtBlock(r *bufio.Reader, header []byte, dst []byte) (uint8, []byte, error) {
	_, err := r.Read(header[:])
	if err != nil {
		return 0, nil, err
	}

	n := binary.BigEndian.Uint64(header[:])
	blockSize := n >> 32
	if cap(dst) < int(blockSize) {
		dst = make([]byte, blockSize)
	}
	_, err = r.Read(dst[:blockSize])
	return uint8(n & 0xFF), dst[:blockSize], err
}
