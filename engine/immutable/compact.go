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

package immutable

import (
	"bytes"
	"container/heap"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	CompactLevels        = 7
	defaultFileSizeLimit = 8 * 1024 * 1024 * 1024
	minFileSizeLimit     = 1 * 1024 * 1024
	MergeFirstSeqLen     = 5
	MergeFirstSeqSize    = 1 * 1024 * 1024
	MergeFirstMergeTime  = 300
	MergeFirstAvgSize    = 10 * 1024 * 1024
	MergeFirstDstSize    = 10 * 1024 * 1024
	MergeFirstRatio      = 0.75
)

var (
	fullCompactingCount int64
	maxFullCompactor    = cpu.GetCpuNum() / 2
	maxCompactor        = cpu.GetCpuNum()
	compLimiter         limiter.Fixed
	ErrCompStopped      = errors.New("compact stopped")
	ErrDroppingMst      = errors.New("measurement is dropped")
	LevelCompactRule    = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
	LeveLMinGroupFiles  = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
	compLogSeq          = uint64(time.Now().UnixNano())

	EnableMergeOutOfOrder       = true
	MaxNumOfFileToMerge         = 256
	MaxSizeOfFileToMerge  int64 = 512 * 1024 * 1024 // 512MB
	log                         = Log.GetLogger()

	stat        = statistics.NewMergeStatistics()
	compactStat = statistics.NewCompactStatistics()
)

func Init() {
	log = Log.GetLogger()
}

func SetMaxCompactor(n int) {
	log = Log.GetLogger().With(zap.String("service", "compact"))
	maxCompactor = n
	if maxCompactor == 0 {
		maxCompactor = cpu.GetCpuNum()
	}

	if maxCompactor < 2 {
		maxCompactor = 2
	}

	if maxCompactor > 32 {
		maxCompactor = 32
	}

	compLimiter = limiter.NewFixed(maxCompactor)
	log.Info("set maxCompactor", zap.Int("number", maxCompactor))
}

func SetMaxFullCompactor(n int) {
	log = Log.GetLogger().With(zap.String("service", "compact"))
	maxFullCompactor = n
	if maxFullCompactor == 0 {
		maxFullCompactor = cpu.GetCpuNum() / 2
		if maxFullCompactor < 1 {
			maxFullCompactor = 1
		}
	}

	if maxFullCompactor >= maxCompactor {
		maxFullCompactor = maxCompactor / 2
	}

	if maxFullCompactor < 1 {
		maxFullCompactor = 1
	}

	if maxFullCompactor > 32 {
		maxFullCompactor = 32
	}

	log.Info("set maxFullCompactor", zap.Int("number", maxFullCompactor))
}

type ChunkIterator struct {
	*FileIterator
	ctx    *ReadContext
	id     uint64
	fields record.Schemas
	rec    *record.Record
	merge  *record.Record
	log    *Log.Logger
}

type ChunkIterators struct {
	closed       chan struct{}
	dropping     *int64
	name         string
	itrs         []*ChunkIterator
	id           uint64
	merged       *record.Record
	estimateSize int
	maxN         int

	log *Log.Logger
}

func (c *ChunkIterators) WithLog(log *Log.Logger) {
	c.log = log
	for i := range c.itrs {
		c.itrs[i].WithLog(log)
	}
}

func (c *ChunkIterators) Len() int      { return len(c.itrs) }
func (c *ChunkIterators) Swap(i, j int) { c.itrs[i], c.itrs[j] = c.itrs[j], c.itrs[i] }
func (c *ChunkIterators) Less(i, j int) bool {
	iID := c.itrs[i].id
	jID := c.itrs[j].id
	if iID != jID {
		return iID < jID
	}

	return c.itrs[i].merge.MinTime(true) < c.itrs[j].merge.MinTime(true)
}

func (c *ChunkIterators) Push(v interface{}) {
	c.itrs = append(c.itrs, v.(*ChunkIterator))
}

func (c *ChunkIterators) Pop() interface{} {
	l := len(c.itrs)
	v := c.itrs[l-1]
	c.itrs = c.itrs[:l-1]
	return v
}

func (c *ChunkIterators) Close() {
	for _, itr := range c.itrs {
		itr.Close()
	}
}

func (c *ChunkIterators) stopCompact() bool {
	if atomic.LoadInt64(c.dropping) > 0 {
		return true
	}

	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *ChunkIterators) Next() (uint64, *record.Record, error) {
	defer func() { c.id = 0 }()

	if c.Len() == 0 {
		return 0, nil, nil
	}

	if c.stopCompact() {
		c.log.Error("compact stopped")
		return 0, nil, ErrCompStopped
	}

	itr, _ := heap.Pop(c).(*ChunkIterator)
	preId, rec := itr.id, itr.merge

	c.id = preId
	c.merged.Reset()
	c.merged.SetSchema(rec.Schema)
	c.merged.ReserveColVal(len(rec.Schema))
	c.merged.ReserveColumnRows(rec.RowNums())
	c.merged.Merge(rec)

	if !itr.Next() {
		itr.Close()
		if itr.err != nil {
			return 0, nil, itr.err
		}
	} else {
		heap.Push(c, itr)
	}

	for c.Len() > 0 {
		itr, _ = heap.Pop(c).(*ChunkIterator)
		if c.id == itr.id {
			c.merged.Merge(itr.merge)
			itr.id = 0
		} else {
			heap.Push(c, itr)
			return c.id, c.merged, nil
		}

		if c.stopCompact() {
			return 0, nil, ErrCompStopped
		}

		if !itr.Next() {
			itr.Close()
			if itr.err != nil {
				return 0, nil, itr.err
			}
			continue
		}

		heap.Push(c, itr)
	}

	return c.id, c.merged, nil
}

func NewChunkIterator(r *FileIterator) *ChunkIterator {
	itr := &ChunkIterator{
		FileIterator: r,
		ctx:          NewReadContext(true),
		merge:        allocRecord(),
		rec:          allocRecord(),
	}

	return itr
}

func (c *ChunkIterator) WithLog(log *Log.Logger) {
	c.log = log
}

func (c *ChunkIterator) Close() {
	c.FileIterator.Close()
	freeRecord(c.rec)
	freeRecord(c.merge)
	c.ctx.Release()
	c.ctx = nil
}

func (c *ChunkIterator) Next() bool {
	if c.err != nil {
		return false
	}

	if c.chunkUsed >= c.chunkN || c.mIndexPos > c.mIndexN {
		return false
	}

	if !c.NextChunkMeta() {
		return false
	}

	if cap(c.fields) < int(c.curtChunkMeta.columnCount) {
		delta := int(c.curtChunkMeta.columnCount) - cap(c.fields)
		c.fields = c.fields[:cap(c.fields)]
		c.fields = append(c.fields, make([]record.Field, delta)...)
	}
	c.fields = c.fields[:c.curtChunkMeta.columnCount]
	for i := range c.curtChunkMeta.colMeta {
		cm := c.curtChunkMeta.colMeta[i]
		c.fields[i].Name = cm.name
		c.fields[i].Type = int(cm.ty)
	}

	if c.err = c.read(); c.err != nil {
		return false
	}

	return true
}

func (c *ChunkIterator) read() error {
	var err error
	c.id = c.curtChunkMeta.sid
	cMeta := c.curtChunkMeta

	c.merge.Reset()
	c.merge.SetSchema(c.fields)
	c.merge.ReserveColVal(len(c.fields))
	timeMeta := cMeta.timeMeta()
	for i := range timeMeta.entries {
		c.rec.Reset()
		c.rec.SetSchema(c.fields)
		c.rec.ReserveColVal(len(c.fields))
		c.rec.ReserveColumnRows(8)
		record.CheckRecord(c.rec)

		c.rec, err = c.r.ReadAt(cMeta, i, c.rec, c.ctx)
		if err != nil {
			c.log.Error("read segment error", zap.String("file", c.r.Path()), zap.Error(err))
			return err
		}

		c.segPos++

		record.CheckRecord(c.rec)
		c.merge.Merge(c.rec)
		record.CheckRecord(c.merge)
	}

	if c.segPos >= len(timeMeta.entries) {
		c.curtChunkMeta = nil
		c.chunkUsed++
	}

	return nil
}

func (m *MmsTables) refMmsTable(name string, refOutOfOrder bool) (orderWg, outOfOrderWg *sync.WaitGroup) {
	m.mu.RLock()
	fs, ok := m.Order[name]
	if ok {
		fs.wg.Add(1)
		orderWg = &fs.wg
	}
	if refOutOfOrder {
		fs, ok = m.OutOfOrder[name]
		if ok {
			fs.wg.Add(1)
			outOfOrderWg = &fs.wg
		}
	}
	m.mu.RUnlock()

	return
}

func (m *MmsTables) unrefMmsTable(orderWg, outOfOrderWg *sync.WaitGroup) {
	if orderWg != nil {
		orderWg.Done()
	}
	if outOfOrderWg != nil {
		outOfOrderWg.Done()
	}
}

func (m *MmsTables) LevelCompact(level uint16, shid uint64) error {
	plans := m.LevelPlan(level)
	for len(plans) > 0 {
		plan := plans[0]
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.refMmsTable(group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					m.unrefMmsTable(orderWg, inorderWg)
					compLimiter.Release()
					m.CompactDone(group.group)
					group.release()
				}()

				fi, err := m.NewFileIterators(group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				if NonStreamingCompaction(fi) {
					err = m.compactToLevel(fi, false)
				} else {
					err = m.streamCompactToLevel(fi, false)
				}

				if err != nil {
					compactStat.AddErrors(1)
					log.Error("compact error", zap.Error(err))
				}
			}(plan)
		}
		plans = plans[1:]
	}

	return nil
}

func (m *MmsTables) NewChunkIterators(group FilesInfo) (*ChunkIterators, error) {
	compItrs := &ChunkIterators{
		closed:   m.closed,
		dropping: group.dropping,
		name:     group.name,
		itrs:     make([]*ChunkIterator, 0, len(group.compIts)),
		merged:   &record.Record{},
	}

	for _, i := range group.compIts {
		itr := NewChunkIterator(i)
		if !itr.Next() {
			itr.Close()
			continue
		}
		compItrs.itrs = append(compItrs.itrs, itr)
	}
	compItrs.maxN = group.maxChunkN
	compItrs.estimateSize = group.estimateSize

	heap.Init(compItrs)

	return compItrs, nil
}

func (m *MmsTables) compactToLevel(group FilesInfo, full bool) error {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	cLog, logEnd := logger.NewOperation(log, "Compaction", group.name)
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))

	lcLog.Debug(fmt.Sprintf("compactionGroup: name=%v, groups=%v", group.name, group.oldFids))

	compItrs, err := m.NewChunkIterators(group)
	if err != nil {
		lcLog.Error("new chunk readers fail", zap.Error(err))
		return err
	}

	compItrs.WithLog(lcLog)
	oldFilesSize := compItrs.estimateSize
	newFiles, err := m.compact(compItrs, group.oldFiles, group.toLevel, true, lcLog)
	if err != nil {
		lcLog.Error("compact fail", zap.Error(err))
		compItrs.Close()
		return err
	}

	compItrs.Close()
	if err = m.ReplaceFiles(group.name, group.oldFiles, newFiles, true, lcLog); err != nil {
		lcLog.Error("replace compacted file error", zap.Error(err))
		return err
	}

	end := time.Now()
	lcLog.Debug("compact file done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = sumFilesSize(newFiles)
	}
	return nil
}

func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *Log.Logger) ([]TSSPFile, error) {
	_, seq := files[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, 0, 0, isOrder)
	tableBuilder := AllocMsBuilder(m.path, itrs.name, m.Conf, itrs.maxN, fileName, *m.tier, nil, itrs.estimateSize)
	defer func(msb **MsBuilder) {
		PutMsBuilder(*msb)
	}(&tableBuilder)
	tableBuilder.WithLog(cLog)
	for {
		select {
		case <-m.closed:
			return nil, ErrCompStopped
		default:
		}

		id, rec, err := itrs.Next()
		if err != nil {
			cLog.Error("read data fail", zap.Error(err))
			return nil, err
		}

		if rec == nil || id == 0 {
			break
		}

		record.CheckRecord(rec)
		tableBuilder, err = tableBuilder.WriteRecord(id, rec, func(fn TSSPFileName) (uint64, uint16, uint16, uint16) {
			ext := fn.extent
			ext++
			return fn.seq, fn.level, 0, ext
		})
		if err != nil {
			cLog.Error("write record fail", zap.Error(err))
			return nil, err
		}
	}

	if tableBuilder.Size() > 0 {
		f, err := tableBuilder.NewTSSPFile(true)
		if err != nil {
			cLog.Error("new tssp file fail", zap.Error(err))
			return nil, err
		}
		if f != nil {
			tableBuilder.Files = append(tableBuilder.Files, f)
		}
	} else {
		tableBuilder.removeEmptyFile()
	}

	newFiles := make([]TSSPFile, 0, len(tableBuilder.Files))
	newFiles = append(newFiles, tableBuilder.Files...)
	return newFiles, nil
}

type CompactedFileInfo struct {
	Name    string
	IsOrder bool
	OldFile []string
	NewFile []string
}

func (info CompactedFileInfo) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.Name)))
	dst = append(dst, info.Name...)
	dst = numberenc.MarshalBool(dst, info.IsOrder)
	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFile)))
	for i := range info.OldFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.OldFile[i])))
		dst = append(dst, info.OldFile[i]...)
	}

	dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFile)))
	for i := range info.NewFile {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(info.NewFile[i])))
		dst = append(dst, info.NewFile[i]...)
	}

	return dst
}

func (info *CompactedFileInfo) reset() {
	info.Name = ""
	info.OldFile = info.OldFile[:0]
	info.NewFile = info.NewFile[:0]
}

func (info *CompactedFileInfo) unmarshal(src []byte) error {
	if len(src) < 2 {
		return fmt.Errorf("too small data for name length, %v", len(src))
	}
	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l+3 {
		return fmt.Errorf("too small data for name, %v < %v", len(src), l+3)
	}
	info.Name, src = record.Bytes2str(src[:l]), src[l:]
	info.IsOrder, src = numberenc.UnmarshalBool(src[0]), src[1:]

	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	if cap(info.OldFile) < l {
		info.OldFile = make([]string, l)
	}
	info.OldFile = info.OldFile[:l]
	for i := range info.OldFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for old file name, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		info.OldFile[i], src = record.Bytes2str(src[:l]), src[l:]
	}

	if len(src) < 2 {
		return fmt.Errorf("too small data new file count, %v", len(src))
	}
	l, src = int(numberenc.UnmarshalUint16(src)), src[2:]
	info.NewFile = make([]string, l)
	if cap(info.NewFile) < l {
		info.NewFile = make([]string, l)
	}
	info.NewFile = info.NewFile[:l]
	for i := range info.NewFile {
		if len(src) < 2 {
			return fmt.Errorf("too small data for new file name, %v", len(src))
		}
		l, src = int(numberenc.UnmarshalUint16(src)), src[2:]

		if len(src) < l {
			return fmt.Errorf("too small data for name, %v < %v", len(src), l)
		}
		info.NewFile[i], src = record.Bytes2str(src[:l]), src[l:]
	}

	return nil
}

var ErrDirtyLog = errors.New("incomplete log file")
var compLogMagic = []byte("2021A5A5")

func genLogFileName() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:8]); err != nil {
		log.Error("read random error", zap.Error(err))
		panic(err)
	}
	seq := atomic.AddUint64(&compLogSeq, 1)
	copy(buf[8:], record.Uint64ToBytes(seq))
	return fmt.Sprintf("%08x-%08x", buf[:8], buf[8:])
}

func (m *MmsTables) writeCompactedFileInfo(name string, oldFiles, newFiles []TSSPFile, shardDir string, isOrder bool) (string, error) {
	info := &CompactedFileInfo{
		Name:    name,
		IsOrder: isOrder,
		OldFile: make([]string, len(oldFiles)),
		NewFile: make([]string, len(newFiles)),
	}

	for i, f := range oldFiles {
		info.OldFile[i] = filepath.Base(f.Path())
	}

	for i, f := range newFiles {
		info.NewFile[i] = filepath.Base(f.Path())
	}

	fDir := filepath.Join(shardDir, compactLogDir)
	_ = fileops.MkdirAll(fDir, 0750)
	fName := filepath.Join(fDir, genLogFileName())

	buf := bufferpool.Get()
	defer bufferpool.Put(buf)

	buf = info.marshal(buf[:0])
	buf = append(buf, compLogMagic...)

	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(fName, os.O_CREATE|os.O_WRONLY, 0640, lock, pri)
	if err != nil {
		log.Error("create file error", zap.String("name", fName), zap.Error(err))
		return "", err
	}

	s, err := fd.Write(buf)
	if err != nil || s != len(buf) {
		err = fmt.Errorf("write compact file info log fail, write %v, size %v, err:%v", s, len(buf), err)
		log.Error("write compact file info log fail", zap.Int("write", s), zap.Int("size", len(buf)), zap.Error(err))
		panic(err)
	}

	if err = fd.Sync(); err != nil {
		log.Error("sync compact log file file")
		panic(err)
	}

	return fName, fd.Close()
}

func readCompactLogFile(name string, info *CompactedFileInfo) error {
	fi, err := fileops.Stat(name)
	if err != nil {
		log.Error("stat compact file fail", zap.String("name", name), zap.Error(err))
		return err
	}

	fSize := fi.Size()
	if fSize < int64(len(compLogMagic)) {
		err = fmt.Errorf("too small compact log file(%v) size %v", name, fSize)
		log.Error(err.Error())
		return ErrDirtyLog
	}

	buf := make([]byte, int(fSize))
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(name, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		log.Error("read compact log file fail", zap.String("name", name), zap.Error(err))
		return err
	}
	defer util.MustClose(fd)

	n, err := fd.Read(buf)
	if err != nil || n != len(buf) {
		err = fmt.Errorf("read compact log file(%v) fail, file size:%v, read size:%v", name, fSize, n)
		log.Error(err.Error())
		return err
	}

	magic := buf[fSize-int64(len(compLogMagic)):]
	if !bytes.Equal(magic, compLogMagic) {
		err = fmt.Errorf("invalid compact log file(%v) magic: exp:%v, read:%v",
			name, record.Bytes2str(compLogMagic), record.Bytes2str(magic))
		log.Error(err.Error())
		return ErrDirtyLog
	}

	if err = info.unmarshal(buf); err != nil {
		log.Error("unmarshal compact log fail", zap.String("name", name), zap.Error(err))
		return err
	}

	return nil
}

func processLog(shardDir string, info *CompactedFileInfo) error {
	mmDir := filepath.Join(shardDir, TsspDirName, info.Name)
	dirs, err := fileops.ReadDir(mmDir)
	if err != nil {
		log.Error("read dir fail", zap.String("path", mmDir), zap.Error(err))
		return err
	}

	newFileExist := func(newFile string) bool {
		normalName := newFile[:len(newFile)-len(tmpTsspFileSuffix)]
		for i := range dirs {
			name := dirs[i].Name()
			if name == normalName || newFile == name {
				return true
			}
		}
		return false
	}

	oldFileExist := func(oldFile string) bool {
		for i := range dirs {
			name := dirs[i].Name()
			tmp := oldFile + tmpTsspFileSuffix
			if name == oldFile || tmp == name {
				return true
			}
		}
		return false
	}

	renameFile := func(nameInLog string) error {
		for i := range dirs {
			name := dirs[i].Name()
			if nameInLog == name {
				lock := fileops.FileLockOption("")
				normalName := nameInLog[:len(nameInLog)-len(tmpTsspFileSuffix)]
				oldName := filepath.Join(mmDir, nameInLog)
				newName := filepath.Join(mmDir, normalName)
				return fileops.RenameFile(oldName, newName, lock)
			}
		}
		return nil
	}

	n := 0
	for i := range info.NewFile {
		if newFileExist(info.NewFile[i]) {
			n++
		}
	}

	if n != len(info.NewFile) {
		count := 0
		for i := range info.OldFile {
			if oldFileExist(info.OldFile[i]) {
				count++
			}
		}

		// all old file exist
		if count == len(info.OldFile) {
			for i := range info.OldFile {
				oName := info.OldFile[i]
				if err := renameFile(oName + tmpTsspFileSuffix); err != nil {
					return err
				}
			}
			return nil
		}

		err = fmt.Errorf("invalid compact log file, name:%v, oldFiles:%v, newFiles:%v, order:%v, dirs:%v",
			info.Name, info.OldFile, info.NewFile, info.IsOrder, dirs)
		log.Error(err.Error())
		return err
	}

	// rename all new files
	for i := range info.NewFile {
		if err := renameFile(info.NewFile[i]); err != nil {
			log.Error("rename file fail", zap.String("name", info.NewFile[i]), zap.Error(err))
			return err
		}
	}

	// delete all old files
	for i := range info.OldFile {
		oldName := info.OldFile[i]
		if oldFileExist(oldName) {
			fName := filepath.Join(mmDir, oldName)
			if _, err := fileops.Stat(fName); os.IsNotExist(err) {
				continue
			}
			lock := fileops.FileLockOption("")
			if err = fileops.Remove(fName, lock); err != nil {
				return err
			}
		}
	}

	return nil
}

func procCompactLog(shardDir string, logDir string) error {
	dirs, err := fileops.ReadDir(logDir)
	if err != nil {
		log.Error("read compact log dir fail", zap.String("path", logDir), zap.Error(err))
		return err
	}

	logInfo := &CompactedFileInfo{}
	for i := range dirs {
		logName := dirs[i].Name()
		logFile := filepath.Join(logDir, logName)
		logInfo.reset()
		err = readCompactLogFile(logFile, logInfo)
		if err != nil {
			if err != ErrDirtyLog {
				return err
			}
			continue
		}

		// continue to handle the rest compact log
		if err = processLog(shardDir, logInfo); err != nil {
			errInfo := errno.NewError(errno.ProcessCompactLogFailed, logInfo.Name, err.Error())
			log.Error("", zap.Error(errInfo))
		}

		lock := fileops.FileLockOption("")
		_ = fileops.Remove(logFile, lock)
	}
	return nil
}

func (m *MmsTables) MergeOutOfOrder(shId uint64) error {
	outs := m.getOutOfOrderFiles(maxCompactor)
	for mn, files := range outs {
		if len(files.Seqs) == 0 {
			continue
		}
		if !m.inMerge.Add(mn) {
			log.Info("merging in progress", zap.String("name", mn))
			continue
		}

		select {
		case <-m.closed:
			log.Warn("shard closed", zap.Uint64("id", shId))
			return fmt.Errorf("store closed, shard id: %v", shId)
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			go func(name string, ctx *OutOfOrderMergeContext) {
				stat.AddActive(1)
				ctx.mstName = name
				ctx.shId = shId
				cLog, logEnd := logger.NewOperation(log, "MergeOutOfOrder", name)
				defer func() {
					stat.AddActive(-1)
					m.wg.Done()
					compLimiter.Release()
					m.inMerge.Del(name)
					logEnd()
					ctx.Release()
				}()

				if m.compactRecovery {
					defer MergeRecovery(m.path, name, ctx)
				}

				m.mergeOutOfOrderFiles(Log.NewLogger(errno.ModuleMerge).SetZapLogger(cLog), ctx)
			}(mn, files)
		}
	}

	return nil
}

func (m *MmsTables) cancelFun(dropping *int64) func() bool {
	cancelFun := func() bool {
		closing := m.closed
		if atomic.LoadInt64(dropping) > 0 {
			return true
		}

		select {
		case <-closing:
			return true
		default:
			return false
		}
	}
	return cancelFun
}

func toString(names ...TSSPFileName) string {
	var sb strings.Builder
	for _, n := range names {
		sb.WriteString(fmt.Sprintf("[%v]", n))
	}
	return sb.String()
}

func (m *MmsTables) mergeOutOfOrderFiles(logger *Log.Logger, ctx *OutOfOrderMergeContext) {
	if len(ctx.Seqs) < MergeFirstSeqLen && ctx.size < MergeFirstSeqSize && time.Since(m.lastMergeTime).Seconds() < MergeFirstMergeTime {
		logger.Info("new and small out of order files, merge later",
			zap.String("name", ctx.mstName),
			zap.Uint64s("out sequences", ctx.Seqs),
			zap.Int64("out size", ctx.size))
		return
	}

	m.matchFilesByTimeRange(ctx)
	if len(ctx.orderSeqs) == 0 {
		return
	}

	sort.Sort(&ctx.OrderInfo)

	success := false
	defer func() {
		if !success {
			stat.AddErrors(1)
		}
	}()

	if !m.acquire(ctx.orderNames) {
		logger.Warn("acquire is false, skip merge", zap.String("names", toString(ctx.orderSeqs...)))
		return
	}
	orderWg, inorderWg := m.refMmsTable(ctx.mstName, false)
	defer func() {
		m.unrefMmsTable(orderWg, inorderWg)
		m.CompactDone(ctx.orderNames)
		m.lastMergeTime = time.Now()
	}()

	unorders, err := m.getFilesBySequences(ctx.mstName, ctx.Names, false)
	if err != nil {
		logger.Error("failed to get files by sequences", zap.Error(err))
		return
	}

	dfs, err := m.getFilesBySequences(ctx.mstName, ctx.orderNames, true)
	if err != nil {
		logger.Error("failed to get files by sequences", zap.Error(err))
		UnrefFiles(unorders.files...)
		return
	}

	orderFileSize := sumFilesSize(dfs.files)

	fs := m.tableFiles(ctx.mstName, true)
	if fs == nil {
		logger.Error("failed to get ordered TSSPFiles", zap.String("name", ctx.mstName))
		UnrefFiles(unorders.files...)
		UnrefFiles(dfs.files...)
		return
	}

	hlp := NewMergeHelper(logger, m.Tier(), ctx.mstName, m.path, m.cancelFun(&fs.closing))
	hlp.Conf = m.Conf
	hlp.stat = statistics.NewMergeStatItem(ctx.mstName, ctx.shId)
	if !hlp.ReadWaitMergedRecords(unorders) {
		UnrefFiles(unorders.files...)
		UnrefFiles(dfs.files...)
		return
	}

	logger.Info("mergeFirst check",
		zap.Uint64s("out sequences", ctx.Seqs),
		zap.Int64("out size", ctx.size),
		zap.Int64("orderFileSize", orderFileSize),
	)
	if mergeFirst(len(ctx.Seqs), ctx.size, orderFileSize) {
		if err = m.mergeToSelf(logger, hlp, unorders, ctx.Seqs, ctx.mstName); err != nil {
			UnrefFiles(unorders.files...)
			logger.Error("merge out of order files to out of order files failed", zap.Error(err))
		}
		UnrefFiles(dfs.files...)
		return
	}

	logger.Info("merge info",
		zap.String("name", ctx.mstName),
		zap.String("order names", toString(ctx.orderSeqs...)),
		zap.Uint64s("out sequences", ctx.Seqs),
		zap.Int("number of out record", len(hlp.waits)),
		zap.Int64("total out file size", ctx.size))

	mergedFiles, err := hlp.MergeTo(dfs)
	if err != nil {
		for _, f := range mergedFiles {
			if f == nil {
				continue
			}
			path := f.Path()
			_ = f.Close()
			if path != "" {
				lock := fileops.FileLockOption("")
				_ = fileops.Remove(path, lock)
			}
		}
		logger.Error("merge failed", zap.Error(err))
		UnrefFiles(unorders.files...)
		UnrefFiles(dfs.files...)
		return
	}

	UnrefFiles(unorders.files...)
	UnrefFiles(dfs.files...)
	if err := m.replaceMergedFiles(ctx.mstName, logger, dfs.Files(), mergedFiles); err != nil {
		logger.Error("failed to replace merged files", zap.Error(err))
		return
	}

	m.deleteOutOfOrderFiles(ctx.mstName, unorders)
	success = true
	hlp.stat.Push()
}

func (m *MmsTables) replaceMergedFiles(name string, logger *Log.Logger, old []TSSPFile, new []TSSPFile) error {
	needReplaced := make(map[string]TSSPFile, len(old))
	for _, of := range old {
		for _, nf := range new {
			_, s1 := of.LevelAndSequence()
			_, s2 := nf.LevelAndSequence()

			if s1 == s2 {
				needReplaced[of.Path()] = of
			}
		}
	}

	old = old[:0]
	for k, f := range needReplaced {
		old = append(old, f)
		logger.Info("replace merged file",
			zap.String("old path", k), zap.Int64("old size", f.FileSize()))
	}

	return m.ReplaceFiles(name, old, new, true, logger)
}

func (m *MmsTables) getOutOfOrderFiles(n int) map[string]*OutOfOrderMergeContext {
	ret := make(map[string]*OutOfOrderMergeContext, n)
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.OutOfOrder {
		v.lock.RLock()
		if v.Len() == 0 {
			v.lock.RUnlock()
			continue
		}

		ret[k] = NewOutOfOrderMergeContext()
		for _, f := range v.Files() {
			if !ret[k].Add(f) {
				break
			}
		}
		v.lock.RUnlock()
		n--
		if n <= 0 {
			break
		}
	}

	return ret
}

func (m *MmsTables) deleteOutOfOrderFiles(name string, outs *TSSPFiles) {
	m.mu.RLock()

	v, ok := m.OutOfOrder[name]
	if !ok {
		m.mu.RUnlock()
		return
	}

	noFiles := false
	v.lock.Lock()
	for _, f := range outs.Files() {
		v.deleteFile(f)
		m.removeFile(f)
	}
	if v.Len() == 0 {
		noFiles = true
	} else {
		sort.Sort(v)
	}
	v.lock.Unlock()

	m.mu.RUnlock()

	if noFiles {
		m.mu.Lock()
		v, ok = m.OutOfOrder[name]
		if ok && v.Len() == 0 {
			delete(m.OutOfOrder, name)
		}
		m.mu.Unlock()
	}
}

func (m *MmsTables) GetOutOfOrderFileNum() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, v := range m.OutOfOrder {
		total += v.Len()
	}
	return total
}

func (m *MmsTables) GetMstFileStat() *statistics.FileStat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fileStat := statistics.NewFileStat()

	statsFunc := func(mstFiles map[string]*TSSPFiles) {
		for mstName, v := range mstFiles {
			files := v.files
			var totalSize int64
			for _, file := range files {
				size := file.FileSize()
				lv, _ := file.LevelAndSequence()
				fileStat.AddLevel(lv, size)
				totalSize += size
			}

			fileStat.AddMst(mstName, v.Len(), totalSize)
		}
	}

	statsFunc(m.Order)
	statsFunc(m.OutOfOrder)
	return fileStat
}

func (m *MmsTables) tableFiles(name string, order bool) *TSSPFiles {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !order {
		mmsTbls = m.OutOfOrder
	}

	return mmsTbls[name]
}

func (m *MmsTables) getFilesBySequences(mstName string, names []string, order bool) (*TSSPFiles, error) {
	files := NewTSSPFiles()
	files.files = make([]TSSPFile, 0, len(names))

	for _, fn := range names {
		if m.isClosed() {
			return nil, ErrCompStopped
		}
		f := m.File(mstName, fn, order)
		if f == nil {
			UnrefFiles(files.Files()...)
			return nil, fmt.Errorf("table %v, %v, %t not find", mstName, fn, order)
		}

		files.Append(f)
	}

	return files, nil
}

func (m *MmsTables) removeFile(f TSSPFile) {
	if f.Inuse() {
		if err := f.Rename(f.Path() + tmpTsspFileSuffix); err != nil {
			log.Error("failed to rename file", zap.String("path", f.Path()), zap.Error(err))
			return
		}
		nodeTableStoreGC.Add(false, f)
		return
	}

	err := f.Remove()
	if err != nil {
		nodeTableStoreGC.Add(false, f)
		log.Error("failed to remove file", zap.String("path", f.Path()), zap.Error(err))
		return
	}
}

func (m *MmsTables) matchFilesByTimeRange(ctx *OutOfOrderMergeContext) {
	m.mu.RLock()
	files, ok := m.Order[ctx.mstName]
	m.mu.RUnlock()
	if !ok {
		log.Error("no matching file", zap.String("name", ctx.mstName))
		return
	}

	var recentFile TSSPFile = nil
	var recentTime int64 = math.MaxInt64
	files.lock.RLock()
	for _, f := range files.Files() {
		if m.isClosed() {
			files.lock.RUnlock()
			return
		}
		min, max, err := f.MinMaxTime()
		if err != nil {
			continue
		}
		name := f.(*tsspFile).name
		fName := f.Path()
		if ctx.tr.Overlaps(min, max) {
			ctx.orderNames = append(ctx.orderNames, fName)
			ctx.orderSeqs = append(ctx.orderSeqs, name)
		} else if min > ctx.tr.Max && min < recentTime {
			recentTime = min
			recentFile = f
		}
	}
	files.lock.RUnlock()

	if recentFile != nil {
		name := recentFile.(*tsspFile).name
		ctx.orderNames = append(ctx.orderNames, recentFile.Path())
		ctx.orderSeqs = append(ctx.orderSeqs, name)
	}
}

func (m *MmsTables) mmsFiles(n int64) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := make([]*CompactGroup, 0, n)
	for k, v := range m.Order {
		if m.isClosed() {
			return nil
		}

		if v.fullCompacted() {
			continue
		}

		if atomic.LoadInt64(&v.closing) > 0 {
			continue
		}

		group := &CompactGroup{
			dropping: &v.closing,
			name:     k,
			group:    make([]string, 0, v.Len()),
		}

		for _, f := range v.files {
			if f.(*tsspFile).ref == 0 {
				panic("file closed")
			}

			name := f.Path()
			if tmpTsspFileSuffix == name[len(name)-len(tmpTsspFileSuffix):] {
				continue
			}

			lv, _ := f.LevelAndSequence()
			if group.toLevel < lv {
				group.toLevel = lv
			}
			group.group = append(group.group, f.Path())
		}

		if m.acquire(group.group) {
			group.toLevel++
			groups = append(groups, group)
		}

		if len(groups) >= int(n) {
			break
		}
	}

	return groups
}

func (m *MmsTables) GetLastFlushTimeBySid(measurement string, sid uint64) int64 {
	lastFlushTime, _ := m.sequencer.Get(measurement, sid)
	return lastFlushTime
}

func (m *MmsTables) GetRowCountsBySid(measurement string, sid uint64) int64 {
	_, rowCounts := m.sequencer.Get(measurement, sid)
	return rowCounts
}

func (m *MmsTables) AddRowCountsBySid(measurement string, sid uint64, rowCounts int64) {
	m.sequencer.AddRowCounts(measurement, sid, rowCounts)
}

func (m *MmsTables) FullCompact(shid uint64) error {
	n := int64(maxFullCompactor) - atomic.LoadInt64(&fullCompactingCount)
	if n < 1 {
		return nil
	}

	plans := m.mmsFiles(n)
	if len(plans) == 0 {
		return nil
	}

	for _, plan := range plans {
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			atomic.AddInt64(&fullCompactingCount, 1)
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.refMmsTable(group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					compLimiter.Release()
					m.unrefMmsTable(orderWg, inorderWg)
					atomic.AddInt64(&fullCompactingCount, -1)
					m.CompactDone(group.group)
				}()

				fi, err := m.NewFileIterators(group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				if NonStreamingCompaction(fi) {
					err = m.compactToLevel(fi, true)
				} else {
					err = m.streamCompactToLevel(fi, true)
				}
				if err != nil {
					compactStat.AddErrors(1)
					log.Error("compact error", zap.Error(err))
				}
			}(plan)
		}
	}

	return nil
}

func (m *MmsTables) Wait() {
	m.wg.Wait()
}

func (m *MmsTables) deleteFiles(files ...TSSPFile) error {
	for _, f := range files {
		fname := f.Path()
		if f.Inuse() {
			if err := f.Rename(fname + tmpTsspFileSuffix); err != nil {
				if err == errFileClosed {
					continue
				}
				log.Error("rename old file error", zap.String("name", fname), zap.Error(err))
				return err
			}
			nodeTableStoreGC.Add(false, f)
		} else {
			if err := f.Remove(); err != nil {
				log.Error("remove compacted fail error", zap.String("name", fname), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (m *MmsTables) mergeToSelf(logger *Log.Logger, helper *mergeHelper, unorders *TSSPFiles, seqs []uint64, name string) error {
	f := unorders.files[0]
	level, seq := f.LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, f.FileNameMerge(), f.FileNameExtend()+1, false)
	logger.Info("needCombineOutOfOrderFiles",
		zap.String("name", name),
		zap.Uint64s("out sequences", seqs),
		zap.Int("number of out record", len(helper.waits)))

	oldSize := int(f.FileSize())
	builder := AllocMsBuilder(helper.path, helper.name, m.Conf, len(helper.waits), fileName, helper.tier, nil, oldSize)
	var err error
	defer func(msb **MsBuilder) {
		if err != nil {
			for _, nf := range (*msb).Files {
				_ = nf.Close()
				_ = nf.Remove()
			}
			nm := (*msb).fd.Name()
			_ = (*msb).fd.Close()
			lock := fileops.FileLockOption("")
			_ = fileops.Remove(nm, lock)
		}

		PutMsBuilder(*msb)
	}(&builder)

	sort.Slice(helper.seriesIds, func(i, j int) bool {
		return helper.seriesIds[i] < helper.seriesIds[j]
	})

	for _, sid := range helper.seriesIds {
		rec := helper.waits[sid]
		builder, err = builder.WriteRecord(sid, rec.rec, func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			return fn.seq, fn.level, fn.merge, fn.extent + 1
		})
		if err != nil {
			return err
		}
	}

	nf, err := builder.NewTSSPFile(true)
	if err != nil || nf == nil {
		logger.Error("new tmp file fail", zap.Error(err))
		return err
	}
	if nf != nil {
		builder.Files = append(builder.Files, nf)
	}

	UnrefFiles(unorders.files...)
	newFiles := builder.Files

	return m.ReplaceFiles(name, unorders.files, newFiles, false, logger)
}

var compRecPool = sync.Pool{}

func allocRecord() *record.Record {
	compactStat.AddRecordPoolGetTotal(1)
	v := compRecPool.Get()
	if v != nil {
		return v.(*record.Record)
	}
	compactStat.AddRecordPoolHitTotal(1)
	return &record.Record{}
}

func freeRecord(rec *record.Record) {
	if rec != nil {
		rec.Reset()
		compRecPool.Put(rec)
	}
}

func CompactRecovery(path string, group *CompactGroup) {
	if err := recover(); err != nil {
		panicInfo := fmt.Sprintf("[Compact Panic:err:%s, name:%s, shard:%d, level:%d, group:%v, path:%s] %s",
			err, group.name, group.shardId, group.toLevel, group.group, path, debug.Stack())
		errMsg := errno.NewError(errno.CompactPanicFail)
		log.Error(panicInfo, zap.Error(errMsg))
	}
}

func MergeRecovery(path string, name string, inputs *OutOfOrderMergeContext) {
	if err := recover(); err != nil {
		panicInfo := fmt.Sprintf("[Merge Panic:err:%s, name:%s, seqs:%v, path:%s] %s",
			err, name, inputs.Seqs, path, debug.Stack())
		errMsg := errno.NewError(errno.CompactPanicFail)
		log.Error(panicInfo, zap.Error(errMsg))
	}
}

func mergeFirst(outLen int, outSize, orderFileSize int64) bool {
	if outLen == 1 {
		return false
	}
	if float64(outSize) > float64(MaxSizeOfFileToMerge)*MergeFirstRatio {
		return false
	}
	var avgMergeFileSize int64
	if outLen != 0 {
		avgMergeFileSize = outSize / int64(outLen)
	} else {
		avgMergeFileSize = outSize
	}

	return avgMergeFileSize < MergeFirstAvgSize && orderFileSize > MergeFirstDstSize
}
