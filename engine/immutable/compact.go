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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

const (
	CompactLevels        = 7
	defaultFileSizeLimit = 8 * 1024 * 1024 * 1024
	minFileSizeLimit     = 1 * 1024 * 1024
)

var (
	fullCompactingCount   int64
	maxFullCompactor      = cpu.GetCpuNum() / 2
	maxCompactor          = cpu.GetCpuNum()
	compLimiter           limiter.Fixed
	ErrCompStopped        = errors.New("compact stopped")
	ErrDownSampleStopped  = errors.New("downSample stopped")
	ErrDroppingMst        = errors.New("measurement is dropped")
	LevelCompactRule      = []uint16{0, 1, 0, 2, 0, 3, 0, 1, 2, 3, 0, 4, 0, 5, 0, 1, 2, 6}
	LevelCompactRuleForCs = []uint16{0, 1, 0, 1, 0, 1} // columnStore currently only doing level 0 and level 1 compaction,but the full functionality is available
	LeveLMinGroupFiles    = [CompactLevels]int{8, 4, 4, 4, 4, 4, 2}
	compLogSeq            = uint64(time.Now().UnixNano())

	EnableMergeOutOfOrder       = true
	MaxNumOfFileToMerge         = 256
	MaxSizeOfFileToMerge  int64 = 512 * 1024 * 1024 // 512MB
	log                         = Log.GetLogger()

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

func (m *MmsTables) refMmsTable(name string, refOutOfOrder bool) (orderWg, outOfOrderWg *sync.WaitGroup) {
	orderWg, outOfOrderWg = m.ImmTable.refMmsTable(m, name, refOutOfOrder)
	return
}

func (m *MmsTables) unrefMmsTable(orderWg, outOfOrderWg *sync.WaitGroup) {
	m.ImmTable.unrefMmsTable(m, orderWg, outOfOrderWg)
}

func (m *MmsTables) LevelCompact(level uint16, shid uint64) error {
	plans := m.ImmTable.LevelPlan(m, level)
	for len(plans) > 0 {
		plan := plans[0]
		plan.shardId = shid
		select {
		case <-m.closed:
			return ErrCompStopped
		case <-m.stopCompMerge:
			log.Info("stop LevelCompact", zap.Uint64("id", shid))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.CompactionEnabled() {
				m.wg.Done()
				return nil
			}
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					m.ImmTable.unrefMmsTable(m, orderWg, inorderWg)
					compLimiter.Release()
					m.CompactDone(group.group)
					group.release()
				}()

				fi, err := m.ImmTable.NewFileIterators(m, group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}
				err = m.ImmTable.compactToLevel(m, fi, false, NonStreamingCompaction(fi))
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
		itr.WithLog(CLog)
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

func (t *tsImmTableImpl) compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error {
	return m.compactToLevel(group, full, isNonStream)
}

func (m *MmsTables) compactToLevel(group FilesInfo, full, isNonStream bool) error {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	var cLog *zap.Logger
	var logEnd func()
	if isNonStream {
		cLog, logEnd = logger.NewOperation(log, "Compaction", group.name)
	} else {
		cLog, logEnd = logger.NewOperation(log, "StreamCompaction", group.name)
	}
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))
	lcLog.Debug(fmt.Sprintf("compactionGroup: name=%v, groups=%v", group.name, group.oldFids))

	var oldFilesSize int
	var newFiles []TSSPFile
	var compactErr error
	if isNonStream {
		compItrs, err := m.NewChunkIterators(group)
		if err != nil {
			lcLog.Error("new chunk iterators fail", zap.Error(err))
			return err
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = m.compact(compItrs, group.oldFiles, group.toLevel, true, lcLog)
		compItrs.Close()
	} else {
		compItrs, err := m.NewStreamIterators(group)
		if err != nil {
			lcLog.Error("new stream iterators fail", zap.Error(err))
			return err
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = compItrs.compact(group.oldFiles, group.toLevel, true)
		compItrs.Close()
	}

	if compactErr != nil {
		lcLog.Error("compact fail", zap.Error(compactErr))
		return compactErr
	}

	if err := m.ReplaceFiles(group.name, group.oldFiles, newFiles, true); err != nil {
		lcLog.Error("replace compacted file error", zap.Error(err))
		return err
	}

	end := time.Now()
	lcLog.Debug("compact file done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = SumFilesSize(newFiles)
	}
	return nil
}

func (c *csImmTableImpl) prepare(m *MmsTables, group FilesInfo, lcLog *Log.Logger) (*FragmentIterators, error) {
	//index prepare
	err := c.UpdatePrimaryKey(group.name, c.mstsInfo)
	if err != nil {
		return nil, err
	}
	sk := c.mstsInfo[group.name].SortKey
	compItrs, err := c.NewFragmentIterators(m, group, sk)
	if err != nil {
		lcLog.Error("new fragment iterators fail", zap.Error(err))
		return nil, err
	}
	compItrs.WithLog(lcLog)
	_, seq := group.oldFiles[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, group.toLevel, 0, 0, true, m.lock)
	compItrs.builder = NewMsBuilder(m.path, compItrs.name, m.lock, m.Conf, 1, fileName, *m.tier, nil, 1)
	compItrs.builder.NewPKIndexWriter()
	dataFilePath := fileName.String()
	compItrs.indexFilePath = path.Join(compItrs.builder.Path, compItrs.name, colstore.AppendIndexSuffix(dataFilePath)+tmpFileSuffix)
	compItrs.PkRec = append(compItrs.PkRec, record.NewRecordBuilder(c.primaryKey[group.name]))
	compItrs.pkRecPosition = 0
	compItrs.recordResultNum = 0
	compItrs.oldIndexFiles = group.oldIndexFiles
	return compItrs, nil
}

func (c *csImmTableImpl) compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	cLog, logEnd := logger.NewOperation(log, "Compaction for column store", group.name)
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact column store file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))
	lcLog.Debug(fmt.Sprintf("compactionGroup for column store: name=%v, groups=%v", group.name, group.oldFids))

	compItrs, err := c.prepare(m, group, lcLog)
	if err != nil {
		return err
	}
	oldFilesSize := compItrs.estimateSize

	newFiles, compactErr := compItrs.compact(c.primaryKey[group.name], m)
	compItrs.Close()
	if compactErr != nil {
		lcLog.Error("compact fail", zap.Error(compactErr))
		return compactErr
	}

	if err = c.replaceIndexFiles(m, group.name, group.oldIndexFiles); err != nil {
		lcLog.Error("replace index file error", zap.Error(err))
		return err
	}

	if err = c.ReplaceFiles(m, group.name, group.oldFiles, newFiles, true); err != nil {
		lcLog.Error("replace compacted file error", zap.Error(err))
		return err
	}

	end := time.Now()
	lcLog.Debug("column store compact files done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = SumFilesSize(newFiles)
	}
	return nil
}

func (c *csImmTableImpl) replaceIndexFiles(m *MmsTables, name string, oldIndexFiles []string) error {
	if len(oldIndexFiles) == 0 {
		return nil
	}
	var err error
	defer func() {
		if e := recover(); e != nil {
			err = errno.NewError(errno.RecoverPanic, e)
			log.Error("replace index file fail", zap.Error(err))
		}
	}()

	mmsTables := m.PKFiles
	m.mu.RLock()
	indexFiles, ok := mmsTables[name]
	m.mu.RUnlock()
	if !ok || indexFiles == nil {
		return errors.New("get index files from mmsTables error")
	}

	for _, f := range oldIndexFiles {
		if m.isClosed() || m.isCompMergeStopped() {
			return ErrCompStopped
		}
		lock := fileops.FileLockOption("")
		err = fileops.Remove(f, lock)
		if err != nil && !os.IsNotExist(err) {
			err = errRemoveFail(f, err)
			log.Error("remove index file fail ", zap.String("file name", f), zap.Error(err))
			return err
		}
	}
	return nil
}

func (c *csImmTableImpl) ReplaceFiles(m *MmsTables, name string, oldFiles, newFiles []TSSPFile, isOrder bool) (err error) {
	if len(newFiles) == 0 || len(oldFiles) == 0 {
		return nil
	}

	defer func() {
		if e := recover(); e != nil {
			err = errno.NewError(errno.RecoverPanic, e)
			log.Error("replace file fail", zap.Error(err))
		}
	}()

	var logFile string
	shardDir := filepath.Dir(m.path)
	logFile, err = m.writeCompactedFileInfo(name, oldFiles, newFiles, shardDir, isOrder)
	if err != nil {
		if len(logFile) > 0 {
			lock := fileops.FileLockOption(*m.lock)
			_ = fileops.Remove(logFile, lock)
		}
		m.logger.Error("write compact log fail", zap.String("name", name), zap.String("dir", shardDir))
		return
	}

	if err := RenameTmpFiles(newFiles); err != nil {
		m.logger.Error("rename new file fail", zap.String("name", name), zap.String("dir", shardDir), zap.Error(err))
		return err
	}

	mmsTables := m.ImmTable.getFiles(m, isOrder)
	m.mu.RLock()
	fs, ok := mmsTables[name]
	m.mu.RUnlock()
	if !ok || fs == nil {
		return ErrCompStopped
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()
	// remove old files
	for _, f := range oldFiles {
		if m.isClosed() || m.isCompMergeStopped() {
			return ErrCompStopped
		}
		fs.deleteFile(f)
		if err = m.deleteFiles(f); err != nil {
			return
		}
	}
	sort.Sort(fs)

	lock := fileops.FileLockOption(*m.lock)
	if err = fileops.Remove(logFile, lock); err != nil {
		m.logger.Error("remove compact log file error", zap.String("name", name), zap.String("dir", shardDir),
			zap.String("log", logFile), zap.Error(err))
	}

	return
}

func (m *MmsTables) compact(itrs *ChunkIterators, files []TSSPFile, level uint16, isOrder bool, cLog *Log.Logger) ([]TSSPFile, error) {
	_, seq := files[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, 0, 0, isOrder, m.lock)
	tableBuilder := NewMsBuilder(m.path, itrs.name, m.lock, m.Conf, itrs.maxN, fileName, *m.tier, nil, itrs.estimateSize)
	tableBuilder.WithLog(cLog)
	for {
		select {
		case <-m.closed:
			return nil, ErrCompStopped
		case <-m.stopCompMerge:
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
		tableBuilder, err = tableBuilder.WriteRecord(id, rec, nil, func(fn TSSPFileName) (uint64, uint16, uint16, uint16) {
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

func (m *MmsTables) fullyCompacted() bool {
	count := 0
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.Order {
		if m.isClosed() || m.isCompMergeStopped() {
			return false
		}
		if v.fullCompacted() {
			count++
		}
	}
	return count == len(m.Order)
}

func (m *MmsTables) FreeSequencer() bool {
	if !m.fullyCompacted() {
		return false
	}
	return m.sequencer.free()
}

func (m *MmsTables) loadIdTimes() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.Order) == 0 {
		return 0, nil
	}

	seq := m.Sequencer()
	defer seq.UnRef()
	loader := newIDTimesLoader(seq)
	// m.OutOfOrder used for statistics row count
	go loader.Load(m.path, m.Order, m.OutOfOrder)

	select {
	case <-loader.Done():
	case <-m.closed:
		loader.close()
		log.Info("table closed")
		return 0, nil
	}

	return loader.ctx.getRowCount(), loader.Error()
}

func (m *MmsTables) mmsFiles(n int64) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	groups := make([]*CompactGroup, 0, n)
	mmsTables := m.ImmTable.getFiles(m, true)
	for k, v := range mmsTables {
		if m.isClosed() || m.isCompMergeStopped() {
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
			if tmpFileSuffix == name[len(name)-len(tmpFileSuffix):] {
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

func (m *MmsTables) SetAddFunc(addFunc func(int64)) {
	m.addFunc = addFunc
}

func (m *MmsTables) GetLastFlushTimeBySid(measurement string, sid uint64) int64 {
	seq := m.Sequencer()
	defer seq.UnRef()

	if seq.isLoading {
		return math.MaxInt64
	}

	if seq.isFree {
		m.ReloadSequencer(seq, true)
		return math.MaxInt64
	}

	lastFlushTime, _ := seq.Get(measurement, sid)
	return lastFlushTime
}

func (m *MmsTables) GetRowCountsBySid(measurement string, sid uint64) (int64, error) {
	seq := m.Sequencer()
	m.ReloadSequencer(seq, false)
	_, rowCounts := seq.Get(measurement, sid)
	seq.UnRef()
	return rowCounts, nil
}

func (m *MmsTables) AddRowCountsBySid(measurement string, sid uint64, rowCounts int64) {
	seq := m.Sequencer()
	seq.AddRowCounts(measurement, sid, rowCounts)
	seq.UnRef()
}

func (m *MmsTables) ReloadSequencer(seq *Sequencer, async bool) {
	if !seq.SetToInLoading() {
		return
	}

	if async {
		go m.reloadSequencer(seq)
		return
	}

	m.reloadSequencer(seq)
}

func (m *MmsTables) reloadSequencer(seq *Sequencer) {
	count, err := m.loadIdTimes()
	if err == nil && m.addFunc != nil && !m.isAdded {
		// add row count to shard
		m.addFunc(count)
		m.isAdded = true
	}

	if err != nil {
		seq.ResetMmsIdTime()
		m.logger.Error("failed to load id time", zap.Error(err))
	}
	seq.SetStat(err != nil, false)
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
		case <-m.stopCompMerge:
			log.Info("stop FullCompact", zap.Uint64("id", shid))
			return nil
		case compLimiter <- struct{}{}:
			m.wg.Add(1)
			if !m.CompactionEnabled() {
				m.wg.Done()
				return nil
			}
			atomic.AddInt64(&fullCompactingCount, 1)
			go func(group *CompactGroup) {
				orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
				if m.compactRecovery {
					defer CompactRecovery(m.path, group)
				}

				defer func() {
					m.wg.Done()
					compLimiter.Release()
					m.ImmTable.unrefMmsTable(m, orderWg, inorderWg)
					atomic.AddInt64(&fullCompactingCount, -1)
					m.CompactDone(group.group)
				}()

				fi, err := m.ImmTable.NewFileIterators(m, group)
				if err != nil {
					log.Error(err.Error())
					compactStat.AddErrors(1)
					return
				}

				err = m.ImmTable.compactToLevel(m, fi, true, NonStreamingCompaction(fi))
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
			if err := f.Rename(fname + tmpFileSuffix); err != nil {
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
