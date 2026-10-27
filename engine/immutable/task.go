// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"container/heap"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/indirect/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding"
	"go.uber.org/zap"
)

type CompactTask struct {
	scheduler.BaseTask

	plan       *CompactGroup
	full       bool
	db         string
	rp         string
	engineType uint8

	table *MmsTables
}

func NewCompactTask(table *MmsTables, plan *CompactGroup, full bool) *CompactTask {
	task := &CompactTask{
		full:       full,
		plan:       plan,
		table:      table,
		db:         table.db,
		rp:         table.rp,
		engineType: uint8(table.ImmTable.GetEngineType()),
	}
	task.Init(plan.name)
	return task
}

func (t *CompactTask) BeforeExecute() bool {
	if !t.table.acquire(t.plan.group) {
		return false
	}
	t.OnFinish(func() {
		t.table.CompactDone(t.plan.group)
		t.table.blockCompactStop(t.plan.name)
	})
	return true
}

func (t *CompactTask) Finish() {
	t.BaseTask.Finish()
	t.plan = nil
	t.table = nil
}

func (t *CompactTask) Execute() {
	group := t.plan
	m := t.table

	if group.Len() == 1 {
		err := m.RenameFileToLevel(group.name, group.group, group.toLevel)
		if err != nil {
			log.Error("compact error", zap.Error(err))
		}
		return
	}

	t.IncrFull(1)
	orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
	defer func() {
		if config.GetStoreConfig().Compact.CompactRecovery {
			CompactRecovery(m.path, group)
		}
		m.ImmTable.unrefMmsTable(orderWg, inorderWg)
		t.IncrFull(-1)
	}()

	if !m.CompactionEnabled() {
		return
	}

	fi, err := m.ImmTable.NewFileIterators(m, group)
	if err != nil {
		log.Error(err.Error())
		compactStat.AddErrors(1)
		return
	}

	err = m.ImmTable.compactToLevel(m, fi, t.full, NonStreamingCompaction(fi))
	if err != nil {
		_, err = t.mergeRepairIfNeeded(fi, nil, err)
	}

	if err != nil {
		compactStat.AddErrors(1)
		log.Error("compact error", zap.Error(err))
	}
}

func (t *CompactTask) ExecuteOnTN() (interface{}, error) {
	group := t.plan
	var fi FilesInfo
	var info *CompactedFileInfo

	if group.Len() == 1 {
		oldFiles := make([]string, len(group.group))
		for i, f := range group.group {
			oldFiles[i] = filepath.Base(f)
		}
		info = &CompactedFileInfo{
			Name:     group.name,
			BasePath: filepath.Join(group.path, group.name),
			OldFile:  oldFiles,
			ToLevel:  group.toLevel,
		}
		return info, nil
	}

	t.IncrFull(1)
	defer func() {
		if config.GetStoreConfig().Compact.CompactRecovery {
			CompactRecovery(group.path, group)
		}
		if t.table == nil {
			for _, f := range fi.oldFiles {
				util.MustClose(f)
			}
		}
		t.IncrFull(-1)
	}()

	fi, err := NewFileIterators(t.table, group)
	if err != nil {
		compactStat.AddErrors(1)
		return nil, err
	}

	if t.engineType == uint8(config.TSSTORE) {
		info, err = t.compactToLevelByTsStore(fi, NonStreamingCompaction(fi))
		if err != nil {
			info, err = t.mergeRepairIfNeeded(fi, info, err)
		}
	}

	if err != nil {
		compactStat.AddErrors(1)
		return nil, err
	}

	return info, nil
}

func (t *CompactTask) mergeRepairIfNeeded(fi FilesInfo, info *CompactedFileInfo, err error) (*CompactedFileInfo, error) {
	if err == nil || !errno.Equal(err, errno.CompactTimeDisorder) {
		return info, nil
	}

	log.Warn("time disorder, use the out-of-order merge function for repair",
		zap.Strings("files", fi.oldFids))

	ctx := NewMergeContext(fi.name, fi.toLevel, true)
	for _, file := range fi.oldFiles {
		ctx.AddUnordered(file)
	}
	mt := newMergeTool(t.table, nil, log)
	mt.initStat(fi.name, fi.shId)
	info = mt.mergeSelfStreamMode(ctx, true)
	if info != nil {
		err = nil
	}
	return info, err
}

func (t *CompactTask) ReplaceFiles(info *CompactedFileInfo) error {
	m := t.table
	if len(info.OldFile) == 0 {
		return errors.New("wrong CompactedFileInfo")
	}
	if len(info.OldFile) == 1 {
		groups := make([]string, len(info.OldFile))
		for i, f := range info.OldFile {
			groups[i] = filepath.Join(info.BasePath, f)
		}
		err := m.RenameFileToLevel(info.Name, groups, info.ToLevel)
		return err
	}
	newFiles, err := GetNewTSSPFiles(info)
	if err != nil {
		return err
	}
	fi, err := m.ImmTable.NewFileIterators(m, t.plan)
	if err != nil {
		log.Error(err.Error())
		compactStat.AddErrors(1)
		return err
	}
	err = m.replaceFiles(info, fi.oldFiles, newFiles)
	if err != nil {
		compactStat.AddErrors(1)
		return err
	}
	if !info.IsNonStream {
		NewHotFileManager().AddAll(newFiles)
	}
	end := time.Now()
	log.Debug("compact file done", zap.Any("files", info.OldFile), zap.Time("end", end))
	return nil
}

func (t *CompactTask) IncrFull(n int64) {
	if t.full {
		atomic.AddInt64(&fullCompactingCount, n)
	}
}

func (t *CompactTask) MarshalBinary(buf []byte) ([]byte, error) {
	buf = encoding.MarshalUint64(buf, t.BaseTask.UUID())
	// plan
	buf, err := t.plan.MarshalBinary(buf)
	if err != nil {
		return buf, err
	}
	// full
	buf = encoding.MarshalBool(buf, t.full)
	// db
	buf = encoding.MarshalUint16(buf, uint16(len(t.db)))
	buf = append(buf, t.db...)
	// rp
	buf = encoding.MarshalUint16(buf, uint16(len(t.rp)))
	buf = append(buf, t.rp...)
	// engineType
	buf = encoding.MarshalUint16(buf, uint16(t.engineType))

	return buf, nil
}

func (t *CompactTask) UnmarshalBinary(buf []byte) ([]byte, error) {
	if len(buf) < 8 {
		return buf, errno.NewError(errno.ShortBufferSize, 8, len(buf))
	}
	uuid := encoding.UnmarshalUint64(buf[:8])
	t.SetUUID(uuid)
	buf = buf[8:]
	t.plan = &CompactGroup{}
	buf, err := t.plan.UnmarshalBinary(buf)
	if err != nil {
		return buf, err
	}

	if len(buf) < 1 {
		return buf, errno.NewError(errno.ShortBufferSize, 1, len(buf))
	}
	t.full = encoding.UnmarshalBool(buf[:1])
	buf = buf[1:]

	buf, t.db, err = UnmarshalString16(buf)
	if err != nil {
		return buf, err
	}

	buf, t.rp, err = UnmarshalString16(buf)
	if err != nil {
		return buf, err
	}

	if len(buf) < 2 {
		return buf, errno.NewError(errno.ShortBufferSize, 2, len(buf))
	}
	t.engineType = uint8(encoding.UnmarshalUint16(buf[:2]))

	return buf, nil
}

func UnmarshalString16(buf []byte) ([]byte, string, error) {
	var str string
	if len(buf) < 2 {
		return buf, str, errno.NewError(errno.ShortBufferSize, 2, len(buf))
	}
	strLen := encoding.UnmarshalUint16(buf[:2])
	buf = buf[2:]
	if len(buf) < int(strLen) {
		return buf, str, errno.NewError(errno.ShortBufferSize, strLen, len(buf))
	}
	str = bytesutil.ToUnsafeString(buf[:strLen])
	buf = buf[strLen:]

	return buf, str, nil
}

func (t *CompactTask) GetPlan() *CompactGroup {
	return t.plan
}

func (t *CompactTask) compactToLevelByTsStore(group FilesInfo, isNonStream bool) (*CompactedFileInfo, error) {
	compactStatItem := statistics.NewCompactStatItem(group.name, group.shId)
	compactStatItem.Full = t.full
	compactStatItem.Level = group.toLevel - 1
	compactStat.AddActive(1)
	defer func() {
		compactStat.AddActive(-1)
		compactStat.PushCompaction(compactStatItem)
	}()

	var cLog *zap.Logger
	var logEnd func()
	if isNonStream {
		cLog, logEnd = logger.GetCompactLogger("Compaction", group.name)
	} else {
		cLog, logEnd = logger.GetCompactLogger("StreamCompaction", group.name)
	}
	defer logEnd()
	lcLog := logger.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	start := time.Now()
	lcLog.Debug("start compact file", zap.Uint64("shid", group.shId), zap.Any("seqs", group.oldFids), zap.Time("start", start))
	lcLog.Debug(fmt.Sprintf("compactionGroup: name=%v, groups=%v", group.name, group.oldFids))

	var oldFilesSize int
	var newFiles []TSSPFile
	var compactErr error
	var events *Events

	defer func() {
		for _, f := range newFiles {
			f.Close()
		}
	}()

	if isNonStream {
		compItrs := NewCompactIterators(group)
		if compItrs == nil {
			group.compIts.Close()
			return nil, nil
		}
		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = compact(compItrs, group, true, lcLog, t.table)
		compItrs.Close()
	} else {
		compItrs := NewStreamIterators(group)
		if compItrs == nil {
			group.compIts.Close()
			return nil, nil
		}

		events = compItrs.InitEvents(group.toLevel)
		defer func() {
			// Ensures that file locks are released even in unexpected situations
			events.Finish(false, nil)
		}()

		compItrs.WithLog(lcLog)
		oldFilesSize = compItrs.estimateSize
		newFiles, compactErr = compItrs.compact(group.oldFiles, group.toLevel, true)
		if compactErr != nil {
			compItrs.RemoveTmpFiles()
		}
		compItrs.Close()
	}

	if compactErr != nil {
		lcLog.Error("compact fail", zap.Error(compactErr))
		return nil, compactErr
	}

	info := GenerateCompactedInfo(group.name, group.oldFiles, newFiles, group.path, true)
	info.Lock = group.lock
	info.IsNonStream = isNonStream

	end := time.Now()
	lcLog.Debug("compact file done", zap.Any("files", group.oldFids), zap.Time("end", end), zap.Duration("time used", end.Sub(start)))

	if oldFilesSize != 0 {
		compactStatItem.OriginalFileCount = int64(len(group.oldFiles))
		compactStatItem.CompactedFileCount = int64(len(newFiles))
		compactStatItem.OriginalFileSize = int64(oldFilesSize)
		compactStatItem.CompactedFileSize = SumFilesSize(newFiles)
	}
	return info, nil
}

type CompactGroupBuilder struct {
	limit        int
	parquetLevel uint16
	lowLevelMode bool
	level        uint16

	group  *CompactGroup
	groups []*CompactGroup
}

func (b *CompactGroupBuilder) Init(name, path string, closing *int64, size int, lock *string) {
	b.group = &CompactGroup{
		dropping: closing,
		name:     name,
		lock:     lock,
		path:     path,
		group:    make([]string, 0, size),
	}
}

func (b *CompactGroupBuilder) AddFile(f TSSPFile) bool {
	if b.lowLevelMode {
		return b.addLowLevelMode(f)
	}

	return b.add(f)
}

func (b *CompactGroupBuilder) add(f TSSPFile) bool {
	lv, _ := f.LevelAndSequence()
	if b.parquetLevel > 0 && lv < b.parquetLevel {
		b.group.reset()
		return false
	}

	b.group.UpdateLevel(lv + 1)
	b.group.Add(f.Path())
	return true
}

func (b *CompactGroupBuilder) addLowLevelMode(f TSSPFile) bool {
	b.group.toLevel = b.level
	lv, _ := f.LevelAndSequence()
	if lv < b.level {
		b.group.Add(f.Path())
		return true
	}

	if b.group.Len() > 0 {
		b.SwitchGroup()
		b.group = &CompactGroup{
			name:     b.group.name,
			dropping: b.group.dropping,
		}
	}

	return true
}

func (b *CompactGroupBuilder) SwitchGroup() {
	if b.group == nil || b.group.Len() == 0 {
		return
	}
	b.groups = append(b.groups, b.group)
}

func (b *CompactGroupBuilder) Limited() bool {
	return len(b.groups) < b.limit
}

func (b *CompactGroupBuilder) Release() {
	b.groups = nil
}

func NewFileIterators(m *MmsTables, group *CompactGroup) (FilesInfo, error) {
	var fi FilesInfo
	fi.compIts = make(FileIterators, 0, len(group.group))
	fi.oldFiles = make([]TSSPFile, 0, len(group.group))
	for _, fn := range group.group {
		if group.dropping != nil && atomic.LoadInt64(group.dropping) > 0 {
			fi.compIts.Close()
			return fi, ErrDroppingMst
		}
		f, err := getFile(m, group, fn)
		if err != nil {
			fi.compIts.Close()
			return fi, err
		}
		fi.oldFiles = append(fi.oldFiles, f)
		itr := NewFileIterator(f, CLog)
		if itr.NextChunkMeta() {
			fi.compIts = append(fi.compIts, itr)
		} else {
			continue
		}

		fi.updatingFilesInfo(f, itr)
	}
	if len(fi.compIts) < 2 {
		return fi, fmt.Errorf("no enough files to do compact, iterator size: %d", len(fi.compIts))
	}
	fi.updateFinalFilesInfo(group)
	return fi, nil
}

func getFile(m *MmsTables, group *CompactGroup, fn string) (TSSPFile, error) {
	if m != nil {
		return m.File(group.name, fn, true), nil
	}
	return OpenTSSPFile(fn, group.lock, true)
}

func NewCompactIterators(group FilesInfo) *ChunkIterators {
	compItrs := &ChunkIterators{
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

	return compItrs
}

func compact(itrs *ChunkIterators, group FilesInfo, isOrder bool, cLog *logger.Logger, m *MmsTables) ([]TSSPFile, error) {
	files, level, shId, lock := group.oldFiles, group.toLevel, group.shId, group.lock
	if len(files) == 0 {
		return []TSSPFile{}, errors.New("files should not be empty")
	}
	storeConf := GetTsStoreConfig()
	_, seq := files[0].LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, 0, 0, isOrder, lock)
	tableBuilder := NewMsBuilder(group.path, itrs.name, lock, storeConf, itrs.maxN, fileName, FilesMergedTire(files),
		nil, itrs.estimateSize, config.TSSTORE, nil, shId)
	tableBuilder.WithLog(cLog)
	success := false
	defer func() {
		if !success {
			tableBuilder.Clean()
		}
	}()

	for {
		id, rec, err := itrs.Next()
		if err != nil {
			cLog.Error("read data fail", zap.Error(err))
			return nil, err
		}

		if rec == nil || id == 0 {
			break
		}

		times := rec.Times()
		if !slices.IsSorted(times) {
			return nil, errno.NewError(errno.CompactTimeDisorder, times[0], times[len(times)-1])
		}
		record.CheckRecord(rec)

		if m != nil && m.indexMergeSet != nil && m.indexMergeSet.HasDeletedTSID(id) {
			continue
		}

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

	success = true
	newFiles := make([]TSSPFile, 0, len(tableBuilder.Files))
	newFiles = append(newFiles, tableBuilder.Files...)
	return newFiles, nil
}

func NewStreamIterators(group FilesInfo) *StreamIterators {
	compItrs := getStreamIterators()
	compItrs.dropping = group.dropping
	compItrs.name = group.name
	compItrs.dir = group.path
	compItrs.lock = group.lock
	compItrs.pair.Reset(group.name)
	compItrs.Conf = GetTsStoreConfig()
	compItrs.itrs = compItrs.itrs[:0]
	for _, fi := range group.compIts {
		itr := NewStreamStreamIterator(fi)
		compItrs.itrs = append(compItrs.itrs, itr)
	}
	compItrs.maxN = group.maxChunkN
	compItrs.estimateSize = group.estimateSize
	compItrs.chunkRows = 0
	compItrs.maxChunkRows = 0
	compItrs.tier = FilesMergedTire(group.oldFiles)
	compItrs.cmw = NewChunkMetaWriter(compItrs.Conf.CompressChunkMeta)

	heap.Init(compItrs)

	return compItrs
}

func GenerateCompactedInfo(name string, oldFiles, newFiles []TSSPFile, basePath string, isOrder bool) *CompactedFileInfo {
	if len(newFiles) == 0 || len(oldFiles) == 0 {
		return nil
	}
	info := &CompactedFileInfo{
		Name:     name,
		BasePath: filepath.Join(basePath, name),
		IsOrder:  isOrder,
		OldFile:  make([]string, len(oldFiles)),
		NewFile:  make([]string, len(newFiles)),
	}

	for i, f := range oldFiles {
		info.OldFile[i] = filepath.Base(f.Path())
	}

	for i, f := range newFiles {
		info.NewFile[i] = filepath.Base(f.Path())
	}

	return info
}
