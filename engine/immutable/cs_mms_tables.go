/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

type csImmTableImpl struct {
	mu       sync.RWMutex
	pkSchema map[string][]record.Field
	mstsInfo map[string]*meta.MeasurementInfo

	// update while open storage
	accumulateMetaIndex map[string]*AccumulateMetaIndex // mst -> accumulateMetaIndex, record metaIndex for detached store
}

type AccumulateMetaIndex struct {
	pkDataOffset uint32 // the start position of writing next primaryKey data
	blockId      uint64 // chunkMeta block start id
	dataOffset   int64  // the start position of writing next chunk data/meta
	offset       int64  // the start position of writing next metaIndexItem offset
}

func (a *AccumulateMetaIndex) GetBlockId() uint64 {
	return a.blockId
}

func (a *AccumulateMetaIndex) SetAccumulateMetaIndex(pkDataOffset uint32, blockId uint64, dataOffset, offset int64) {
	a.pkDataOffset = pkDataOffset
	a.blockId = blockId
	a.dataOffset = dataOffset
	a.offset = offset
}

func NewCsImmTableImpl() *csImmTableImpl {
	csImmTable := &csImmTableImpl{
		pkSchema:            make(map[string][]record.Field),
		mstsInfo:            make(map[string]*meta.MeasurementInfo),
		accumulateMetaIndex: make(map[string]*AccumulateMetaIndex),
	}
	return csImmTable
}

func (c *csImmTableImpl) GetEngineType() config.EngineType {
	return config.COLUMNSTORE
}

func (c *csImmTableImpl) GetCompactionType(name string) config.CompactionType {
	mstInfo, ok := c.mstsInfo[name]
	if !ok || mstInfo.ColStoreInfo == nil {
		return config.COMPACTIONTYPEEND
	}
	return mstInfo.ColStoreInfo.CompactionType
}

func (c *csImmTableImpl) LevelPlan(m *MmsTables, level uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}
	var plans []*CompactGroup
	minGroupFileN := LeveLMinGroupFiles[level]

	m.mu.RLock()
	for k, v := range m.CSFiles {
		plans = m.getMmsPlan(k, v, level, minGroupFileN, plans)
	}
	m.mu.RUnlock()
	return plans
}

// not use
func (c *csImmTableImpl) AddBothTSSPFiles(flushed *bool, m *MmsTables, name string, orderFiles []TSSPFile, unorderFiles []TSSPFile) {
}

func (c *csImmTableImpl) AddTSSPFiles(m *MmsTables, name string, isOrder bool, files ...TSSPFile) {
	m.mu.RLock()
	tables := m.CSFiles
	fs, ok := tables[name]
	m.mu.RUnlock()

	if !ok || fs == nil {
		m.mu.Lock()
		fs, ok = tables[name]
		if !ok {
			fs = NewTSSPFiles()
			tables[name] = fs
		}
		m.mu.Unlock()
	}

	for _, f := range files {
		statistics.IOStat.AddIOSnapshotBytes(f.FileSize())
	}

	fs.lock.Lock()
	fs.files = append(fs.files, files...)
	sort.Sort(fs)
	fs.lock.Unlock()
}

func (c *csImmTableImpl) addTSSPFile(m *MmsTables, isOrder bool, f TSSPFile, nameWithVer string) {
	mmsTbls := m.CSFiles
	v, ok := mmsTbls[nameWithVer]
	if !ok || v == nil {
		v = NewTSSPFiles()
		mmsTbls[nameWithVer] = v
	}

	v.lock.Lock()
	v.files = append(v.files, f)
	v.lock.Unlock()
}

func (c *csImmTableImpl) getFiles(m *MmsTables, isOrder bool) map[string]*TSSPFiles {
	return m.CSFiles
}

func (c *csImmTableImpl) GetMstInfo(name string) (*meta.MeasurementInfo, bool) {
	c.mu.RLock()
	mstInfo, isExist := c.mstsInfo[name]
	c.mu.RUnlock()
	return mstInfo, isExist
}

func (c *csImmTableImpl) GetPkSchema(name string) (*[]record.Field, bool) {
	c.mu.RLock()
	pkSchema, isExist := c.pkSchema[name]
	c.mu.RUnlock()
	return &pkSchema, isExist
}

func (c *csImmTableImpl) SetMstInfo(name string, mstInfo *meta.MeasurementInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mstsInfo[name] = mstInfo

	pk := mstInfo.ColStoreInfo.PrimaryKey
	c.pkSchema[name] = make([]record.Field, 0, len(pk)+1)
	if mstInfo.ColStoreInfo.TimeClusterDuration > 0 {
		c.pkSchema[name] = append(c.pkSchema[name], record.Field{
			Type: influx.Field_Type_Int,
			Name: record.TimeClusterCol,
		})
	}
	mstInfo.SchemaLock.RLock()
	defer mstInfo.SchemaLock.RUnlock()
	for _, key := range pk {
		if key == record.TimeField {
			c.pkSchema[name] = append(c.pkSchema[name], record.Field{
				Type: influx.Field_Type_Int,
				Name: key,
			})
		} else {
			v, _ := mstInfo.Schema.GetTyp(key)
			c.pkSchema[name] = append(c.pkSchema[name], record.Field{
				Type: record.ToPrimitiveType(v),
				Name: key,
			})
		}
	}
}

func (c *csImmTableImpl) getTSSPFiles(m *MmsTables, mstName string, isOrder bool) (*TSSPFiles, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.CSFiles
	files, ok := mmsTbls[mstName]
	return files, ok
}

func (c *csImmTableImpl) refMmsTable(m *MmsTables, name string, refOutOfOrder bool) (*sync.WaitGroup, *sync.WaitGroup) {
	var csWg *sync.WaitGroup
	m.mu.RLock()
	fs, ok := m.CSFiles[name]
	if ok {
		fs.wg.Add(1)
		csWg = &fs.wg
	}
	m.mu.RUnlock()
	return nil, csWg
}

func (c *csImmTableImpl) unrefMmsTable(tsWg, csWg *sync.WaitGroup) {
	if csWg != nil {
		csWg.Done()
	}
}

func (c *csImmTableImpl) NewFileIterators(m *MmsTables, group *CompactGroup) (FilesInfo, error) {
	var fi FilesInfo
	fi.compIts = make(FileIterators, 0, len(group.group))
	fi.oldFiles = make([]TSSPFile, 0, len(group.group))
	fi.oldIndexFiles = make([]string, 0, len(group.group))
	for _, fn := range group.group {
		if m.isClosed() || m.isCompMergeStopped() {
			fi.compIts.Close()
			return fi, ErrCompStopped
		}
		if atomic.LoadInt64(group.dropping) > 0 {
			fi.compIts.Close()
			return fi, ErrDroppingMst
		}
		f := m.File(group.name, fn, true)
		if f == nil {
			fi.compIts.Close()
			return fi, fmt.Errorf("table %v, %v, %v not find", group.name, fn, true)
		}
		fi.oldFiles = append(fi.oldFiles, f)
		indexFile := c.getIndexFileByTssp(m, group.name, fn)
		if indexFile == "" {
			fi.compIts.Close()
			return fi, fmt.Errorf("table %v, %v corresponding index file not find", group.name, fn)
		}
		fi.oldIndexFiles = append(fi.oldIndexFiles, indexFile)

		itr := NewFileIterator(f, CLog)
		if itr.NextChunkMeta() {
			fi.compIts = append(fi.compIts, itr)
			fi.totalSegmentCount += uint64(itr.curtChunkMeta.segCount)
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

func (c *csImmTableImpl) getIndexFileByTssp(m *MmsTables, mstName string, tsspFileName string) string {
	pkFileName := tsspFileName[:len(tsspFileName)-len(tsspFileSuffix)] + colstore.IndexFileSuffix
	files, ok := m.getPKFiles(mstName)
	if !ok || files == nil {
		return ""
	}
	f, exist := files.GetPKInfo(pkFileName)
	if !exist || f == nil {
		return ""
	}
	return pkFileName
}

func (c *csImmTableImpl) getAccumulateMetaIndex(name string) *AccumulateMetaIndex {
	c.mu.RLock()
	aMetaIndex, ok := c.accumulateMetaIndex[name]
	c.mu.RUnlock()
	if !ok {
		return &AccumulateMetaIndex{}
	}
	return aMetaIndex
}

func (c *csImmTableImpl) UpdateAccumulateMetaIndexInfo(name string, index *AccumulateMetaIndex) {
	c.mu.Lock()
	c.accumulateMetaIndex[name] = index
	c.mu.Unlock()
}

func (c *csImmTableImpl) NewFragmentIterators(m *MmsTables, group FilesInfo, lcLog *Log.Logger, mstInfo *meta.MeasurementInfo) (*FragmentIterators, error) {
	fragItrs := getFragmentIterators()

	//index prepare
	fragItrs.tcDuration = mstInfo.ColStoreInfo.TimeClusterDuration
	pkSchema, ok := c.GetPkSchema(group.name)
	if !ok {
		return nil, errors.New("pkSchema is not found")
	}
	sk := mstInfo.ColStoreInfo.SortKey
	timeSorted := TimeSorted(sk)
	switch mstInfo.ColStoreInfo.CompactionType {
	case config.BLOCK:
		aMetaIndex := c.getAccumulateMetaIndex(group.name)
		bfCols := mstInfo.IndexRelation.GetBloomFilterColumns()
		err := fragItrs.updateIterators(m, group, sk, *pkSchema, mstInfo, bfCols)
		if err != nil {
			return nil, err
		}
		err = fragItrs.newIteratorByBlock(group, bfCols, aMetaIndex)
		if err != nil {
			return nil, err
		}
	case config.ROW:
		err := fragItrs.updateIterators(m, group, sk, *pkSchema, mstInfo, mstInfo.IndexRelation.GetBloomFilterColumns())
		if err != nil {
			return nil, err
		}
		err = fragItrs.newIteratorByRow()
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unKnown compaction type")
	}
	fragItrs.WithLog(lcLog)
	fragItrs.builder.SetTimeSorted(timeSorted)
	heap.Init(fragItrs)
	return fragItrs, nil
}

func TimeSorted(sortKeys []string) bool {
	if len(sortKeys) == 0 {
		return false
	}
	return sortKeys[0] == record.TimeField
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
	mstInfo, ok := c.GetMstInfo(group.name)
	if !ok {
		return errors.New("measurements info not found")
	}
	compItrs, err := c.NewFragmentIterators(m, group, lcLog, mstInfo)
	if err != nil {
		return err
	}

	pkSchema, ok := c.GetPkSchema(group.name)
	if !ok {
		return errors.New("pkSchema is not found")
	}

	oldFilesSize := compItrs.estimateSize
	newFiles, compactErr := compItrs.compact(*pkSchema, m)
	compItrs.Close()
	if compactErr != nil {
		lcLog.Error("compact fail", zap.Error(compactErr))
		return compactErr
	}

	if err = c.replaceIndexFiles(m, group.name, group.oldIndexFiles); err != nil {
		lcLog.Error("replace index file error", zap.Error(err))
		return err
	}

	if err = c.ReplaceFiles(m, group.name, group.oldFiles, newFiles, true, mstInfo.IndexRelation.GetBloomFilterColumns()); err != nil {
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

func (c *csImmTableImpl) ReplaceFiles(m *MmsTables, name string, oldFiles, newFiles []TSSPFile, isOrder bool, iList []string) (err error) {
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

	for i := range newFiles {
		if err := RenameIndexFiles(newFiles[i].Path(), iList); err != nil {
			m.logger.Error("rename new file fail", zap.String("name", name), zap.String("dir", shardDir), zap.Error(err))
			return err
		}
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
		for i := range iList {
			fName := f.Path()
			skipIndexFileName := fName[:len(fName)-tsspFileSuffixLen] + "." + iList[i] + colstore.BloomFilterIndexFileSuffix
			err = fileops.Remove(skipIndexFileName)
			if err != nil {
				return err
			}
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

func (c *csImmTableImpl) FullyCompacted(m *MmsTables) bool {
	count := 0
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.CSFiles {
		if v.fullCompacted() {
			count++
		}
	}
	return count == len(m.CSFiles)
}

func RenameIndexFiles(fname string, indexList []string) error {
	lock := fileops.FileLockOption("")
	// rename pk index file
	for j := range indexList {
		skipIndexFileName := fname[:len(fname)-tsspFileSuffixLen] + "." + indexList[j] + colstore.BloomFilterIndexFileSuffix
		tmpSkipIndexFileName := skipIndexFileName + tmpFileSuffix
		if err := fileops.RenameFile(tmpSkipIndexFileName, skipIndexFileName, lock); err != nil {
			err = errno.NewError(errno.RenameFileFailed, zap.String("old", tmpSkipIndexFileName), zap.String("new", skipIndexFileName), err)
			log.Error("rename file fail", zap.Error(err))
			return err
		}
	}
	return nil
}
