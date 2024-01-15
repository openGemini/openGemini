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
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	influxLogger "github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	stats "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/savsgio/dictpool"
	"go.uber.org/zap"
)

type TablesStore interface {
	SetOpId(shardId uint64, opId uint64)
	Open() (int64, error)
	Close() error
	AddTable(ms *MsBuilder, isOrder bool, tmp bool)
	AddTSSPFiles(name string, isOrder bool, f ...TSSPFile)
	AddPKFile(name, file string, rec *record.Record, mark fragment.IndexFragment, tcLocation int8)
	GetPKFile(mstName string, file string) (pkInfo *colstore.PKInfo, ok bool)
	FreeAllMemReader()
	ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool) error
	GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange) ([]TSSPFile, []TSSPFile)
	ReplaceDownSampleFiles(mstNames []string, originFiles [][]TSSPFile, newFiles [][]TSSPFile, isOrder bool, callBack func()) error
	NextSequence() uint64
	Sequencer() *Sequencer
	GetTSSPFiles(mm string, isOrder bool) (*TSSPFiles, bool)
	GetCSFiles(mm string) (*TSSPFiles, bool)
	Tier() uint64
	File(name string, namePath string, isOrder bool) TSSPFile
	CompactDone(seq []string)
	CompactionEnable()
	CompactionDisable()
	MergeEnable()
	MergeDisable()
	CompactionEnabled() bool
	MergeEnabled() bool
	IsOutOfOrderFilesExist() bool
	MergeOutOfOrder(shId uint64, force bool) error
	LevelCompact(level uint16, shid uint64) error
	FullCompact(shid uint64) error
	SetAddFunc(addFunc func(int64))
	GetLastFlushTimeBySid(measurement string, sid uint64) int64
	GetRowCountsBySid(measurement string, sid uint64) (int64, error)
	AddRowCountsBySid(measurement string, sid uint64, rowCounts int64)
	GetOutOfOrderFileNum() int
	GetMstFileStat() *stats.FileStat
	DropMeasurement(ctx context.Context, name string) error
	GetFileSeq() uint64
	DisableCompAndMerge()
	EnableCompAndMerge()
	FreeSequencer() bool
	SetImmTableType(engineType config.EngineType)
	SetMstInfo(name string, mstInfo *meta.MeasurementInfo)
	SetAccumulateMetaIndex(name string, aMetaIndex *AccumulateMetaIndex)
	GetMstInfo(name string) (*meta.MeasurementInfo, bool)
	SeriesTotal() uint64
	SetLockPath(lock *string)
}

type ImmTable interface {
	refMmsTable(m *MmsTables, name string, refOutOfOrder bool) (*sync.WaitGroup, *sync.WaitGroup)
	unrefMmsTable(m *MmsTables, orderWg, outOfOrderWg *sync.WaitGroup)
	addTSSPFile(m *MmsTables, isOrder bool, f TSSPFile, nameWithVer string)
	getTSSPFiles(m *MmsTables, mstName string, isOrder bool) (*TSSPFiles, bool)
	GetEngineType() config.EngineType
	GetCompactionType(name string) config.CompactionType
	getFiles(m *MmsTables, isOrder bool) map[string]*TSSPFiles
	compactToLevel(m *MmsTables, group FilesInfo, full, isNonStream bool) error
	NewFileIterators(m *MmsTables, group *CompactGroup) (FilesInfo, error)
	AddTSSPFiles(m *MmsTables, name string, isOrder bool, files ...TSSPFile)
	LevelPlan(m *MmsTables, level uint16) []*CompactGroup
	SetMstInfo(name string, mstInfo *meta.MeasurementInfo)
	GetMstInfo(name string) (*meta.MeasurementInfo, bool)
	UpdateAccumulateMetaIndexInfo(name string, index *AccumulateMetaIndex)
}

type MmsTables struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	path string
	lock *string

	shardId uint64 // this is only to track MmsTables open duration
	opId    uint64 // this is only to track MmsTables open duration

	closed          chan struct{}
	stopCompMerge   chan struct{}
	Order           map[string]*TSSPFiles        // {"cpu_0001": *TSSPFiles}
	OutOfOrder      map[string]*TSSPFiles        // {"cpu_0001": *TSSPFiles}
	CSFiles         map[string]*TSSPFiles        // {"cpu_0001": *TSSPFiles} tsspFiles for columnStore
	PKFiles         map[string]*colstore.PKFiles // {"cpu_0001": *PKFiles} PKFiles for columnStore
	fileSeq         uint64
	tier            *uint64
	compactionEn    int32
	mergeEn         int32
	inCompLock      sync.RWMutex
	inCompact       map[string]struct{}
	inMerge         *MeasurementInProcess
	inBlockCompact  *MeasurementInProcess
	lmt             *lastMergeTime
	sequencer       *Sequencer
	compactRecovery bool
	logger          *logger.Logger
	ImmTable        ImmTable

	Conf *Config

	isAdded bool // set true if addFunc called
	addFunc func(int64)
}

func NewTableStore(dir string, lock *string, tier *uint64, compactRecovery bool, config *Config) *MmsTables {
	store := &MmsTables{
		path:            dir,
		lock:            lock,
		closed:          make(chan struct{}),
		stopCompMerge:   make(chan struct{}),
		Order:           make(map[string]*TSSPFiles, defaultCap),
		OutOfOrder:      make(map[string]*TSSPFiles, defaultCap),
		CSFiles:         make(map[string]*TSSPFiles, defaultCap),
		PKFiles:         make(map[string]*colstore.PKFiles, defaultCap),
		tier:            tier,
		inCompact:       make(map[string]struct{}, defaultCap),
		inMerge:         NewMeasurementInProcess(),
		inBlockCompact:  NewMeasurementInProcess(),
		lmt:             NewLastMergeTime(),
		mergeEn:         1,
		compactionEn:    1,
		sequencer:       NewSequencer(),
		compactRecovery: compactRecovery,
		Conf:            config,
		logger:          logger.NewLogger(errno.ModuleShard),
	}
	return store
}

func addMemSize(levelName string, memSize, memOrderSize, memUnOrderSize int64) {
	if memSize == 0 && memOrderSize == 0 && memUnOrderSize == 0 {
		return
	}
	atomic.AddInt64(&nodeImmTableSizeUsed, memSize)
	stats.ImmutableStat.Mu.Lock()
	stats.ImmutableStat.AddMemSize(levelName, memSize, memOrderSize, memUnOrderSize)
	stats.ImmutableStat.Mu.Unlock()
}

func (m *MmsTables) SetImmTableType(engineType config.EngineType) {
	if engineType == config.TSSTORE {
		m.ImmTable = NewTsImmTable()
	} else if engineType == config.COLUMNSTORE {
		m.ImmTable = NewCsImmTableImpl()
	}
}

func (m *MmsTables) SetMstInfo(name string, mstInfo *meta.MeasurementInfo) {
	m.ImmTable.SetMstInfo(name, mstInfo)
}

func (m *MmsTables) SetAccumulateMetaIndex(name string, aMetaIndex *AccumulateMetaIndex) {
	m.ImmTable.UpdateAccumulateMetaIndexInfo(name, aMetaIndex)
}

func (m *MmsTables) GetMstInfo(name string) (*meta.MeasurementInfo, bool) {
	return m.ImmTable.GetMstInfo(name)
}

func (m *MmsTables) Tier() uint64 {
	tier := *(m.tier)
	return tier
}

func (m *MmsTables) GetFileSeq() uint64 {
	return m.fileSeq
}

func (m *MmsTables) disableCompAndMerge() {
	m.inCompLock.Lock()
	defer m.inCompLock.Unlock()

	if !m.CompactionEnabled() {
		return
	}

	m.CompactionDisable()
	m.MergeDisable()
	if m.stopCompMerge != nil {
		close(m.stopCompMerge)
		m.stopCompMerge = nil
	}
}

func (m *MmsTables) DisableCompAndMerge() {
	m.disableCompAndMerge()
	m.wg.Wait()
}

func (m *MmsTables) EnableCompAndMerge() {
	m.inCompLock.Lock()
	defer m.inCompLock.Unlock()

	if m.CompactionEnabled() {
		return
	}

	m.CompactionEnable()
	m.MergeEnable()
	m.stopCompMerge = make(chan struct{})
}

func (m *MmsTables) CompactionEnabled() bool {
	return atomic.LoadInt32(&m.compactionEn) == 1
}

func (m *MmsTables) CompactionEnable() {
	atomic.StoreInt32(&m.compactionEn, 1)
}

func (m *MmsTables) CompactionDisable() {
	atomic.StoreInt32(&m.compactionEn, 0)
}

func (m *MmsTables) MergeEnable() {
	atomic.StoreInt32(&m.mergeEn, 1)
}

func (m *MmsTables) MergeDisable() {
	atomic.StoreInt32(&m.mergeEn, 0)
}

func (m *MmsTables) MergeEnabled() bool {
	return atomic.LoadInt32(&m.mergeEn) > 0
}

func (m *MmsTables) cacheFileData() bool {
	conf := CacheMetaInMemory() || CacheDataInMemory()
	if *m.tier == util.Hot {
		return conf
	} else if *m.tier == util.Warm {
		return false
	}

	return conf
}
func (m *MmsTables) SetOpId(shardId uint64, opId uint64) {
	m.shardId = shardId
	m.opId = opId
}

func (m *MmsTables) Open() (int64, error) {
	lg := m.logger.With(zap.String("path", m.path))
	lg.Info("table store open start", zap.Uint64("id", m.shardId), zap.Uint64("opId", m.opId))
	start := time.Now()
	shardDir := filepath.Dir(m.path)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := recoverFile(shardDir, m.lock, m.ImmTable.GetEngineType()); err != nil {
		errInfo := errno.NewError(errno.RecoverFileFailed, shardDir)
		lg.Error("", zap.Error(errInfo))
		return 0, errInfo
	}

	stats.ShardStepDuration(m.shardId, m.opId, "RecoverCompactDuration", time.Since(start).Nanoseconds(), false)
	tm := time.Now()
	dirs, err := fileops.ReadDir(m.path)
	if err != nil {
		lg.Error("read table store dir fail", zap.Error(err))
		return 0, err
	}

	if len(dirs) == 0 {
		return 0, nil
	}

	m.sequencer.free()
	ctx := &fileLoadContext{}
	loader := newFileLoader(m, ctx)
	loader.maxRowsPerSegment = m.Conf.maxRowsPerSegment
	for i := range dirs {
		mst := dirs[i].Name() // measurement name with version
		loader.Load(filepath.Join(m.path, mst), mst, true)
	}
	loader.Wait()
	stats.ShardStepDuration(m.shardId, m.opId, "FileLoaderDuration", time.Since(tm).Nanoseconds(), false)
	// this is a normal situation, don't need any following operations
	if loader.total == 0 {
		return 0, nil
	}

	tm = time.Now()
	maxSeq := ctx.getMaxSeq()
	if maxSeq > m.fileSeq {
		m.fileSeq = maxSeq
	}
	errCnt, err := ctx.getError()
	// if not all file open success, just log error and continue
	if errCnt > 0 {
		errInfo := errno.NewError(errno.NotAllTsspFileOpenSuccess, loader.total, errCnt)
		lg.Error("", zap.Error(errInfo), zap.String("first error", err.Error()))
	}

	m.sortTSSPFiles()
	stats.ShardStepDuration(m.shardId, m.opId, "SortTSSPFileDuration", time.Since(tm).Nanoseconds(), false)

	lg.Info("table store open done",
		zap.Int("file count", loader.total), zap.Duration("time used", time.Since(start)),
		zap.Uint64("id", m.shardId), zap.Uint64("opId", m.opId))

	return ctx.getMaxTime(), nil
}

func (m *MmsTables) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}

func (m *MmsTables) isCompMergeStopped() bool {
	select {
	case <-m.stopCompMerge:
		return true
	default:
		return false
	}
}

func (m *MmsTables) isPreLoading() bool {
	return *m.lock == ""
}

func (m *MmsTables) Close() error {
	if !m.isClosed() {
		close(m.closed)
	}
	m.wg.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, v := range m.Order {
		for i := range v.files {
			if err := v.files[i].Close(); err != nil {
				return err
			}
		}
	}

	for _, v := range m.OutOfOrder {
		for i := range v.files {
			if err := v.files[i].Close(); err != nil {
				return err
			}
		}
	}

	for _, v := range m.CSFiles {
		for i := range v.files {
			if err := v.files[i].Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *MmsTables) getFiles(inFiles *TSSPFiles, hasTimeFilter bool, tr util.TimeRange) []TSSPFile {
	reFiles := make([]TSSPFile, 0, inFiles.Len())
	for _, f := range inFiles.files {
		if hasTimeFilter {
			contains, err := f.ContainsByTime(tr)
			if !contains || err != nil {
				continue
			}
		}
		f.Ref()
		reFiles = append(reFiles, f)
	}
	return reFiles
}

func (m *MmsTables) GetBothFilesRef(measurement string, hasTimeFilter bool, tr util.TimeRange) ([]TSSPFile, []TSSPFile) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	order, orderOk := m.Order[measurement]
	unorder, unorderOk := m.OutOfOrder[measurement]
	if orderOk {
		order.lock.RLock()
		defer order.lock.RUnlock()
	}
	if unorderOk {
		unorder.lock.RLock()
		defer unorder.lock.RUnlock()
	}
	var orderFiles []TSSPFile = nil
	var unorderFiles []TSSPFile = nil
	if orderOk {
		orderFiles = m.getFiles(order, hasTimeFilter, tr)
	}
	if unorderOk {
		unorderFiles = m.getFiles(unorder, hasTimeFilter, tr)
	}
	return orderFiles, unorderFiles
}

func (m *MmsTables) NextSequence() uint64 {
	return atomic.AddUint64(&m.fileSeq, 1)
}

func (m *MmsTables) Sequencer() *Sequencer {
	m.sequencer.addRef()
	return m.sequencer
}

func (m *MmsTables) IsOutOfOrderFilesExist() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.OutOfOrder) != 0
}

func (m *MmsTables) addPKFile(mstName string, file string, rec *record.Record, mark fragment.IndexFragment, tcLocation int8) {
	fs, ok := m.PKFiles[mstName]

	if !ok {
		fs = colstore.NewPKFiles()
		m.PKFiles[mstName] = fs
	}

	fs.SetPKInfo(file, rec, mark, tcLocation)
}

func (m *MmsTables) AddPKFile(mstName string, file string, rec *record.Record, mark fragment.IndexFragment, tcLocation int8) {
	m.mu.RLock()
	tables := m.PKFiles
	fs, ok := tables[mstName]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		fs, ok = tables[mstName]
		if !ok {
			fs = colstore.NewPKFiles()
			m.PKFiles[mstName] = fs
		}
		m.mu.Unlock()
	}

	fs.SetPKInfo(file, rec, mark, tcLocation)
}

func (m *MmsTables) GetPKFile(mstName string, file string) (pkInfo *colstore.PKInfo, ok bool) {
	m.mu.RLock()
	tables := m.PKFiles
	fs, ok := tables[mstName]
	m.mu.RUnlock()

	if !ok {
		return
	}
	pkInfo, ok = fs.GetPKInfo(file)
	return
}

func (m *MmsTables) ReplacePKFile(mstName string, file string, rec *record.Record, mark fragment.IndexFragment, oldIndexFiles []string) error {
	m.mu.RLock()
	tables := m.PKFiles
	fs, ok := tables[mstName]
	m.mu.RUnlock()
	if !ok {
		return errors.New("mst is not exist in pkFiles when replace pkFiles")
	}

	m.mu.Lock()
	for i := range oldIndexFiles {
		fs.DelPKInfo(oldIndexFiles[i])
	}
	m.mu.Unlock()

	fs.SetPKInfo(file, rec, mark, colstore.DefaultTCLocation)
	return nil
}

func (m *MmsTables) getPKFiles(mstName string) (*colstore.PKFiles, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files, ok := m.PKFiles[mstName]
	return files, ok
}

func (m *MmsTables) sortTSSPFiles() {
	for _, item := range m.Order {
		sort.Sort(item)
	}
	for _, item := range m.OutOfOrder {
		sort.Sort(item)
	}

	for _, item := range m.CSFiles {
		sort.Sort(item)
	}
}

func (m *MmsTables) GetTSSPFiles(name string, isOrder bool) (files *TSSPFiles, ok bool) {
	m.mu.RLock()
	if isOrder {
		files, ok = m.Order[name]
	} else {
		files, ok = m.OutOfOrder[name]
	}
	if ok {
		for i := range files.Files() {
			files.Files()[i].Ref()
			files.Files()[i].RefFileReader()
		}
	}
	m.mu.RUnlock()
	return
}

func (m *MmsTables) GetCSFiles(name string) (files *TSSPFiles, ok bool) {
	m.mu.RLock()
	files, ok = m.CSFiles[name]
	if ok {
		for i := range files.Files() {
			files.Files()[i].Ref()
			files.Files()[i].RefFileReader()
		}
	}
	m.mu.RUnlock()
	return
}

func (m *MmsTables) DropMeasurement(_ context.Context, name string) error {
	var orderWg, inorderWg, csWg *sync.WaitGroup
	mstPath := filepath.Join(m.path, name)
	log.Info("drop measurement start...", zap.String("name", name), zap.String("path", mstPath))
	m.mu.RLock()
	order, ok := m.Order[name]
	orderWg = stopFiles(ok, order)

	unOrder, ok := m.OutOfOrder[name]
	inorderWg = stopFiles(ok, unOrder)

	csFiles, ok := m.CSFiles[name]
	csWg = stopFiles(ok, csFiles)

	pkFiles := m.PKFiles[name]
	m.mu.RUnlock()

	if orderWg != nil {
		orderWg.Wait()
	}
	if inorderWg != nil {
		inorderWg.Wait()
	}
	if csWg != nil {
		csWg.Wait()
	}

	err := m.deleteFilesForDropMeasurement(order, unOrder, csFiles, name, mstPath)
	if err != nil {
		return err
	}
	err = m.deletePKFilesForDropMeasurement(pkFiles)
	if err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.Order, name)
	delete(m.OutOfOrder, name)
	delete(m.CSFiles, name)
	delete(m.PKFiles, name)

	m.sequencer.DelMmsIdTime(name)
	m.mu.Unlock()

	mmsDir := filepath.Join(m.path, name)
	lockFile := fileops.FileLockOption(*m.lock)
	_ = fileops.RemoveAll(mmsDir, lockFile)
	log.Info("drop measurement done", zap.String("name", name), zap.String("path", mstPath))

	return nil
}

func (m *MmsTables) deleteFilesForDropMeasurement(order, unOrder, csFiles *TSSPFiles, name, mstPath string) error {
	var err error
	err = deleteFiles(order, m)
	if err != nil {
		log.Error("drop order files fail", zap.String("name", name), zap.String("path", mstPath), zap.Error(err))
		return err
	}

	err = deleteFiles(unOrder, m)
	if err != nil {
		log.Error("drop out of order files fail", zap.String("name", name),
			zap.String("path", mstPath), zap.Error(err))
		return err
	}

	err = deleteFiles(csFiles, m)
	if err != nil {
		log.Error("drop column store files fail", zap.String("name", name),
			zap.String("path", mstPath), zap.Error(err))
		return err
	}
	return err
}

func (m *MmsTables) deletePKFilesForDropMeasurement(pkFiles *colstore.PKFiles) error {
	var err error
	if pkFiles != nil {
		for filename := range pkFiles.GetPKInfos() {
			lock := fileops.FileLockOption("")
			err := fileops.Remove(filename, lock)
			if err != nil && !os.IsNotExist(err) {
				err = errRemoveFail(filename, err)
				log.Error("remove file fail ", zap.String("file name", filename), zap.Error(err))
				return err
			}
			pkFiles.DelPKInfo(filename)
		}
	}
	return err
}

func (m *MmsTables) SeriesTotal() uint64 {
	return m.sequencer.SeriesTotal()
}

func stopFiles(ok bool, tsspfiles *TSSPFiles) *sync.WaitGroup {
	var wg *sync.WaitGroup
	if ok && tsspfiles != nil {
		tsspfiles.StopFiles()
		wg = &tsspfiles.wg
	}
	return wg
}

func deleteFiles(tsspFiles *TSSPFiles, m *MmsTables) error {
	var err error
	if tsspFiles != nil {
		tsspFiles.lock.Lock()
		err = m.deleteFiles(tsspFiles.files...)
		tsspFiles.lock.Unlock()
	}
	return err
}

func (ctx *memReaderEvictCtx) runEvictMemReaders() {
	timer := time.NewTicker(time.Millisecond * 200)
	for range timer.C {
		evictSize := getImmTableEvictSize()
		if evictSize > 0 {
			ctx.evictMemReader(evictSize)
		}
	}
	timer.Stop()
}

func (m *MmsTables) AddTSSPFiles(name string, isOrder bool, files ...TSSPFile) {
	m.ImmTable.AddTSSPFiles(m, name, isOrder, files...)
}

func (m *MmsTables) AddTable(mb *MsBuilder, isOrder bool, tmp bool) {
	mb.FileName.SetOrder(isOrder)
	f, err := mb.NewTSSPFile(tmp)
	if err != nil {
		panic(err)
	}
	if f != nil {
		m.AddTSSPFiles(mb.Name(), isOrder, f)
	}
}

func (m *MmsTables) ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool) (err error) {
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
	// add new files
	fs.files = append(fs.files, newFiles...)
	sort.Sort(fs)

	lock := fileops.FileLockOption(*m.lock)
	if err = fileops.Remove(logFile, lock); err != nil {
		m.logger.Error("remove compact log file error", zap.String("name", name), zap.String("dir", shardDir),
			zap.String("log", logFile), zap.Error(err))
	}

	return
}

func (m *MmsTables) ReplaceDownSampleFiles(mstNames []string, originFiles [][]TSSPFile, newFiles [][]TSSPFile, isOrder bool, callBack func()) (err error) {
	for k := range mstNames {
		if err := RenameTmpFiles(newFiles[k]); err != nil {
			return err
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTables := m.Order
	if !isOrder {
		mmsTables = m.OutOfOrder
	}

	// remove old files
	allFs := make([]*TSSPFiles, 0)
	defer func() {
		for i := range allFs {
			allFs[i].lock.Unlock()
		}
	}()
	for k, v := range mstNames {
		fs, ok := mmsTables[v]
		if !ok || fs == nil {
			return ErrDownSampleStopped
		}
		allFs = append(allFs, fs)
		fs.lock.Lock()

		for _, f := range originFiles[k] {
			if m.isClosed() {
				return ErrDownSampleStopped
			}
			fs.deleteFile(f)
			if err = m.deleteFiles(f); err != nil {
				return
			}
		}
	}

	for k := range allFs {
		allFs[k].files = append(allFs[k].files, newFiles[k]...)
		sort.Sort(allFs[k])
	}

	callBack()

	return
}

func RenameTmpFiles(newFiles []TSSPFile) error {
	for i := range newFiles {
		f := newFiles[i]
		tmpName := f.Path()
		if IsTempleFile(filepath.Base(tmpName)) {
			fname := tmpName[:len(tmpName)-len(tmpFileSuffix)]
			if err := f.FreeFileHandle(); err != nil {
				return err
			}
			if err := f.Rename(fname); err != nil {
				log.Error("rename file error", zap.String("name", tmpName), zap.Error(err))
				if _, e := fileops.Stat(fname); e != nil {
					return os.ErrNotExist
				}
				return err
			}
		}
	}

	return nil
}

func RenameTmpFilesWithPKIndex(newFiles []TSSPFile, indexList []string) error {
	lock := fileops.FileLockOption("")
	for i := range newFiles {
		f := newFiles[i]
		tmpName := f.Path()
		if IsTempleFile(filepath.Base(tmpName)) {
			// rename tssp file
			fname := tmpName[:len(tmpName)-len(tmpFileSuffix)]
			if err := f.FreeFileHandle(); err != nil {
				return err
			}
			if err := f.Rename(fname); err != nil {
				log.Error("rename file error", zap.String("name", tmpName), zap.Error(err))
				if _, e := fileops.Stat(fname); e != nil {
					return os.ErrNotExist
				}
				return err
			}

			// rename pk index file
			IndexFileName := fname[:len(fname)-tsspFileSuffixLen] + colstore.IndexFileSuffix
			tmpIndexFileName := IndexFileName + tmpFileSuffix
			if err := fileops.RenameFile(tmpIndexFileName, IndexFileName, lock); err != nil {
				err = errno.NewError(errno.RenameFileFailed, zap.String("old", tmpIndexFileName), zap.String("new", IndexFileName), err)
				log.Error("rename file fail", zap.Error(err))
				return err
			}

			if len(indexList) != 0 {
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
			}
		}
	}

	return nil
}

func (m *MmsTables) FreeAllMemReader() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	kvMap := []map[string]*TSSPFiles{m.Order, m.OutOfOrder, m.CSFiles}
	for _, kvEntry := range kvMap {
		for _, v := range kvEntry {
			v.lock.Lock()
			for _, f := range v.files {
				if !f.Inuse() {
					_ = f.FreeMemory(true)
					continue
				}
				nodeTableStoreGC.Add(true, f)
			}
			v.lock.Unlock()
		}
	}
}

func (m *MmsTables) acquire(files []string) bool {
	m.inCompLock.Lock()
	defer m.inCompLock.Unlock()

	for _, name := range files {
		if _, ok := m.inCompact[name]; ok {
			return false
		}
	}

	for _, name := range files {
		m.inCompact[name] = struct{}{}
	}

	return true
}

func (m *MmsTables) CompactDone(files []string) {
	m.inCompLock.Lock()
	defer m.inCompLock.Unlock()
	for _, name := range files {
		delete(m.inCompact, name)
	}
}

var (
	seqMapPool = sync.Pool{New: func() interface{} { return &dictpool.Dict{} }}
)

func (m *MmsTables) genCompactGroup(seqMap *dictpool.Dict, name string, level uint16) *CompactGroup {
	if m.ImmTable.GetCompactionType(name) == config.BLOCK && !m.inBlockCompact.Add(name) {
		log.Debug("block compact in process", zap.String("name", name))
		return nil
	}

	group := NewCompactGroup(name, level+1, seqMap.Len())
	for i, kv := range seqMap.D {
		f := kv.Value.(TSSPFile)
		fn := f.Path()
		group.group[i] = fn
	}

	if !m.acquire(group.group) {
		group.release()
		return nil
	}

	return group
}

func (m *MmsTables) mmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup {
	if m.isClosed() || m.isCompMergeStopped() || atomic.LoadInt64(&files.closing) > 0 {
		return plans
	}

	seqMap := seqMapPool.Get().(*dictpool.Dict)
	seqMap.Reset()
	defer seqMapPool.Put(seqMap)

	idx := 0
	for idx < files.Len() {
		f := files.files[idx]
		lv, seq := f.LevelAndSequence()
		if lv != level {
			// if seqMap.Len() >= minGroupFileN, but the next file is another level, we will create the plan
			// and reserve the split file check logic
			plans = m.genCompactPlan(seqMap, minGroupFileN, name, level, files, plans)
			seqMap.Reset()
			idx++
			continue
		}

		seqByte := record.Uint64ToBytesUnsafe(seq)
		if !seqMap.HasBytes(seqByte) {
			plans = m.genCompactPlan(seqMap, minGroupFileN, name, level, files, plans)
			seqMap.SetBytes(seqByte, f)
			idx++
		} else {
			i := idx + 1
			for i < files.Len() {
				f = files.files[i]
				if !levelSequenceEqual(level, seq, f) {
					break
				}
				i++
			}
			idx = i
			seqMap.Reset()
		}

	}

	plans = m.genCompactPlan(seqMap, minGroupFileN, name, level, files, plans)

	return plans
}

func (m *MmsTables) getMmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup {
	files.lock.RLock()
	defer files.lock.RUnlock()
	if atomic.LoadInt64(&files.closing) > 0 || files.Len() < minGroupFileN {
		return plans
	}
	plans = m.mmsPlan(name, files, level, minGroupFileN, plans)
	return plans
}

func (m *MmsTables) genCompactPlan(seqMap *dictpool.Dict, minGroupFileN int, name string, level uint16,
	files *TSSPFiles, plans []*CompactGroup) []*CompactGroup {
	if seqMap.Len() >= minGroupFileN {
		plan := m.genCompactGroup(seqMap, name, level)
		if plan != nil {
			plan.dropping = &files.closing
			plans = append(plans, plan)
		}
		seqMap.Reset()
	}
	return plans
}

func (m *MmsTables) File(mstName string, fileName string, isOrder bool) TSSPFile {
	files, ok := m.ImmTable.getTSSPFiles(m, mstName, isOrder)
	if !ok || files == nil {
		return nil
	}

	files.lock.RLock()
	defer files.lock.RUnlock()

	if files.closing > 0 {
		return nil
	}

	for _, f := range files.Files() {
		if f.Path() == fileName {
			return f
		}
	}

	return nil
}

func (m *MmsTables) getTSSPFiles(mstName string, isOrder bool) (*TSSPFiles, bool) {
	return m.ImmTable.getTSSPFiles(m, mstName, isOrder)
}

func (m *MmsTables) NewStreamWriteFile(mst string) *StreamWriteFile {
	sw := getStreamWriteFile()
	sw.closed = m.closed
	sw.name = mst
	sw.dir = m.path
	sw.pair.Reset(mst)
	sw.Conf = m.Conf
	sw.chunkRows = 0
	sw.maxChunkRows = 0
	cLog, _ := influxLogger.NewOperation(log, "StreamDownSample", mst)
	sw.log = logger.NewLogger(errno.ModuleDownSample).SetZapLogger(cLog)
	sw.colSegs = make([]record.ColVal, 1)
	sw.lock = m.lock
	sw.colBuilder.timePreAggBuilder = acquireTimePreAggBuilder()
	return sw
}

func (m *MmsTables) SetLockPath(lock *string) {
	m.lock = lock
}

func levelSequenceEqual(level uint16, seq uint64, f TSSPFile) bool {
	lv, n := f.LevelAndSequence()
	return lv == level && seq == n
}

func recoverFile(shardDir string, lockPath *string, engineType config.EngineType) error {
	dirs, err := fileops.ReadDir(shardDir)
	if err != nil {
		log.Error("read table store dir fail", zap.String("path", shardDir), zap.Error(err))
		return err
	}

	for i := range dirs {
		mn := dirs[i].Name()
		if mn != compactLogDir {
			continue
		}

		logDir := filepath.Join(shardDir, compactLogDir)
		err := procCompactLog(shardDir, logDir, lockPath, engineType)
		if err != nil {
			if err != ErrDirtyLog {
				return err
			}
		}
	}

	return nil
}

//lint:ignore U1000 test used only
func compareFile(f1, f2 interface{}) bool {
	firstMin, firstMax, _ := f1.(TSSPFile).MinMaxTime()
	secondMin, secondMax, _ := f2.(TSSPFile).MinMaxTime()
	if firstMin == secondMin {
		return firstMax <= secondMax
	}
	if firstMin < secondMin {
		return false
	}
	return true
}

//lint:ignore U1000 test used only
func compareFileByDescend(f1, f2 interface{}) bool {
	firstMin, firstMax, _ := f1.(TSSPFile).MinMaxTime()
	secondMin, secondMax, _ := f2.(TSSPFile).MinMaxTime()
	if firstMax == secondMax {
		return firstMin <= secondMin
	}

	if firstMax > secondMax {
		return true
	}
	return false
}
