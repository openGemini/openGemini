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

package immutable

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	unorderedDir   = "out-of-order"
	tsspFileSuffix = ".tssp"

	tmpFileSuffix     = ".init"
	tmpSuffixNameLen  = len(tmpFileSuffix)
	tsspFileSuffixLen = len(tsspFileSuffix)
	compactLogDir     = "compact_log"
	DownSampleLogDir  = "downsample_log"
	ShardMoveLogDir   = "shard_move_log"

	TsspDirName        = "tssp"
	ColumnStoreDirName = obs.ColumnStoreDirName
	CountBinFile       = "count.txt"
	CapacityBinFile    = "capacity.txt"

	defaultCap = 64
)

func RemoveTsspSuffix(dataPath string) string {
	return dataPath[:len(dataPath)-tsspFileSuffixLen]
}

var errFileClosed = fmt.Errorf("tssp file closed")

type TSSPFile interface {
	Path() string
	Name() string
	FileName() TSSPFileName
	LevelAndSequence() (uint16, uint64)
	FileNameMerge() uint16
	FileNameExtend() uint16
	IsOrder() bool
	Ref()
	Unref()
	RefFileReader()
	UnrefFileReader()
	Stop()
	Inuse() bool
	MetaIndexAt(idx int) (*MetaIndex, error)
	MetaIndex(id uint64, tr util.TimeRange) (int, *MetaIndex, error)
	ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *ChunkMetaContext, ioPriority int) (*ChunkMeta, error)
	ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext, ioPriority int) (*record.Record, error)
	ReadData(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error)
	ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta, ioPriority int) ([]ChunkMeta, error)

	FileStat() *Trailer
	// FileSize get the size of the disk occupied by file
	FileSize() int64
	// InMemSize get the size of the memory occupied by file
	InMemSize() int64
	Contains(id uint64) (bool, error)
	ContainsByTime(tr util.TimeRange) (bool, error)
	ContainsValue(id uint64, tr util.TimeRange) (bool, error)
	MinMaxTime() (int64, int64, error)

	Open() error
	Close() error
	LoadIntoMemory() error
	LoadComponents() error
	LoadIdTimes(p *IdTimePairs) error
	Rename(newName string) error
	UpdateLevel(level uint16)
	Remove() error
	FreeMemory(evictLock bool) int64
	FreeFileHandle() error
	Version() uint64
	AverageChunkRows() int
	MaxChunkRows() int
	MetaIndexItemNum() int64
	AddToEvictList(level uint16)
	RemoveFromEvictList(level uint16)
	GetFileReaderRef() int64
	RenameOnObs(obsName string, tmp bool, opt *obs.ObsOptions) error

	ChunkMetaCompressMode() uint8
}

type TSSPFiles struct {
	lock    sync.RWMutex
	ref     int64
	wg      sync.WaitGroup
	closing int64
	files   []TSSPFile
}

func NewTSSPFiles() *TSSPFiles {
	return &TSSPFiles{
		files: make([]TSSPFile, 0, 32),
		ref:   1,
	}
}

func (f *TSSPFiles) fullCompacted() bool {
	f.lock.RLock()
	defer f.lock.RUnlock()

	if len(f.files) <= 1 {
		return true
	}

	sameLeve := true
	lv, seq := f.files[0].LevelAndSequence()
	for i := 1; i < len(f.files); i++ {
		if sameLeve {
			level, curSeq := f.files[i].LevelAndSequence()
			sameLeve = lv == level && curSeq == seq
		} else {
			break
		}
	}
	return sameLeve
}

func (f *TSSPFiles) Len() int      { return len(f.files) }
func (f *TSSPFiles) Swap(i, j int) { f.files[i], f.files[j] = f.files[j], f.files[i] }
func (f *TSSPFiles) Less(i, j int) bool {
	_, iSeq := f.files[i].LevelAndSequence()
	_, jSeq := f.files[j].LevelAndSequence()
	iExt, jExt := f.files[i].FileNameExtend(), f.files[j].FileNameExtend()
	if iSeq != jSeq {
		return iSeq < jSeq
	}
	return iExt < jExt
}

func (f *TSSPFiles) StopFiles() {
	atomic.AddInt64(&f.closing, 1)
	f.lock.RLock()
	for _, tf := range f.files {
		tf.Stop()
	}
	f.lock.RUnlock()
}

func (f *TSSPFiles) fileIndex(tbl TSSPFile) int {
	if len(f.files) == 0 {
		return -1
	}

	idx := -1
	_, seq := tbl.LevelAndSequence()
	left, right := 0, f.Len()-1
	for left < right {
		mid := (left + right) / 2
		_, n := f.files[mid].LevelAndSequence()
		if seq == n {
			idx = mid
			break
		} else if seq < n {
			right = mid
		} else {
			left = mid + 1
		}
	}

	if idx != -1 {
		for i := idx; i >= 0; i-- {
			if _, n := f.files[i].LevelAndSequence(); n != seq {
				break
			}
			if f.files[i].Path() == tbl.Path() {
				return i
			}
		}

		for i := idx + 1; i < f.Len(); i++ {
			if _, n := f.files[i].LevelAndSequence(); n != seq {
				break
			}
			if f.files[i].Path() == tbl.Path() {
				return i
			}
		}
	}

	if f.files[left].Path() == tbl.Path() {
		return left
	}

	return -1
}

func (f *TSSPFiles) Files() []TSSPFile {
	return f.files
}

func (f *TSSPFiles) deleteFile(tbl TSSPFile) {
	idx := f.fileIndex(tbl)
	if idx < 0 || idx >= f.Len() {
		logger.GetLogger().Warn("file not find", zap.String("file", tbl.Path()))
		return
	}

	f.files = append(f.files[:idx], f.files[idx+1:]...)
}

func (f *TSSPFiles) Append(file ...TSSPFile) {
	f.files = append(f.files, file...)
}

func (f *TSSPFiles) RLock() {
	f.lock.RLock()
}

func (f *TSSPFiles) RUnlock() {
	f.lock.RUnlock()
}

func (f *TSSPFiles) MaxMerged() uint16 {
	if f.Len() == 0 {
		return 0
	}

	maxMerged := uint16(0)
	for _, file := range f.files {
		merge := file.FileNameMerge()
		if merge > maxMerged {
			maxMerged = merge
		}
	}
	return maxMerged
}

type tsspFile struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	name TSSPFileName
	ref  int32
	flag uint32 // flag > 0 indicates that the files is need close.
	lock *string

	memEle *list.Element // lru node
	reader FileReader
}

func OpenTSSPFile(name string, lockPath *string, isOrder bool, cacheData bool) (TSSPFile, error) {
	var fileName TSSPFileName
	if err := fileName.ParseFileName(name); err != nil {
		return nil, err
	}
	fileName.SetOrder(isOrder)

	fr, err := NewTSSPFileReader(name, lockPath)
	if err != nil || fr == nil {
		return nil, err
	}

	fr.inMemBlock = emptyMemReader
	if cacheData {
		idx := calcBlockIndex(int(fr.trailer.dataSize))
		fr.inMemBlock = NewMemoryReader(blockSize[idx])
	}

	if err = fr.Open(); err != nil {
		return nil, err
	}

	return &tsspFile{
		name:   fileName,
		reader: fr,
		ref:    1,
		lock:   lockPath,
	}, nil
}

func (f *tsspFile) stopped() bool {
	return atomic.LoadUint32(&f.flag) > 0
}

func (f *tsspFile) Stop() {
	atomic.AddUint32(&f.flag, 1)
}

func (f *tsspFile) Inuse() bool {
	return atomic.LoadInt32(&f.ref) > 1
}

func (f *tsspFile) Ref() {
	if f.stopped() {
		return
	}

	atomic.AddInt32(&f.ref, 1)
	f.wg.Add(1)
}

func (f *tsspFile) Unref() {
	if atomic.AddInt32(&f.ref, -1) <= 0 {
		if f.stopped() {
			return
		}
		panic("file closed")
	}
	f.wg.Done()
}

func (f *tsspFile) RefFileReader() {
	f.mu.RLock()
	f.reader.Ref()
	f.mu.RUnlock()
}

func (f *tsspFile) UnrefFileReader() {
	if f.stopped() {
		return
	}
	if f.reader == nil {
		return
	}
	if f.reader.Unref() > 0 {
		return
	}

	err := f.FreeFileHandle()
	if err != nil {
		log.Error("freeFile failed", zap.Error(err))
	}
}

func (f *tsspFile) LevelAndSequence() (uint16, uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name.level, f.name.seq
}

func (f *tsspFile) FileNameMerge() uint16 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name.merge
}

func (f *tsspFile) FileNameExtend() uint16 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name.extent
}

func (f *tsspFile) Path() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return ""
	}
	return f.reader.FileName()
}

func (f *tsspFile) Name() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return ""
	}

	return f.reader.Name()
}

func (f *tsspFile) FileName() TSSPFileName {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name
}

func (f *tsspFile) IsOrder() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name.order
}

func (f *tsspFile) FreeMemory(evictLock bool) int64 {
	f.mu.Lock()
	if f.Inuse() {
		f.mu.Unlock()
		nodeTableStoreGC.Add(true, f)
		return 0
	}

	size := f.reader.FreeMemory()
	level := f.name.level
	order := f.name.order
	f.mu.Unlock()

	if order {
		addMemSize(levelName(level), -size, -size, 0)
	} else {
		addMemSize(levelName(level), -size, 0, -size)
	}

	if evictLock {
		f.RemoveFromEvictList(level)
	} else {
		f.RemoveFromEvictListUnSafe(level)
	}

	return size
}

func (f *tsspFile) FreeFileHandle() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped() {
		return nil
	}
	if err := f.reader.FreeFileHandle(); err != nil {
		return err
	}
	return nil
}

func (f *tsspFile) MetaIndex(id uint64, tr util.TimeRange) (int, *MetaIndex, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return 0, nil, errFileClosed
	}
	return f.reader.MetaIndex(id, tr)
}

func (f *tsspFile) MetaIndexAt(idx int) (*MetaIndex, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return nil, errFileClosed
	}
	return f.reader.MetaIndexAt(idx)
}

func (f *tsspFile) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *ChunkMetaContext, ioPriority int) (*ChunkMeta, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return nil, errFileClosed
	}
	return f.reader.ChunkMeta(id, offset, size, itemCount, metaIdx, ctx, ioPriority)
}

func (f *tsspFile) ReadData(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return nil, errFileClosed
	}

	return f.reader.Read(offset, size, dst, ioPriority)
}

func (f *tsspFile) ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta, ioPriority int) ([]ChunkMeta, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return dst, errFileClosed
	}

	return f.reader.ReadChunkMetaData(metaIdx, m, dst, ioPriority)
}

func (f *tsspFile) ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext, ioPriority int) (*record.Record, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return nil, errFileClosed
	}

	if segment < 0 || segment >= cm.segmentCount() {
		err := fmt.Errorf("segment index %d out of range %d", segment, cm.segmentCount())
		log.Error(err.Error())
		return nil, err
	}

	return f.reader.ReadData(cm, segment, dst, decs, ioPriority)
}

func (f *tsspFile) FileStat() *Trailer {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.reader.Stat()
}

func (f *tsspFile) InMemSize() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.reader.InMemSize()
}

func (f *tsspFile) FileSize() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.reader.FileSize()
}

func (f *tsspFile) Contains(id uint64) (contains bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return false, errFileClosed
	}

	contains = f.reader.ContainsId(id)

	return
}

func (f *tsspFile) ContainsValue(id uint64, tr util.TimeRange) (contains bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return false, errFileClosed
	}
	contains = f.reader.Contains(id, tr)

	return
}

func (f *tsspFile) ContainsByTime(tr util.TimeRange) (contains bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return false, errFileClosed
	}

	contains = f.reader.ContainsTime(tr)

	return
}

func (f *tsspFile) Rename(newName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped() {
		return errFileClosed
	}
	return f.reader.Rename(newName)
}

func (f *tsspFile) UpdateLevel(level uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.name.level = level
}

func (f *tsspFile) Remove() error {
	atomic.AddUint32(&f.flag, 1)
	if atomic.AddInt32(&f.ref, -1) == 0 {
		f.wg.Wait()

		f.mu.Lock()

		name := f.reader.FileName()
		memSize := f.reader.InMemSize()
		level := f.name.level
		order := f.name.order

		log.Debug("remove file", zap.String("file", name))
		_ = f.reader.Close()
		lock := fileops.FileLockOption(*f.lock)
		err := fileops.Remove(name, lock)
		if err != nil && !os.IsNotExist(err) {
			err = errRemoveFail(name, err)
			log.Error("remove file fail", zap.Error(err))
			f.mu.Unlock()
			return err
		}
		f.mu.Unlock()

		evict := memSize > 0

		if evict {
			if order {
				addMemSize(levelName(level), -memSize, -memSize, 0)
			} else {
				addMemSize(levelName(level), -memSize, 0, -memSize)
			}
			f.RemoveFromEvictList(level)
		}

	}
	return nil
}

func (f *tsspFile) Close() error {
	f.Stop()

	f.mu.Lock()
	memSize := f.reader.InMemSize()
	level := f.name.level
	order := f.name.order
	name := f.reader.FileName()
	tmp := IsTempleFile(filepath.Base(name))
	f.mu.Unlock()

	f.Unref()
	f.wg.Wait()

	f.mu.Lock()
	_ = f.reader.Close()
	f.mu.Unlock()

	if memSize > 0 && !tmp {
		if order {
			addMemSize(levelName(level), -memSize, -memSize, 0)
		} else {
			addMemSize(levelName(level), -memSize, 0, -memSize)
		}
		f.RemoveFromEvictList(level)
	}

	return nil
}

func (f *tsspFile) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return nil
}

func (f *tsspFile) LoadIdTimes(p *IdTimePairs) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.reader == nil {
		err := fmt.Errorf("disk file not init")
		log.Error("disk file not init", zap.Uint64("seq", f.name.seq), zap.Uint16("leve", f.name.level))
		return err
	}
	fr := f.reader

	if err := fr.LoadIdTimes(f.IsOrder(), p); err != nil {
		return err
	}

	return nil
}

func (f *tsspFile) LoadComponents() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reader == nil {
		err := fmt.Errorf("disk file not init")
		log.Error("disk file not init", zap.Uint64("seq", f.name.seq), zap.Uint16("leve", f.name.level))
		return err
	}

	return f.reader.LoadComponents()
}

func (f *tsspFile) LoadIntoMemory() error {
	f.mu.Lock()

	if f.reader == nil {
		f.mu.Unlock()
		err := fmt.Errorf("disk file not init")
		log.Error("disk file not init", zap.Uint64("seq", f.name.seq), zap.Uint16("leve", f.name.level))
		return err
	}

	if err := f.reader.LoadIntoMemory(); err != nil {
		f.mu.Unlock()
		return err
	}

	level := f.name.level
	size := f.reader.InMemSize()
	order := f.name.order
	f.mu.Unlock()

	if order {
		addMemSize(levelName(level), size, size, 0)
	} else {
		addMemSize(levelName(level), size, 0, size)
	}
	f.AddToEvictList(level)

	return nil
}

func (f *tsspFile) Version() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reader.Version()
}

func (f *tsspFile) MinMaxTime() (int64, int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return 0, 0, errFileClosed
	}
	return f.reader.MinMaxTime()
}

func (f *tsspFile) AddToEvictList(level uint16) {
	l := levelEvictListLock(level)
	if f.memEle != nil {
		panic("memEle need to nil")
	}
	f.memEle = l.PushFront(f)
	levelEvictListUnLock(level)
}

func (f *tsspFile) RemoveFromEvictList(level uint16) {
	l := levelEvictListLock(level)
	if f.memEle != nil {
		l.Remove(f.memEle)
		f.memEle = nil
	}
	levelEvictListUnLock(level)
}

func (f *tsspFile) RemoveFromEvictListUnSafe(level uint16) {
	l := levelEvictList(level)
	if f.memEle != nil {
		l.Remove(f.memEle)
		f.memEle = nil
	}
}

func (f *tsspFile) AverageChunkRows() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.reader.AverageChunkRows()
}

func (f *tsspFile) MaxChunkRows() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.reader.MaxChunkRows()
}

func (f *tsspFile) MetaIndexItemNum() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.reader.Stat().MetaIndexItemNum()
}

func (f *tsspFile) GetFileReaderRef() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.reader != nil {
		return f.reader.GetFileReaderRef()
	}
	return 0
}

func (f *tsspFile) RenameOnObs(oldName string, tmp bool, obsOpt *obs.ObsOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped() {
		return errFileClosed
	}
	localFileName := f.reader.FileName()
	err := f.reader.RenameOnObs(oldName, tmp, obsOpt)
	if err != nil {
		return err
	}

	if fileops.RemoveLocalEnabled(localFileName, obsOpt) {
		// remove local file
		lock := fileops.FileLockOption(*f.lock)
		return fileops.RemoveLocal(localFileName, lock)
	}
	return nil
}

func (f *tsspFile) ChunkMetaCompressMode() uint8 {
	return f.reader.ChunkMetaCompressMode()
}

var (
	_ TSSPFile = (*tsspFile)(nil)
)

var compactGroupPool = sync.Pool{New: func() interface{} { return &CompactGroup{group: make([]string, 0, 8)} }}

type CompactGroup struct {
	name    string
	shardId uint64
	toLevel uint16
	group   []string

	dropping *int64
}

func NewCompactGroup(name string, toLevle uint16, count int) *CompactGroup {
	g := compactGroupPool.Get().(*CompactGroup)
	g.name = name
	g.toLevel = toLevle
	g.group = g.group[:count]
	return g
}

func (g *CompactGroup) reset() {
	g.name = ""
	g.shardId = 0
	g.toLevel = 0
	g.group = g.group[:0]
	g.dropping = nil
}

func (g *CompactGroup) release() {
	g.reset()
	compactGroupPool.Put(g)
}

func (g *CompactGroup) Len() int {
	return len(g.group)
}

func (g *CompactGroup) Add(item string) {
	g.group = append(g.group, item)
}

func (g *CompactGroup) UpdateLevel(lv uint16) {
	if g.toLevel < lv {
		g.toLevel = lv
	}
}

type FilesInfo struct {
	name              string // measurement name with version
	shId              uint64
	totalSegmentCount uint64
	dropping          *int64
	compIts           FileIterators
	oldFiles          []TSSPFile
	oldIndexFiles     []string
	oldFids           []string
	maxColumns        int
	maxChunkRows      int
	avgChunkRows      int
	estimateSize      int
	maxChunkN         int
	toLevel           uint16
}

func (fi *FilesInfo) updatingFilesInfo(f TSSPFile, itr *FileIterator) {
	maxRows, avgRows := f.MaxChunkRows(), f.AverageChunkRows()
	if fi.maxChunkRows < maxRows {
		fi.maxChunkRows = maxRows
	}

	fi.avgChunkRows += avgRows
	if fi.maxChunkN < itr.chunkN {
		fi.maxChunkN = itr.chunkN
	}

	if fi.maxColumns < int(itr.curtChunkMeta.columnCount) {
		fi.maxColumns = int(itr.curtChunkMeta.columnCount)
	}

	fi.estimateSize += int(itr.r.FileSize())
}

func (fi *FilesInfo) updateFinalFilesInfo(group *CompactGroup) {
	fi.avgChunkRows /= len(fi.compIts)
	fi.dropping = group.dropping
	fi.name = group.name
	fi.shId = group.shardId
	fi.toLevel = group.toLevel
	fi.oldFids = group.group
}

func GetTmpFileSuffix() string {
	return tmpFileSuffix
}

func FileOperation(f TSSPFile, op func()) {
	if op == nil {
		return
	}

	f.Ref()
	f.RefFileReader()
	defer func() {
		f.UnrefFileReader()
		f.Unref()
	}()
	op()
}

var fileQueryCache *QueryfileCache

func InitQueryFileCache(cap uint32, enable bool) {
	if enable {
		fileQueryCache = NewQueryfileCache(cap)
	}
}

type QueryfileCache struct {
	cache    chan TSSPFile
	cacheCap uint32
}

func GetQueryfileCache() *QueryfileCache {
	return fileQueryCache
}

func NewQueryfileCache(cap uint32) *QueryfileCache {
	if cap == 0 {
		return &QueryfileCache{
			cache:    make(chan TSSPFile, cpu.GetCpuNum()*8),
			cacheCap: uint32(cpu.GetCpuNum() * 8),
		}
	} else {
		return &QueryfileCache{
			cache:    make(chan TSSPFile, cap),
			cacheCap: cap,
		}
	}
}

func (qfc *QueryfileCache) Put(f TSSPFile) {
	if f.GetFileReaderRef() > 1 {
		f.UnrefFileReader()
		return
	}
	for {
		select {
		case qfc.cache <- f:
			return
		default:
			qfc.Get()
		}
	}
}

func (qfc *QueryfileCache) Get() {
	select {
	case f := <-qfc.cache:
		f.UnrefFileReader()
	default:
		return
	}
}

func (qfc *QueryfileCache) GetCap() uint32 {
	return qfc.cacheCap
}
