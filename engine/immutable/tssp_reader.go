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
	"container/list"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	stats "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

const (
	unorderedDir      = "out-of-order"
	tsspFileSuffix    = ".tssp"
	tmpTsspFileSuffix = ".init"
	tmpSuffixNameLen  = len(tmpTsspFileSuffix)
	tsspFileSuffixLen = len(tsspFileSuffix)
	compactLogDir     = "compact_log"
	TsspDirName       = "tssp"

	defaultCap = 64
)

var errFileClosed = fmt.Errorf("tssp file closed")

type TSSPFile interface {
	Path() string
	Name() string
	LevelAndSequence() (uint16, uint64)
	FileNameMerge() uint16
	FileNameExtend() uint16
	IsOrder() bool
	Ref()
	Unref()
	Stop()
	Inuse() bool
	MetaIndexAt(idx int) (*MetaIndex, error)
	MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error)
	ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta) (*ChunkMeta, error)
	Read(id uint64, tr record.TimeRange, dst *record.Record) (*record.Record, error)
	ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error)
	ChunkAt(index int) (*ChunkMeta, error)
	ReadData(offset int64, size uint32, dst *[]byte) ([]byte, error)
	ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error)

	CreateTime() int64
	FileStat() *Trailer
	// FileSize get the size of the disk occupied by file
	FileSize() int64
	// InMemSize get the size of the memory occupied by file
	InMemSize() int64
	Contains(id uint64) (bool, error)
	ContainsByTime(tr record.TimeRange) (bool, error)
	ContainsValue(id uint64, tr record.TimeRange) (bool, error)
	MinMaxTime() (int64, int64, error)

	Delete(ids []int64) error
	DeleteRange(ids []int64, min, max int64) error
	HasTombstones() bool
	TombstoneFiles() []TombstoneFile

	Open() error
	Close() error
	LoadIntoMemory() error
	LoadIndex() error
	LoadIdTimes(p *IdTimePairs) error
	Rename(newName string) error
	Remove() error
	FreeMemory(evictLock bool) int64
	Version() uint64
	AverageChunkRows() int
	MaxChunkRows() int
	AddToEvictList(level uint16)
	RemoveFromEvictList(level uint16)
}

type memReaderEvictCtx struct {
	evictList     []memReaderEvictList
	evictListName []string
}

type memReaderEvictList struct {
	mu   sync.Mutex
	list list.List
}

func NewMemReaderEvictCtx() *memReaderEvictCtx {
	ctx := &memReaderEvictCtx{}

	ctx.evictList = make([]memReaderEvictList, CompactLevels+1)
	for i := range ctx.evictList {
		ctx.evictList[i].list.Init()
	}

	ctx.evictListName = make([]string, CompactLevels+1)
	for i := range ctx.evictList {
		ctx.evictListName[i] = strconv.Itoa(i)
	}

	return ctx
}

func (ctx *memReaderEvictCtx) evictMemReader(evictSize int64) {
	for i := len(ctx.evictList) - 1; i >= 0; i-- {
		l := levelEvictListLock(uint16(i))
		e := l.Back()
		for e != nil {
			f := e.Value.(TSSPFile)
			e = e.Prev()
			size := f.FreeMemory(false)
			if size > 0 {
				evictSize -= size
			}

			if evictSize <= 0 {
				levelEvictListUnLock(uint16(i))
				return
			}
		}
		levelEvictListUnLock(uint16(i))
	}
}

func getEvictListIdx(level uint16) uint16 {
	listLen := uint16(len(nodeEvictCtx.evictList))
	if level >= listLen {
		level = listLen - 1
	}
	return level
}

func levelEvictListLock(level uint16) *list.List {
	idx := getEvictListIdx(level)
	nodeEvictCtx.evictList[idx].mu.Lock()
	return &nodeEvictCtx.evictList[idx].list
}

func levelEvictList(level uint16) *list.List {
	idx := getEvictListIdx(level)
	return &nodeEvictCtx.evictList[idx].list
}

func levelEvictListUnLock(level uint16) {
	idx := getEvictListIdx(level)
	nodeEvictCtx.evictList[idx].mu.Unlock()
}

func levelName(level uint16) string {
	idx := getEvictListIdx(level)
	return nodeEvictCtx.evictListName[idx]
}

var (
	fileLoadLimiter  limiter.Fixed
	nodeEvictCtx     *memReaderEvictCtx
	nodeTableStoreGC TablesGC
)

func init() {
	n := cpu.GetCpuNum() * 4
	if n > 256 {
		n = 256
	}
	fileLoadLimiter = limiter.NewFixed(n)

	nodeEvictCtx = NewMemReaderEvictCtx()
	go nodeEvictCtx.runEvictMemReaders()

	nodeTableStoreGC = NewTableStoreGC()
	go nodeTableStoreGC.GC()
}

func UnrefFiles(files ...TSSPFile) {
	for _, f := range files {
		f.Unref()
	}
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
		panic(fmt.Sprintf("file not file, %v", tbl.Path()))
	}

	f.files = append(f.files[:idx], f.files[idx+1:]...)
}

func (f *TSSPFiles) Append(file TSSPFile) {
	f.files = append(f.files, file)
}

type tsspFile struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	name TSSPFileName
	ref  int32
	flag uint32 // flag > 0 indicates that the files is need close.

	memEle *list.Element // lru node
	reader TableReader
}

func OpenTSSPFile(name string, isOrder bool, cacheData bool) (TSSPFile, error) {
	var fileName TSSPFileName
	if err := fileName.ParseFileName(name); err != nil {
		return nil, err
	}
	fileName.SetOrder(isOrder)

	fr, err := NewTSSPFileReader(name)
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

func (f *tsspFile) CreateTime() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.reader.CreateTime()
}

func (f *tsspFile) Name() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return ""
	}

	return f.reader.Name()
}

func (f *tsspFile) IsOrder() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.name.order
}

func (f *tsspFile) FreeMemory(evictLock bool) int64 {
	f.mu.Lock()
	if f.Inuse() {
		nodeTableStoreGC.Add(true, f)
		f.mu.Unlock()
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

func (f *tsspFile) MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error) {
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

func (f *tsspFile) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta) (*ChunkMeta, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return nil, errFileClosed
	}
	return f.reader.ChunkMeta(id, offset, size, itemCount, metaIdx, dst)
}

func (f *tsspFile) Read(uint64, record.TimeRange, *record.Record) (*record.Record, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	panic("impl me")
}

func (f *tsspFile) ReadData(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return nil, errFileClosed
	}

	return f.reader.Read(offset, size, dst)
}

func (f *tsspFile) ReadChunkMetaData(metaIdx int, m *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return dst, errFileClosed
	}

	return f.reader.ReadChunkMetaData(metaIdx, m, dst)
}

func (f *tsspFile) ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error) {
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

	return f.reader.ReadData(cm, segment, dst, decs)
}

func (f *tsspFile) ChunkAt(index int) (*ChunkMeta, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return nil, errFileClosed
	}
	return f.reader.ChunkMetaAt(index)
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

func (f *tsspFile) ContainsValue(id uint64, tr record.TimeRange) (contains bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped() {
		return false, errFileClosed
	}
	contains = f.reader.Contains(id, tr)

	return
}

func (f *tsspFile) ContainsByTime(tr record.TimeRange) (contains bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.stopped() {
		return false, errFileClosed
	}

	contains = f.reader.ContainsTime(tr)

	return
}

func (f *tsspFile) Delete([]int64) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	panic("impl me")
}

func (f *tsspFile) DeleteRange([]int64, int64, int64) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	panic("impl me")
}

func (f *tsspFile) HasTombstones() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	panic("impl me")
}

func (f *tsspFile) TombstoneFiles() []TombstoneFile {
	f.mu.RLock()
	defer f.mu.RUnlock()
	panic("impl me")
}

func (f *tsspFile) Rename(newName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped() {
		return errFileClosed
	}
	return f.reader.Rename(newName)
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
		lock := fileops.FileLockOption("")
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
	_ = f.reader.Close()

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
	if f.reader == nil {
		err := fmt.Errorf("disk file not init")
		log.Error("disk file not init", zap.Uint64("seq", f.name.seq), zap.Uint16("leve", f.name.level))
		return err
	}
	fr, ok := f.reader.(*TSSPFileReader)
	if !ok {
		err := fmt.Errorf("LoadIdTimes: disk file isn't *TSSPFileReader type")
		log.Error("disk file isn't *TSSPFileReader", zap.Uint64("seq", f.name.seq),
			zap.Uint16("leve", f.name.level))
		return err
	}

	if err := fr.loadIdTimes(f.IsOrder(), p); err != nil {
		return err
	}

	return nil
}

func (f *tsspFile) LoadIndex() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reader == nil {
		err := fmt.Errorf("disk file not init")
		log.Error("disk file not init", zap.Uint64("seq", f.name.seq), zap.Uint16("leve", f.name.level))
		return err
	}

	return f.reader.LoadIndex()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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

var (
	_ TSSPFile = (*tsspFile)(nil)
)

type TablesStore interface {
	Open() (int64, int64, error)
	Close() error
	AddTable(ms *MsBuilder, isOrder bool, tmp bool)
	AddTSSPFiles(name string, isOrder bool, f ...TSSPFile)
	FreeAllMemReader()
	ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool, log *Log.Logger) error
	GetFilesRef(measurement string, isOrder bool) []TSSPFile
	GetFilesRefByAscending(measurement string, isOrder bool, ascending bool, tr record.TimeRange) []TSSPFile
	NextSequence() uint64
	Sequencer() *Sequencer
	Tier() uint64
	File(name string, namePath string, isOrder bool) TSSPFile
	CompactDone(seq []string)
	CompactionEnable()
	CompactionDisable()
	MergeEnable()
	MergeDisable()
	CompactionEnabled() bool
	MergeEnabled() bool
	MergeOutOfOrder(shId uint64) error
	LevelCompact(level uint16, shid uint64) error
	FullCompact(shid uint64) error
	GetLastFlushTimeBySid(measurement string, sid uint64) int64
	GetRowCountsBySid(measurement string, sid uint64) int64
	AddRowCountsBySid(measurement string, sid uint64, rowCounts int64)
	GetOutOfOrderFileNum() int
	GetMstFileStat() *stats.FileStat
	DropMeasurement(ctx context.Context, name string) error
}

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

type FilesInfo struct {
	name         string
	shId         uint64
	dropping     *int64
	compIts      FileIterators
	oldFiles     []TSSPFile
	oldFids      []string
	maxColumns   int
	maxChunkRows int
	avgChunkRows int
	estimateSize int
	maxChunkN    int
	toLevel      uint16
}

type MmsTables struct {
	mu   sync.RWMutex
	wg   sync.WaitGroup
	path string

	closed          chan struct{}
	Order           map[string]*TSSPFiles
	OutOfOrder      map[string]*TSSPFiles
	fileSeq         uint64
	tier            *uint64
	compactionEn    int32
	mergeEn         int32
	inCompLock      sync.RWMutex
	inCompact       map[string]struct{}
	inMerge         *InMerge
	sequencer       *Sequencer
	compactRecovery bool

	Conf          *Config
	lastMergeTime time.Time
}

func NewTableStore(dir string, tier *uint64, compactRecovery bool, config *Config) *MmsTables {
	store := &MmsTables{
		path:            dir,
		closed:          make(chan struct{}),
		Order:           make(map[string]*TSSPFiles, defaultCap),
		OutOfOrder:      make(map[string]*TSSPFiles, defaultCap),
		tier:            tier,
		inCompact:       make(map[string]struct{}, defaultCap),
		inMerge:         NewInMerge(),
		mergeEn:         1,
		compactionEn:    1,
		sequencer:       NewSequencer(),
		compactRecovery: compactRecovery,
		Conf:            config,
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

func (m *MmsTables) Tier() uint64 {
	tier := *(m.tier)
	return tier
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

type loadFilesInfo struct {
	order     []string
	inorder   []string
	totalSize int64
}

func (m *MmsTables) files(dir, mn string, filesInfo *loadFilesInfo, fCount *int, isOrder bool) error {
	nameDirs, err := fileops.ReadDir(dir)
	if err != nil {
		log.Error("read measurement dir fail", zap.String("path", dir), zap.Error(err))
		return err
	}

	count := 0
	for i := range nameDirs {
		d := nameDirs[i]
		if d.IsDir() && d.Name() == unorderedDir {
			tmpDir := filepath.Join(dir, unorderedDir)
			if err = m.files(tmpDir, mn, filesInfo, fCount, false); err != nil {
				return err
			}
			continue
		}

		name := d.Name()
		if !validFileName(name) {
			fName := filepath.Join(dir, name)
			lock := fileops.FileLockOption("")
			_ = fileops.Remove(fName, lock)
			continue
		}

		if name[len(name)-tmpSuffixNameLen:] == tmpTsspFileSuffix {
			fName := filepath.Join(dir, name)
			lock := fileops.FileLockOption("")
			_ = fileops.Remove(fName, lock)
			continue
		}

		fName := filepath.Join(dir, name)
		if isOrder {
			filesInfo.order = append(filesInfo.order, fName)
		} else {
			filesInfo.inorder = append(filesInfo.inorder, fName)
		}

		fi, err := fileops.Stat(fName)
		if err != nil {
			return err
		}
		filesInfo.totalSize += fi.Size()

		count++
	}

	*fCount += count

	return nil
}

func (m *MmsTables) openMmsFiles(files []string, isOrder bool, errs chan error, lock *sync.Mutex) uint64 {
	maxSeq := uint64(0)
	var fileName TSSPFileName
	for i := range files {
		fPath := files[i]
		name := filepath.Base(fPath)
		_ = fileName.ParseFileName(name)
		if maxSeq < fileName.seq {
			maxSeq = fileName.seq
		}
		select {
		case fileLoadLimiter <- struct{}{}:
			go func(fName string) {
				defer func() {
					fileLoadLimiter.Release()
				}()

				cacheData := m.cacheFileData()
				f, err := OpenTSSPFile(fName, isOrder, cacheData)
				if err != nil || f == nil {
					errs <- err
					return
				}
				lock.Lock()
				m.addTSSPFile(isOrder, f)
				lock.Unlock()
				errs <- nil
			}(fPath)
		case <-m.closed:
			return 0
		}
	}

	return maxSeq
}

func (m *MmsTables) cacheFileData() bool {
	conf := CacheMetaInMemory() || CacheDataInMemory()
	if *m.tier == meta.Hot {
		return conf
	} else if *m.tier == meta.Warm {
		return false
	}

	return conf
}

func (m *MmsTables) openFiles(mnFiles []*loadFilesInfo, fileCount int) error {
	start := time.Now()
	log.Info("start open files in")

	errs := make(chan error, fileCount)
	defer close(errs)

	openLock := &sync.Mutex{}
	for _, info := range mnFiles {
		maxSeq := m.openMmsFiles(info.order, true, errs, openLock)
		if m.fileSeq < maxSeq {
			m.fileSeq = maxSeq
		}

		maxSeq = m.openMmsFiles(info.inorder, false, errs, openLock)
		if m.fileSeq < maxSeq {
			m.fileSeq = maxSeq
		}
	}

	var errCnt int
	for i := 0; i < fileCount; i++ {
		err := <-errs
		if err != nil {
			log.Error("open file fail", zap.Error(err))
			errCnt++
		}
	}
	// if not all file open success, just log error and continue
	if errCnt > 0 {
		errInfo := errno.NewError(errno.NotAllTsspFileOpenSuccess, fileCount, errCnt)
		log.Error("", zap.Error(errInfo))
	}

	m.sortTSSPFiles()

	d := time.Since(start)
	log.Info("open file done", zap.Int("files", fileCount), zap.Duration("time used", d))

	return nil
}

func (m *MmsTables) loadIdTimes(filesN int) (int64, int64, error) {
	log.Info("start load id time")
	start := time.Now()
	totalRows := int64(0)
	maxTime := int64(math.MinInt64)
	var tmLock sync.Mutex

	errs := make(chan error, filesN)
	defer close(errs)
	n := 0
	for k, v := range m.Order {
		n += v.Len()
		for _, f := range v.files {
			select {
			case fileLoadLimiter <- struct{}{}:
				go func(tblFile TSSPFile, name string) {
					p := GetIDTimePairs(name)
					err := tblFile.LoadIdTimes(p)
					if err == nil {
						m.sequencer.BatchUpdateCheckTime(p)
						// update max time
						maxT := int64(math.MinInt64)
						for i := range p.Tms {
							if p.Tms[i] > maxT {
								maxT = p.Tms[i]
							}
						}
						tmLock.Lock()
						if maxT > maxTime {
							maxTime = maxT
						}
						tmLock.Unlock()

						// update rows
						rows := int64(0)
						for i := range p.Rows {
							rows += p.Rows[i]
						}
						atomic.AddInt64(&totalRows, rows)
					}
					PutIDTimePairs(p)

					errs <- err
					fileLoadLimiter.Release()
				}(f, k)
			case <-m.closed:
				return maxTime, totalRows, nil
			}
		}
	}

	for k, v := range m.OutOfOrder {
		n += v.Len()
		for _, f := range v.files {
			select {
			case fileLoadLimiter <- struct{}{}:
				go func(tblFile TSSPFile, name string) {
					p := GetIDTimePairs(name)
					err := tblFile.LoadIdTimes(p)
					if err == nil {
						m.sequencer.BatchUpdateCheckTime(p)

						// update rows
						rows := int64(0)
						for i := range p.Rows {
							rows += p.Rows[i]
						}
						atomic.AddInt64(&totalRows, rows)
					}
					PutIDTimePairs(p)

					errs <- err
					fileLoadLimiter.Release()
				}(f, k)
			case <-m.closed:
				return maxTime, totalRows, nil
			}
		}
	}

	var err error
	for i := 0; i < n; i++ {
		er := <-errs
		if er != nil {
			err = er
			log.Error("load id time fail", zap.Error(err))
		}
	}

	d := time.Since(start)
	log.Info("load id time done", zap.Int("load files", n), zap.Duration("time used", d))
	return maxTime, totalRows, err
}

func (m *MmsTables) loadFiles(filesN int) error {
	log.Info("start load files...", zap.Int("files", filesN))
	start := time.Now()

	allFiles := make([]TSSPFile, 0, filesN)
	for _, v := range m.Order {
		allFiles = append(allFiles, v.files...)
	}

	sort.Slice(allFiles, func(i, j int) bool {
		si := allFiles[i].FileStat()
		sj := allFiles[j].FileStat()

		return si.maxTime > sj.maxTime
	})

	for _, v := range m.OutOfOrder {
		allFiles = append(allFiles, v.files...)
	}
	errs := make(chan error, len(allFiles))
	defer close(errs)
	n := 0

	isHot := *m.tier == meta.Hot
	for _, f := range allFiles {
		isOrder := f.IsOrder()
		loadIntoMem := isHot && isOrder
		size := f.InMemSize()
		if atomic.LoadInt64(&loadSizeLimit) < size {
			loadIntoMem = false
		} else {
			atomic.AddInt64(&loadSizeLimit, -size)
		}

		select {
		case fileLoadLimiter <- struct{}{}:
			n++
			go func(tblFile TSSPFile, toMem bool) {
				var err error
				if toMem {
					err = tblFile.LoadIntoMemory()
				} else {
					err = tblFile.LoadIndex()
				}
				errs <- err
				fileLoadLimiter.Release()
			}(f, loadIntoMem)
		case <-m.closed:
			return nil
		}
	}

	log.Info("wait for load file done", zap.Int("load", n))

	var errCnt int
	for i := 0; i < n; i++ {
		err := <-errs
		if err != nil {
			log.Error("load file fail", zap.Error(err))
			errCnt++
		}
	}
	if errCnt > 0 {
		errInfo := errno.NewError(errno.NotAllTsspFileLoadSuccess, filesN, errCnt)
		log.Error("", zap.Error(errInfo))
	}

	d := time.Since(start)
	log.Info("load files done", zap.Int64("load size", nodeImmTableSizeUsed), zap.Int("load files", filesN), zap.Duration("time used", d))

	return nil
}

func recoverFile(shardDir string) error {
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
		err := procCompactLog(shardDir, logDir)
		if err != nil {
			if err != ErrDirtyLog {
				return err
			}
		}
	}

	return nil
}

func (m *MmsTables) Open() (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Info("table store open start")
	start := time.Now()

	shardDir := filepath.Dir(m.path)
	if err := recoverFile(shardDir); err != nil {
		errInfo := errno.NewError(errno.RecoverFileFailed, shardDir)
		log.Error("", zap.Error(errInfo))
		return 0, 0, errInfo
	}

	dirs, err := fileops.ReadDir(m.path)
	if err != nil {
		log.Error("read table store dir fail", zap.String("path", m.path), zap.Error(err))
		return 0, 0, err
	}

	mnFiles := make([]*loadFilesInfo, 0, len(dirs))
	count := 0
	totalSize := int64(0)
	for i := range dirs {
		mn := dirs[i].Name()
		if mn == compactLogDir {
			continue
		}
		mmsPath := filepath.Join(m.path, mn)
		fInfo := &loadFilesInfo{}
		if err = m.files(mmsPath, mn, fInfo, &count, true); err != nil {
			return 0, 0, err
		}
		mnFiles = append(mnFiles, fInfo)
		totalSize += fInfo.totalSize
	}

	if err = m.openFiles(mnFiles, count); err != nil {
		return 0, 0, err
	}

	maxTime, totalRows, err := m.loadIdTimes(count)
	if err != nil {
		return 0, 0, err
	}

	if err = m.loadFiles(count); err != nil {
		errInfo := errno.NewError(errno.LoadFilesFailed)
		log.Error("", zap.Error(errInfo))
		return 0, 0, err
	}

	d := time.Since(start)
	log.Info("table store open done", zap.Duration("time used", d))

	return maxTime, totalRows, nil
}

func (m *MmsTables) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}

func (m *MmsTables) Close() error {
	close(m.closed)
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

	return nil
}

func (m *MmsTables) GetFilesRef(measurement string, isOrder bool) []TSSPFile {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	v, ok := mmsTbls[measurement]
	if ok && v != nil {
		v.lock.RLock()
		files := make([]TSSPFile, 0, v.Len())
		for _, f := range v.files {
			f.Ref()
			files = append(files, f)
		}
		v.lock.RUnlock()
		return files
	}
	return nil
}

/*
	Get files by time range and ascending.
	For example, we have files which time range are [1,3],[1,4],[2,5], [3,7]
	While ascending is true, we first compare minTime and then compare maxTime. So we got [1,4], [1,3],[2,5],[3,7].
	Because minTime is equal, if we first init a bigger time range,
	While ascending is false, we first compare maxTime and the compare minTime. So we got [1,3],[1,4],[2,5],[3,7].
*/
func (m *MmsTables) GetFilesRefByAscending(measurement string, isOrder bool, ascending bool, tr record.TimeRange) []TSSPFile {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	v, ok := mmsTbls[measurement]
	if ok && v != nil {
		var filesList *comm.TopNList
		v.lock.RLock()
		vLen := v.Len()
		if ascending {
			filesList = comm.NewTopNList(vLen, compareFile)
		} else {
			filesList = comm.NewTopNList(vLen, compareFileByDescend)
		}

		for _, f := range v.files {
			contains, err := f.ContainsByTime(tr)
			if !contains || err != nil {
				continue
			}
			f.Ref()
			filesList.Insert(f)
		}
		v.lock.RUnlock()
		files := make([]TSSPFile, 0, vLen)
		if filesList.Back() != nil {
			currentNode := filesList.Back()
			for {
				files = append(files, currentNode.Value.(TSSPFile))
				if currentNode.Prev() == nil {
					break
				}
				currentNode = currentNode.Prev()
			}
		}
		return files
	}
	return nil
}

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

func (m *MmsTables) NextSequence() uint64 {
	return atomic.AddUint64(&m.fileSeq, 1)
}

func (m *MmsTables) Sequencer() *Sequencer {
	return m.sequencer
}

func (m *MmsTables) addTSSPFile(isOrder bool, f TSSPFile) {
	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	Name := f.Name()
	v, ok := mmsTbls[Name]
	if !ok || v == nil {
		v = NewTSSPFiles()
		mmsTbls[Name] = v
	}
	v.lock.Lock()
	v.files = append(v.files, f)
	v.lock.Unlock()
}

func (m *MmsTables) sortTSSPFiles() {
	for _, item := range m.Order {
		sort.Sort(item)
	}
	for _, item := range m.OutOfOrder {
		sort.Sort(item)
	}
}

func (m *MmsTables) DropMeasurement(_ context.Context, name string) error {
	var orderWg, inorderWg *sync.WaitGroup
	mstPath := filepath.Join(m.path, name)
	log.Info("start drop measurement...", zap.String("name", name), zap.String("path", mstPath))
	m.mu.RLock()
	order, ok := m.Order[name]
	if ok && order != nil {
		order.StopFiles()
		orderWg = &order.wg
	}

	inorder, ok := m.OutOfOrder[name]
	if ok && inorder != nil {
		inorder.StopFiles()
		inorderWg = &inorder.wg
	}
	m.mu.RUnlock()

	if orderWg != nil {
		orderWg.Wait()
	}
	if inorderWg != nil {
		inorderWg.Wait()
	}

	if order != nil {
		order.lock.Lock()
		err := m.deleteFiles(order.files...)
		order.lock.Unlock()
		if err != nil {
			log.Error("drop order files fail", zap.String("name", name), zap.String("path", mstPath), zap.Error(err))
			return err
		}
	}

	if inorder != nil {
		inorder.lock.Lock()
		err := m.deleteFiles(inorder.files...)
		inorder.lock.Unlock()
		if err != nil {
			log.Error("drop out of order files fail", zap.String("name", name),
				zap.String("path", mstPath), zap.Error(err))
			return err
		}
	}

	m.mu.Lock()
	delete(m.Order, name)
	delete(m.OutOfOrder, name)
	m.mu.Unlock()

	mmsDir := filepath.Join(m.path, name)
	lockFile := fileops.FileLockOption("")
	_ = fileops.RemoveAll(mmsDir, lockFile)
	log.Info("drop measurement done", zap.String("name", name), zap.String("path", mstPath))

	return nil
}

func getImmTableEvictSize() int64 {
	nodeSize := atomic.LoadInt64(&nodeImmTableSizeUsed)
	if nodeSize > nodeImmTableSizeLimit {
		return nodeSize - nodeImmTableSizeLimit
	}
	return 0
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
	m.mu.RLock()
	tables := m.Order
	if !isOrder {
		tables = m.OutOfOrder
	}
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
		stats.IOStat.AddIOSnapshotBytes(f.FileSize())
	}

	fs.lock.Lock()
	fs.files = append(fs.files, files...)
	sort.Sort(fs)
	fs.lock.Unlock()
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

func (m *MmsTables) ReplaceFiles(name string, oldFiles, newFiles []TSSPFile, isOrder bool, log *Log.Logger) (err error) {
	if len(newFiles) == 0 || len(oldFiles) == 0 {
		return nil
	}

	defer func() {
		if e := recover(); e != nil {
			err = errno.NewError(errno.RecoverPanic)
			log.Error("replace file fail", zap.Error(err))
		}
	}()

	var logFile string
	shardDir := filepath.Dir(m.path)
	logFile, err = m.writeCompactedFileInfo(name, oldFiles, newFiles, shardDir, isOrder)
	if err != nil {
		if len(logFile) > 0 {
			lock := fileops.FileLockOption("")
			_ = fileops.Remove(logFile, lock)
		}
		log.Error("write compact log fail", zap.String("name", name), zap.String("dir", shardDir))
		return
	}

	if err := RenameTmpFiles(newFiles); err != nil {
		log.Error("rename new file fail", zap.String("name", name), zap.String("dir", shardDir), zap.Error(err))
		return err
	}

	mmsTables := m.Order
	if !isOrder {
		mmsTables = m.OutOfOrder
	}

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
		if m.isClosed() {
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

	lock := fileops.FileLockOption("")
	if err = fileops.Remove(logFile, lock); err != nil {
		log.Error("remove compact log file error", zap.String("name", name), zap.String("dir", shardDir),
			zap.String("log", logFile), zap.Error(err))
	}

	return
}

func RenameTmpFiles(newFiles []TSSPFile) error {
	for i := range newFiles {
		f := newFiles[i]
		tmpName := f.Path()
		if IsTempleFile(filepath.Base(tmpName)) {
			fname := tmpName[:len(tmpName)-len(tmpTsspFileSuffix)]
			if err := f.Rename(fname); err != nil {
				log.Error("rename file error", zap.String("name", tmpName), zap.Error(err))
				return err
			}
		}
	}

	return nil
}

func (m *MmsTables) FreeAllMemReader() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, v := range m.Order {
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

	for _, v := range m.OutOfOrder {
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

func levelSequenceEqual(level uint16, seq uint64, f TSSPFile) bool {
	lv, n := f.LevelAndSequence()
	return lv == level && seq == n
}

func (m *MmsTables) mmsPlan(name string, files *TSSPFiles, level uint16, minGroupFileN int, plans []*CompactGroup) []*CompactGroup {
	if m.isClosed() || atomic.LoadInt64(&files.closing) > 0 {
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
			idx++
			continue
		}

		seqByte := record.Uint64ToBytes(seq)
		if !seqMap.HasBytes(seqByte) {
			if seqMap.Len() >= minGroupFileN {
				plan := m.genCompactGroup(seqMap, name, level)
				if plan != nil {
					plan.dropping = &files.closing
					plans = append(plans, plan)
				}
				seqMap.Reset()
			}
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

	if seqMap.Len() >= minGroupFileN {
		plan := m.genCompactGroup(seqMap, name, level)
		if plan != nil {
			plan.dropping = &files.closing
			plans = append(plans, plan)
		}
	}

	return plans
}

func (m *MmsTables) LevelPlan(level uint16) []*CompactGroup {
	if !m.CompactionEnabled() {
		return nil
	}

	var plans []*CompactGroup
	minGroupFileN := LeveLMinGroupFiles[level]

	m.mu.RLock()
	for k, v := range m.Order {
		v.lock.RLock()
		if atomic.LoadInt64(&v.closing) > 0 || v.Len() < minGroupFileN {
			v.lock.RUnlock()
			continue
		}
		plans = m.mmsPlan(k, v, level, minGroupFileN, plans)
		v.lock.RUnlock()
	}
	m.mu.RUnlock()

	return plans
}

func (m *MmsTables) File(mstName string, fileName string, isOrder bool) TSSPFile {
	m.mu.RLock()

	mmsTbls := m.Order
	if !isOrder {
		mmsTbls = m.OutOfOrder
	}

	v, ok := mmsTbls[mstName]
	if !ok || v == nil {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	v.lock.RLock()
	for _, f := range v.files {
		if f.Path() == fileName {
			f.Ref()
			v.lock.RUnlock()
			return f
		}
	}
	v.lock.RUnlock()

	return nil
}

var (
	_ TablesStore = (*MmsTables)(nil)
)

type TablesGC interface {
	Add(free bool, files ...TSSPFile)
	GC()
}

type TableStoreGC struct {
	mu          sync.RWMutex
	freeFiles   map[string]TSSPFile
	removeFiles map[string]TSSPFile
}

func NewTableStoreGC() TablesGC {
	return &TableStoreGC{
		freeFiles:   make(map[string]TSSPFile, 8),
		removeFiles: make(map[string]TSSPFile, 8),
	}
}

func (sgc *TableStoreGC) Add(free bool, files ...TSSPFile) {
	sgc.mu.Lock()
	for _, f := range files {
		fName := f.Path()
		if free {
			sgc.freeFiles[fName] = f
		} else {
			delete(sgc.freeFiles, fName)
			sgc.removeFiles[f.Path()] = f
		}
	}
	sgc.mu.Unlock()
}

func (sgc *TableStoreGC) GC() {
	timer := time.NewTicker(time.Millisecond * 200)
	for range timer.C {
		sgc.mu.Lock()
		if len(sgc.freeFiles) == 0 && len(sgc.removeFiles) == 0 {
			sgc.mu.Unlock()
			continue
		}

		for fn, f := range sgc.freeFiles {
			if !f.Inuse() {
				_ = f.FreeMemory(true)
				delete(sgc.freeFiles, fn)
			}
		}

		for fn, f := range sgc.removeFiles {
			if !f.Inuse() {
				err := f.Remove()
				if err != nil {
					log.Error("gc remove file fail", zap.String("file", fn), zap.Error(err))
				} else {
					log.Info("remove file", zap.String("file", fn))
					delete(sgc.removeFiles, fn)
				}
			}
		}
		sgc.mu.Unlock()
	}
	timer.Stop()
}
