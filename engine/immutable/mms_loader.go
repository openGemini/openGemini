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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type fileLoader struct {
	mst      *MmsTables
	fileName TSSPFileName

	mu sync.Mutex
	wg sync.WaitGroup

	ctx *fileLoadContext
	lg  *zap.Logger

	total             int
	maxRowsPerSegment int
	pkFiles           map[string]struct{}
}

func newFileLoader(mst *MmsTables, ctx *fileLoadContext) *fileLoader {
	return &fileLoader{
		mst:      mst,
		fileName: TSSPFileName{},
		ctx:      ctx,
		lg:       logger.GetLogger(),
		pkFiles:  make(map[string]struct{}),
	}
}

func (fl *fileLoader) Wait() {
	fl.wg.Wait()
}

func (fl *fileLoader) Load(dir, mst string, isOrder bool) {
	fullDirs, err := fileops.ReadDir(dir)
	if err != nil {
		fl.lg.Error("read measurement dir fail", zap.String("path", dir), zap.Error(err))
		fl.ctx.setError(err)
		return
	}
	if len(fullDirs) == 0 {
		return
	}
	dirDepth := DirDepth(fullDirs[0].Name())
	var nameDirs []fs.FileInfo
	for _, dir := range fullDirs {
		if dirDepth != 1 && DirDepth(dir.Name()) != dirDepth+1 {
			continue
		}
		if filepath.Base(filepath.Clean(dir.Name())) == config.IndexFileDirectory {
			continue
		}
		nameDirs = append(nameDirs, dir)
	}

	// load primary key files
	for i := range nameDirs {
		item := nameDirs[i]
		if !item.IsDir() && filepath.Ext(item.Name()) == colstore.IndexFileSuffix {
			fl.pkFiles[filepath.Join(dir, item.Name())] = struct{}{}
		}
	}

	for i := range nameDirs {
		item := nameDirs[i]
		itemName := filepath.Base(filepath.Clean(item.Name()))
		if item.IsDir() {
			if !isOrder || itemName != unorderedDir {
				// Skip invalid directories
				continue
			}

			fl.Load(filepath.Join(dir, unorderedDir), mst, false)
			continue
		}

		switch filepath.Ext(itemName) {
		case tsspFileSuffix:
			fl.loadTsspFile(filepath.Join(dir, itemName), mst, isOrder, false)
		default:
			fl.removeTmpFile(filepath.Join(dir, itemName)) // skip invalid file, remove if it is a temp file
		}

		fl.total++
	}
}

func (fl *fileLoader) LoadRemote(dir, mst string, obsOpt *obs.ObsOptions, reload bool) {
	if obsOpt == nil {
		return
	}
	remotePath := fileops.GetRemoteDataPath(obsOpt, dir)
	remotePrefixPath := fileops.GetRemotePrefixPath(obsOpt)
	nameDirs, err := fileops.ReadDir(remotePath)
	if err != nil {
		fl.lg.Error("read measurement dir fail", zap.String("path", dir), zap.Error(err))
		fl.ctx.setError(err)
		return
	}
	fl.loadDirs(nameDirs, mst, remotePrefixPath, reload)
}

func (fl *fileLoader) loadDirs(nameDirs []os.FileInfo, mst, remotePrefixPath string, reload bool) {
	for i := range nameDirs {
		item := nameDirs[i]
		switch filepath.Ext(item.Name()) {
		case obs.ObsFileSuffix:
			fl.loadTsspFile(fmt.Sprintf("%s/%s", remotePrefixPath, item.Name()), mst, remoteDirIsOrder(item.Name()), reload)
		default:
			fl.removeTmpFile(fmt.Sprintf("%s/%s", remotePrefixPath, item.Name())) // skip invalid file, remove if it is a temp file
		}
		fl.total++
	}
}

func (fl *fileLoader) ReloadFiles(maxRetries int) {
	for retry := 0; retry <= maxRetries; retry++ {
		files := fl.ctx.reloadFiles
		if len(files) == 0 {
			return
		}
		fl.lg.Warn("reload failed file", zap.Int("retrys", retry))
		fl.ctx.resetForReload()
		for mst, reloadFiles := range files {
			for i := 0; i < len(reloadFiles); i++ {
				if retry != maxRetries {
					fl.loadTsspFile(reloadFiles[i].FilePath(), mst, reloadFiles[i].Order(), true)
				} else {
					fl.lg.Error("reload file failed", zap.String("measurement", mst), zap.String("file", reloadFiles[i].FilePath()))
					reloadFile := &tsspInfo{
						order: reloadFiles[i].Order(),
						file:  reloadFiles[i].FilePath(),
					}
					_ = reloadFile.name.ParseFileName(reloadFile.FilePath())
					fl.mst.ImmTable.addUnloadFile(fl.mst, reloadFile.Order(), reloadFile, mst)
				}
			}
		}
		fl.Wait()
		if len(fl.ctx.reloadFiles) != 0 && retry != maxRetries {
			time.Sleep(100 * time.Millisecond * (1 << retry))
		}
	}
}

func (fl *fileLoader) ReloadSpecifiedFiles(tsspFiles *TSSPFiles) {
	maxRetries := 1
	for retry := 0; retry <= maxRetries; retry++ {
		files := fl.ctx.reloadFiles
		if len(files) == 0 {
			fl.lg.Info("retry to reload file successully", zap.Int("retrys", retry))
			return
		}
		fl.ctx.resetForReload()
		for mst, reloadFiles := range files {
			for i := 0; i < len(reloadFiles); i++ {
				if retry != maxRetries {
					fl.serialLoadTsspFile(tsspFiles, reloadFiles[i].FilePath(), mst, reloadFiles[i].Order())
				} else {
					fl.lg.Error("reload file failed", zap.String("measurement", mst), zap.String("file", reloadFiles[i].FilePath()))
					reloadFile := &tsspInfo{
						order: reloadFiles[i].Order(),
						file:  reloadFiles[i].FilePath(),
					}
					_ = reloadFile.name.ParseFileName(reloadFile.FilePath())
					tsspFiles.AppendReloadFiles(reloadFile)
				}
			}
		}
	}
}

func ReloadSpecifiedFiles(m *MmsTables, mst string, tsspFiles *TSSPFiles) {
	loader := newFileLoader(m, NewFileLoadContext())
	loader.ctx.reloadFiles[mst] = tsspFiles.unloadFiles
	tsspFiles.unloadFiles = make([]TSSPInfo, 0) // reinit
	loader.ReloadSpecifiedFiles(tsspFiles)
}

func remoteDirIsOrder(path string) bool {
	lastIndex := strings.LastIndex(path, "/")
	if lastIndex == -1 {
		return false
	}
	tmpPath := path[:lastIndex]
	return !strings.HasSuffix(tmpPath, unorderedDir)
}

func (fl *fileLoader) removeTmpFile(file string) {
	if IsTempleFile(file) {
		fl.removeFile(file)
		return
	}
}

func (fl *fileLoader) loadTsspFile(file, mst string, isOrder, isReload bool) {
	if !isReload {
		if err := fl.fileName.ParseFileName(file); err != nil {
			fl.lg.Error("failed to parse file name",
				zap.Error(err), zap.String("file", file))
			fl.removeFile(file)
			return
		}

		if !fl.verifySeq(fl.fileName.seq, mst, isOrder) {
			return
		}
	}

	select {
	case fileLoadLimiter <- struct{}{}:
		fl.wg.Add(1)
		go func() {
			defer func() {
				fileLoadLimiter.Release()
				fl.wg.Done()
			}()

			fl.openTSSPFile(file, mst, isOrder)
		}()
	case <-fl.mst.closed:
		return
	}
}

func (fl *fileLoader) verifySeq(seq uint64, mst string, isOrder bool) bool {
	if seq > fl.mst.fileSeq {
		return true
	}

	files, ok := fl.mst.Order[mst]
	if !isOrder {
		files, ok = fl.mst.OutOfOrder[mst]
	}
	if !ok {
		return true
	}

	files.RLock()
	defer files.RUnlock()

	var maxSeq uint64 = 0
	for i := range files.Files() {
		_, val := files.Files()[i].LevelAndSequence()
		if maxSeq < val {
			maxSeq = val
		}
	}
	return seq > maxSeq
}

func (fl *fileLoader) removeFile(file string) {
	// do not remove tmp file in pre-load phase
	if fl.mst.isPreLoading() {
		return
	}
	lock := fileops.FileLockOption(*fl.mst.lock)
	err := fileops.Remove(file, lock)
	fl.lg.Info("remove file", zap.String("path", file), zap.Error(err))
}

func (fl *fileLoader) openPKIndexFile(file string) *colstore.PKInfo {
	file = BuildPKFilePathFromTSSP(file)
	_, ok := fl.pkFiles[file]
	if !ok {
		// not column store or primary index file not exists
		return nil
	}

	f, err := colstore.NewPrimaryKeyReader(file, fl.mst.lock)
	if err != nil || f == nil {
		fl.lg.Error("open index file failed", zap.Error(err), zap.String("file", file))
		fl.ctx.setError(err)
		return nil
	}

	defer util.MustClose(f)

	rec, tcLocation, err := f.ReadData()
	if err != nil {
		fl.lg.Error("read index file failed", zap.Error(err), zap.String("file", file))
		fl.ctx.setError(err)
		return nil
	}

	var mark fragment.IndexFragment
	if rec.LastSchema().Name == record.FragmentField {
		// fragment stored in the last column of the primary key Record
		fragments := rec.ColVals[rec.Len()-1]
		mark = fragment.NewIndexFragmentVariable(util.Bytes2Uint64Slice(util.Int64Slice2byte(fragments.IntegerValues())))
	} else {
		// compatibility with older versions
		mark = fragment.NewIndexFragmentFixedSize(uint32(rec.RowNums()-1), uint64(fl.maxRowsPerSegment))
	}

	return colstore.NewPKInfo(rec, mark, tcLocation)
}

func (fl *fileLoader) openTSSPFile(file, mst string, isOrder bool) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("failed to open tssp file:", file, "; ", err)
			fl.lg.Error("open tssp file failed", zap.Any("error", err), zap.String("file", file))
		}
	}()

	f, err := OpenTSSPFile(file, fl.mst.lock, isOrder)
	if err != nil || f == nil {
		fl.lg.Error("open tssp file failed", zap.Error(err), zap.String("file", file))
		fl.ctx.setError(err)
		if err != nil && !strings.Contains(err.Error(), "invalid") {
			fl.mu.Lock()
			defer fl.mu.Unlock()
			fl.ctx.appendReloadFile(mst, &tsspInfo{file: file, order: isOrder})
		}
		return
	}

	pkInfo := fl.openPKIndexFile(file)
	f.SetPkInfo(pkInfo)
	fl.addTSSPFile(file, mst, isOrder, f)
}

// addTSSPFile if LoadComponents raises an error, addTSSPFile is not loaded to avoid subsequent errors.
func (fl *fileLoader) addTSSPFile(file, mst string, isOrder bool, f TSSPFile) {
	if err := f.LoadComponents(); err != nil {
		fl.ctx.setError(err)
		fl.lg.Error("LoadComponents failed", zap.Error(err), zap.String("mst", mst), zap.String("file", file))
		return
	}

	func() {
		fl.mu.Lock()
		defer fl.mu.Unlock()
		fl.mst.ImmTable.addTSSPFile(fl.mst, isOrder, f, mst)
	}()

	fl.loadIntoMemory(f)
	fl.ctx.update(f)
}

func (fl *fileLoader) serialLoadTsspFile(files *TSSPFiles, file, mst string, isOrder bool) {
	select {
	case fileLoadLimiter <- struct{}{}:
		defer func() {
			fileLoadLimiter.Release()
		}()
		f, err := OpenTSSPFile(file, fl.mst.lock, isOrder)
		if err != nil || f == nil {
			fl.lg.Error("open tssp file failed", zap.Error(err), zap.String("file", file))
			fl.ctx.setError(err)
			if err != nil && !strings.Contains(err.Error(), "invalid") {
				fl.ctx.appendReloadFile(mst, &tsspInfo{file: file, order: isOrder})
			}
			return
		}
		// append and sort
		files.Lock()
		defer files.Unlock()
		files.Append(f)
		sort.Sort(files)
		// statics
		fl.ctx.setError(f.LoadComponents())
		fl.loadIntoMemory(f)
		fl.ctx.update(f)
	case <-fl.mst.closed:
		return
	}
}

func (fl *fileLoader) loadIntoMemory(f TSSPFile) {
	fl.ctx.setError(f.LoadIntoMemory())
}

type fileLoadContext struct {
	maxSeq   uint64
	firstErr error
	errCount int64
	maxTime  int64
	rowCount int64

	//reloadFiles []TSSPInfo
	reloadFiles map[string][]TSSPInfo
	mu          sync.Mutex
}

func NewFileLoadContext() *fileLoadContext {
	return &fileLoadContext{
		reloadFiles: make(map[string][]TSSPInfo, 0),
	}
}

func (fc *fileLoadContext) resetForReload() {
	fc.firstErr = nil
	fc.errCount = 0
	fc.reloadFiles = make(map[string][]TSSPInfo, 0)
}

func (fc *fileLoadContext) appendReloadFile(mst string, fi TSSPInfo) {
	fc.reloadFiles[mst] = append(fc.reloadFiles[mst], fi)
}

func (fc *fileLoadContext) getMaxTime() int64 {
	return fc.maxTime
}

func (fc *fileLoadContext) getMaxSeq() uint64 {
	return fc.maxSeq
}

func (fc *fileLoadContext) getError() (int64, error) {
	return fc.errCount, fc.firstErr
}

func (fc *fileLoadContext) setError(err error) {
	if err == nil {
		return
	}

	n := atomic.AddInt64(&fc.errCount, 1)
	if n == 1 {
		fc.firstErr = err
	}
}

func (fc *fileLoadContext) addRowCount(i int64) {
	atomic.AddInt64(&fc.rowCount, i)
}

func (fc *fileLoadContext) getRowCount() int64 {
	return fc.rowCount
}

func (fc *fileLoadContext) update(f TSSPFile) {
	_, seq := f.LevelAndSequence()
	_, max, err := f.MinMaxTime()
	fc.setError(err)

	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.maxSeq < seq {
		fc.maxSeq = seq
	}

	if max > fc.maxTime {
		fc.maxTime = max
	}
}

type idTimesLoader struct {
	ctx    *fileLoadContext
	seq    *Sequencer
	signal chan struct{}
	closed bool

	err error
	mu  sync.Mutex
	wg  sync.WaitGroup
}

func newIDTimesLoader(seq *Sequencer) *idTimesLoader {
	return &idTimesLoader{
		ctx:    &fileLoadContext{},
		seq:    seq,
		signal: make(chan struct{}),
	}
}

func (tl *idTimesLoader) Done() <-chan struct{} {
	return tl.signal
}

func (tl *idTimesLoader) setError(err error) {
	if err == nil || tl.closed {
		return
	}
	tl.err = err
	tl.close()
}

func (tl *idTimesLoader) Error() error {
	return tl.err
}

func (tl *idTimesLoader) close() {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if tl.closed {
		return
	}

	close(tl.signal)
	tl.closed = true
}

func (tl *idTimesLoader) Load(path string, order, unordered map[string]*TSSPFiles) {
	logger.GetLogger().Info("start load id time")
	start := time.Now()

	var fileNums = 0
	var loadFiles = func(data map[string]*TSSPFiles) {
		if len(data) == 0 {
			return
		}

		for mst, files := range data {
			fileNums += files.Len()
			tl.loadFromTSSPFiles(mst, files)
		}
	}
	loadFiles(order)
	loadFiles(unordered)

	tl.wg.Wait()
	tl.close()
	d := time.Since(start)
	logger.GetLogger().Info("load id time done",
		zap.String("path", path),
		zap.Int("load files", fileNums),
		zap.Duration("time used", d))
}

func (tl *idTimesLoader) loadFromTSSPFiles(mst string, files *TSSPFiles) {
	files.lock.RLock()
	defer files.lock.RUnlock()

	for _, f := range files.Files() {
		select {
		case fileLoadLimiter <- struct{}{}:
			tl.wg.Add(1)
			go func(file TSSPFile) {
				defer func() {
					tl.wg.Done()
					fileLoadLimiter.Release()
				}()
				tl.loadFromTSSPFile(file, mst, func(p *IdTimePairs) {
					for _, i := range p.Rows {
						tl.ctx.addRowCount(i)
					}
				})
			}(f)
		case <-tl.signal:
			return
		}
	}
}

func (tl *idTimesLoader) loadFromTSSPFile(tblFile TSSPFile, name string, hook func(p *IdTimePairs)) {
	if tl.closed {
		return
	}

	p := GetIDTimePairs(name)

	var err error
	FileOperation(tblFile, func() {
		err = tblFile.LoadIdTimes(p)
	})

	if err != nil {
		logger.NewLogger(errno.ModuleStorageEngine).Error("load id time fail",
			zap.String("measurement", name), zap.Error(err))
		tl.setError(err)
		return
	}

	if hook != nil {
		hook(p)
	}

	start := time.Now()
	tl.seq.BatchUpdateCheckTime(p, true)
	PutIDTimePairs(p)
	log.Info("batch update check time success",
		zap.String("time used", time.Since(start).String()),
		zap.Int("series ids", len(p.Ids)))
}
