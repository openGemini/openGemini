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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type fileLoader struct {
	mst      *MmsTables
	fileName TSSPFileName

	mu sync.Mutex
	wg sync.WaitGroup

	itl *idTimesLoader
	ctx *fileLoadContext
	lg  *zap.Logger

	total int
}

func newFileLoader(mst *MmsTables, ctx *fileLoadContext) *fileLoader {
	return &fileLoader{
		mst:      mst,
		fileName: TSSPFileName{},
		ctx:      ctx,
		lg:       logger.GetLogger(),
		itl:      newIDTimesLoader(mst.sequencer),
	}
}

func (fl *fileLoader) Close() {
	fl.itl.close()
}

func (fl *fileLoader) Wait() {
	fl.wg.Wait()
	fl.Close()
}

func (fl *fileLoader) Load(dir, mst string, isOrder bool) {
	nameDirs, err := fileops.ReadDir(dir)
	if err != nil {
		fl.lg.Error("read measurement dir fail", zap.String("path", dir), zap.Error(err))
		fl.ctx.setError(err)
		return
	}

	for i := range nameDirs {
		item := nameDirs[i]
		if item.IsDir() {
			if !isOrder || item.Name() != unorderedDir {
				// Skip invalid directories
				continue
			}

			fl.Load(filepath.Join(dir, unorderedDir), mst, false)
			continue
		}

		fl.loadFile(filepath.Join(dir, item.Name()), mst, isOrder)
		fl.total++
	}
}

func (fl *fileLoader) loadFile(file, mst string, isOrder bool) {
	if IsTempleFile(file) {
		fl.removeFile(file)
		return
	}

	if err := fl.fileName.ParseFileName(file); err != nil {
		fl.lg.Error("failed to parse file name",
			zap.Error(err), zap.String("file", file))
		fl.removeFile(file)
		return
	}

	if fl.fileName.seq <= fl.mst.fileSeq {
		return
	}

	select {
	case fileLoadLimiter <- struct{}{}:
		fl.wg.Add(1)
		go func() {
			defer func() {
				fileLoadLimiter.Release()
				fl.wg.Done()
			}()

			fl.openFile(file, mst, isOrder)
		}()
	case <-fl.mst.closed:
		return
	}
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

func (fl *fileLoader) openFile(file, mst string, isOrder bool) {
	cacheData := fl.mst.cacheFileData()
	f, err := OpenTSSPFile(file, fl.mst.lock, isOrder, cacheData)
	if err != nil || f == nil {
		fl.lg.Error("open file failed", zap.Error(err), zap.String("file", file))
		fl.ctx.setError(err)
		return
	}

	func() {
		fl.mu.Lock()
		defer fl.mu.Unlock()
		fl.mst.addTSSPFile(isOrder, f, mst)
	}()

	fl.ctx.setError(f.LoadComponents())
	fl.loadIntoMemory(f)
	fl.ctx.update(f)
	fl.itl.loadFromTSSPFile(f, mst, true, nil)
}

func (fl *fileLoader) loadIntoMemory(f TSSPFile) {
	if *fl.mst.tier != util.Hot || !f.IsOrder() {
		return
	}

	size := f.InMemSize()
	if atomic.AddInt64(&loadSizeLimit, -size) < 0 {
		return
	}

	fl.ctx.setError(f.LoadIntoMemory())
}

type fileLoadContext struct {
	maxSeq   uint64
	firstErr error
	errCount int64
	maxTime  int64
	rowCount int64

	mu sync.Mutex
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

		for mst, files := range order {
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
	files.lock.Lock()
	defer files.lock.Unlock()

	for _, f := range files.Files() {
		select {
		case fileLoadLimiter <- struct{}{}:
			tl.wg.Add(1)
			go func(file TSSPFile) {
				defer func() {
					tl.wg.Done()
					fileLoadLimiter.Release()
				}()
				tl.loadFromTSSPFile(file, mst, false, func(p *IdTimePairs) {
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

func (tl *idTimesLoader) loadFromTSSPFile(tblFile TSSPFile, name string, lazyOpen bool, hook func(p *IdTimePairs)) {
	if tl.closed {
		return
	}

	if lazyOpen {
		tl.seq.free()
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
	tl.seq.isLoading = true

	if hook != nil {
		hook(p)
	}
	go tl.seq.BatchUpdateCheckTime(p)
}
