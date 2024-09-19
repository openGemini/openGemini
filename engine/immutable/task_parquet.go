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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/parquet"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var parquetFileLock *MeasurementInProcess

func init() {
	DefaultEventBus().Register(EventTypeMergeSelf, &MergeSelfParquetEvent{})
	DefaultEventBus().Register(EventTypeStreamCompact, &StreamCompactParquetEvent{})
	parquetFileLock = NewMeasurementInProcess()
}

func InParquetProcess(files ...string) bool {
	for i := range files {
		if parquetFileLock.Has(files[i]) {
			return true
		}
	}
	return false
}

func delTSSP2ParquetProcess(files ...string) {
	for i := range files {
		parquetFileLock.Del(files[i])
	}
}

func addTSSP2ParquetProcess(files ...string) {
	for i := range files {
		parquetFileLock.Add(files[i])
	}
}

type TSSP2ParquetPlan struct {
	Mst    string
	Schema map[string]uint8
	Files  []string

	enable bool
}

func (p *TSSP2ParquetPlan) Init(mst string, level uint16) {
	p.Mst = mst
	pl := config.GetStoreConfig().TSSPToParquetLevel
	p.enable = pl > 0 && pl == level
	if p.enable {
		p.Schema = make(map[string]uint8)
	}
}

type TSSP2ParquetEvent struct {
	Event
	mu   sync.Mutex
	plan TSSP2ParquetPlan
	task *ParquetTask

	interrupted bool
}

func (e *TSSP2ParquetEvent) Init(mst string, level uint16) {
	e.plan.Init(mst, level)
}

func (e *TSSP2ParquetEvent) OnNewFile(f TSSPFile) {
	e.plan.Files = append(e.plan.Files, f.Path())
}

func (e *TSSP2ParquetEvent) Enable() bool {
	return e.plan.enable
}

func (e *TSSP2ParquetEvent) OnReplaceFile(shardDir string, lockFile string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.interrupted {
		return nil
	}

	logFile, err := SaveReliabilityLog(&e.plan, filepath.Join(shardDir, parquetLogDir), lockFile, GenParquetLogName)
	if err != nil {
		return err
	}

	e.task = &ParquetTask{
		lockFile: lockFile,
		logFile:  logFile,
		plan:     e.plan,
	}

	return nil
}

func (e *TSSP2ParquetEvent) OnInterrupt() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.interrupted = true
	if e.task == nil {
		return
	}

	util.MustRun(func() error {
		lock := fileops.FileLockOption(e.task.lockFile)
		return fileops.RemoveAll(e.task.logFile, lock)
	})
}

func (e *TSSP2ParquetEvent) OnFinish(ctx EventContext) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.task == nil || e.interrupted {
		return
	}

	e.task.Execute(ctx)
	e.task = nil
}

type MergeSelfParquetEvent struct {
	TSSP2ParquetEvent
}

func (e *MergeSelfParquetEvent) Instance() Event {
	return &MergeSelfParquetEvent{}
}

func (e *MergeSelfParquetEvent) OnWriteRecord(rec *record.Record) {
	for i := range rec.Schema {
		e.plan.Schema[rec.Schema[i].Name] = uint8(rec.Schema[i].Type)
	}
}

type StreamCompactParquetEvent struct {
	TSSP2ParquetEvent
}

func (e *StreamCompactParquetEvent) Instance() Event {
	return &StreamCompactParquetEvent{}
}

func (e *StreamCompactParquetEvent) OnWriteChunkMeta(cm *ChunkMeta) {
	for i := range cm.colMeta {
		e.plan.Schema[cm.colMeta[i].Name()] = cm.colMeta[i].Type()
	}
}

type ParquetTask struct {
	scheduler.BaseTask
	plan     TSSP2ParquetPlan
	lockFile string
	logFile  string
}

func (t *ParquetTask) Execute(ctx EventContext) {
	defer util.MustRun(func() error {
		lock := fileops.FileLockOption(t.lockFile)
		return fileops.RemoveAll(t.logFile, lock)
	})
	parquetMappings, err := t.prepare()
	if err != nil {
		log.Error("[ParquetTask] prepare failed", zap.Error(err))
		return
	}
	for tsspFile, parquetFile := range parquetMappings {
		if err := t.process(tsspFile, parquetFile, "", ctx); err != nil {
			log.Error("[ParquetTask] process failed", zap.Error(err))
			return
		}
	}
}

func (t *ParquetTask) prepare() (map[string]string, error) {
	parquetMapping := make(map[string]string, len(t.plan.Files))
	for _, file := range t.plan.Files {
		fname := file
		if IsTempleFile(filepath.Base(file)) {
			fname = file[:len(file)-len(tmpFileSuffix)]
		}
		parquetDir, parquetFile, err := t.prepareDir(fname)
		if err != nil {
			return parquetMapping, err
		}
		lockFile := fileops.FileLockOption("")
		if err := fileops.MkdirAll(parquetDir, 0750, lockFile); err != nil {
			return parquetMapping, err
		}
		parquetMapping[fname] = parquetFile
	}
	return parquetMapping, nil
}

type parquetFileInfo struct {
	instanceId string
	db         string
	rp         string
	shardInfo  string
	mst        string
	dt         string
	fileName   string
	isMerged   bool
}

func transformMergedFileName(mergedFile string) (string, error) {
	// in case of conflicting with ordered file name, we need to transform filename
	// we will modify the first 4 bit of last sequence, just like
	// 00000001-0007-00000000.tssp to 00000001-0007-00100000.tssp
	tmp := TSSPFileName{}
	if err := tmp.ParseFileName(mergedFile); err != nil {
		return "", err
	}
	tmp.merge |= uint16(0x0010)
	return tmp.String() + tsspFileSuffix, nil
}

func initParquetFileInfo(args []string, isMerged bool) (*parquetFileInfo, error) {
	var fileName = args[9]
	var err error
	if isMerged {
		if fileName, err = transformMergedFileName(args[10]); err != nil {
			return nil, nil
		}
	}
	return &parquetFileInfo{
		instanceId: args[1],
		db:         args[3],
		rp:         args[5],
		shardInfo:  args[6],
		mst:        args[8],
		dt:         "dt=",
		fileName:   fileName,
		isMerged:   isMerged,
	}, nil
}

const parquetSuffix = ".parquet"

func (p *parquetFileInfo) genParquetName() {
	tmp := strings.Split(p.shardInfo, "_")
	shardId := tmp[0]
	fileName := strings.ReplaceAll(p.fileName, tsspFileSuffix, parquetSuffix)
	p.fileName = shardId + "_" + fileName
}

func (p *parquetFileInfo) getDateDir() error {
	tmp := strings.Split(p.shardInfo, "_")
	startTime, err := strconv.ParseInt(tmp[1], 10, 64)
	if err != nil {
		return err
	}
	// current  dir timeZone should be CST
	var cstZone = time.FixedZone("CST", 8*3600)
	p.dt += time.Unix(0, startTime).In(cstZone).Format("2006-01-02")
	return nil
}

func (p *parquetFileInfo) fileDirAndFilePath() (dirPath, parquetPath string) {
	var builder strings.Builder
	builder.WriteString("/tsdb/")
	builder.WriteString(p.instanceId)
	builder.WriteString("/")
	builder.WriteString("parquet")
	builder.WriteString("/")
	builder.WriteString(p.db)
	builder.WriteString("/")
	builder.WriteString(p.rp)
	builder.WriteString("/")
	builder.WriteString(p.mst)
	builder.WriteString("/")
	builder.WriteString(p.dt)
	builder.WriteString("/")

	dirPath = builder.String()
	builder.WriteString(p.fileName)
	parquetPath = builder.String()
	return
}

func (p *parquetFileInfo) getFullPath() (string, string, error) {
	p.genParquetName()
	if err := p.getDateDir(); err != nil {
		return "", "", err
	}
	dir, path := p.fileDirAndFilePath()
	return dir, path, nil
}

func (t *ParquetTask) prepareDir(tsspPath string) (parquetDir, parquetPath string, err error) {
	/* transfer tssp file from
	 /tsdb/instanceId/data/db/dbpt/rp/shardId_startTime_endTime_indexId/tssp/mst/xxxxx.tssp
	 /tsdb/instanceId/data/db/dbpt/rp/shardId_startTime_endTime_indexId/tssp/out-of-order/mst/xxxxx.tssp
	to parquet file
	 /tsdb/instanceId/parquet/db/rp/table/dt=2024-06-14/shardId_xxxxx.parquet
	*/
	tmps := strings.Split(tsspPath, `/`)

	//skip empty
	paths := make([]string, 0, len(tmps))
	for _, tmp := range tmps {
		if tmp != "" {
			paths = append(paths, tmp)
		}
	}
	if len(paths) < 10 || len(paths) > 11 {
		return "", "", fmt.Errorf("invalid file path: %s", tsspPath)
	}

	var parquetInfo *parquetFileInfo
	parquetInfo, err = initParquetFileInfo(paths, strings.Contains(tsspPath, unorderedDir))
	if err != nil {
		return
	}
	parquetDir, parquetPath, err = parquetInfo.getFullPath()
	return
}

func (t *ParquetTask) process(tsspFilePath, parquetPath, lockPath string, ctx EventContext) error {
	addTSSP2ParquetProcess(tsspFilePath)
	defer delTSSP2ParquetProcess(tsspFilePath)

	f, err := OpenTSSPFile(tsspFilePath, &lockPath, true, true)
	if err != nil {
		return err
	}
	defer f.Close()

	w, err := parquet.NewWriter(parquetPath, lockPath, parquet.MetaData{Mst: t.plan.Mst, Schemas: t.plan.Schema})
	if err != nil {
		return err
	}
	defer w.Close()

	if err := t.export2TSSPFile(f, w, ctx); err != nil {
		if err != nil {
			return err
		}
	}
	return w.WriteStop()
}

func (t *ParquetTask) GetSeries(sId uint64, ctx EventContext) (string, error) {
	var sb strings.Builder
	if err := ctx.mergeSet.GetSeries(sId, []byte{}, nil, func(key *influx.SeriesKey) {
		for _, tag := range key.TagSet {
			sb.Write(tag.Key)
			sb.Write([]byte("="))
			sb.Write(tag.Value)
			sb.Write([]byte(","))
		}
	}); err != nil {
		return "", err
	}
	return strings.TrimRight(sb.String(), ","), nil
}

func (t *ParquetTask) export2TSSPFile(f TSSPFile, writer *parquet.Writer, ctx EventContext) error {
	fi := NewFileIterator(f, CLog)
	itr := NewChunkIterator(fi)

	for {
		if !itr.Next() {
			break
		}

		sid := itr.GetSeriesID()
		if sid == 0 {
			err := errors.New("series ID is zero")
			log.Error("[ParquetTask] read error", zap.Error(err), zap.String("path", f.Path()))
			return err
		}
		rec := itr.GetRecord()
		record.CheckRecord(rec)

		series, err := t.GetSeries(sid, ctx)
		if err != nil {
			return err
		}

		if err := writer.WriteRecord(series, rec); err != nil {
			return err
		}
	}
	return nil
}

const (
	parquetLogDir = "parquet_log"
)

var parquetLogSeq = uint64(time.Now().UnixNano())

func GenParquetLogName() string {
	return fmt.Sprintf("%d.log", atomic.AddUint64(&parquetLogSeq, 1))
}

func SaveReliabilityLog(data interface{}, dir string, lockFile string, nameGenerator func() string) (string, error) {
	buf, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	lock := fileops.FileLockOption(lockFile)
	if err := fileops.MkdirAll(dir, 0750, lock); err != nil {
		log.Error("mkdir error", zap.String("dir name", dir), zap.Error(err))
		return "", err
	}
	fName := filepath.Join(dir, nameGenerator())

	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(fName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640, lock, pri)
	if err != nil {
		log.Error("create file error", zap.String("name", fName), zap.Error(err))
		return "", err
	}

	s, err := fd.Write(buf)
	if err != nil || s != len(buf) {
		err = fmt.Errorf("write reliability log fail, write %v, size %v, err:%v", s, len(buf), err)
		log.Error("write parquet plan log fail", zap.Int("write", s),
			zap.String("file", fName), zap.Int("size", len(buf)), zap.Error(err))
		panic(err)
	}

	if err = fd.Sync(); err != nil {
		log.Error("sync parquet log file file")
		panic(err)
	}

	return fName, fd.Close()
}

func ReadReliabilityLog(file string, dst interface{}) error {
	_, err := fileops.Stat(file)
	if err != nil {
		log.Error("stat reliability log file fail", zap.String("file", file), zap.Error(err))
		return err
	}

	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	fd, err := fileops.OpenFile(file, os.O_RDONLY, 0640, lock, pri)
	if err != nil {
		log.Error("read reliability log file fail", zap.String("file", file), zap.Error(err))
		return err
	}
	defer util.MustClose(fd)

	buf, err := io.ReadAll(fd)
	if err != nil {
		log.Error("read reliability log file fail", zap.Error(err), zap.String("file", file))
		return err
	}

	if err := json.Unmarshal(buf, dst); err != nil {
		log.Error("unmarshal reliability log fail", zap.String("file", file), zap.Error(err))
		return err
	}

	return nil
}

func ProcParquetLog(logDir string, lockPath *string, ctx EventContext) error {
	dirs, err := fileops.ReadDir(logDir)
	if err != nil {
		log.Error("read compact log dir fail", zap.String("path", logDir), zap.Error(err))
		return err
	}

	for i := range dirs {
		logName := dirs[i].Name()
		logFile := filepath.Join(logDir, logName)

		plan := &TSSP2ParquetPlan{}
		err = ReadReliabilityLog(logFile, plan)
		if err != nil {
			return err
		}

		task := &ParquetTask{
			BaseTask: scheduler.BaseTask{},
			lockFile: *lockPath,
			logFile:  logFile,
			plan:     *plan,
		}
		task.Execute(ctx)
	}
	return nil
}
