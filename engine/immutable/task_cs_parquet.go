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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/parquet"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const maxRetryTime = 3

var csParquetManager = &CSParquetManager{stopped: true}

func NewCSParquetManager() *CSParquetManager {
	return csParquetManager
}

type CSParquetManager struct {
	id      uint64
	logDir  string
	dataDir string
	stopped bool
	ts      *scheduler.TaskScheduler
}

func (m *CSParquetManager) Run() {
	conf := config.GetStoreConfig().ParquetTask
	if !conf.Enabled {
		return
	}

	m.stopped = false
	m.logDir = conf.GetReliabilityLogDir()
	m.dataDir = conf.GetOutputDir()
	m.id = uint64(time.Now().UnixNano())
	m.ts = scheduler.NewTaskScheduler(func(chan struct{}, func()) {}, compLimiter)
	m.Recover()
}

func (m *CSParquetManager) Stop() {
	if m.stopped {
		return
	}

	m.stopped = true
	m.ts.CloseAll()
	m.ts.Wait()
}

func (m *CSParquetManager) Wait() {
	m.ts.Wait()
}

func (m *CSParquetManager) Recover() {
	dirs, err := fileops.ReadDir(m.logDir)
	if err != nil {
		logger.GetLogger().Error("recover failed", zap.Error(err), zap.String("path", m.logDir))
		return
	}

	for i := range dirs {
		if dirs[i].IsDir() {
			continue
		}
		file := filepath.Join(m.logDir, dirs[i].Name())
		plan := &CSParquetPlan{}
		plan.SetLogFile(file)
		err = ReadReliabilityLog(file, plan)
		if err == nil {
			m.execute(plan)
			continue
		}

		util.MustRun(func() error {
			return fileops.RemoveAll(file)
		})
		logger.GetLogger().Error("failed to read reliability log", zap.Error(err), zap.String("file", file))
	}
}

func (m *CSParquetManager) Convert(files []TSSPFile, db string, rp string, mst string) {
	if m.stopped {
		return
	}

	for _, f := range files {
		err := m.convert(f.Path(), db, rp, mst)
		if err != nil {
			logger.GetLogger().Error("failed to create parquet plan",
				zap.String("file", f.Path()), zap.Error(err))
		}
	}
}

func (m *CSParquetManager) convert(file string, db string, rp string, mst string) error {
	if m.stopped {
		return nil
	}
	plan := &CSParquetPlan{
		Mst:      mst,
		Id:       atomic.AddUint64(&m.id, 1),
		TSSPFile: file,
	}

	dstPath := path.Join(m.dataDir, db, rp, mst[:len(mst)-util.MeasurementVersionLength])
	util.MustRun(func() error {
		return os.MkdirAll(dstPath, 0700)
	})
	plan.DstFile = path.Join(dstPath, fmt.Sprintf("%d%s", plan.Id, parquetSuffix))

	reliabilityLog, err := SaveReliabilityLog(plan, m.logDir, "", func() string {
		return fmt.Sprintf("%d.log", plan.Id)
	})
	if err == nil {
		plan.SetLogFile(reliabilityLog)
		m.execute(plan)
	}
	return err
}

func (m *CSParquetManager) execute(plan *CSParquetPlan) {
	task := &CSParquetTask{plan: plan}
	task.LockFiles()
	task.OnFinish(func() {
		task.UnLockFiles()
	})

	m.ts.Execute(task, make(chan struct{}), true)
}

type CSParquetTask struct {
	scheduler.BaseTask
	plan    *CSParquetPlan
	stopped bool
}

func (t *CSParquetTask) LockFiles() {
	AddTSSP2ParquetProcess(t.plan.TSSPFile)
}

func (t *CSParquetTask) UnLockFiles() {
	DelTSSP2ParquetProcess(t.plan.TSSPFile)
}

func (t *CSParquetTask) Stop() {
	t.stopped = true
}

func (t *CSParquetTask) Execute() {
	for i := 0; i < maxRetryTime; i++ {
		err := t.execute()
		if err == nil {
			t.plan.removeLog()
			return
		}

		if err == io.EOF {
			return
		}

		logger.GetLogger().Error("failed to convert to parquet",
			zap.Error(err),
			zap.String("file", t.plan.TSSPFile))
		time.Sleep(time.Second)
	}
}

func (t *CSParquetTask) execute() (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	plan := t.plan
	if err = plan.BeforeRun(); err != nil {
		return
	}
	defer util.MustClose(plan.reader)

	pw, err := parquet.NewWriter(plan.DstFile, "", plan.buildParquetSchema())
	if err != nil {
		return err
	}

	err = plan.IterRecord(func(r *record.Record) error {
		if t.stopped {
			return io.EOF
		}

		return pw.WriteRecord(nil, r)
	})

	if err == nil {
		err = pw.WriteStop()
	}
	pw.Close()

	return err
}

type CSParquetPlan struct {
	Mst      string
	Id       uint64
	DstFile  string
	TSSPFile string

	log    string
	reader TSSPFile
	cm     *ChunkMeta
}

func (p *CSParquetPlan) SetLogFile(file string) {
	p.log = file
}

func (p *CSParquetPlan) BeforeRun() error {
	lock := ""
	f, err := OpenTSSPFile(p.TSSPFile, &lock, true)
	if err != nil {
		return err
	}

	itr := NewFileIterator(f, CLog)
	defer itr.Close()
	if !itr.NextChunkMeta() {
		return itr.err
	}
	p.reader = itr.r
	p.cm = itr.GetCurtChunkMeta()
	return nil
}

func (p *CSParquetPlan) buildParquetSchema() parquet.MetaData {
	md := parquet.MetaData{
		Mst:     p.Mst,
		Schemas: make(map[string]uint8),
	}
	cm := p.cm
	for i := range cm.colMeta {
		md.Schemas[cm.colMeta[i].Name()] = cm.colMeta[i].Type()
	}
	return md
}

func (p *CSParquetPlan) IterRecord(handler func(*record.Record) error) error {
	cm := p.cm

	rec := &record.Record{
		Schema:  make(record.Schemas, len(cm.colMeta)),
		ColVals: make([]record.ColVal, len(cm.colMeta)),
	}
	for i := range cm.colMeta {
		rec.Schema[i].Name = cm.colMeta[i].Name()
		rec.Schema[i].Type = int(cm.colMeta[i].Type())
	}

	ctx := NewReadContext(true)
	var err error

	for i := range cm.timeRange {
		rec.InitColVal(0, rec.ColNums())
		rec, err = p.reader.ReadAt(cm, i, rec, ctx, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			return err
		}
		err = handler(rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *CSParquetPlan) removeLog() {
	util.MustRun(func() error {
		return fileops.RemoveAll(p.log)
	})
}
