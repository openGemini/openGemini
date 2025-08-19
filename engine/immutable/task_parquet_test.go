/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/parquet"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestParquetTaskPrepare(t *testing.T) {
	convey.Convey("test task prepare", t, func() {
		testSchema := map[string]uint8{"test": 1}
		const (
			tsspFile          = "/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/xxxxx.tssp"
			mergedFile        = "/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/out-of-order/00000001-0007-00000001.tssp"
			parquetFile       = "/tsdb/instanceId/parquet/db/rp/mst/dt=2024-05-05/shardId_xxxxx.parquet"
			mergedParquetFile = "/tsdb/instanceId/parquet/db/rp/mst/dt=2024-05-05/shardId_00000001-0007-00100001.parquet"
		)
		task := ParquetTask{plan: TSSP2ParquetPlan{
			Mst:    "test",
			Schema: testSchema,
			Files:  []string{tsspFile, mergedFile},
			enable: false,
		}}
		p1 := gomonkey.ApplyFunc(fileops.Mkdir, func(_ string, _ os.FileMode, _ ...fileops.FSOption) error {
			return nil
		})
		defer p1.Reset()
		mapping, err := task.prepare()
		if err != nil {
			t.Fatal("task prepare failed, error:", err.Error())
		}
		convey.So(mapping[tsspFile], convey.ShouldEqual, parquetFile)
		convey.So(mapping[mergedFile], convey.ShouldEqual, mergedParquetFile)
	})
}

func TestTaskExport2TSSPFile(t *testing.T) {
	convey.Convey("test export tsspFile", t, func() {
		f := tsspFile{}
		p00 := gomonkey.ApplyMethod(&f, "Path", func(_ *tsspFile) string {
			return "test"
		})
		defer p00.Reset()

		fakeIterator := &ChunkIterator{}
		p0 := gomonkey.ApplyFunc(NewFileIterator, func(r TSSPFile, log *Log.Logger) *FileIterator {
			return &FileIterator{}
		})
		defer p0.Reset()

		p1 := gomonkey.ApplyFunc(NewChunkIterator, func() *ChunkIterator {
			return fakeIterator
		})
		defer p1.Reset()
		outputs := []gomonkey.OutputCell{
			{Values: gomonkey.Params{true}},
			{Values: gomonkey.Params{true}},
			{Values: gomonkey.Params{false}},
		}
		p2 := gomonkey.ApplyMethodSeq(fakeIterator, "Next", outputs)
		defer p2.Reset()
		task := &ParquetTask{}

		convey.Convey("get series id failed", func() {
			err := task.export2TSSPFile(&f, &parquet.Writer{})
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("write ok", func() {
			fakeIterator.id = 100
			p3 := gomonkey.ApplyMethod(task, "GetSeries", func(_ *ParquetTask, _ uint64) (map[string]string, error) {
				return map[string]string{"t1": "1"}, nil
			})
			defer p3.Reset()

			fakeWriter := &parquet.Writer{}
			p4 := gomonkey.ApplyMethod(fakeWriter, "WriteRecord", func(_ *parquet.Writer, _ map[string]string, __ *record.Record) error {
				return nil
			})
			defer p4.Reset()

			p5 := gomonkey.ApplyFunc(record.CheckRecord, func(rec *record.Record) {
				return
			})
			defer p5.Reset()

			err := task.export2TSSPFile(&f, &parquet.Writer{})
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestParquetTaskGetSeries(t *testing.T) {
	convey.Convey("test get series", t, func() {
		fakeTsi := &MockIndexMergeSet{}
		task := ParquetTask{}
		task.mergeSet = fakeTsi

		fakeTsi.GetSeriesFn = func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
			callback(&influx.SeriesKey{Measurement: []byte("test"), TagSet: []influx.TagKV{{Key: []byte("t1"), Value: []byte("1")}}})
			return nil
		}

		series, err := task.GetSeries(0)
		convey.So(err, convey.ShouldBeNil)
		val, _ := series["t1"]
		convey.So(val, convey.ShouldEqual, "1")
	})
}

func TestParquetTaskProcess(t *testing.T) {
	convey.Convey("test task process", t, func() {
		task := ParquetTask{}
		convey.Convey("open failed", func() {
			err := task.process("", "", "")
			convey.So(err, convey.ShouldNotBeNil)
		})

		fakeFile := &tsspFile{}
		p0 := gomonkey.ApplyFunc(OpenTSSPFile, func(name string, lockPath *string, isOrder bool) (TSSPFile, error) {
			return fakeFile, nil
		})
		defer p0.Reset()

		p00 := gomonkey.ApplyMethod(fakeFile, "Close", func() error {
			return nil
		})
		defer p00.Reset()

		convey.Convey("test get tag keys failed", func() {
			p := gomonkey.ApplyMethod(&task, "GetTagKeys", func(_ *ParquetTask) (map[string]struct{}, error) {
				return map[string]struct{}{"test": {}}, errors.New("failed")
			})
			defer p.Reset()
			err := task.process("", "", "")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test export2TSSPFile failed", func() {
			const errorMsg = "export failed"
			p := gomonkey.ApplyMethod(&task, "GetTagKeys", func(_ *ParquetTask) (map[string]struct{}, error) {
				ans := make(map[string]struct{}, 1)
				ans["test"] = struct{}{}
				return ans, nil
			})
			defer p.Reset()

			p1 := gomonkey.ApplyPrivateMethod(&task, "export2TSSPFile", func(_ *ParquetTask, _ TSSPFile, _ *parquet.Writer) error {
				return errors.New(errorMsg)
			})
			defer p1.Reset()
			task.plan.Schema = make(map[string]uint8, 1)
			err := task.process("", "", "")
			convey.So(err.Error(), convey.ShouldContainSubstring, errorMsg)
		})
	})
}

func TestParquetGetTagKeysAndSeries(t *testing.T) {
	convey.Convey("test get tag key", t, func() {
		var mockError error
		task := ParquetTask{}
		task.mergeSet = &MockIndexMergeSet{SearchSeriesKeysFn: func(series [][]byte, name []byte, condition influxql.Expr) ([][]byte, error) {
			return [][]byte{[]byte("test,vin=1")}, mockError
		},
			GetSeriesFn: func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
				callback(&influx.SeriesKey{TagSet: []influx.TagKV{{Key: []byte("vin"), Value: []byte("test")}}})
				return mockError
			},
		}

		convey.Convey("process ok", func() {
			ans, _ := task.GetTagKeys()
			_, ok := ans["vin"]
			convey.So(ok, convey.ShouldBeTrue)

			ans2, _ := task.GetSeries(0)
			_, ok = ans2["vin"]
			convey.So(ok, convey.ShouldBeTrue)
		})

		convey.Convey("process failed", func() {
			mockError = fmt.Errorf("foo")

			_, err := task.GetTagKeys()
			convey.So(err, convey.ShouldNotBeNil)

			_, err = task.GetSeries(0)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestMarkParquetTaskDone(t *testing.T) {
	convey.Convey("test mark parquet done", t, func() {
		paths := []string{"/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/00000001-0000-00000000.tssp",
			"/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/out-of-order/00000001-0000-00000000.tssp"}
		p := gomonkey.ApplyFunc(fileops.Create, func(name string, opt ...fileops.FSOption) (fileops.File, error) {
			return nil, nil
		})
		defer p.Reset()

		for _, path := range paths {
			err := markParquetTaskDone(path, 1)
			convey.So(err, convey.ShouldBeNil)
		}
	})
}

func TestParquetTestExecute(t *testing.T) {
	convey.Convey("test task execute", t, func() {
		task := &ParquetTask{}

		p := gomonkey.ApplyFunc(fileops.RemoveAll, func(_ string, opt ...fileops.FSOption) error {
			return nil
		})
		defer p.Reset()

		convey.Convey("prepare failed", func() {
			p1 := gomonkey.ApplyPrivateMethod(task, "prepare", func(_ *ParquetTask) (map[string]string, error) {
				return map[string]string{}, errors.New("failed")
			})
			defer p1.Reset()
			convey.So(func() { task.Execute() }, convey.ShouldNotPanic)
		})

		convey.Convey("process failed ", func() {
			p2 := gomonkey.ApplyPrivateMethod(task, "prepare", func(_ *ParquetTask) (map[string]string, error) {
				return map[string]string{"./1.tssp": "./1.parquet"}, nil
			})
			defer p2.Reset()
			p3 := gomonkey.ApplyPrivateMethod(task, "process", func(_ *ParquetTask, _, _, _ string) error {
				return errors.New("failed")
			})
			defer p3.Reset()
			convey.So(func() { task.Execute() }, convey.ShouldNotPanic)
		})
	})
}

func TestCSTSSP2Parquet(t *testing.T) {
	dir := t.TempDir()
	defer csParquetConfig(dir)()

	lock := ""
	mg := NewCSParquetManager()
	mg.Run()
	file := buildColumnStoreTsspFile(t, 1000)
	tssp, err := OpenTSSPFile(file, &lock, true)
	require.NoError(t, err)
	defer util.MustClose(tssp)

	mg.Convert([]TSSPFile{tssp}, "db0", "default", "foo_0000")
	ok, err := listeningFileExists(config.GetStoreConfig().ParquetTask.GetOutputDir()+"/db0/default/foo", parquetSuffix, 1000)

	mg.Stop()
	mg.Wait()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRecoverCSTSSP2Parquet(t *testing.T) {
	dir := t.TempDir()
	defer csParquetConfig(dir)()

	lock := ""
	conf := config.GetStoreConfig().ParquetTask
	mg := NewCSParquetManager()
	mg.Run()
	file := buildColumnStoreTsspFile(t, 50000)
	tssp, err := OpenTSSPFile(file, &lock, true)
	require.NoError(t, err)
	defer util.MustClose(tssp)

	mg.Convert([]TSSPFile{tssp}, "db0", "default", "foo_0000")

	// stop now, file conversion is not complete
	mg.Stop()
	mg.Stop()
	mg.Wait()

	// stopped. Invalid operation
	mg.Convert([]TSSPFile{tssp}, "db0", "default", "foo_0000")

	// add an invalid reliability log
	err = os.WriteFile(conf.GetReliabilityLogDir()+"/111.log", make([]byte, 10), 0600)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(conf.GetReliabilityLogDir()+"/invalid", 0700))

	mg.Run()
	ok, err := listeningFileExists(conf.OutputDir+"/db0/default/foo", parquetSuffix, 1000)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestExecuteCSParquetTask(t *testing.T) {
	dir := t.TempDir()
	defer csParquetConfig(dir)()

	conf := config.GetStoreConfig().ParquetTask
	file := buildColumnStoreTsspFile(t, 100000)

	plan := &CSParquetPlan{
		Mst:      "foo_0000",
		Id:       100,
		DstFile:  conf.GetOutputDir() + "/10000.parquet",
		TSSPFile: file,
	}
	task := &CSParquetTask{plan: plan}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.Execute()
	}()
	time.Sleep(time.Millisecond)
	task.Stop()
	wg.Wait()
	ok, err := listeningFileExists(conf.GetOutputDir(), parquetSuffix, 1)
	require.NoError(t, err)
	require.False(t, ok)
}

func csParquetConfig(dir string) func() {
	conf := config.GetStoreConfig().ParquetTask
	conf.Enabled = true
	conf.OutputDir = dir + "/parquet"
	conf.ReliabilityLogDir = dir + "/parquet_log"
	_ = os.MkdirAll(conf.OutputDir, 0700)
	_ = os.MkdirAll(conf.ReliabilityLogDir, 0700)
	return func() {
		conf.Enabled = false
	}
}

func listeningFileExists(dir string, suffix string, n int) (bool, error) {
	for i := 0; i < n; i++ {
		glob, err := filepath.Glob(dir + "/*")
		if err != nil {
			return false, err
		}

		if len(glob) > 0 && strings.HasSuffix(glob[0], suffix) {
			return true, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false, nil
}

func buildColumnStoreTsspFile(t *testing.T, recRows int) string {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 100
	conf.FragmentsNumPerFlush = 1
	tier := uint64(util.Hot)
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetDbRp("db0", "rp0")
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "field3_bool", "field2_int"}
	sortKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int"}
	sort := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "time", "sortKey_int1", "field3_bool", "field2_int"}

	defer clearMstInfo()
	_, pkSchema, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas) *record.Record {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.WritePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := 1
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)

	needMerge := false
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		msb.NewPKIndexWriter()
		oldRec = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema)
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			t.Fatal(err)
		}
		store.AddTSSPFiles(msb.Name(), false, msb.Files...)
		for _, v := range data {
			recs = append(recs, v)
		}
		setPKInfo(msb)
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	return store.CSFiles["mst"].files[0].Path()
}
