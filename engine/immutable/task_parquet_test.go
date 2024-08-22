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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/parquet"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/smartystreets/goconvey/convey"
)

// skip
/*
func TestParquetTaskPrepare(t *testing.T) {
	testSchema := map[string]uint8{"test": 1}
	const (
		tsspFile          = "/opt/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/xxxxx.tssp"
		mergedFile        = "/opt/tsdb/instanceId/data/db/dbpt/rp/shardId_1714867200000000000_1715040000000000000_872/tssp/mst/out-of-order/00000001-0007-00000001.tssp"
		parquetFile       = "/opt/tsdb/instanceId/db/rp/table/dt=2024-05-05/shardId_xxxxx.parquet"
		mergedParquetFile = "/opt/tsdb/instanceId/db/rp/table/dt=2024-05-05/shardId_00000001-0007-00100001.tssp.parquet"
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
	convey.ShouldContain(mapping[tsspFile], parquetFile)
	convey.ShouldContain(mapping[mergedFile], mergedParquetFile)
}
*/

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
			err := task.export2TSSPFile(&f, &parquet.Writer{}, EventContext{})
			convey.ShouldNotBeNil(err)
		})

		convey.Convey("write ok", func() {
			fakeIterator.id = 100
			p3 := gomonkey.ApplyMethod(task, "GetSeries", func(_ *ParquetTask, _ uint64, _ EventContext) (string, error) {
				return "mst.t1=1", nil
			})
			defer p3.Reset()

			fakeWriter := &parquet.Writer{}
			p4 := gomonkey.ApplyMethod(fakeWriter, "WriteRecord", func(_ *parquet.Writer, _ string, __ *record.Record) error {
				return nil
			})
			defer p4.Reset()

			p5 := gomonkey.ApplyFunc(record.CheckRecord, func(rec *record.Record) {
				return
			})
			defer p5.Reset()

			err := task.export2TSSPFile(&f, &parquet.Writer{}, EventContext{})
			convey.ShouldBeNil(err)
		})
	})
}

func TestParquetTaskGetSeries(t *testing.T) {
	fakeTsi := &MockIndexMergeSet{}
	ctx := EventContext{mergeSet: fakeTsi}

	task := ParquetTask{}

	fakeTsi.GetSeriesFn = func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		callback(&influx.SeriesKey{Measurement: []byte("test"), TagSet: []influx.TagKV{{Key: []byte("t1"), Value: []byte("1")}}})
		return nil
	}

	series, err := task.GetSeries(0, ctx)
	convey.ShouldBeNil(err)
	convey.ShouldContainSubstring(series, "t1=1")
}

func TestParquetTaskProcess(t *testing.T) {
	convey.Convey("test task process", t, func() {
		task := ParquetTask{}
		convey.Convey("open failed", func() {
			err := task.process("", "", "", EventContext{})
			convey.ShouldNotBeNil(err)
		})

		convey.Convey("test export2TSSPFile failed", func() {
			const errorMsg = "export failed"
			p := gomonkey.ApplyPrivateMethod(&task, "export2TSSPFile", func(_ *ParquetTask, _ TSSPFile, _ *parquet.Writer, _ EventContext) error {
				return errors.New(errorMsg)
			})
			defer p.Reset()
			err := task.process("", "", "", EventContext{})
			convey.ShouldContainSubstring(err.Error(), errorMsg)
		})
	})
}
