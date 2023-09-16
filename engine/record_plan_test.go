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

package engine

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	assert1 "github.com/stretchr/testify/assert"
)

func init() {
	initMaxDownSampleParallelism(4)
}

func TestWriteIntoStorageTransformErr(t *testing.T) {
	w := &WriteIntoStorageTransform{}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	opt := &query.ProcessorOptions{
		ChunkSize: 100,
		Interval: hybridqp.Interval{
			Duration: 1,
		},
		Ascending: true,
	}
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
	)
	querySchema := executor.NewQuerySchema(fields, []string{"id", "name"}, opt, nil)
	w.schema = querySchema
	srcRec := record.NewRecord(schema, true)
	srcRec.ColVals[0].AppendIntegers([]int64{1, 2, 3, 4, 5}...)
	srcRec.RecMeta.Times = make([][]int64, 1)
	srcRec.RecMeta.Times[0] = []int64{1, 2, 3, 4, 5}
	srcRec.AppendTime([]int64{6, 11, 17, 32}...)
	w.currRecord = srcRec
	f := MocTsspFile{}
	lock := ""
	tier := uint64(0)
	m := immutable.NewTableStore("/tmp", &lock, &tier, false, nil)
	m.SetImmTableType(config.TSSTORE)
	w.m = m
	w.currStreamWriteFile, _ = immutable.NewWriteScanFile("mst", m, f, schema)
	defer m.Close()
	_ = w.InitFile(executor.NewSeriesRecord(srcRec, 0, f, 0, nil, nil))
	_ = w.InitFile(executor.NewSeriesRecord(srcRec, 0, f, 0, nil, nil))
}

func TestFileSequenceAggregator(t *testing.T) {
	opt := &query.ProcessorOptions{
		ChunkSize: 100,
		Interval: hybridqp.Interval{
			Duration: 1,
		},
		Ascending: true,
	}
	querySchema := executor.NewQuerySchema(createFields(), []string{"id", "value", "alive", "name"}, opt, nil)
	fa := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)

	schema1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec1 := record.NewRecord(schema1, false)
	rec1.ColVals[0].AppendIntegers([]int64{1, 2, 3, 4, 5}...)
	rec1.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema2 := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec2 := record.NewRecord(schema2, false)
	rec2.ColVals[0].AppendFloats([]float64{1.1, 2.2, 3.3, 4.4, 5.5}...)
	rec2.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema3 := record.Schemas{
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec3 := record.NewRecord(schema3, false)
	rec3.ColVals[0].AppendBooleans([]bool{true, true, true, true, true}...)
	rec3.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema4 := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec4 := record.NewRecord(schema4, false)
	rec4.ColVals[0].AppendStrings([]string{"1", "2", "3", "4", "5"}...)
	rec4.AppendTime([]int64{1, 3, 5, 7, 9}...)

	File1 := &MocTsspFile{path: "tmp/mst1"}
	File2 := &MocTsspFile{path: "tmp/mst1"}
	File3 := &MocTsspFile{path: "tmp/mst2"}
	File4 := &MocTsspFile{path: "tmp/mst3"}
	sender := NewMocDataSender([]*record.Record{rec1, rec2, rec3, rec4}, []immutable.TSSPFile{File1, File2, File3, File4}, []uint64{1, 2, 3, 4}, []uint64{5, 6, 7, 8})
	outPort := fa.GetOutputs()[0]
	inPort := fa.GetInputs()[0]
	inPort.(*executor.SeriesRecordPort).ConnectWithoutCache(sender.output)
	checkRecord := func() {
		recPort := executor.NewSeriesRecordPort(nil)
		recPort.ConnectWithoutCache(outPort)
		for {
			select {
			case r, ok := <-recPort.State:
				if !ok {
					return
				}
				times := r.GetRec().Times()
				rowNum := r.GetRec().RowNums()
				for i := 1; i < rowNum; i++ {
					if times[i]-times[i-1] != int64(opt.GetInterval()) {
						t.Errorf("unexpected time to fill")
					}
				}
			}
		}
	}
	go sender.Work()
	go checkRecord()

	ctx := context.Background()
	fa.Work(ctx)
}

func TestFileSequenceAggregator_Empty(t *testing.T) {
	opt := &query.ProcessorOptions{
		ChunkSize: 100,
		Interval: hybridqp.Interval{
			Duration: 1,
		},
		Ascending: true,
	}
	querySchema := executor.NewQuerySchema(createAllFields(), []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11", "field12", "field13"}, opt, nil)
	fa := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)

	schema1 := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec1 := record.NewRecord(schema1, false)
	rec1.ColVals[0].AppendIntegerNulls(5)
	rec1.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema2 := record.Schemas{
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec2 := record.NewRecord(schema2, false)
	rec1.ColVals[0].AppendFloatNulls(5)
	rec2.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema3 := record.Schemas{
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec3 := record.NewRecord(schema3, false)
	rec3.ColVals[0].AppendBooleanNulls(5)
	rec3.AppendTime([]int64{1, 3, 5, 7, 9}...)

	schema4 := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec4 := record.NewRecord(schema4, false)
	rec4.ColVals[0].AppendStringNulls(5)
	rec4.AppendTime([]int64{1, 3, 5, 7, 9}...)

	File1 := &MocTsspFile{path: "tmp/mst1"}
	File2 := &MocTsspFile{path: "tmp/mst1"}
	File3 := &MocTsspFile{path: "tmp/mst2"}
	File4 := &MocTsspFile{path: "tmp/mst3"}
	sender := NewMocDataSender([]*record.Record{rec1, rec2, rec3, rec4}, []immutable.TSSPFile{File1, File2, File3, File4}, []uint64{1, 2, 3, 4}, []uint64{5, 6, 7, 8})
	outPort := fa.GetOutputs()[0]
	inPort := fa.GetInputs()[0]
	inPort.(*executor.SeriesRecordPort).ConnectWithoutCache(sender.output)
	checkRecord := func() {
		recPort := executor.NewSeriesRecordPort(nil)
		recPort.ConnectWithoutCache(outPort)
		for {
			select {
			case r, ok := <-recPort.State:
				if !ok {
					return
				}
				times := r.GetRec().Times()
				rowNum := r.GetRec().RowNums()
				for i := 1; i < rowNum; i++ {
					if times[i]-times[i-1] != int64(opt.GetInterval()) {
						t.Errorf("unexpected time to fill")
					}
				}
			}
		}
	}
	go sender.Work()
	go checkRecord()

	ctx := context.Background()
	fa.Work(ctx)
}

func createAllFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "min",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "min",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "float",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "float",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "float",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "float",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "boolean",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "boolean",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "min",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "boolean",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "first",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

func createFields() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "sum",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int",
						Type: influxql.Integer,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "min",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "float",
						Type: influxql.Float,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "max",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "boolean",
						Type: influxql.Boolean,
					},
				},
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.Call{
				Name: "count",
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
				},
			},
			Alias: "",
		},
	)

	return fields
}

type MocDataSender struct {
	records []*record.Record
	output  *executor.SeriesRecordPort
	newSeqs []uint64
	files   []immutable.TSSPFile
	sids    []uint64
}

func NewMocDataSender(records []*record.Record, files []immutable.TSSPFile, sids []uint64, newSeqs []uint64) *MocDataSender {
	return &MocDataSender{
		records: records,
		output:  executor.NewSeriesRecordPort(nil),
		files:   files,
		newSeqs: newSeqs,
		sids:    sids,
	}
}

func (m *MocDataSender) Work() {
	for i := range m.records {
		sRec := executor.NewSeriesRecord(m.records[i], m.sids[i], m.files[i], m.newSeqs[i], &util.TimeRange{Max: 100}, nil)
		m.output.State <- sRec
	}
	close(m.output.State)
}

func Test_DownSampleRecovery(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	m := mockMetaClient()
	downSampleLog := filepath.Join(sh.dataPath, immutable.DownSampleLogDir)
	err := os.Mkdir(downSampleLog, 0755)
	if err != nil {
		t.Fatal(err)
		return
	}
	file, err := os.Create(filepath.Join(downSampleLog, "1"))
	if err != nil {
		t.Fatal(err)
		return
	}
	defer file.Close()
	assert1.NoError(t, sh.DownSampleRecover(m))
	err = sh.removeFile(filepath.Join(downSampleLog, "2"))
	if err == nil {
		t.Fail()
	}
}

func Test_BuildRecordDag(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2021-01-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			node := executor.NewLogicalTSSPScan(querySchema)
			executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
			executor.RegistryTransformCreator(&executor.LogicalWriteIntoStorage{}, &WriteIntoStorageTransform{})
			readers, _ := sh.GetTSSPFiles(msNames[0], true)
			newSeqs := sh.GetNewFilesSeqs(readers.Files())
			node.SetNewSeqs(newSeqs)
			node.SetFiles(readers)
			node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
			node2.SetMmsTables(sh.GetTableStore().(*immutable.MmsTables))

			sidSequenceReader := NewTsspSequenceReader(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, readers, newSeqs, make(chan struct{}))
			writeIntoStorage := NewWriteIntoStorageTransform(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, immutable.NewTsStoreConfig(), sh.GetTableStore().(*immutable.MmsTables), true)
			fileSequenceAgg := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)
			sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
			fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])

			ctx := context.Background()
			go sidSequenceReader.Work(ctx)
			go fileSequenceAgg.Work(ctx)
			go writeIntoStorage.Work(ctx)
			time.Sleep(time.Second * 2)
			for _, r := range readers.Files() {
				r.Unref()
			}
			if writeIntoStorage.(*WriteIntoStorageTransform).GetRowCount() != 4056 {
				t.Fail()
			}
			var schema record.Schemas
			var rowCount int
			schema = querySchema.BuildDownSampleSchema(true)
			recordPool := record.NewCircularRecordPool(TsspSequencePool, 3, schema, false)
			newFiles := writeIntoStorage.(*WriteIntoStorageTransform).newFiles
			decs := immutable.NewReadContext(true)
			for _, v := range newFiles {
				metaIndex, _ := v.MetaIndexAt(0)
				chunkMeta, _ := v.ReadChunkMetaData(0, metaIndex, nil, fileops.IO_PRIORITY_LOW_READ)
				for _, c := range chunkMeta {
					for s := 0; s < c.SegmentCount(); s++ {
						rec := recordPool.Get()
						rec.Schema = schema
						rec, _ = v.ReadAt(&c, s, rec, decs, fileops.IO_PRIORITY_LOW_READ)
						rowCount += rec.RowNums()
					}
				}
			}
			if rowCount != 2028 {
				t.Fail()
			}
			planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
			node2.ExplainIterms(planWriter)
		})
	}
}

func TestShardDownSamplePolicy(t *testing.T) {
	testDir := t.TempDir()
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	policy1 := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Float),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
			{
				SampleInterval: 10 * time.Hour,
				TimeInterval:   250 * time.Second,
				WaterMark:      10 * time.Hour,
			},
		},
		Duration: 2400 * time.Hour,
	}
	if sh.GetShardDownSamplePolicy(policy1) != nil {
		t.Fatal("expected nil")
	}
	downSampleInorder = true
	if sh.GetShardDownSamplePolicy(policy1) != nil {
		t.Fatal("expected nil")
	}
}

func TestDownSamplesExecutor(t *testing.T) {
	info := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	e := &Engine{
		DownSamplePolicies: map[string]*meta.StoreDownSamplePolicy{
			"db.p1": {
				Alive: true,
				Info:  info,
			},
			"db.p2": {
				Alive: true,
				Info:  info,
			},
		},
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"db": {1: &DBPTInfo{
				shards: map[uint64]Shard{1: &shard{
					opened:  true,
					ident:   &meta.ShardIdentifier{Policy: "p1"},
					storage: &tsstoreImpl{},
					log:     logger.NewLogger(errno.ModuleUnknown),
				}},
			}},
		},
		log: logger.NewLogger(errno.ModuleUnknown),
	}
	policies := &meta.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta.DownSamplePolicyInfoWithDbRp{
			{
				Info:   info,
				DbName: "db",
				RpName: "p1",
			},
			{
				Info:   info,
				DbName: "db",
				RpName: "p3",
			},
		},
	}
	e.UpdateDownSampleInfo(policies)
	e.GetShardDownSamplePolicyInfos(&mocMeta{})
	e.GetDownSamplePolicy("db.p1")
	ident := &meta.ShardIdentifier{
		ShardID: 1,
		OwnerDb: "db",
		Policy:  "rp",
		OwnerPt: 1,
	}
	shardInfos := &meta.ShardDownSampleUpdateInfos{
		Infos: []*meta.ShardDownSampleUpdateInfo{{
			Ident:         ident,
			DownSampleLvl: 1,
		}},
	}
	e.UpdateShardDownSampleInfo(shardInfos)
	if len(e.DownSamplePolicies) != 2 {
		t.Fatalf("expecte only two downSample Policy is Alive")
	}
}

func Test_DownSampleStatePort(t *testing.T) {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "difference(\"age\", 'absolute')", Type: influxql.Float},
		influxql.VarRef{Val: "difference(\"height\", 'absolute')", Type: influxql.Integer},
	)
	port1 := executor.NewDownSampleStatePort(rowDataType)
	port2 := executor.NewDownSampleStatePort(rowDataType)
	port2.Connect(port1)
	port2.Redirect(port1)
	id := port1.ConnectionId()
	if id == 0 {
		t.Error("get id error")
	}
	equ := port1.Equal(port2)
	if !equ {
		t.Error("RowDataType not true")
	}
}

func Test_ShardDownSampleTaskNotExist(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	info := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   30 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	e := &Engine{
		DownSamplePolicies: map[string]*meta.StoreDownSamplePolicy{
			"db0.rp0": {
				Alive: true,
				Info:  info,
			},
		},
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"db0": {1: &DBPTInfo{
				shards: map[uint64]Shard{sh.GetID(): sh},
			}},
		},
	}
	policies := &meta.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta.DownSamplePolicyInfoWithDbRp{
			{
				Info:   info,
				DbName: "db0",
				RpName: "rp0",
			},
		},
	}
	e.UpdateDownSampleInfo(policies)

	infos, _ := e.GetShardDownSamplePolicyInfos(&mocMeta{})
	downSampleInfo := infos[0]
	downSampleInfo.DbName = "test"
	e.StartDownSampleTask(downSampleInfo, nil, logger.GetLogger(), &mocMeta{})
}

func Test_ShardDownSampleTask(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2022-07-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	info := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max"},
				DataType: int64(influxql.Integer),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   30 * time.Second,
				WaterMark:      time.Hour,
			},
		},
		Duration: 240 * time.Hour,
	}
	e := &Engine{
		DownSamplePolicies: map[string]*meta.StoreDownSamplePolicy{
			"db0.rp0": {
				Alive: true,
				Info:  info,
			},
		},
		DBPartitions: map[string]map[uint32]*DBPTInfo{
			"db0": {1: &DBPTInfo{
				shards: map[uint64]Shard{sh.GetID(): sh},
			}},
		},
	}
	policies := &meta.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta.DownSamplePolicyInfoWithDbRp{
			{
				Info:   info,
				DbName: "db0",
				RpName: "rp0",
			},
		},
	}
	e.UpdateDownSampleInfo(policies)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}
	infos, _ := e.GetShardDownSamplePolicyInfos(&mocMeta{})
	downSampleInfo := infos[0]
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchemaWithSources(stmt.Fields, source, stmt.ColumnNames(), &opt, nil)
			var c []hybridqp.Catalog
			c = append(c, querySchema)
			e.StartDownSampleTask(downSampleInfo, c, logger.GetLogger(), &mocMeta{})
			var schema record.Schemas
			schema = querySchema.BuildDownSampleSchema(true)
			recordPool := record.NewCircularRecordPool(TsspSequencePool, 3, schema, false)
			decs := immutable.NewReadContext(true)
			files, _ := sh.GetTSSPFiles(msNames[0], true)
			var rowCount int
			for _, v := range files.Files() {
				metaIndex, _ := v.MetaIndexAt(0)
				chunkMeta, _ := v.ReadChunkMetaData(0, metaIndex, nil, fileops.IO_PRIORITY_LOW_READ)
				for _, c := range chunkMeta {
					for s := 0; s < c.SegmentCount(); s++ {
						rec := recordPool.Get()
						rec.Schema = schema
						rec, _ = v.ReadAt(&c, s, rec, decs, fileops.IO_PRIORITY_LOW_READ)
						rowCount += rec.RowNums()
					}
				}
			}
			if rowCount != 2028 {
				t.Fail()
			}
		})
	}
}

func Test_ShardDownSampleQueryRewrite(t *testing.T) {
	executor.EnableFileCursor(true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}
	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT sum(field2_int),count(field2_int) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val4", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			cancelStatus := []int{0, 1}
			for cancelState := range cancelStatus {
				testDir := t.TempDir()
				executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
				msNames := []string{"cpu"}
				startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
				pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

				// **** start write data to the shard.
				sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
				sh2, _ := createShard("db0", "rp0", 2, testDir, config.TSSTORE)
				defer sh2.Close()
				defer sh2.indexBuilder.Close()
				defer sh.Close()
				defer sh.indexBuilder.Close()
				if err := sh.WriteRows(pts, nil); err != nil {
					t.Fatal(err)
				}
				sh2.UpdateShardReadOnly(&mocMeta{})
				if e := sh2.WriteRows(pts, nil); e != nil {
					t.Fatal()
				}

				sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
				time.Sleep(time.Second * 1)
				sh.ForceFlush()
				time.Sleep(time.Second * 1)
				info := &meta.DownSamplePolicyInfo{
					Calls: []*meta.DownSampleOperators{
						{
							AggOps:   []string{"min", "max"},
							DataType: int64(influxql.Integer),
						},
					},
					DownSamplePolicies: []*meta.DownSamplePolicy{
						{
							SampleInterval: time.Hour,
							TimeInterval:   30 * time.Second,
							WaterMark:      time.Hour,
						},
					},
					Duration: 240 * time.Hour,
				}
				e := &Engine{
					DownSamplePolicies: map[string]*meta.StoreDownSamplePolicy{
						"db0.rp0": {
							Alive: true,
							Info:  info,
						},
					},
					DBPartitions: map[string]map[uint32]*DBPTInfo{
						"db0": {1: &DBPTInfo{
							shards: map[uint64]Shard{sh.GetID(): sh},
						}},
					},
				}
				policies := &meta.DownSamplePoliciesInfoWithDbRp{
					Infos: []*meta.DownSamplePolicyInfoWithDbRp{
						{
							Info:   info,
							DbName: "db0",
							RpName: "rp0",
						},
					},
				}
				e.UpdateDownSampleInfo(policies)

				infos, _ := e.GetShardDownSamplePolicyInfos(&mocMeta{})
				downSampleInfo := infos[0]
				shardGroup := &mockShardGroup{
					sh:     sh,
					Fields: fields,
				}
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchemaWithSources(stmt.Fields, source, stmt.ColumnNames(), &opt, nil)
				var c []hybridqp.Catalog
				c = append(c, querySchema)
				err := e.StartDownSampleTask(downSampleInfo, c, logger.GetLogger(), &mocMeta{})
				if err != nil {
					t.Fatal(err)
				}
				trans := executor.NewIndexScanTransform(tt.outputRowDataType, nil, querySchema, nil, nil, make(chan struct{}, 2))
				s := trans.BuildDownSampleSchema(querySchema)
				if s.GetQueryFields()[0].Expr.(*influxql.Call).Args[0].(*influxql.VarRef).Val == "min_field4_float" {
					t.Fatal("build down sample query schema failed")
				}
				if querySchema.HasAuxTag() {
					t.Fatal("downsample doesn't has aux tag")
				}
				p, err := trans.BuildDownSamplePlan(s)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := p.(*executor.LogicalAggregate); !ok {
					t.Fatal("build downsample plan err")
				}
				req := &executor.RemoteQuery{
					Opt: opt,
				}
				sInfo := &executor.IndexScanExtraInfo{
					ShardID: uint64(10),
					Req:     req,
				}
				a := executor.NewIndexScanTransform(tt.outputRowDataType, nil, querySchema, p, sInfo, make(chan struct{}, 2))
				_, _, err = a.BuildPlan(1)
				if err != nil {
					t.Fatal(err)
				}
				updateNameMap := make(map[string]string, 0)
				for k, v := range querySchema.Fields() {
					updateNameMap[tt.outputRowDataType.Fields()[k].Expr.(*influxql.VarRef).Val] = v.Expr.(*influxql.VarRef).Val
				}
				tt.outputRowDataType.UpdateByDownSampleFields(updateNameMap)
				ctx := context.Background()
				ctx, cancel := context.WithCancel(ctx)
				ch := executor.NewChunkPort(tt.outputRowDataType)
				a.GetInputs()[0].(*executor.ChunkPort).ConnectNoneCache(ch)
				ch2 := executor.NewChunkPort(tt.outputRowDataType)
				a.GetOutputs()[0].(*executor.ChunkPort).ConnectNoneCache(ch2)
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					chunkT := executor.NewChunkImpl(tt.outputRowDataType, "test")
					chunkT.ResetTagsAndIndexes(nil, []int{0})
					chunkT.SetTime([]int64{0})
					chunkT.ResetIntervalIndex(0)
					c1 := executor.NewColumnImpl(influxql.Integer)
					c1.AppendIntegerValue(1)
					c2 := executor.NewColumnImpl(influxql.Integer)
					c2.AppendIntegerValue(2)
					chunkT.AddColumn(c1, c2)
					ch.State <- chunkT
					if cancelState == 0 {
						cancel()
					} else {
						ch.Close()
					}
					wg.Done()
				}()
				go func() {
					for {
						select {
						case _, ok := <-a.GetOutputs()[0].(*executor.ChunkPort).State:
							if !ok {
								return
							}
						}
					}
				}()
				a.SetDownSampleLevel(1)
				l := e.GetShardDownSampleLevel("db0", 1, 1)
				if l != 1 {
					t.Fatal("get err sample leve")
				}
				var procs []executor.Processor
				exec := executor.NewPipelineExecutor(procs)
				a.SetPipelineExecutor(exec)
				a.Running(ctx)
				wg.Wait()
				a.Close()
			}
		})
	}
}

func Test_BuildRecordDag_Error(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			node := executor.NewLogicalTSSPScan(querySchema)
			executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
			executor.RegistryTransformCreator(&executor.LogicalWriteIntoStorage{}, &WriteIntoStorageTransform{})
			readers, _ := sh.GetTSSPFiles(querySchema.Options().OptionsName(), true)
			node.SetFiles(readers)
			newSeqs := sh.GetNewFilesSeqs(readers.Files())
			node.SetNewSeqs(newSeqs)
			node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
			node2.SetMmsTables(sh.GetTableStore().(*immutable.MmsTables))

			sidSequenceReader := NewTsspSequenceReader(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, readers, newSeqs, make(chan struct{}))
			writeIntoStorage := NewWriteIntoStorageTransform(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, immutable.NewTsStoreConfig(), sh.GetTableStore().(*immutable.MmsTables), true)
			fileSequenceAgg := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)
			sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
			fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])
			ch := executor.NewDownSampleStatePort(nil)
			writeIntoStorage.GetOutputs()[0].Connect(ch)
			ctx := context.Background()
			go sidSequenceReader.Work(ctx)
			go fileSequenceAgg.Work(ctx)
			go writeIntoStorage.Work(ctx)
			writeIntoStorage.(*WriteIntoStorageTransform).GetClosed() <- struct{}{}
			for {
				select {
				case state, _ := <-ch.State:
					if state.GetErr() == nil {
						t.Error("downsample should be cancle")
					}
					for _, r := range readers.Files() {
						r.Unref()
					}
					return
				}
			}
		})
	}
}

func Test_CanDownSampleRewrite(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			trans := executor.NewIndexScanTransform(tt.outputRowDataType, nil, querySchema, nil, nil, make(chan struct{}, 2))
			p, _ := trans.BuildDownSamplePlan(querySchema)
			newTrans := executor.NewIndexScanTransform(tt.outputRowDataType, nil, querySchema, p, nil, make(chan struct{}, 2))
			check := newTrans.CanDownSampleRewrite(0)
			if !check {
				t.Fatal("DownSample rewrite check failed")
			}
			check = newTrans.CanDownSampleRewrite(1)
			if !check {
				t.Fatal("DownSample rewrite check failed")
			}
			stmt = MustParseSelectStatement(fmt.Sprintf(`SELECT percentile(field2_int) from cpu group by time(5s)`))
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			querySchema2 := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			p, _ = trans.BuildDownSamplePlan(querySchema2)
			newTrans = executor.NewIndexScanTransform(tt.outputRowDataType, nil, querySchema2, p, nil, make(chan struct{}, 2))
			check = newTrans.CanDownSampleRewrite(1)
			if check {
				t.Fatal("DownSample rewrite check failed")
			}
		})
	}
}

func Test_DownSampleCancel1(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2021-01-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			node := executor.NewLogicalTSSPScan(querySchema)
			executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
			executor.RegistryTransformCreator(&executor.LogicalWriteIntoStorage{}, &WriteIntoStorageTransform{})
			readers, _ := sh.GetTSSPFiles(msNames[0], true)
			newSeqs := sh.GetNewFilesSeqs(readers.Files())
			node.SetNewSeqs(newSeqs)
			node.SetFiles(readers)
			node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
			node2.SetMmsTables(sh.GetTableStore().(*immutable.MmsTables))

			sidSequenceReader := NewTsspSequenceReader(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, readers, newSeqs, make(chan struct{}))
			writeIntoStorage := NewWriteIntoStorageTransform(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, immutable.NewTsStoreConfig(), sh.GetTableStore().(*immutable.MmsTables), true)
			fileSequenceAgg := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)
			sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
			fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])

			ctx := context.Background()
			go sh.Close()
			time.Sleep(time.Second)
			go sidSequenceReader.Work(ctx)
			go fileSequenceAgg.Work(ctx)
			go writeIntoStorage.Work(ctx)
		})
	}
}

func Test_DownSample_EmptyColumn(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2021-01-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(influxql.Fields{}, []string{}, &opt, nil)
			node := executor.NewLogicalTSSPScan(querySchema)
			executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
			executor.RegistryTransformCreator(&executor.LogicalWriteIntoStorage{}, &WriteIntoStorageTransform{})
			readers, _ := sh.GetTSSPFiles(msNames[0], true)
			newSeqs := sh.GetNewFilesSeqs(readers.Files())
			node.SetNewSeqs(newSeqs)
			node.SetFiles(readers)
			node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
			node2.SetMmsTables(sh.GetTableStore().(*immutable.MmsTables))

			sidSequenceReader := NewTsspSequenceReader(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, readers, newSeqs, make(chan struct{}))
			writeIntoStorage := NewWriteIntoStorageTransform(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, immutable.NewTsStoreConfig(), sh.GetTableStore().(*immutable.MmsTables), true)
			fileSequenceAgg := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)
			sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
			fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])

			ctx := context.Background()
			go sidSequenceReader.Work(ctx)
			go fileSequenceAgg.Work(ctx)
			go writeIntoStorage.Work(ctx)
			time.Sleep(time.Second * 2)
			for _, r := range readers.Files() {
				r.Unref()
			}
			if writeIntoStorage.(*WriteIntoStorageTransform).GetRowCount() != 0 {
				t.Fail()
			}
			var schema record.Schemas
			var rowCount int
			schema = querySchema.BuildDownSampleSchema(true)
			recordPool := record.NewCircularRecordPool(TsspSequencePool, 3, schema, false)
			newFiles := writeIntoStorage.(*WriteIntoStorageTransform).newFiles
			decs := immutable.NewReadContext(true)
			for _, v := range newFiles {
				metaIndex, _ := v.MetaIndexAt(0)
				chunkMeta, _ := v.ReadChunkMetaData(0, metaIndex, nil, fileops.IO_PRIORITY_LOW_READ)
				for _, c := range chunkMeta {
					for s := 0; s < c.SegmentCount(); s++ {
						rec := recordPool.Get()
						rec.Schema = schema
						rec, _ = v.ReadAt(&c, s, rec, decs, fileops.IO_PRIORITY_LOW_READ)
						rowCount += rec.RowNums()
					}
				}
			}
			if rowCount != 0 {
				t.Fail()
			}
			planWriter := executor.NewLogicalPlanWriterImpl(&strings.Builder{})
			node2.ExplainIterms(planWriter)
		})
	}
}

type MocTsspFile struct {
	path string
}

func (m MocTsspFile) Path() string {
	return m.path
}

func (m MocTsspFile) Name() string {
	return ""
}

func (m MocTsspFile) FileName() immutable.TSSPFileName {
	return immutable.TSSPFileName{}
}

func (m MocTsspFile) LevelAndSequence() (uint16, uint64) {
	return 0, 0
}

func (m MocTsspFile) FileNameMerge() uint16 {
	return 0
}

func (m MocTsspFile) FileNameExtend() uint16 {
	return 0
}

func (m MocTsspFile) IsOrder() bool {
	return false
}

func (m MocTsspFile) Ref() {
	return
}

func (m MocTsspFile) Unref() {
	return
}

func (m MocTsspFile) RefFileReader() {
	return
}

func (m MocTsspFile) UnrefFileReader() {
	return
}

func (m MocTsspFile) LoadComponents() error {
	return nil
}

func (m MocTsspFile) FreeFileHandle() error {
	return nil
}

func (m MocTsspFile) Stop() {
	return
}

func (m MocTsspFile) Inuse() bool {
	return false
}

func (m MocTsspFile) MetaIndexAt(idx int) (*immutable.MetaIndex, error) {
	return nil, nil
}

func (m MocTsspFile) MetaIndex(id uint64, tr util.TimeRange) (int, *immutable.MetaIndex, error) {
	return 0, nil, nil
}

func (m MocTsspFile) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *immutable.ChunkMeta, buffer *[]byte, ioPriority int) (*immutable.ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record, decs *immutable.ReadContext, ioPriority int) (*record.Record, error) {
	return nil, nil
}

func (m MocTsspFile) ReadData(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error) {
	return nil, nil
}

func (m MocTsspFile) ReadChunkMetaData(metaIdx int, me *immutable.MetaIndex, dst []immutable.ChunkMeta, ioPriority int) ([]immutable.ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) CreateTime() int64 {
	return 0
}

func (m MocTsspFile) FileStat() *immutable.Trailer {
	return &immutable.Trailer{}
}

func (m MocTsspFile) FileSize() int64 {
	return 0
}

func (m MocTsspFile) InMemSize() int64 {
	return 0
}

func (m MocTsspFile) Contains(id uint64) (bool, error) {
	return false, nil
}

func (m MocTsspFile) ContainsByTime(tr util.TimeRange) (bool, error) {
	return false, nil
}

func (m MocTsspFile) ContainsValue(id uint64, tr util.TimeRange) (bool, error) {
	return false, nil
}

func (m MocTsspFile) MinMaxTime() (int64, int64, error) {
	return 0, 0, nil
}

func (m MocTsspFile) Open() error {
	return nil
}

func (m MocTsspFile) Close() error {
	return nil
}

func (m MocTsspFile) LoadIntoMemory() error {
	return nil
}

func (m MocTsspFile) LoadIndex() error {
	return nil
}

func (m MocTsspFile) LoadIdTimes(p *immutable.IdTimePairs) error {
	return nil
}

func (m MocTsspFile) Rename(newName string) error {
	return nil
}

func (m MocTsspFile) Remove() error {
	return nil
}

func (m MocTsspFile) FreeMemory(evictLock bool) int64 {
	return 0
}

func (m MocTsspFile) Version() uint64 {
	return 0
}

func (m MocTsspFile) AverageChunkRows() int {
	return 0
}

func (m MocTsspFile) MaxChunkRows() int {
	return 0
}

func (m MocTsspFile) MetaIndexItemNum() int64 {
	return 0
}

func (m MocTsspFile) AddToEvictList(level uint16) {
	return
}

func (m MocTsspFile) RemoveFromEvictList(level uint16) {
	return
}

func (m MocTsspFile) GetFileReaderRef() int64 {
	return 0
}

func TestCanDoDownSample(t *testing.T) {
	endTime := time.Date(2022, time.November, 16, 0, 0, 0, 0, time.UTC)
	sh := &shard{
		ident: &meta.ShardIdentifier{
			DownSampleID:    1,
			DownSampleLevel: 1,
		},
		endTime: endTime,
	}
	p := &meta.DownSamplePolicy{
		SampleInterval: time.Hour,
		TimeInterval:   time.Minute,
	}
	if sh.checkDownSample(2, p, 1, time.Now().UTC()) {
		t.Fatal()
	}
	if sh.checkDownSample(1, p, 1, endTime) {
		t.Fatal()
	}
	if sh.checkDownSample(1, p, 0, time.Now().UTC()) {
		t.Fatal()
	}
	if !sh.checkDownSample(1, p, 1, time.Now().UTC()) {
		t.Fatal()
	}
}

func TestCancelDownSample(t *testing.T) {
	sh := &shard{
		cacheClosed:             0,
		shardDownSampleTaskInfo: &shardDownSampleTaskInfo{sdsp: &meta.ShardDownSamplePolicyInfo{}, schema: []hybridqp.Catalog{}, log: nil},
	}
	sh.stopDownSample = make(chan struct{})
	close(sh.stopDownSample)
	if sh.StartDownSample(0, 0, nil, &mocMeta{}) != nil {
		t.Fatal()
	}
}

func TestDownSampleZeroTask(t *testing.T) {
	sh := &shard{
		cacheClosed:             0,
		shardDownSampleTaskInfo: &shardDownSampleTaskInfo{sdsp: &meta.ShardDownSamplePolicyInfo{}, schema: []hybridqp.Catalog{}, log: nil},
	}
	filesMap := make(map[int]*immutable.TSSPFiles, 0)
	allDownSampleFiles := make(map[int][]immutable.TSSPFile, 0)
	schema := make([]hybridqp.Catalog, 0)
	e := sh.StartDownSampleTaskBySchema(0, filesMap, allDownSampleFiles, schema, nil, logger.GetLogger())
	if e != nil {
		t.Fatal()
	}
}

func TestDownSampleZeroMst(t *testing.T) {
	sh := &shard{
		cacheClosed:             0,
		shardDownSampleTaskInfo: &shardDownSampleTaskInfo{sdsp: &meta.ShardDownSamplePolicyInfo{}, schema: []hybridqp.Catalog{}, log: nil},
		ident:                   &meta.ShardIdentifier{},
	}

	e := sh.ReplaceDownSampleFiles(nil, nil, nil, nil, 1, 1, &meta.ShardDownSamplePolicyInfo{
		TaskID:                1,
		DownSamplePolicyLevel: 2,
	}, &mocMeta{})
	if e != nil {
		t.Fatal()
	}
	if sh.ident.DownSampleID != 1 || sh.ident.DownSampleLevel != 2 {
		t.Fatal()
	}
}

func TestDownSampleNoneMstTask(t *testing.T) {
	testDir := t.TempDir()
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	filesMap := make(map[int]*immutable.TSSPFiles, 0)
	allDownSampleFiles := make(map[int][]immutable.TSSPFile, 0)
	schema := make([]hybridqp.Catalog, 0)
	schema = append(schema, executor.NewQuerySchema(nil, nil, &query.ProcessorOptions{Name: "test"}, nil))
	e := sh.StartDownSampleTaskBySchema(0, filesMap, allDownSampleFiles, schema, nil, logger.GetLogger())
	if e != nil {
		t.Fatal()
	}
}

type mocMeta struct {
}

func (m *mocMeta) UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error {
	return nil
}

func Test_ShardDownSampleTaskErrorDeleteFiles(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2021-01-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int],min[float]",
			q:      fmt.Sprintf(`SELECT min(field2_int),min(field4_float) from cpu group by time(5s)`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			node := executor.NewLogicalTSSPScan(querySchema)
			executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
			executor.RegistryTransformCreator(&executor.LogicalWriteIntoStorage{}, &WriteIntoStorageTransform{})
			readers, _ := sh.GetTSSPFiles(msNames[0], true)
			newSeqs := sh.GetNewFilesSeqs(readers.Files())
			node.SetNewSeqs(newSeqs)
			node.SetFiles(readers)
			node2 := executor.NewLogicalWriteIntoStorage(node, querySchema)
			node2.SetMmsTables(sh.GetTableStore().(*immutable.MmsTables))

			sidSequenceReader := NewTsspSequenceReader(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, readers, newSeqs, make(chan struct{}))
			writeIntoStorage := NewWriteIntoStorageTransform(tt.outputRowDataType, tt.readerOps, nil, source, querySchema, immutable.NewTsStoreConfig(), sh.GetTableStore().(*immutable.MmsTables), true)
			fileSequenceAgg := NewFileSequenceAggregator(querySchema, true, 0, math.MaxInt64)
			sidSequenceReader.GetOutputs()[0].Connect(fileSequenceAgg.GetInputs()[0])
			fileSequenceAgg.GetOutputs()[0].Connect(writeIntoStorage.GetInputs()[0])

			ctx := context.Background()
			go sidSequenceReader.Work(ctx)
			go fileSequenceAgg.Work(ctx)
			go writeIntoStorage.Work(ctx)
			time.Sleep(time.Second * 2)
			for _, r := range readers.Files() {
				r.Unref()
			}
			if writeIntoStorage.(*WriteIntoStorageTransform).GetRowCount() != 4056 {
				t.Fail()
			}
			newFiles := writeIntoStorage.(*WriteIntoStorageTransform).newFiles
			allDownSampleFiles := make(map[int][]immutable.TSSPFile, 0)
			allDownSampleFiles[0] = newFiles
			sh.DeleteDownSampleFiles(allDownSampleFiles)
		})
	}
}
func TestStopShardDownSample(t *testing.T) {
	sh := &shard{
		cacheClosed:             0,
		shardDownSampleTaskInfo: &shardDownSampleTaskInfo{sdsp: &meta.ShardDownSamplePolicyInfo{}, schema: []hybridqp.Catalog{}, log: nil},
	}
	sh.stopDownSample = make(chan struct{})
	close(sh.stopDownSample)
	sh.EnableDownSample()
	sh.DisableDownSample()
	if sh.StartDownSample(0, 0, nil, &mocMeta{}) != nil {
		t.Fatal()
	}
}

func TestAppendRecWithNilRows(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	srcRec := record.NewRecord(schema, true)
	srcRec.ColVals[0].AppendIntegers([]int64{1, 2, 3, 4, 5}...)
	srcRec.RecMeta.Times = make([][]int64, 1)
	srcRec.RecMeta.Times[0] = []int64{1, 2, 3, 4, 5}
	srcRec.AppendTime([]int64{6, 11, 17, 32}...)

	dstRec := record.NewRecord(schema, true)
	dstRec.RecMeta.Times = make([][]int64, 1)
	opt := &query.ProcessorOptions{
		Interval:  hybridqp.Interval{Duration: 5},
		Ascending: true,
	}
	AppendRecWithNilRows(dstRec, srcRec, opt, 0, 45, 0, false)
	times := dstRec.Times()
	if times[0] != 5 || times[len(times)-1] != 30 {
		t.Fatal("wrong start and end time")
	}
	for i := 0; i < len(times)-1; i++ {
		if times[i+1]-times[i] != int64(opt.Interval.Duration) {
			t.Fatal("wrong time interval")
		}
	}

	srcRec1 := record.NewRecord(schema, true)
	srcRec1.ColVals[0].AppendIntegers([]int64{1, 2, 3, 4, 5}...)
	srcRec1.RecMeta.Times = make([][]int64, 1)
	srcRec1.RecMeta.Times[0] = []int64{1, 2, 3, 4, 5}
	srcRec1.AppendTime([]int64{41, 52, 57, 67}...)

	dstRec1 := record.NewRecord(schema, true)
	dstRec1.RecMeta.Times = make([][]int64, 1)
	AppendRecWithNilRows(dstRec1, srcRec1, opt, 30, 75, 0, true)
	times2 := dstRec1.Times()
	if times2[0] != 35 || times2[len(times2)-1] != 70 {
		t.Fatal("wrong start and end time")
	}
	for i := 0; i < len(times2)-1; i++ {
		if times2[i+1]-times2[i] != int64(opt.Interval.Duration) {
			t.Fatal("wrong time interval")
		}
	}

	srcRec2 := record.NewRecord(schema, true)
	srcRec2.ColVals[0].AppendIntegers([]int64{1, 2, 3, 4, 5}...)
	srcRec2.RecMeta.Times = make([][]int64, 1)
	srcRec2.RecMeta.Times[0] = []int64{1, 2, 3, 4, 5}
	srcRec2.AppendTime([]int64{6, 11, 17, 32}...)

	dstRec2 := record.NewRecord(schema, true)
	dstRec2.RecMeta.Times = make([][]int64, 1)
	opt2 := &query.ProcessorOptions{
		Interval:  hybridqp.Interval{Duration: 7},
		Ascending: true,
	}
	AppendRecWithNilRows(dstRec2, srcRec2, opt2, 0, 45, 3, false)
	times3 := dstRec2.Times()
	if times3[0] != 3 || times3[len(times3)-1] != 28 {
		t.Fatal("wrong start and end time")
	}
	for i := 1; i < len(times3)-1; i++ {
		if times3[i+1]-times3[i] != int64(opt2.Interval.Duration) {
			t.Fatal("wrong time interval")
		}
	}
}
