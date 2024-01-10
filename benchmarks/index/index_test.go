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

package index

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

var DefaultEngineOption netstorage.EngineOptions
var defaultString = "abcdefghijklmnopqrstuvwxyz"

const defaultDb = "db0"
const defaultRp = "rp0"
const defaultShGroupId = uint64(1)
const defaultShardId = uint64(1)
const defaultPtId = uint32(1)
const defaultChunkSize = 1000
const defaultMeasurementName = "cpu"

func init() {
	DefaultEngineOption = netstorage.NewEngineOptions()
	DefaultEngineOption.WriteColdDuration = time.Second * 5000
	DefaultEngineOption.ShardMutableSizeLimit = 30 * 1024 * 1024
	DefaultEngineOption.NodeMutableSizeLimit = 1e9
	DefaultEngineOption.MaxWriteHangTime = time.Second
	DefaultEngineOption.MemDataReadEnabled = true
	DefaultEngineOption.WalSyncInterval = 100 * time.Millisecond
	DefaultEngineOption.WalEnabled = true
	DefaultEngineOption.WalReplayParallel = false
	DefaultEngineOption.DownSampleWriteDrop = true
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ShardsParallelismRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.SeriesParallelismRes, 0, 0)
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func createShard(db, rp string, ptId uint32, pathName string, duration ...time.Duration) (engine.Shard, error) {
	dataPath := pathName + "/data"
	walPath := pathName + "/wal"
	lockPath := filepath.Join(dataPath, "LOCK")
	indexPath := filepath.Join(pathName, defaultDb, "/index/data")
	ident := &meta.IndexIdentifier{OwnerDb: db, OwnerPt: ptId, Policy: rp}
	ident.Index = &meta.IndexDescriptor{IndexID: 1, IndexGroupID: 2, TimeRange: meta.TimeRangeInfo{}}
	ltime := uint64(time.Now().Unix())
	opts := new(tsi.Options).
		Ident(ident).
		Path(indexPath).
		IndexType(tsi.MergeSet).
		EngineType(config.TSSTORE).
		StartTime(time.Now()).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)
	indexBuilder := tsi.NewIndexBuilder(opts)
	primaryIndex, err := tsi.NewIndex(opts)
	if err != nil {
		return nil, err
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	indexRelation, _ := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
	indexBuilder.Relations[uint32(tsi.MergeSet)] = indexRelation
	err = indexBuilder.Open()
	if err != nil {
		return nil, err
	}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1970-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2099-01-01T01:00:00Z")}
	shardIdent := &meta.ShardIdentifier{ShardID: defaultShardId, ShardGroupID: 1, OwnerDb: db, OwnerPt: ptId, Policy: rp}
	sh := engine.NewShard(dataPath, walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, config.TSSTORE)
	sh.SetIndexBuilder(indexBuilder)
	if len(duration) > 0 {
		sh.SetWriteColdDuration(duration[0])
	}
	if err := sh.OpenAndEnable(nil); err != nil {
		_ = sh.Close()
		return nil, err
	}
	return sh, nil
}

func GenDataRecord(seriesNum, pointNumOfPerSeries int,
	tm time.Time, fullField, inc bool, fixBool bool, tags, fields map[string]interface{}) ([]influx.Row, int64, int64) {
	tm = tm.Truncate(time.Second)
	pts := make([]influx.Row, 0, seriesNum)

	var indexKeyPool []byte
	vInt, vFloat := int64(1), 1.1
	for i := 0; i < seriesNum; i++ {

		if !fullField {
			if i%10 == 0 {
				delete(fields, "field1_string")
			}

			if i%25 == 0 {
				delete(fields, "field4_float")
			}

			if i%35 == 0 {
				delete(fields, "field3_bool")
			}
		}

		r := influx.Row{}

		// fields init
		r.Fields = make([]influx.Field, len(fields))
		j := 0
		for k, v := range fields {
			r.Fields[j].Key = k
			switch v.(type) {
			case int64:
				r.Fields[j].Type = influx.Field_Type_Int
				r.Fields[j].NumValue = float64(v.(int64))
			case float64:
				r.Fields[j].Type = influx.Field_Type_Float
				r.Fields[j].NumValue = v.(float64)
			case string:
				r.Fields[j].Type = influx.Field_Type_String
				r.Fields[j].StrValue = v.(string)
			case bool:
				r.Fields[j].Type = influx.Field_Type_Boolean
				if v.(bool) == false {
					r.Fields[j].NumValue = 0
				} else {
					r.Fields[j].NumValue = 1
				}
			}
			j++
		}

		sort.Sort(&r.Fields)

		vInt++
		vFloat += 1.1

		// tags init
		r.Tags = make(influx.PointTags, len(tags))
		j = 0
		for k, v := range tags {
			r.Tags[j].Key = k
			switch va := v.(type) {
			case int64:
				r.Tags[j].Value = fmt.Sprintf("%d", i)
			case float64:
				r.Tags[j].Value = fmt.Sprintf("%f", va+float64(i))
			case bool:
				r.Tags[j].Value = fmt.Sprintf("%v", i%2 == 0)
			case string:
				r.Tags[j].Value = fmt.Sprintf("abcdefghij_%d", i)
			}
			j++
		}
		sort.Sort(&r.Tags)

		r.Name = defaultMeasurementName
		r.Timestamp = tm.UnixNano()
		r.UnmarshalIndexKeys(indexKeyPool)
		r.UnmarshalShardKeyByTag(nil)
		tm = tm.Add(time.Duration(1))

		pts = append(pts, r)
	}
	if pointNumOfPerSeries > 1 {
		copyRs := copyPointRows(pts, pointNumOfPerSeries-1, time.Duration(1), inc)
		pts = append(pts, copyRs...)
	}

	sort.Slice(pts, func(i, j int) bool {
		return pts[i].Timestamp < pts[j].Timestamp
	})

	return pts, pts[0].Timestamp, pts[len(pts)-1].Timestamp
}

func copyPointRows(rs []influx.Row, copyCnt int, interval time.Duration, inc bool) []influx.Row {
	copyRs := make([]influx.Row, 0, copyCnt*len(rs))
	for i := 0; i < copyCnt; i++ {
		cnt := int64(i + 1)
		for index, endIndex := 0, len(rs); index < endIndex; index++ {
			tmpPointRow := rs[index]
			tmpPointRow.Timestamp += int64(interval) * cnt
			// fields need regenerate
			if inc {
				tmpPointRow.Fields = make([]influx.Field, len(rs[index].Fields))
				if len(rs[index].Fields) > 0 {
					tmpPointRow.Copy(&rs[index])
					for idx, field := range rs[index].Fields {
						switch field.Type {
						case influx.Field_Type_Int:
							tmpPointRow.Fields[idx].NumValue += float64(cnt)
						case influx.Field_Type_Float:
							tmpPointRow.Fields[idx].NumValue += float64(cnt)
						case influx.Field_Type_Boolean:
							tmpPointRow.Fields[idx].NumValue = float64(cnt & 1)
						case influx.Field_Type_String:
							tmpPointRow.Fields[idx].StrValue += fmt.Sprintf("-%d", cnt)
						}
					}
				}
			}

			copyRs = append(copyRs, tmpPointRow)
		}
	}
	return copyRs
}

func writeData(sh engine.Shard, rs []influx.Row, forceFlush bool) error {
	var buff []byte
	var err error
	buff, err = influx.FastMarshalMultiRows(buff, rs)
	if err != nil {
		return err
	}

	for i := range rs {
		sort.Sort(&rs[i].Fields)
	}

	err = sh.WriteRows(rs, buff)
	if err != nil {
		return err
	}

	// wait index flush
	//time.Sleep(time.Second * 1)
	if forceFlush {
		// wait mem table flush
		sh.ForceFlush()
	}
	return nil
}

func createFieldAux(fieldsName []string) []influxql.VarRef {
	var fieldAux []influxql.VarRef
	fieldMap := make(map[string]influxql.DataType, 4)
	fieldMap["field1_string"] = influxql.String
	fieldMap["field2_int"] = influxql.Integer
	fieldMap["field3_bool"] = influxql.Boolean
	fieldMap["field4_float"] = influxql.Float

	if len(fieldsName) == 0 {
		fieldsName = append(fieldsName, "field1_string", "field2_int", "field3_bool", "field4_float")
	}
	for _, fieldName := range fieldsName {
		if tp, ok := fieldMap[fieldName]; ok {
			fieldAux = append(fieldAux, influxql.VarRef{Val: fieldName, Type: tp})
		}
	}

	return fieldAux
}

func createSingleFieldAux(fieldsName []string) []influxql.VarRef {
	var fieldAux []influxql.VarRef
	fieldMap := make(map[string]influxql.DataType, 4)
	fieldMap["field1_string"] = influxql.String
	fieldMap["field2_int"] = influxql.Integer
	fieldMap["field3_bool"] = influxql.Boolean
	fieldMap["field4_float"] = influxql.Float

	if len(fieldsName) == 0 {
		fieldsName = append(fieldsName, "field4_float")
	}
	for _, fieldName := range fieldsName {
		if tp, ok := fieldMap[fieldName]; ok {
			fieldAux = append(fieldAux, influxql.VarRef{Val: fieldName, Type: tp})
		}
	}

	return fieldAux
}

func addFilterFieldCondition(filterCondition string, opt *query.ProcessorOptions) {
	if filterCondition == "" {
		return
	}
	fieldMap := make(map[string]influxql.DataType, 4)
	fieldMap["field1_string"] = influxql.String
	fieldMap["field2_int"] = influxql.Integer
	fieldMap["field3_bool"] = influxql.Boolean
	fieldMap["field4_float"] = influxql.Float

	opt.Condition = influxql.MustParseExpr(filterCondition)
	influxql.WalkFunc(opt.Condition, func(n influxql.Node) {
		ref, ok := n.(*influxql.VarRef)
		if !ok {
			return
		}
		ty, ok := fieldMap[ref.Val]
		if ok {
			ref.Type = ty
		}
	})
}

func genQueryOpt(filterField string, ascending bool, dims []string, startTime, endTime int64, aux []influxql.VarRef) *query.ProcessorOptions {
	var opt query.ProcessorOptions
	opt.Name = defaultMeasurementName
	opt.Dimensions = dims
	opt.Ascending = ascending
	opt.FieldAux = aux
	opt.MaxParallel = 8
	opt.ChunkSize = defaultChunkSize
	opt.StartTime = startTime
	opt.EndTime = endTime

	addFilterFieldCondition(filterField, &opt)

	return &opt
}

func genQuerySchema(fieldAux []influxql.VarRef, opt *query.ProcessorOptions) *executor.QuerySchema {
	var fields influxql.Fields
	var columnNames []string
	for i := range fieldAux {
		f := &influxql.Field{
			Expr:  &fieldAux[i],
			Alias: "",
		}
		fields = append(fields, f)
		columnNames = append(columnNames, fieldAux[i].Val)
	}
	return executor.NewQuerySchema(fields, columnNames, opt, nil)
}

func closeShard(sh engine.Shard) error {
	if err := sh.GetIndexBuilder().Close(); err != nil {
		return err
	}
	if err := sh.Close(); err != nil {
		return err
	}
	return nil
}

func BenchmarkTestIndex(b *testing.B) {
	b.StopTimer()
	testDir := b.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, 1000*time.Second) // not flush data to snapshot
	if err != nil {
		b.Fatal(err)
	}

	mutable.SetSizeLimit(10000000000)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, startTime, endTime := GenDataRecord(500000, 1, time.Now(), true, true, false, map[string]interface{}{
		"agentSN": defaultString, "bn": defaultString, "chunkdid": defaultString, "client_ip": defaultString, "cmpt": defaultString, "collection": defaultString, "dbname": defaultString, "errcode": defaultString, "metrictype": defaultString, "mk": defaultString, "ns": defaultString, "opt": defaultString, "request_method": defaultString, "rgn": defaultString, "schema_name": defaultString, "shard_ip": defaultString, "svc": defaultString,
	}, map[string]interface{}{
		"field4_float": int64(1),
	})
	err = writeData(sh, rows, false)
	if err != nil {
		b.Fatal(err)
	}

	aux := createFieldAux([]string{"field4_float"})
	opt := genQueryOpt("", true, nil, startTime, endTime, aux)
	querySchema := genQuerySchema(aux, opt)
	time.Sleep(time.Second * 5)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, err = sh.Scan(nil, querySchema, resourceallocator.DefaultSeriesAllocateFunc)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTestIndexNew(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			tsi.NewTagSetInfo()
		}
	}
}

func BenchmarkTestIndexNewSingle(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			tsi.NewSingleTagSetInfo()
		}
	}
}

func BenchmarkTestString(b *testing.B) {
	b.ReportAllocs()
	bb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			_ = string(bb)
		}
	}
}

func BenchmarkTestString2(b *testing.B) {
	b.ReportAllocs()
	bb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			_ = bytesutil.ToUnsafeString(bb)
		}
	}
}

func BenchmarkTestQuery(b *testing.B) {
	b.StopTimer()
	testDir := b.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, 1000*time.Second) // not flush data to snapshot
	if err != nil {
		b.Fatal(err)
	}

	mutable.SetSizeLimit(10000000000)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, startTime, endTime := GenDataRecord(50000, 1, time.Now(), true, true, false, map[string]interface{}{
		"agentSN": defaultString, "bn": defaultString, "chunkdid": defaultString, "client_ip": defaultString, "cmpt": defaultString, "collection": defaultString, "dbname": defaultString, "errcode": defaultString, "metrictype": defaultString, "mk": defaultString, "ns": defaultString, "opt": defaultString, "request_method": defaultString, "rgn": defaultString, "schema_name": defaultString, "shard_ip": defaultString, "svc": defaultString,
	}, map[string]interface{}{
		"field4_float": int64(1),
	})
	err = writeData(sh, rows, false)
	if err != nil {
		b.Fatal(err)
	}

	aux := createFieldAux([]string{"field4_float"})
	opt := genQueryOpt("", true, []string{
		"agentSN", "bn", "chunkdid", "client_ip", "cmpt", "collection", "dbname", "errcode", "metrictype", "mk", "ns", "opt", "request_method", "rgn", "schema_name", "shard_ip", "svc",
	}, startTime, endTime, aux)
	querySchema := genQuerySchema(aux, opt)
	time.Sleep(time.Second * 5)

	b.StartTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cursors, err := sh.CreateCursor(context.Background(), querySchema)
		if err != nil {
			b.Fatal(err)
		}
		for i := range cursors {
			engine.SetNextMethod(cursors[i])
			for {
				if r, _, _ := cursors[i].Next(); r == nil {
					cursors[i].Close()
					break
				}
			}
		}
	}
	b.StopTimer()
	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		b.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	testDir := t.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, 1000*time.Second) // not flush data to snapshot
	if err != nil {
		t.Fatal(err)
	}

	mutable.SetSizeLimit(10000000000)

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, startTime, endTime := GenDataRecord(50, 10, time.Now(), true, true, false, map[string]interface{}{
		"agentSN": defaultString, "bn": defaultString, "chunkdid": defaultString, "client_ip": defaultString, "cmpt": defaultString, "collection": defaultString, "dbname": defaultString, "errcode": defaultString, "metrictype": defaultString, "mk": defaultString, "ns": defaultString, "opt": defaultString, "request_method": defaultString, "rgn": defaultString, "schema_name": defaultString, "shard_ip": defaultString, "svc": defaultString,
	}, map[string]interface{}{
		"field4_float": int64(1),
	})
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}

	aux := createFieldAux([]string{"field4_float"})
	opt := genQueryOpt("", true, []string{
		"agentSN", "bn", "chunkdid", "client_ip", "cmpt", "collection", "dbname", "errcode", "metrictype", "mk", "ns", "opt", "request_method", "rgn", "schema_name", "shard_ip", "svc",
	}, startTime, endTime, aux)
	querySchema := genQuerySchema(aux, opt)
	time.Sleep(time.Second * 5)

	cursors, err := sh.CreateCursor(context.Background(), querySchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := range cursors {
		engine.SetNextMethod(cursors[i])
		for {
			if r, _, _ := cursors[i].Next(); r == nil {
				break
			}
		}
	}

	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}
