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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	set "github.com/deckarep/golang-set"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing/fields"
	originql "github.com/influxdata/influxql"
	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	immutable.EnableMergeOutOfOrder = false
	logger.InitLogger(config.NewLogger(config.AppStore))
	immutable.Init()
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
}

const defaultDb = "db0"
const defaultRp = "rp0"
const defaultShGroupId = uint64(1)
const defaultShardId = uint64(1)
const defaultPtId = uint32(1)
const defaultChunkSize = 1000
const defaultMeasurementName = "cpu"

type checkSingleCursorFunction func(cur comm.KeyCursor, expectRecords *sync.Map, total *int, ascending bool, stop chan struct{}) error

func GenDataRecord(msNames []string, seriesNum, pointNumOfPerSeries int, interval time.Duration,
	tm time.Time, fullField, inc bool, fixBool bool, tv ...int) ([]influx.Row, int64, int64) {
	tm = tm.Truncate(time.Second)
	pts := make([]influx.Row, 0, seriesNum)
	names := msNames
	if len(msNames) == 0 {
		names = []string{defaultMeasurementName}
	}

	mms := func(i int) string {
		return names[i%len(names)]
	}

	var indexKeyPool []byte
	vInt, vFloat := int64(1), 1.1
	tv1, tv2, tv3, tv4 := 1, 1, 1, 1
	for _, tgv := range tv {
		tv1 = tgv
	}
	for i := 0; i < seriesNum; i++ {
		fields := map[string]interface{}{
			"field2_int":    vInt,
			"field3_bool":   i%2 == 0,
			"field4_float":  vFloat,
			"field1_string": fmt.Sprintf("test-test-test-test-%d", i),
		}
		if fixBool {
			fields["field3_bool"] = (i%2 == 0)
		} else {
			fields["field3_bool"] = (rand.Int31n(100) > 50)
		}

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
		tags := map[string]string{
			"tagkey1": fmt.Sprintf("tagvalue1_%d", tv1),
			"tagkey2": fmt.Sprintf("tagvalue2_%d", tv2),
			"tagkey3": fmt.Sprintf("tagvalue3_%d", tv3),
			"tagkey4": fmt.Sprintf("tagvalue4_%d", tv4),
		}

		// tags init
		r.Tags = make(influx.PointTags, len(tags))
		j = 0
		for k, v := range tags {
			r.Tags[j].Key = k
			r.Tags[j].Value = v
			j++
		}
		sort.Sort(&r.Tags)
		tv4++
		tv3++
		tv2++
		tv1++

		name := mms(i)
		r.Name = name
		r.Timestamp = tm.UnixNano()
		r.UnmarshalIndexKeys(indexKeyPool)
		r.UnmarshalShardKeyByTag(nil)
		tm = tm.Add(interval)

		pts = append(pts, r)
	}
	if pointNumOfPerSeries > 1 {
		copyRs := copyPointRows(pts, pointNumOfPerSeries-1, interval, inc)
		pts = append(pts, copyRs...)
	}

	sort.Slice(pts, func(i, j int) bool {
		return pts[i].Timestamp < pts[j].Timestamp
	})

	return pts, pts[0].Timestamp, pts[len(pts)-1].Timestamp
}

func genRecord() *record.Record {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})

	return rec
}

// create field aux, if input slice is nil, means create for all default fields
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

func createShard(db, rp string, ptId uint32, pathName string, engineType config.EngineType, duration ...time.Duration) (*shard, error) {
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
		IndexType(index.MergeSet).
		EngineType(engineType).
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
	indexBuilder.Relations[uint32(index.MergeSet)] = indexRelation
	err = indexBuilder.Open()
	if err != nil {
		return nil, err
	}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1970-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2099-01-01T01:00:00Z")}
	shardIdent := &meta.ShardIdentifier{ShardID: defaultShardId, ShardGroupID: 1, OwnerDb: db, OwnerPt: ptId, Policy: rp}
	if engineType == config.COLUMNSTORE {
		immutable.SetSnapshotTblNum(8)
		immutable.SetCompactionEnabled(false)
	}
	sh := NewShard(dataPath, walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, engineType)
	sh.indexBuilder = indexBuilder
	sh.storage.SetClient(&MockMetaClient{})
	if len(duration) > 0 {
		sh.SetWriteColdDuration(duration[0])
	}
	if err := sh.OpenAndEnable(nil); err != nil {
		_ = sh.Close()
		return nil, err
	}
	return sh, nil
}

func closeShard(sh *shard) error {
	if err := sh.indexBuilder.Close(); err != nil {
		return err
	}
	if err := sh.Close(); err != nil {
		return err
	}
	return nil
}

func writeData(sh *shard, rs []influx.Row, forceFlush bool) error {
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

func writeRec(sh *shard, rec *record.Record, forceFlush bool) error {
	buff, err := coordinator.MarshalWithMeasurements(nil, defaultMeasurementName, rec)
	if err != nil {
		return err
	}

	err = sh.WriteCols(defaultMeasurementName, rec, buff)
	if err != nil {
		return err
	}

	if forceFlush {
		// wait mem table flush
		sh.ForceFlush()
	}
	return nil
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

func genQueryOpt(tc *TestCase, msName string, ascending bool) *query.ProcessorOptions {
	var opt query.ProcessorOptions
	opt.Name = msName
	opt.Dimensions = tc.dims
	opt.Ascending = ascending
	opt.FieldAux = tc.fieldAux
	opt.MaxParallel = 8
	opt.ChunkSize = defaultChunkSize
	opt.StartTime = tc.startTime
	opt.EndTime = tc.endTime

	addFilterFieldCondition(tc.fieldFilter, &opt)

	return &opt
}

func appendFieldValueToRecord(rec *record.Record, fields []influx.Field, timeStamp int64) {
	fieldExist := false
	for _, v := range fields {
		recIndex := rec.FieldIndexs(v.Key)
		if recIndex == -1 {
			continue
		}
		fieldExist = true
		if v.Type == influx.Field_Type_Int {
			rec.ColVals[recIndex].AppendInteger(int64(v.NumValue))
		} else if v.Type == influx.Field_Type_Float {
			rec.ColVals[recIndex].AppendFloat(v.NumValue)
		} else if v.Type == influx.Field_Type_Boolean {
			if 0 == v.NumValue {
				rec.ColVals[recIndex].AppendBoolean(false)
			} else {
				rec.ColVals[recIndex].AppendBoolean(true)
			}
		} else if v.Type == influx.Field_Type_String {
			rec.ColVals[recIndex].AppendString(v.StrValue)
		} else {
			panic("error type")
		}
	}
	if fieldExist {
		rec.ColVals[len(rec.Schema)-1].AppendInteger(timeStamp)
	}
}

func transRowToRecordNew(row *influx.Row, schema record.Schemas) *record.Record {
	var rec record.Record
	rec.Schema = schema
	rec.ColVals = make([]record.ColVal, len(schema))

	appendFieldValueToRecord(&rec, row.Fields, row.Timestamp)

	sort.Sort(rec)
	return &rec
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

func copyIntervalStepPointRows(rs []influx.Row, copyCnt int, interval time.Duration, inc bool) []influx.Row {
	copyRs := make([]influx.Row, 0, copyCnt*len(rs))
	for i := 0; i < copyCnt; i++ {
		for index, endIndex := 0, len(rs); index < endIndex; index++ {
			tmpPointRow := rs[index]
			tmpPointRow.Timestamp += int64(interval)
			// fields need regenerate
			if inc {
				tmpPointRow.Fields = make([]influx.Field, len(rs[index].Fields))
				if len(rs[index].Fields) > 0 {
					tmpPointRow.Copy(&rs[index])
				}
			}

			copyRs = append(copyRs, tmpPointRow)
		}
	}
	return copyRs
}

type seriesData struct {
	rec  *record.Record   // record of this series
	tags influx.PointTags // used for filter and make group key later
}

func genExpectRecordsMap(rs []influx.Row, querySchema *executor.QuerySchema) *sync.Map {
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].Timestamp < rs[j].Timestamp
	})
	opt := querySchema.Options()

	var filterFields []*influxql.VarRef
	var auxTags []string
	var recSchema record.Schemas
	filterFields, _ = getFilterFieldsByExpr(opt.GetCondition(), filterFields[:0])
	_, recSchema = NewRecordSchema(querySchema, auxTags[:0], recSchema[:0], filterFields, config.TSSTORE)

	mm := make(map[string]*seriesData, 0)
	limit := opt.GetOffset() + opt.GetLimit()
	for i := range rs {
		if !opt.IsAscending() {
			i = len(rs) - 1 - i
		}
		point := rs[i]
		if point.Name != opt.OptionsName() || point.Timestamp < opt.GetStartTime() || point.Timestamp > opt.GetEndTime() {
			continue
		}
		var seriesKey []byte
		seriesKey = influx.Parse2SeriesKey(point.IndexKey, seriesKey, true)

		ptRec := transRowToRecordNew(&point, recSchema)
		newSeries := seriesData{tags: point.Tags, rec: ptRec}

		val, exist := mm[string(seriesKey)]
		if !exist {
			mm[string(seriesKey)] = &newSeries
		} else {
			if limit == 0 || (limit > 0 && val.rec.Len() < limit) {
				appendFieldValueToRecord(val.rec, point.Fields, point.Timestamp)
			}
		}
	}

	var ret sync.Map

	if len(filterFields) > 0 {
		idFields := make([]int, 0, 5)
		idTags := make([]string, 0, 4)
		var valueMap map[string]interface{}

		for _, f := range filterFields {
			idx := recSchema.FieldIndex(f.Val)
			if idx >= 0 && f.Type != influxql.Unknown {
				idFields = append(idFields, idx)
			} else if f.Type != influxql.Unknown {
				idTags = append(idTags, f.Val)
			}
		}
		valueMap = prepareForFilter(recSchema, idFields)

		// filter field value by cond, if filter rec is nil, remove from the map
		var emptyKeys []string
		filterOption := immutable.BaseFilterOptions{}
		for k, v := range mm {
			rec := v.rec
			filterOption.FiltersMap = make(map[string]*influxql.FilterMapValue)
			for key, value := range valueMap {
				filterOption.FiltersMap.SetFilterMapValue(key, value)
			}
			filterOption.FieldsIdx = idFields
			filterOption.FilterTags = idTags
			filterRec := immutable.FilterByField(rec, nil, &filterOption, opt.GetCondition(), nil, &v.tags, nil, nil)
			if filterRec == nil {
				emptyKeys = append(emptyKeys, k)
			} else {
				// todo: should use this method later
				// v.rec = kickFilterCol(filterRec, querySchema.GetColumnNames())
				v.rec = filterRec
			}
		}

		for i := range emptyKeys {
			delete(mm, emptyKeys[i])
		}
	}

	for k, v := range mm {
		ret.Store(k, *v.rec)
	}

	return &ret
}

func sameOffset(expectOffset, mergeOffset []uint32) bool {
	if len(expectOffset) == 0 && len(mergeOffset) == 0 {
		return true
	}
	return reflect.DeepEqual(expectOffset, mergeOffset)
}

func isRecEqual(mergeRec, expRec *record.Record) bool {
	for i := range mergeRec.Schema {
		if mergeRec.Schema[i].Name != expRec.Schema[i].Name || mergeRec.Schema[i].Type != expRec.Schema[i].Type {
			return false
		}
	}

	for i := range mergeRec.ColVals {
		if isColHaveNoData(&expRec.ColVals[i]) && isColHaveNoData(&mergeRec.ColVals[i]) {
			continue
		}
		if mergeRec.ColVals[i].Len != expRec.ColVals[i].Len ||
			mergeRec.ColVals[i].NilCount != expRec.ColVals[i].NilCount ||
			!bytes.Equal(mergeRec.ColVals[i].Val, expRec.ColVals[i].Val) ||
			!sameOffset(mergeRec.ColVals[i].Offset, expRec.ColVals[i].Offset) {
			return false
		}
		for j := 0; j < mergeRec.ColVals[i].Len; j++ {
			firstBitIndex := mergeRec.ColVals[i].BitMapOffset + j
			secBitIndex := expRec.ColVals[i].BitMapOffset + j
			firstBit, secBit := 0, 0
			if mergeRec.ColVals[i].Bitmap[firstBitIndex>>3]&record.BitMask[firstBitIndex&0x07] != 0 {
				firstBit = 1
			}
			if expRec.ColVals[i].Bitmap[secBitIndex>>3]&record.BitMask[secBitIndex&0x07] != 0 {
				secBit = 1
			}

			if firstBit != secBit {
				return false
			}
		}
	}

	return true
}

func isStringsEqual(firstStrings, secStrings []string) bool {
	if len(firstStrings) != len(secStrings) {
		return false
	}

	for i := range firstStrings {
		if !reflect.DeepEqual(firstStrings[i], secStrings[i]) {
			return false
		}
	}

	return true
}

func isColHaveNoData(cv *record.ColVal) bool {
	if cv.Val == nil && cv.NilCount == 0 && cv.Len == 0 && cv.Bitmap == nil && cv.BitMapOffset == 0 && cv.Offset == nil {
		return true
	}

	if cv.Len == cv.NilCount {
		return true
	}

	return false
}

func testRecsEqual(mergeRec, expRec *record.Record) bool {
	for i, v := range mergeRec.Schema {
		if isColHaveNoData(&expRec.ColVals[i]) && isColHaveNoData(&mergeRec.ColVals[i]) {
			continue
		}
		if v.Type == influx.Field_Type_Int {
			mergeIntegerVals := mergeRec.ColVals[i].IntegerValues()
			expIntegerVals := expRec.ColVals[i].IntegerValues()
			isEqual := reflect.DeepEqual(mergeIntegerVals, expIntegerVals)
			if !isEqual {
				return false
			}
		} else if v.Type == influx.Field_Type_Boolean {
			mergeBooleanVals := mergeRec.ColVals[i].BooleanValues()
			expBooleanVals := expRec.ColVals[i].BooleanValues()
			isEqual := reflect.DeepEqual(mergeBooleanVals, expBooleanVals)
			if !isEqual {
				return false
			}
		} else if v.Type == influx.Field_Type_Float {
			mergeFloatVals := mergeRec.ColVals[i].FloatValues()
			expFloatVals := expRec.ColVals[i].FloatValues()
			isEqual := reflect.DeepEqual(mergeFloatVals, expFloatVals)
			if !isEqual {
				return false
			}
		} else if v.Type == influx.Field_Type_String {
			mergeStrings := mergeRec.ColVals[i].StringValues(nil)
			expStrings := expRec.ColVals[i].StringValues(nil)
			isEqual := isStringsEqual(mergeStrings, expStrings)
			if !isEqual {
				return false
			}
		} else {
			panic("error type")
		}
	}

	return isRecEqual(mergeRec, expRec)
}

func closeChan(c chan struct{}) {
	select {
	case <-c:
	default:
		close(c)
	}
}

func checkQueryResultParallel(errs chan error, cursors []comm.KeyCursor, expectRecords *sync.Map, ascending bool, function checkSingleCursorFunction) {
	var total int
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(len(cursors))
	for _, cur := range cursors {
		go func(cur comm.KeyCursor) {
			if err := function(cur, expectRecords, &total, ascending, stop); err != nil {
				wg.Done()
				errs <- err
				closeChan(stop)
			} else {
				errs <- nil
				wg.Done()
			}
		}(cur)
	}
	wg.Wait()
	closeChan(stop)
}

func checkQueryResultForSingleCursorNew(cur comm.KeyCursor, expectRecords *sync.Map, total *int, ascending bool, stop chan struct{}) error {
	defer cur.Close()
	for {
		if groupCursor, isTagSet := cur.(*groupCursor); isTagSet {
			for i := range groupCursor.tagSetCursors {
				t := groupCursor.tagSetCursors[i].(*tagSetCursor)
				t.SetSchema(t.GetSchema())
			}
		}

		select {
		case <-stop:
			return nil
		default:
		}

		rec, _, err := cur.Next()
		if err != nil {
			return err
		}
		if rec == nil {
			break
		}
		f := func() error {
			key, index := rec.GetTagIndexAndKey()
			v := rec.ColVals[len(rec.ColVals)-1].IntegerValues()
			for i := 0; i < len(key)-1; i++ {
				r := bytes.Compare(executor.DecodeBytes(*key[i]), executor.DecodeBytes(*key[i+1]))
				if (r < 0) != ascending {
					return errors.New("wrong data tag order")
				}
				for j := index[i]; j < index[i+1]-1; j++ {
					if v[j] == v[j+1] {
						continue
					}
					if (v[j] < v[j+1]) != ascending {
						return errors.New("wrong data time order")
					}
				}
			}
			rowNum := rec.RowNums() - 1
			for j := index[len(index)-1]; j < rowNum; j++ {
				if v[j] == v[j+1] {
					continue
				}
				if (v[j] < v[j+1]) != ascending {
					return errors.New("wrong data time order")
				}
			}
			return nil
		}
		if e := f(); e != nil {
			return e
		}
		*total += rec.RowNums()
	}
	return nil
}

func checkAggQueryResultParallel(errs chan error, cursors []comm.KeyCursor, expectRecords *sync.Map, ascending bool, call string, result aggResult) {
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(len(cursors))
	for _, cur := range cursors {
		go func(cur comm.KeyCursor) {
			if err := checkAggFunc(cur, stop, result, call); err != nil {
				wg.Done()
				errs <- err
				closeChan(stop)
			} else {
				errs <- nil
				wg.Done()
			}
		}(cur)
	}
	wg.Wait()
	closeChan(stop)
}

func checkAggFunc(cur comm.KeyCursor, stop chan struct{}, result aggResult, call string) error {
	defer cur.Close()
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		rec, _, err := cur.Next()
		if err != nil {
			return err
		}
		if rec == nil || rec.RowNums() == 0 {
			return nil
		}
		if call == "count" {
			return nil
		} else {
			for i := 0; i < len(rec.Schema)-1; i++ {
				f := rec.Schema[i]
				if rec.ColVals[i].IsNil(0) {
					continue
				}
				switch f.Type {
				case influx.Field_Type_Float:
					if rec.ColVals[i].FloatValues()[0] != result[call][3].(float64) {
						return errors.New(fmt.Sprintf("unexpected value, expected: %f,actual: %f", result[call][3].(float64), rec.ColVals[i].FloatValues()[0]))
					}
				case influx.Field_Type_Int:
					if rec.ColVals[i].IntegerValues()[0] != result[call][1].(int64) {
						return errors.New(fmt.Sprintf("unexpected value, expected: %d,actual: %d", result[call][1].(int64), rec.ColVals[i].IntegerValues()[0]))
					}
				case influx.Field_Type_Boolean:
					if rec.ColVals[i].BooleanValues()[0] != result[call][2].(bool) {
						return errors.New(fmt.Sprintf("unexpected value, expected: %t,actual: %t", result[call][2].(bool), rec.ColVals[i].BooleanValues()[0]))
					}
				case influx.Field_Type_String:
					if s, _ := rec.ColVals[i].StringValueSafe(0); s != result[call][0].(string) {
						return errors.New(fmt.Sprintf("unexpected value, expected: %s,actual: %s", result[call][0].(string), s))
					}
				}
			}
		}
		if err != nil {
			return err
		}
		if rec == nil {
			break
		}
	}
	f := func() error {
		return nil
	}
	return f()
}

func checkRecord(query *record.Record, seriesKey string, expectRecords *sync.Map, name string) error {
	rowCnt := query.RowNums()
	val, ok := expectRecords.Load(seriesKey)
	if !ok {
		return fmt.Errorf("can not find record for series key %s", seriesKey)
	}
	r := val.(record.Record)
	var comp, res record.Record
	comp.SliceFromRecord(&r, 0, rowCnt)
	if !testRecsEqual(query, &comp) {
		return fmt.Errorf("%v record not equal, exp:%v\nget:%v", name, comp.String(), query.String())
	}
	// update map value
	rowNum := r.RowNums()
	if rowCnt < rowNum {
		res.SliceFromRecord(&r, rowCnt, rowNum)
		expectRecords.Store(seriesKey, res)
	}
	return nil
}

func checkQueryResultForSingleCursor(cur comm.KeyCursor, expectRecords *sync.Map, total *int, ascending bool, stop chan struct{}) error {
	defer cur.Close()

	name := cur.Name()
	/*	var preKey string
		var preMergeRec *comm.Record*/
	for {
		if groupCursor, isTagSet := cur.(*groupCursor); isTagSet {
			groupCursor.preAgg = true
			for i := range groupCursor.tagSetCursors {
				groupCursor.tagSetCursors[i].(*tagSetCursor).SetNextMethod()
			}
		}

		select {
		case <-stop:
			return nil
		default:
		}

		rec, info, err := cur.Next()
		if err != nil {
			return err
		}
		if rec == nil {
			break
		}
		key := info.GetSeriesKey()
		k := string(key)
		_, ok := expectRecords.Load(k)
		if !ok {
			return fmt.Errorf("key %s not exist", k)
		}
		if err = checkRecord(rec, k, expectRecords, name); err != nil {
			return err
		}

		*total += rec.RowNums()
	}
	return nil
}

type TestConfig struct {
	seriesNum         int
	pointNumPerSeries int
	interval          time.Duration
	short             bool
}

type TestCase struct {
	Name               string            // testcase name
	startTime, endTime int64             // query time range
	fieldAux           []influxql.VarRef // fields to select
	fieldFilter        string            // field filter condition
	dims               []string          // group by dimensions
	ascending          bool              // order by time
	expRecord          *record.Record    // expect record after filter by field
}

type LimitTestParas struct {
	limit  int
	offset int
}

func TestCreateGroupCursorWithLimiter(t *testing.T) {
	if e := resourceallocator.InitResAllocator(16, 2, 1, -1, resourceallocator.ChunkReaderRes, 0, 0); e == nil {
		t.Fatal()
	}
	if e := resourceallocator.InitResAllocator(16, 2, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0); e != nil {
		t.Fatal(e)
	}
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	seriesNum := 8
	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, startTime, endTime := GenDataRecord([]string{"mst"}, seriesNum, 1, 1, time.Now(), false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	c := TestCase{"AllField", startTime, endTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil}
	opt := genQueryOpt(&c, "mst", true)
	opt.Limit = 1
	opt.Offset = 1
	opt.GroupByAllDims = true
	querySchema := genQuerySchema(c.fieldAux, opt)

	_, span := tracing.NewTrace("root")
	ctx := tracing.NewContextWithSpan(context.Background(), span)
	cursors, err := sh.CreateCursor(ctx, querySchema)
	defer func() {
		_ = resourceallocator.FreeRes(resourceallocator.ChunkReaderRes, int64(seriesNum), int64(seriesNum))
		_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
	}()
	if len(cursors) != 8 {
		t.Fatal()
	}
	for i := range cursors {
		cursors[i].Close()
	}
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestShard_NewColStoreShard(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, false)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	require.Equal(t, 4*100, int(sh.rowCount))
	require.NoError(t, closeShard(sh))
}

func TestShard_NewColStoreShardWithPKIndex(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 1, len(files))
	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, pkInfo1.GetTCLocation(), colstore.DefaultTCLocation)
		require.EqualValues(t, pkInfo2.GetTCLocation(), colstore.DefaultTCLocation)
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
}

func TestShard_NewColStoreShardWithPKIndexAndTC(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:             []string{},
			PrimaryKey:          []string{"field1_string", "field2_int"},
			TimeClusterDuration: 60000000,
		},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 1, len(files))
	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, pkInfo1.GetTCLocation(), 0)
		require.EqualValues(t, pkInfo2.GetTCLocation(), 0)
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
}

func TestShard_NewColStoreShardWithPKIndexAndTCNilPrimaryKey(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:             []string{},
			PrimaryKey:          nil,
			TimeClusterDuration: 60000000,
		},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 1, len(files))
	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, pkInfo1.GetTCLocation(), 0)
		require.EqualValues(t, pkInfo2.GetTCLocation(), 0)
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
}

func TestShard_NewColStoreShardWithPKIndexMultiFiles(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxSegmentLimit(2)
	conf.SetMaxRowsPerSegment(5)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 25, len(files))

	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		require.EqualValues(t, pkInfo1.GetTCLocation(), colstore.DefaultTCLocation)
		require.EqualValues(t, pkInfo2.GetTCLocation(), colstore.DefaultTCLocation)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
	// recover config so that not to affect other tests
	conf.SetMaxSegmentLimit(immutable.DefaultMaxSegmentLimit4ColStore)
	conf.SetMaxRowsPerSegment(immutable.DefaultMaxRowsPerSegment4ColStore)
}

func TestShard_NewColStoreShardWithPKIndexAndTCMultiFiles(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxSegmentLimit(2)
	conf.SetMaxRowsPerSegment(5)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:             []string{},
			PrimaryKey:          []string{"field1_string", "field2_int"},
			TimeClusterDuration: 60000000,
		},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 25, len(files))

	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		require.EqualValues(t, pkInfo1.GetTCLocation(), 0)
		require.EqualValues(t, pkInfo2.GetTCLocation(), 0)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
	// recover config so that not to affect other tests
	conf.SetMaxSegmentLimit(immutable.DefaultMaxSegmentLimit4ColStore)
	conf.SetMaxRowsPerSegment(immutable.DefaultMaxRowsPerSegment4ColStore)
}

func TestShard_NewColStoreShardWithPKIndexAndTCNilPrimaryKeyMultiFiles(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxSegmentLimit(2)
	conf.SetMaxRowsPerSegment(5)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:             []string{},
			PrimaryKey:          nil,
			TimeClusterDuration: 60000000,
		},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	time.Sleep(3 * time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 25, len(files))

	pkFiles1 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	require.NoError(t, closeShard(sh))
	// reopen shard to load data
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	pkFiles2 := sh.GetTableStore().(*immutable.MmsTables).PKFiles
	for _, file := range files {
		pkInfo1, _ := pkFiles1[defaultMeasurementName].GetPKInfo(file)
		pkInfo2, _ := pkFiles2[defaultMeasurementName].GetPKInfo(file)
		require.EqualValues(t, pkInfo1.GetTCLocation(), 0)
		require.EqualValues(t, pkInfo2.GetTCLocation(), 0)
		rec1 := pkInfo1.GetRec().ColVals
		rec2 := pkInfo2.GetRec().ColVals
		require.EqualValues(t, len(rec1), len(rec2))
		for i := range rec1 {
			require.EqualValues(t, rec1[i].Val, rec2[i].Val)
		}

		mark1 := pkInfo1.GetMark()
		mark2 := pkInfo2.GetMark()
		require.EqualValues(t, mark1, mark2)

		schema1 := pkInfo1.GetRec().Schema
		schema2 := pkInfo2.GetRec().Schema
		require.EqualValues(t, schema1, schema2)

		meta1 := pkInfo1.GetRec().RecMeta
		meta2 := pkInfo2.GetRec().RecMeta
		require.EqualValues(t, meta1, meta2)
	}
	require.NoError(t, closeShard(sh))
	// recover config so that not to affect other tests
	conf.SetMaxSegmentLimit(immutable.DefaultMaxSegmentLimit4ColStore)
	conf.SetMaxRowsPerSegment(immutable.DefaultMaxRowsPerSegment4ColStore)
}

func TestColStoreWriteSkipIndex(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	list := make([]*influxql.IndexList, 1)
	iList := influxql.IndexList{IList: []string{"field1_string"}}
	list[0] = &iList
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, false)

	require.Equal(t, err, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	require.Equal(t, err, nil)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mstsInfo["cpu1"] = mstsInfo[defaultMeasurementName]
	mstsInfo["cpu1"].Name = "cpu1"
	sh.SetMstInfo(mstsInfo["cpu1"])
	err = sh.WriteCols("cpu1", rec, nil)
	require.Equal(t, err, nil)
	sh.ForceFlush()
	time.Sleep(3 * time.Second)
	if err = closeShard(sh); err != nil {
		t.Fatal(err)
	}
}

func TestColStoreWriteSkipIndexSwitchFile(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxSegmentLimit(2)
	conf.SetMaxRowsPerSegment(5)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	list := make([]*influxql.IndexList, 1)
	iList := influxql.IndexList{IList: []string{"field1_string"}}
	list[0] = &iList
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, false)

	require.Equal(t, err, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	require.Equal(t, err, nil)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mstsInfo["cpu1"] = mstsInfo[defaultMeasurementName]
	mstsInfo["cpu1"].Name = "cpu1"
	sh.SetMstInfo(mstsInfo["cpu1"])
	err = sh.WriteCols("cpu1", rec, nil)
	require.Equal(t, err, nil)
	sh.ForceFlush()
	time.Sleep(3 * time.Second)
	if err = closeShard(sh); err != nil {
		t.Fatal(err)
	}
}

func TestShard_AsyncWalReplay_serial(t *testing.T) {
	testDir := t.TempDir()
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{"mst"}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 4*100, int(sh.rowCount))
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
	copyOptions := DefaultEngineOption
	copyOptions.WalReplayAsync = true
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, copyOptions, config.TSSTORE)
	newSh.indexBuilder = sh.indexBuilder
	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}
	if err = writeData(newSh, rows, false); err != nil {
		t.Fatal(err)
	}
	for newSh.replayingWal {
		time.Sleep(10 * time.Millisecond)
		fmt.Println("wait load wal done")
	}
	require.Equal(t, 800, int(newSh.rowCount))

	if err = closeShard(newSh); err != nil {
		t.Fatal(err)
	}
}

func TestShard_AsyncWalReplay_parallel_withCancel(t *testing.T) {
	testDir := t.TempDir()
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{"mst"}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 4*100, int(sh.rowCount))
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
	copyOptions := DefaultEngineOption
	copyOptions.WalReplayAsync = true
	copyOptions.WalReplayParallel = true
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, copyOptions, config.TSSTORE)
	newSh.indexBuilder = sh.indexBuilder
	require.Equal(t, 0, len(newSh.wal.logReplay[0].fileNames))
	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}
	// cancel wal replay
	for newSh.cancelFn != nil {
		newSh.cancelFn()
	}

	if err = writeData(newSh, rows, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	require.GreaterOrEqual(t, 800, int(newSh.rowCount))

	if err = closeShard(newSh); err != nil {
		t.Fatal(err)
	}
}

func TestShard_AsyncWalReplay_serial_ArrowFlight(t *testing.T) {
	testDir := t.TempDir()
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	rec := genRecord()
	err = writeRec(sh, rec, false)
	if err != nil {
		t.Fatal(err)
	}
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	got := msInfo.GetWriteChunk().WriteRec.GetRecord()
	if !testRecsEqual(rec, got) {
		t.Fatal("error result")
	}
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
	copyOptions := DefaultEngineOption
	copyOptions.WalReplayAsync = true
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, copyOptions, config.COLUMNSTORE)
	newSh.indexBuilder = sh.indexBuilder
	newSh.SetMstInfo(mstsInfo[defaultMeasurementName])
	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}

	if err = writeRec(newSh, rec, false); err != nil {
		t.Fatal(err)
	}
	rec.AppendRec(rec, 0, rec.RowNums())
	msInfo, err = newSh.activeTbl.GetMsInfo(defaultMeasurementName)
	tmp := msInfo.GetWriteChunk().WriteRec.GetRecord()
	got.ResetWithSchema(tmp.Schema)
	got.AppendRec(tmp, 0, tmp.RowNums())
	for newSh.replayingWal {
		time.Sleep(13 * time.Millisecond)
		fmt.Println("wait load wal done")
	}

	hlp := record.NewSortHelper()
	rec = hlp.Sort(rec)
	got = hlp.Sort(got)

	if !testRecsEqual(rec, got) {
		t.Fatal("error result")
	}

	if err = closeShard(newSh); err != nil {
		t.Fatal(err)
	}
}

func TestShard_AsyncWalReplay_parallel_ArrowFlight(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	rec := genRecord()
	err = writeRec(sh, rec, false)
	if err != nil {
		t.Fatal(err)
	}
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	record := msInfo.GetWriteChunk().WriteRec.GetRecord()
	if !testRecsEqual(rec, record) {
		t.Fatal("error result")
	}
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
	copyOptions := DefaultEngineOption
	copyOptions.WalReplayAsync = true
	copyOptions.WalReplayParallel = true
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, copyOptions, config.COLUMNSTORE)
	newSh.indexBuilder = sh.indexBuilder
	newSh.SetMstInfo(mstsInfo[defaultMeasurementName])
	require.Equal(t, 0, len(newSh.wal.logReplay[0].fileNames))
	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}

	err = writeRec(newSh, rec, false)
	msInfo, err = newSh.activeTbl.GetMsInfo(defaultMeasurementName)
	record = msInfo.GetWriteChunk().WriteRec.GetRecord()
	rec.AppendRec(rec, 0, rec.RowNums())
	for newSh.replayingWal {
		time.Sleep(13 * time.Millisecond)
		fmt.Println("wait load wal done")
	}
	if !testRecsEqual(rec, record) {
		t.Fatal("error result")
	}

	if err = closeShard(newSh); err != nil {
		t.Fatal(err)
	}
}

func TestShard_AsyncWalReplay_parallel_ArrowFlight_WithCancel(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	sh.SetWriteColdDuration(time.Minute)

	// step2: write data
	rec := genRecord()
	err = writeRec(sh, rec, false)
	if err != nil {
		t.Fatal(err)
	}
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	record := msInfo.GetWriteChunk().WriteRec.GetRecord()
	if !testRecsEqual(rec, record) {
		t.Fatal("error result")
	}
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
	copyOptions := DefaultEngineOption
	copyOptions.WalReplayAsync = true
	copyOptions.WalReplayParallel = true
	shardIdent := &meta.ShardIdentifier{ShardID: sh.ident.ShardID, Policy: sh.ident.Policy, OwnerDb: sh.ident.OwnerDb, OwnerPt: sh.ident.OwnerPt}
	tr := &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}
	newSh := NewShard(sh.dataPath, sh.walPath, sh.lock, shardIdent, sh.durationInfo, tr, copyOptions, config.COLUMNSTORE)
	newSh.indexBuilder = sh.indexBuilder
	newSh.SetMstInfo(mstsInfo[defaultMeasurementName])
	require.Equal(t, 0, len(newSh.wal.logReplay[0].fileNames))
	if err = newSh.OpenAndEnable(nil); err != nil {
		t.Fatal(err)
	}
	// cancel wal replay
	for newSh.cancelFn != nil {
		newSh.cancelFn()
	}

	err = writeRec(newSh, rec, false)
	msInfo, err = newSh.activeTbl.GetMsInfo(defaultMeasurementName)
	record = msInfo.GetWriteChunk().WriteRec.GetRecord()
	if !testRecsEqual(rec, record) {
		t.Fatal("error result")
	}

	if err = closeShard(newSh); err != nil {
		t.Fatal(err)
	}
}

func TestChangeShardTierToWarm(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
	}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		rows, _, _ := GenDataRecord([]string{"cpu"}, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}

		sh.ChangeShardTierToWarm()
		if sh.tier != 2 {
			t.Fatal("Chang Shard to Warm Faled")
		}
		sh.endTime = mustParseTime(time.RFC3339Nano, "2019-01-01T01:00:00Z")
		sh.durationInfo.Tier = util.Warm
		tier := sh.GetTier()
		exp := sh.IsTierExpired()
		if tier != util.Warm || !exp {
			t.Fatal("Change Shard to Warm Failed")
		}
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestShardTierToDownSampleShard(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
	}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}
		sh.ident.ReadOnly = true

		rows, _, _ := GenDataRecord([]string{"cpu"}, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
		_ = writeData(sh, rows, true)
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestWriteIndexForColsError(t *testing.T) {
	testDir := t.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	record := genRecord()
	storage := sh.storage.(*columnstoreImpl)
	err = storage.WriteIndexForCols(sh, record, "cpu")
	require.Equal(t, err, errors.New("measurement info is not found"))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteRowsToColumnStoreError(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	ctx := getMstWriteCtx(time.Millisecond*10, config.TSSTORE)
	putMstWriteCtx(ctx)
	storage := sh.storage.(*columnstoreImpl)
	err = storage.updateMstMap(sh, rows, ctx)
	if err != nil {
		t.Fatal(err)
	}
	mmPoints := ctx.getMstMap()
	ctx.initWriteRowsCtx(sh.getLastFlushTime, sh.addRowCountsBySid, storage.mstsInfo)
	ctx.writeRowsCtx.MstsInfo.Delete(defaultMeasurementName)
	err = sh.activeTbl.MTable.WriteRows(sh.activeTbl, mmPoints, ctx.writeRowsCtx)
	require.Equal(t, err, errors.New("measurement info is not found"))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteExceptionError(t *testing.T) {
	testDir := t.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	sh.ident.ReadOnly = true

	setDownSampleWriteDrop(false)
	defer setDownSampleWriteDrop(true)

	rows, _, _ := GenDataRecord([]string{"cpu"}, 2, 2, time.Second, time.Now(), false, true, false)
	err = writeData(sh, rows, true)
	if err == nil {
		t.Fatal("setDownSampleWriteDrop failed")
	}

	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteInterruptError(t *testing.T) {
	testDir := t.TempDir()

	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	rows, _, _ := GenDataRecord([]string{"cpu"}, 2, 2, time.Second, time.Now(), false, true, false)

	sh.closed.Close()
	err = writeData(sh, rows, true)
	require.Equal(t, err.Error(), "shard closed 1")

	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}

}

func TestAggQueryOnlyInImmutable(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{1, 50, time.Second, false},
		{20, 1, time.Second, false},
		{30, 2, time.Second, false},
		{50, 3, time.Second, false},
	}
	for _, conf := range configs {
		fileops.EnableMmapRead(false)
		executor.EnableFileCursor(true)
		if testing.Short() && conf.short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, maxTime, result := GenAggDataRecord([]string{"cpu"}, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		// query data and judge
		cases := []TestAggCase{
			{"PartFieldFilter_all_columns", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}), "", nil, true, []string{"first", "last", "count"}},
			{"PartFieldFilter_all_columns_except_string", minTime, maxTime, createFieldAux([]string{"field2_int", "field3_bool", "field4_float"}), "", nil, true, []string{"min", "max", "first", "last", "count"}},
			{"PartFieldFilter_all_columns_int_float", minTime, maxTime, createFieldAux([]string{"field2_int", "field4_float"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
			{"PartFieldFilter_single_column_string", minTime, maxTime, createFieldAux([]string{"field1_string"}), "", nil, true, []string{"first", "last", "count"}},
			{"PartFieldFilter_single_column_int", minTime, maxTime, createFieldAux([]string{"field2_int"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
			{"PartFieldFilter_single_column_bool", minTime, maxTime, createFieldAux([]string{"field3_bool"}), "", nil, true, []string{"first", "last", "min", "max", "count"}},
			{"PartFieldFilter_single_column_float", minTime, maxTime, createFieldAux([]string{"field4_float"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
		}
		chunkSize := []int{1, 2}
		timeOrder := []bool{true, false}
		for _, ascending := range timeOrder {
			for _, c := range cases {
				for _, size := range chunkSize {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {
						for i := range c.aggCall {
							opt := genAggQueryOpt(&c, "cpu", ascending, size, conf.interval)
							calls := genCall(c.fieldAux, c.aggCall[i])
							querySchema := genAggQuerySchema(c.fieldAux, calls, opt)
							ops := genOps(c.fieldAux, calls)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							updateClusterCursor(cursors, ops, c.aggCall[i])
							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkAggQueryResultParallel(errs, cursors, m, ascending, c.aggCall[i], *result)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						}
					})
				}
			}

		}
		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
	executor.EnableFileCursor(false)
}

func TestAggQueryOnlyInMemtable(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{50, 3, time.Second, true},
	}
	for _, conf := range configs {
		fileops.EnableMmapRead(false)
		executor.EnableFileCursor(true)
		if testing.Short() && conf.short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatal(err)
		}
		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, _, _ := GenAggDataRecord([]string{"cpu"}, conf.seriesNum-10, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		rows2, _, maxTime, result := GenAggDataRecord([]string{"cpu"}, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now().Add(time.Second*1000), false, true, false)
		err = writeData(sh, rows2, false)
		if err != nil {
			t.Fatal(err)
		}
		sh.indexBuilder.Flush()
		// query data and judge
		cases := []TestAggCase{
			{"PartFieldFilter_single_column_int", minTime, maxTime, createFieldAux([]string{"field2_int"}), "", nil, true, []string{"min"}},
		}
		chunkSize := []int{1, 2}
		timeOrder := []bool{true, false}
		for _, ascending := range timeOrder {
			for _, c := range cases {
				for _, size := range chunkSize {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {
						for i := range c.aggCall {
							opt := genAggQueryOpt(&c, "cpu", ascending, size, conf.interval)
							calls := genCall(c.fieldAux, c.aggCall[i])
							querySchema := genAggQuerySchema(c.fieldAux, calls, opt)
							ops := genOps(c.fieldAux, calls)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							updateClusterCursor(cursors, ops, c.aggCall[i])
							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkAggQueryResultParallel(errs, cursors, m, ascending, c.aggCall[i], *result)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						}
					})
				}
			}

		}
		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
	executor.EnableFileCursor(false)
}

func TestAggQueryOnlyInImmutable_NoEmpty(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{2, 50, time.Second, false},
		{20, 1, time.Second, false},
		{30, 2, time.Second, false},
		{50, 3, time.Second, false},
	}
	for _, conf := range configs {
		fileops.EnableMmapRead(false)
		executor.EnableFileCursor(true)
		if testing.Short() && conf.short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, maxTime, result := GenAggDataRecord([]string{"cpu"}, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), true, true, true)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		// query data and judge
		cases := []TestAggCase{
			{"PartFieldFilter_all_columns", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}), "", nil, true, []string{"first", "last", "count"}},
			{"PartFieldFilter_all_columns_except_string", minTime, maxTime, createFieldAux([]string{"field2_int", "field3_bool", "field4_float"}), "", nil, true, []string{"min", "max", "first", "last", "count"}},
			{"PartFieldFilter_all_columns_int_float", minTime, maxTime, createFieldAux([]string{"field2_int", "field4_float"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
			{"PartFieldFilter_single_column_string", minTime, maxTime, createFieldAux([]string{"field1_string"}), "", nil, true, []string{"first", "last", "count"}},
			{"PartFieldFilter_single_column_int", minTime, maxTime, createFieldAux([]string{"field2_int"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
			{"PartFieldFilter_single_column_bool", minTime, maxTime, createFieldAux([]string{"field3_bool"}), "", nil, true, []string{"first", "last", "min", "max", "count"}},
			{"PartFieldFilter_single_column_float", minTime, maxTime, createFieldAux([]string{"field4_float"}), "", nil, true, []string{"first", "min", "max", "last", "count", "sum"}},
		}
		chunkSize := []int{1, 2}
		timeOrder := []bool{true, false}
		for _, ascending := range timeOrder {
			for _, c := range cases {
				for _, size := range chunkSize {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {
						for i := range c.aggCall {
							opt := genAggQueryOpt(&c, "cpu", ascending, size, conf.interval)
							calls := genCall(c.fieldAux, c.aggCall[i])
							querySchema := genAggQuerySchema(c.fieldAux, calls, opt)
							ops := genOps(c.fieldAux, calls)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							updateClusterCursor(cursors, ops, c.aggCall[i])
							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkAggQueryResultParallel(errs, cursors, m, ascending, c.aggCall[i], *result)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						}
					})
				}
			}

		}
		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
	executor.EnableFileCursor(false)
}

func TestAggQueryOnlyInMemtable_NoEmpty(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{50, 3, time.Second, false},
	}
	for _, conf := range configs {
		fileops.EnableMmapRead(false)
		executor.EnableFileCursor(true)
		if testing.Short() && conf.short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatal(err)
		}
		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, _, _ := GenAggDataRecord([]string{"cpu"}, conf.seriesNum-10, conf.pointNumPerSeries, conf.interval, time.Now(), true, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		rows2, _, maxTime, result := GenAggDataRecord([]string{"cpu"}, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now().Add(time.Second*1000), false, true, false)
		err = writeData(sh, rows2, false)
		if err != nil {
			t.Fatal(err)
		}
		sh.indexBuilder.Flush()
		// query data and judge
		cases := []TestAggCase{
			{"PartFieldFilter_single_column_int", minTime, maxTime, createFieldAux([]string{"field2_int"}), "", nil, true, []string{"min"}},
		}
		chunkSize := []int{1, 2}
		timeOrder := []bool{true, false}
		for _, ascending := range timeOrder {
			for _, c := range cases {
				for _, size := range chunkSize {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {
						for i := range c.aggCall {
							opt := genAggQueryOpt(&c, "cpu", ascending, size, conf.interval)
							calls := genCall(c.fieldAux, c.aggCall[i])
							querySchema := genAggQuerySchema(c.fieldAux, calls, opt)
							ops := genOps(c.fieldAux, calls)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							updateClusterCursor(cursors, ops, c.aggCall[i])
							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkAggQueryResultParallel(errs, cursors, m, ascending, c.aggCall[i], *result)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						}
					})
				}
			}

		}
		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
	executor.EnableFileCursor(false)
}

// data: all data in immutable
func TestQueryOnlyInImmutable(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
		{200, 1001, time.Second, true},
		{100, 2321, time.Second, true},
		{100000, 2, time.Second, true},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursor, checkQueryResultForSingleCursorNew}
	for _, f := range checkFunctions {
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			_ = os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllFieldFilter", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
					{"PartFieldFilter", minTime, maxTime, createFieldAux([]string{"field1_string", "field3_bool"}), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
					{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
					{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
					{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"OverlapTime2", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
					{"TagNotEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
				}

				timeOrder := []bool{true, false}
				for _, ascending := range timeOrder {
					for _, c := range cases {
						c := c
						ascending := ascending
						t.Run(c.Name, func(t *testing.T) {

							opt := genQueryOpt(&c, msNames[nameIdx], ascending)
							querySchema := genQuerySchema(c.fieldAux, opt)
							_, span := tracing.NewTrace("root")
							ctx := tracing.NewContextWithSpan(context.Background(), span)
							cursors, err := sh.CreateCursor(ctx, querySchema)
							if err != nil {
								t.Fatal(err)
							}

							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkQueryResultParallel(errs, cursors, m, ascending, f)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						})

					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// data: all data in immutable
func TestQueryOnlyInImmutableWithLimit(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 1001, time.Second, false},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursor, checkQueryResultForSingleCursorNew}
	for _, f := range checkFunctions {
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			_ = os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
					{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
					{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
					{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
					{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"OverlapTime", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
					{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
					{"TagNotEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
				}

				timeOrder := []bool{true, false}
				for _, ascending := range timeOrder {
					for _, c := range cases {
						c := c
						LimitTestCases := []LimitTestParas{
							{offset: 100, limit: 100},
						}
						for _, limitCase := range LimitTestCases {
							t.Run(c.Name, func(t *testing.T) {
								opt := genQueryOpt(&c, msNames[nameIdx], ascending)
								opt.Limit = limitCase.limit
								opt.Offset = limitCase.offset
								querySchema := genQuerySchema(c.fieldAux, opt)

								cursors, err := sh.CreateCursor(context.Background(), querySchema)
								if err != nil {
									t.Fatal(err)
								}

								// step5: loop all cursors to query data from shard
								// key is indexKey, value is Record
								m := genExpectRecordsMap(rows, querySchema)
								errs := make(chan error, len(cursors))
								checkQueryResultParallel(errs, cursors, m, ascending, f)
								close(errs)
								for i := 0; i < len(cursors); i++ {
									err = <-errs
									if err != nil {
										t.Fatal(err)
									}
								}
							})
						}
					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestQueryOnlyInImmutableWithLimit_Lazy(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursorNew}
	for _, f := range checkFunctions {
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			_ = os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
				}

				timeOrder := []bool{true, false}
				for _, ascending := range timeOrder {
					for _, c := range cases {
						c := c
						LimitTestCases := []LimitTestParas{
							{offset: 0, limit: 1},
						}
						for _, limitCase := range LimitTestCases {
							t.Run(c.Name, func(t *testing.T) {
								opt := genQueryOpt(&c, msNames[nameIdx], ascending)
								opt.Limit = limitCase.limit
								opt.Offset = limitCase.offset
								opt.GroupByAllDims = true
								querySchema := genQuerySchema(c.fieldAux, opt)

								_, span := tracing.NewTrace("root")
								ctx := tracing.NewContextWithSpan(context.Background(), span)
								cursors, err := sh.CreateCursor(ctx, querySchema)

								if err != nil {
									t.Fatal(err)
								}
								childPlan := executor.NewLogicalSeries(querySchema)
								plan := executor.NewLogicalMerge([]hybridqp.QueryNode{childPlan}, querySchema)
								for _, g := range cursors {
									group := g.(*groupCursor)
									for _, ts := range group.tagSetCursors {
										ts.(*tagSetCursor).lazyTagSetCursorPara.plan = plan
										_, childSpan := tracing.NewTrace("root")
										ts.(*tagSetCursor).span = childSpan
									}
								}

								// step5: loop all cursors to query data from shard
								// key is indexKey, value is Record
								m := genExpectRecordsMap(rows, querySchema)
								errs := make(chan error, len(cursors))
								checkQueryResultParallel(errs, cursors, m, ascending, f)
								close(errs)
								for i := 0; i < len(cursors); i++ {
									err = <-errs
									if err != nil {
										t.Fatal(err)
									}
								}
							})
						}
					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestQueryOnlyInImmutableWithLimitOptimize(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 1001, time.Second, false},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursor, checkQueryResultForSingleCursorNew}
	for _, f := range checkFunctions {
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			_ = os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			nowTime := time.Now()

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, nowTime, false, true, true)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}

			rows2, _, _ := GenDataRecord(msNames, configs[index].seriesNum-100, configs[index].pointNumPerSeries, configs[index].interval, nowTime, false, true, true)
			err = writeData(sh, rows2, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllField", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
				}

				timeOrder := []bool{true, false}
				for _, ascending := range timeOrder {
					for _, c := range cases {
						c := c
						LimitTestCases := []LimitTestParas{
							{offset: 1, limit: 5},
						}
						for _, limitCase := range LimitTestCases {
							t.Run(c.Name, func(t *testing.T) {
								opt := genQueryOpt(&c, msNames[nameIdx], ascending)
								opt.Limit = limitCase.limit
								opt.Offset = limitCase.offset
								querySchema := genQuerySchema(c.fieldAux, opt)

								cursors, err := sh.CreateCursor(context.Background(), querySchema)
								if err != nil {
									t.Fatal(err)
								}

								// step5: loop all cursors to query data from shard
								// key is indexKey, value is Record
								m := genExpectRecordsMap(rows, querySchema)
								errs := make(chan error, len(cursors))
								checkQueryResultParallel(errs, cursors, m, ascending, f)
								close(errs)
								for i := 0; i < len(cursors); i++ {
									err = <-errs
									if err != nil {
										t.Fatal(err)
									}
								}
							})
						}
					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestQueryOnlyInImmutableWithLimitWithGroupBy(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
		{200, 1001, time.Second, true},
		{100, 2321, time.Second, true},
	}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursor, checkQueryResultForSingleCursorNew}
	for _, f := range checkFunctions {
		msNames := []string{"cpu", "cpu1", "disk"}
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllDims", minTime, maxTime, createFieldAux(nil), "", []string{"tagkey1", "tagkey2", "tagkey3", "tagkey4"}, false, nil},
					{"PartDims1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", []string{"tagkey1", "tagkey2"}, false, nil},
					{"PartDims2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", []string{"tagkey3", "tagkey4"}, false, nil},
					{"PartDims3", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", []string{"tagkey1"}, false, nil},
				}

				for _, c := range cases {
					c := c
					LimitTestCases := []LimitTestParas{
						{offset: 100, limit: 100},
					}
					ascending := true
					for _, limitCase := range LimitTestCases {
						t.Run(c.Name, func(t *testing.T) {
							opt := genQueryOpt(&c, msNames[nameIdx], ascending)
							opt.Limit = limitCase.limit
							opt.Offset = limitCase.offset
							querySchema := genQuerySchema(c.fieldAux, opt)

							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							childPlan := executor.NewLogicalSeries(querySchema)
							plan := executor.NewLogicalMerge([]hybridqp.QueryNode{childPlan}, querySchema)
							for _, g := range cursors {
								group := g.(*groupCursor)
								for _, ts := range group.tagSetCursors {
									ts.(*tagSetCursor).lazyTagSetCursorPara.plan = plan
								}
							}
							checkQueryResultParallel(errs, cursors, m, ascending, f)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						})
					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// data: all data in immutable
func TestQueryOnlyInImmutableGroupBy(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
		{200, 1001, time.Second, true},
		{100, 2321, time.Second, true},
	}
	checkFunctions := []checkSingleCursorFunction{checkQueryResultForSingleCursor, checkQueryResultForSingleCursorNew}
	msNames := []string{"cpu", "cpu1", "disk"}
	for _, f := range checkFunctions {
		for index := range configs {
			if testing.Short() && configs[index].short {
				t.Skip("skipping test in short mode.")
			}
			// step1: clean env
			_ = os.RemoveAll(testDir)

			// step2: create shard
			sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
			if err != nil {
				t.Fatal(err)
			}

			// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
			rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
			err = writeData(sh, rows, true)
			if err != nil {
				t.Fatal(err)
			}
			for nameIdx := range msNames {
				// query data and judge
				cases := []TestCase{
					{"AllDims", minTime, maxTime, createFieldAux(nil), "", []string{"tagkey1", "tagkey2", "tagkey3", "tagkey4"}, false, nil},
					{"PartDims1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", []string{"tagkey1", "tagkey2"}, false, nil},
					{"PartDims2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", []string{"tagkey3", "tagkey4"}, false, nil},
					{"PartDims3", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", []string{"tagkey1"}, false, nil},
				}

				timeOrder := []bool{true, false}

				for _, ascending := range timeOrder {
					for _, c := range cases {
						c := c
						ascending := ascending
						t.Run(c.Name, func(t *testing.T) {
							opt := genQueryOpt(&c, msNames[nameIdx], ascending)
							querySchema := genQuerySchema(c.fieldAux, opt)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkQueryResultParallel(errs, cursors, m, ascending, f)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						})
					}
				}
			}
			// step6: close shard
			err = closeShard(sh)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

// data: all data in mutable
func TestQueryOnlyInMutableTable(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{1001, 1, time.Second, false},
		{200, 2, time.Second, false},
		{200, 1001, time.Second, false},
		{100, 2321, time.Second, true},
		{100000, 2, time.Second, true},
	}
	msNames := []string{"cpu", "mem", "disk"}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE, 1000*time.Second) // not flush data to snapshot
		if err != nil {
			t.Fatal(err)
		}

		mutable.SetSizeLimit(10000000000)

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
		err = writeData(sh, rows, false)
		if err != nil {
			t.Fatal(err)
		}
		for nameIdx := range msNames {
			// query data and judge
			cases := []TestCase{
				{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
				{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"OverlapTime", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
				{"TagNotEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
			}

			timeOrder := []bool{true, false}

			for _, ascending := range timeOrder {
				for _, c := range cases {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {

						opt := genQueryOpt(&c, msNames[nameIdx], ascending)
						querySchema := genQuerySchema(c.fieldAux, opt)
						cursors, err := sh.CreateCursor(context.Background(), querySchema)
						if err != nil {
							t.Fatal(err)
						}

						// step5: loop all cursors to query data from shard
						// key is indexKey, value is Record
						m := genExpectRecordsMap(rows, querySchema)

						errs := make(chan error, len(cursors))
						checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)
						close(errs)
						for i := 0; i < len(cursors); i++ {
							err = <-errs
							if err != nil {
								t.Fatal(err)
							}
						}
					})
				}
			}

		}

		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// data: all data in immutable, write twice, time range are equal
// select tag/field: no tag, all field
// condition: no tag filter, query different time range
func TestQueryImmutableUnorderedNoOverlap(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
		{200, 1001, time.Second, false},
		{100, 2321, time.Second, false},
		{100000, 2, time.Second, true},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		_ = os.RemoveAll(testDir)
		// create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// first write
		tm := time.Now()
		rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm, false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}

		// second write, same data(shardkey and timestamp)
		tm2 := tm
		rows2, minTime2, maxTime2 := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm2, false, true, false)
		err = writeData(sh, rows2, true)
		if err != nil {
			t.Fatal(err)
		}

		if minTime != minTime2 || maxTime != maxTime2 {
			t.Fatalf("time range error, %d, %d", maxTime2, minTime)
		}
		for nameIdx := range msNames {

			// query data and judge
			cases := []TestCase{
				{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
				{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"OverlapTime1", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
			}

			timeOrder := []bool{true, false}

			for _, ascending := range timeOrder {
				for _, c := range cases {
					c := c
					t.Run(c.Name, func(t *testing.T) {

						opt := genQueryOpt(&c, msNames[nameIdx], ascending)
						querySchema := genQuerySchema(c.fieldAux, opt)
						cursors, err := sh.CreateCursor(context.Background(), querySchema)
						if err != nil {
							t.Fatal(err)
						}

						// step5: loop all cursors to query data from shard
						// key is indexKey, value is Record
						m := genExpectRecordsMap(rows2, querySchema)

						errs := make(chan error, len(cursors))
						checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)
						close(errs)
						for i := 0; i < len(cursors); i++ {
							err = <-errs
							if err != nil {
								t.Fatal(err)
							}
						}
					})
				}
			}
		}

		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// data: all data in mutable, write twice, time range are equal
// select tag/field: no tag, all field
// condition: no tag filter, query different time range
func TestQueryMutableUnorderedNoOverlap(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
		{200, 1001, time.Second, true},
		{100, 2321, time.Second, true},
		{100000, 2, time.Second, true},
	}
	msNames := []string{"cpu", "mem", "disk"}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		_ = os.RemoveAll(testDir)
		// create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// first write
		tm := time.Now()
		rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm, false, true, false)
		err = writeData(sh, rows, false)
		if err != nil {
			t.Fatal(err)
		}

		// second write, same data(shardkey and timestamp)
		tm2 := tm
		rows2, minTime2, maxTime2 := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm2, false, true, false)
		err = writeData(sh, rows2, false)
		if err != nil {
			t.Fatal(err)
		}

		if minTime != minTime2 || maxTime != maxTime2 {
			t.Fatalf("time range error, %d, %d", maxTime2, minTime)
		}
		for nameIdx := range msNames {
			// query data and judge
			cases := []TestCase{
				{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
				{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"OverlapTime", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, true, nil},
			}

			timeOrder := []bool{true, false}

			for _, ascending := range timeOrder {
				for _, c := range cases {
					c := c
					t.Run(c.Name, func(t *testing.T) {
						opt := genQueryOpt(&c, msNames[nameIdx], ascending)
						querySchema := genQuerySchema(c.fieldAux, opt)
						cursors, err := sh.CreateCursor(context.Background(), querySchema)
						if err != nil {
							t.Fatal(err)
						}

						// step5: loop all cursors to query data from shard
						// key is indexKey, value is Record
						m := genExpectRecordsMap(rows2, querySchema)

						errs := make(chan error, len(cursors))
						checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)
						close(errs)
						for i := 0; i < len(cursors); i++ {
							err = <-errs
							if err != nil {
								t.Fatal(err)
							}
						}
					})
				}
			}
		}

		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// data: all data in mutable, write twice, time range are equal
// select tag/field: no tag, all field
// condition: no tag filter, query different time range
func TestQueryImmutableSequenceWrite(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{1001, 1, time.Second, false},
		{20, 2, time.Second, false},
		{2, 1001, time.Second, false},
		{100, 2321, time.Second, true},
	}
	msNames := []string{"cpu", "mem", "disk"}
	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		_ = os.RemoveAll(testDir)
		// create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// first write
		tm := time.Now().Truncate(time.Second)
		rows, minTime, maxTime := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm, false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}

		// second write, start time larger than last write end time
		tm2 := time.Unix(0, maxTime+int64(time.Second))
		rows2, minTime2, maxTime2 := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, tm2, false, true, false)
		err = writeData(sh, rows2, true)
		if err != nil {
			t.Fatal(err)
		}

		totalRow := rows
		totalRow = append(totalRow, rows2...)
		if maxTime >= minTime2 || minTime2 > maxTime2 {
			t.Fatalf("time range error, %d, %d", maxTime2, minTime)
		}
		for nameIdx := range msNames {
			// query data and judge
			cases := []TestCase{
				{"AllField", minTime, maxTime2, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, true, nil},
				{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"BeyondMaxTime", maxTime2 + int64(time.Second), maxTime2 + int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"OverlapTime", minTime + 20*int64(time.Second), maxTime2 - 10*int64(time.Second), createFieldAux(nil), "", nil, true, nil},
				{"partField1", minTime, maxTime2, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"partField2", minTime, maxTime2, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, true, nil},
				{"BeyondMaxTime + partField2", maxTime2 + int64(time.Second), maxTime2 + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"OverlapTime2", minTime + 10*int64(time.Second), maxTime2 - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, true, nil},
				{"TagEqual", minTime, maxTime2, createFieldAux(nil), "", nil, true, nil},
			}

			for _, c := range cases {
				c := c
				ascending := true
				t.Run(c.Name, func(t *testing.T) {
					opt := genQueryOpt(&c, msNames[nameIdx], ascending)
					querySchema := genQuerySchema(c.fieldAux, opt)
					cursors, err := sh.CreateCursor(context.Background(), querySchema)
					if err != nil {
						t.Fatal(err)
					}

					// step5: loop all cursors to query data from shard
					// key is indexKey, value is Record
					m := genExpectRecordsMap(totalRow, querySchema)
					errs := make(chan error, len(cursors))
					checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)
					close(errs)
					for i := 0; i < len(cursors); i++ {
						err = <-errs
						if err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}

		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueryOnlyInImmutableReload(t *testing.T) {
	testDir := t.TempDir()
	conf := TestConfig{100, 1001, time.Second, false}
	if testing.Short() && conf.short {
		t.Skip("skipping test in short mode.")
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime := GenDataRecord(msNames, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	if err = sh.immTables.Close(); err != nil {
		t.Fatal(err)
	}

	lockPath := ""
	sh.immTables = immutable.NewTableStore(sh.filesPath, &lockPath, &sh.tier, true, immutable.NewTsStoreConfig())
	sh.immTables.SetImmTableType(config.TSSTORE)
	if _, err = sh.immTables.Open(); err != nil {
		t.Fatal(err)
	}

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false, nil},
			{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, false, nil},
			{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, false, nil},
			{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"OverlapTime", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, false, nil},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				cursors, err := sh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}

				// step5: loop all cursors to query data from shard
				// key is indexKey, value is Record
				m := genExpectRecordsMap(rows, querySchema)
				errs := make(chan error, len(cursors))
				checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)

				close(errs)
				for i := 0; i < len(cursors); i++ {
					err = <-errs
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
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

// generate new record by query schema, need rid cols which only used for filter
// eg, select f1, f2 from mst where f3 > 1, we should kick column 'f3'
func kickFilterCol(rec *record.Record, fields []string) *record.Record {
	var dstSchema record.Schemas
	ridIdx := make(map[int]struct{}, 0)

	for i := range rec.Schema {
		srcSchema := rec.Schema[i]
		for j := range fields {
			if srcSchema.Name == fields[j] || srcSchema.Name == record.TimeField {
				dstSchema = append(dstSchema, record.Field{Name: srcSchema.Name, Type: srcSchema.Type})
				ridIdx[i] = struct{}{}
				break
			}
		}
	}

	dstRec := record.NewRecordBuilder(dstSchema)
	rowNum := rec.RowNums()
	srcSchema := rec.Schema
	var dstIdx int
	for i := range srcSchema {
		if _, exist := ridIdx[i]; exist {
			dstRec.ColVals[dstIdx].AppendColVal(&rec.ColVals[i], srcSchema[i].Type, 0, rowNum)
			dstIdx++
		}
	}
	return dstRec
}

func prepareForFilter(schema record.Schemas, idFields []int) map[string]interface{} {
	initMap := map[int]interface{}{
		influx.Field_Type_Float:   (*float64)(nil),
		influx.Field_Type_Int:     (*int64)(nil),
		influx.Field_Type_String:  (*string)(nil),
		influx.Field_Type_Boolean: (*bool)(nil),
	}
	// init map
	valueMap := make(map[string]interface{})
	for _, id := range idFields {
		valueMap[schema[id].Name] = initMap[schema[id].Type]
	}
	return valueMap
}

func TestCheckRecordLen(t *testing.T) {
	tagsetCursor := &tagSetCursor{
		limitCount:      0,
		outOfLimitBound: false,
		schema: genQuerySchema(nil, &query.ProcessorOptions{
			Limit:       10,
			Offset:      10,
			ChunkedSize: 1000,
		}),
		heapCursor: &heapCursor{items: make([]*TagSetCursorItem, 10)},
	}
	if !tagsetCursor.CheckRecordLen() {
		t.Fatal("result wrong")
	}
}

func TestFreeSequencer(t *testing.T) {
	dir := t.TempDir()
	msNames := []string{"cpu", "cpu1"}
	lockPath := ""

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	sh.ident.ShardType = influxql.RANGE
	if err = sh.NewShardKeyIdx("range", sh.dataPath, &lockPath); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 20, time.Second, tm, false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	primaryIndex := sh.indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)

	id, err := primaryIndex.GetSeriesIdBySeriesKey(rows[0].IndexKey, bytesutil.ToUnsafeBytes(msNames[0]))
	if err != nil {
		t.Fatal(err)
	}

	_, err = sh.immTables.GetRowCountsBySid("cpu", id)
	if err != nil {
		t.Fatal(err)
	}

	seq := sh.immTables.Sequencer()
	lastFlushTime, rowCnt := seq.Get(msNames[0], id)
	seq.UnRef()

	assert2.Equal(t, true, rowCnt != 0)
	// free sequencer
	if !sh.immTables.FreeSequencer() {
		t.Fatal(fmt.Errorf("free sequencer failed"))
	}

	// cannot get lastFlushTime after free
	seq = sh.immTables.Sequencer()
	_, rowCnt2 := seq.Get(msNames[0], id)
	seq.UnRef()
	assert2.Equal(t, true, rowCnt2 == 0)

	// sequencer will be reloaded after write rows
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	seq = sh.immTables.Sequencer()
	isloading := seq.IsLoading()
	for isloading {
		// waiting sequencer load success
		isloading = seq.IsLoading()
		time.Sleep(time.Second / 10)
	}
	lastFlushTime2, _ := seq.Get(msNames[0], id)
	seq.UnRef()
	assert2.Equal(t, true, lastFlushTime == lastFlushTime2)
}

func TestShard_GetSplitPoints(t *testing.T) {
	dir := t.TempDir()
	msNames := []string{"cpu"}
	lockPath := ""

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	sh.ident.ShardType = influxql.RANGE
	if err = sh.NewShardKeyIdx("range", sh.dataPath, &lockPath); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()
	tm := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord(msNames, 10, 20, time.Second, tm, false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	sh.skIdx.ForceFlush()
	fmt.Printf("rows len %d\n", len(rows))
	splitKeys, err := sh.GetSplitPoints([]int64{100})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("split keys %+v\n", splitKeys)
}

func TestDropMeasurement(t *testing.T) {
	testDir := t.TempDir()
	msNames := []string{"cpu", "cpu1"}

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}

	tm := time.Now().Truncate(time.Second)
	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, _, _ := GenDataRecord(msNames, 10, 20, time.Second, tm, false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	tm = tm.Add(time.Second * 10)
	rows, _, _ = GenDataRecord(msNames, 10, 20, time.Second, tm, false, true, false)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	store := sh.GetTableStore()

	if store.GetOutOfOrderFileNum() != 2 {
		t.Fatal("store.GetOutOfOrderFileNum() != 2")
	}
	orderFiles, unorderedFiles, _ := store.GetBothFilesRef(msNames[0], false, util.TimeRange{}, nil)
	if len(unorderedFiles) != 1 {
		t.Fatalf("len(unorderedFiles) != 1")
	}
	if len(orderFiles) != 2 {
		t.Fatalf("len(orderFiles) != 2")
	}

	immutable.UnrefFiles(orderFiles...)
	immutable.UnrefFiles(unorderedFiles...)
	if err := sh.DropMeasurement(context.TODO(), msNames[0]); err != nil {
		t.Fatal(err)
	}

	orderFiles, unorderedFiles, _ = store.GetBothFilesRef(msNames[1], false, util.TimeRange{}, nil)
	if len(unorderedFiles) != 1 {
		t.Fatalf("len(unorderedFiles) != 1")
	}
	if len(orderFiles) != 2 {
		t.Fatalf("len(orderFiles) != 2")
	}
	immutable.UnrefFiles(orderFiles...)
	immutable.UnrefFiles(unorderedFiles...)
	if err := sh.DropMeasurement(context.TODO(), msNames[1]); err != nil {
		t.Fatal(err)
	}

	if store.GetOutOfOrderFileNum() != 0 {
		t.Fatal("store.GetOutOfOrderFileNum() != 0")
	}

	// step4: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDropMeasurementOnWalReplay(t *testing.T) {
	sh := &shard{stopDownSample: util.NewSignal()}
	sh.replayingWal = true
	err := sh.DropMeasurement(context.Background(), "mst")
	require.EqualError(t, err, "async replay wal not finish")
}

func TestDropMeasurementForColStore(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxSegmentLimit(2)
	conf.SetMaxRowsPerSegment(5)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{"field1_string", "field2_int"}},
		Schema: map[string]int32{
			"field1_string": influx.Field_Type_String,
			"field2_int":    influx.Field_Type_Int}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	err = writeData(sh, rows, true)
	sh.ForceFlush()
	sh.waitSnapshot()
	sh.indexBuilder.Flush()
	time.Sleep(time.Second)
	require.Equal(t, err, nil)
	engineType := sh.GetEngineType()
	require.Equal(t, config.COLUMNSTORE, engineType)
	dataPath := sh.GetDataPath()
	var files []string
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 25, len(files))

	if err := sh.DropMeasurement(context.TODO(), defaultMeasurementName); err != nil {
		t.Fatal(err)
	}

	files = files[:0]

	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".idx" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("index does not exist!")
	}
	require.Equal(t, 0, len(files))
	// recover config so that not to affect other tests
	conf.SetMaxSegmentLimit(immutable.DefaultMaxSegmentLimit4ColStore)
	conf.SetMaxRowsPerSegment(immutable.DefaultMaxRowsPerSegment4ColStore)

	// step4: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_DropMeasurement(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = eng.Close()
	}()

	msNames := []string{"cpu", "cpu1"}
	seriesNum := 10
	tm := time.Now().Truncate(time.Second)
	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, _, _ := GenDataRecord(msNames, seriesNum, 200, time.Second, tm, false, true, false)
	for len(rows) > 0 {
		if len(rows) > 200 {
			if err := eng.WriteRows("db0", "rp0", 0, 1, rows[:200], nil); err != nil {
				t.Fatal(err)
			}
			rows = rows[100:]
			continue
		}

		if err := eng.WriteRows("db0", "rp0", 0, 1, rows, nil); err != nil {
			t.Fatal(err)
		}
		rows = rows[len(rows):]
	}

	dbInfo := eng.DBPartitions["db0"][0]
	idx := dbInfo.indexBuilder[659].GetPrimaryIndex().(*tsi.MergeSetIndex)
	idx.DebugFlush()

	ret, err := eng.SeriesExactCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0]), []byte(msNames[1])}, nil, globalTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != len(msNames) {
		t.Fatalf("len(ret) != %v", len(msNames))
	}
	for _, mst := range msNames {
		n, ok := ret[mst]
		if !ok || n < 1 {
			t.Fatalf("series cardinality < %v", 1)
		}
	}

	seriesKeys, err := eng.SeriesKeys("db0", []uint32{0}, [][]byte{[]byte(msNames[0]), []byte(msNames[1])}, nil, globalTime)
	if err != nil {
		t.Fatal(err)
	}

	if seriesNum != len(seriesKeys) {
		t.Fatalf("len(ret) != %v", seriesNum)
	}

	if err = eng.DropMeasurement("db0", "rp0", msNames[0], []uint64{1}); err != nil {
		t.Fatal(err)
	}

	ret, err = eng.SeriesExactCardinality("db0", []uint32{0}, [][]byte{[]byte(msNames[0]), []byte(msNames[1])}, nil, globalTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret) != 2 {
		t.Fatalf("len(ret) != %v", 1)
	}

	n, _ := ret[msNames[0]]
	if n != 5 {
		t.Fatalf("index dropped fail, %v exist after drop", msNames[0])
	}

	n, ok := ret[msNames[1]]
	if !ok || n < 1 {
		t.Fatalf("ret[i].Name < %v", msNames[1])
	}
}

func TestEngine_GetShard(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = eng.Close()
	}()

	sh, err := eng.GetShard("db0", uint32(0), uint64(1))
	assert2.NoError(t, err)
	assert2.NotEmpty(t, sh)

	sh2, err := eng.GetShard("db0", uint32(0), uint64(10))
	assert2.NoError(t, err)
	assert2.Nil(t, sh2)
}

func TestEngine_Statistics_Shard(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{200, 2, time.Second, false},
	}
	msNames := []string{"cpu", "cpu1", "disk"}
	var sh *shard
	var err error
	defer func() {
		if sh != nil {
			_ = closeShard(sh)
		}
	}()

	for index := range configs {
		if testing.Short() && configs[index].short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		if sh != nil {
			_ = closeShard(sh)
			_ = os.RemoveAll(testDir)
			sh = nil
		}

		// step2: create shard
		sh, err = createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, _, _ := GenDataRecord(msNames, configs[index].seriesNum, configs[index].pointNumPerSeries, configs[index].interval, time.Now(), false, true, false)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}

		var bufferPool = bufferpool.NewByteBufferPool(0, 0, bufferpool.MaxLocalCacheLen)
		buf := bufferPool.Get()
		buf, err = sh.GetStatistics(buf)
		if err != nil {
			t.Fatal(err)
		}
		require.Contains(t, string(buf), "level")
		require.Contains(t, string(buf), "database")
		require.Contains(t, string(buf), "retentionPolicy")

	}
}

func TestSnapshotLimitTsspFiles(t *testing.T) {
	testDir := t.TempDir()
	msNames := []string{"cpu"}
	// step1: clean env
	_ = os.RemoveAll(testDir)

	// step2: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
	if err != nil {
		t.Fatal(err)
	}

	var rows []influx.Row
	var minTime, maxTime int64
	st := time.Now().Truncate(time.Second)

	immutable.SetMaxRowsPerSegment4TsStore(16)
	immutable.SetMaxSegmentLimit4TsStore(5)
	lines := 16*5 - 1
	// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
	rows, minTime, maxTime = GenDataRecord([]string{"mst"}, 4, lines, time.Second, st, true, true, false, 1)
	st = time.Unix(0, maxTime+time.Second.Nanoseconds())
	rows1, min, max := GenDataRecord([]string{"mst"}, 1, lines+1+10, time.Second, st, true, true, false, 1+4)
	rows = append(rows, rows1...)
	if min < minTime {
		minTime = min
	}
	if max > maxTime {
		maxTime = max
	}

	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}

	files, _, _ := sh.immTables.GetBothFilesRef("mst", false, util.TimeRange{}, nil)
	if len(files) != 2 {
		t.Fatalf("wire fail, exp:2 files, get:%v files", len(files))
	}
	immutable.UnrefFiles(files...)

	st = time.Unix(0, minTime)
	rows1, _, _ = GenDataRecord([]string{"mst"}, 1, lines+1+10, time.Second, st, true, true, false, 1+4)
	err = writeData(sh, rows1, true)
	if err != nil {
		t.Fatal(err)
	}

	_, files, _ = sh.immTables.GetBothFilesRef("mst", false, util.TimeRange{}, nil)
	if len(files) != 2 {
		t.Fatalf("wire fail, exp:2 files, get:%v files", len(files))
	}
	immutable.UnrefFiles(files...)

	for nameIdx := range msNames {
		// query data and judge
		cases := []TestCase{
			{"AllField", minTime, maxTime, createFieldAux(nil), "field2_int < 5 AND field4_float < 10.0", nil, false, nil},
			{"BelowMinTime", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"BeyondMaxTime", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"OverlapTime", minTime + 20*int64(time.Second), maxTime - 10*int64(time.Second), createFieldAux(nil), "", nil, false, nil},
			{"partField1", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int"}), "", nil, false, nil},
			{"partField2", minTime, maxTime, createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"BelowMinTime + partField1", minTime - int64(time.Second), minTime - int64(time.Second), createFieldAux([]string{"field1_string", "field2_int"}), "", nil, false, nil},
			{"BeyondMaxTime + partField2", maxTime + int64(time.Second), maxTime + int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"OverlapTime", minTime + 10*int64(time.Second), maxTime - 30*int64(time.Second), createFieldAux([]string{"field3_bool", "field4_float"}), "", nil, false, nil},
			{"TagEqual", minTime, maxTime, createFieldAux(nil), "", nil, false, nil},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				cursors, err := sh.CreateCursor(context.Background(), querySchema)
				if err != nil {
					t.Fatal(err)
				}

				// step5: loop all cursors to query data from shard
				// key is indexKey, value is Record
				m := genExpectRecordsMap(rows, querySchema)
				errs := make(chan error, len(cursors))
				checkQueryResultParallel(errs, cursors, m, ascending, checkQueryResultForSingleCursor)

				close(errs)
				for i := 0; i < len(cursors); i++ {
					err = <-errs
					if err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// step6: close shard
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFilterByFiledWithFastPath(t *testing.T) {
	msNames := []string{"cpu"}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "field1_string"},
		record.Field{Type: influx.Field_Type_Int, Name: "field2_int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "field3_bool"},
		record.Field{Type: influx.Field_Type_Float, Name: "field4_float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	minTime, maxTime := int64(0), int64(23)

	for nameIdx := range msNames {
		cases := []TestCase{
			{"FilterByTime01", 0, 20, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 1100},
					[]int{1, 1}, []float64{1002.4, 0},
					[]int{1, 1}, []string{"hc", "hd"},
					[]int{1, 1}, []bool{false, false},
					[]int64{20, 19})},
			{"FilterByTime02", influxql.MinTime, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"FilterByTime03", 20, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{1000, 0, 0},
					[]int{1, 1, 1}, []float64{1001.3, 1002.4, 1002.4},
					[]int{1, 1, 1}, []string{"ha", "hb", "hc"},
					[]int{1, 1, 1}, []bool{true, true, false},
					[]int64{22, 21, 20})},
			{"FilterByTime04", influxql.MinTime, 21, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldLTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int < 1100 AND field1_string < 'hb' OR field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTCondition01", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int >=1000 OR field1_string > 'ha' AND field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldLTCondition02", influxql.MinTime, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int < 1100 AND field1_string < 'hb' OR field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTCondition03", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int < 1100 OR field1_string > 'ha' AND field2_int <= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTCondition04", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field2_int < 1100 OR field1_string > 'ha' AND field2_int <= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTCondition05", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field1_string > 'ha' AND field4_float > 1000 OR field1_string > 'hb' AND field4_float >= 1000", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 0},
					[]int{1, 1}, []float64{1002.4, 1002.4},
					[]int{1, 1}, []string{"hb", "hc"},
					[]int{1, 1}, []bool{true, false},
					[]int64{21, 20})},
			{"AllFieldLTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1000 < field2_int AND ('hb' < field1_string OR 1002 < field4_float)", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldLTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int <= 1100 AND field1_string <= 'hb' OR field4_float <= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"AllFieldLTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1000 <= field2_int AND 'hb' <= field1_string OR  1002.4 <= field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldGTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int > 1100 AND field1_string > 'hb' OR field4_float > 1002.4", nil, false, nil},
			{"AllFieldGTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1100 > field2_int AND 'hb' > field1_string OR 1002.4 > field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldGTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int >= 1100 AND field1_string >= 'hb' OR field4_float >= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldGTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"1100 >= field2_int AND 'hb' >= field1_string OR 1002.4 >= field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 1, 1, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 1, 1, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"AllFieldEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int = 0 AND field1_string = 'hb' OR field4_float = 1002.4 AND field3_bool = true", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{0},
					[]int{1}, []float64{1002.4},
					[]int{1}, []string{"hb"},
					[]int{1}, []bool{true},
					[]int64{21})},
			{"AllFieldNEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int != 1000 AND field1_string != 'hd' OR field4_float != 1002.4 AND field3_bool != true", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{1, 1, 1}, []float64{1002.4, 1002.4, 0},
					[]int{1, 1, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				var filterConditions []*influxql.VarRef
				var err error
				filterConditions, err = getFilterFieldsByExpr(querySchema.Options().GetCondition(), filterConditions[:0])
				if err != nil {
					t.Fatal(err)
				}
				queryCtx := &QueryCtx{}
				queryCtx.auxTags, queryCtx.schema = NewRecordSchema(querySchema, queryCtx.auxTags[:0], queryCtx.schema[:0], filterConditions, config.TSSTORE)
				if queryCtx.auxTags == nil && queryCtx.schema.Len() <= 1 {
					return
				}
				queryCtx.filterFieldsIdx = queryCtx.filterFieldsIdx[:0]
				queryCtx.filterTags = queryCtx.filterTags[:0]
				for _, f := range filterConditions {
					idx := queryCtx.schema.FieldIndex(f.Val)
					if idx >= 0 && f.Type != influxql.Unknown {
						queryCtx.filterFieldsIdx = append(queryCtx.filterFieldsIdx, idx)
					} else if f.Type != influxql.Unknown {
						queryCtx.filterTags = append(queryCtx.filterTags, f.Val)
					}
				}
				timeCond := binaryfilterfunc.GetTimeCondition(util.TimeRange{Min: opt.StartTime, Max: opt.EndTime}, queryCtx.schema, len(queryCtx.schema)-1)
				conFunctions, _ := binaryfilterfunc.NewCondition(timeCond, querySchema.Options().GetCondition(), queryCtx.schema, nil)

				// with time condition
				var newRecord *record.Record
				newRecord = record.NewRecordBuilder(rec.Schema)
				filterOption := &immutable.BaseFilterOptions{}
				filterOption.CondFunctions = conFunctions
				filterOption.RedIdxMap = make(map[int]struct{})
				filterBitmap := bitmap.NewFilterBitmap(conFunctions.NumFilter())

				if conFunctions != nil && conFunctions.HaveFilter() {
					newRecord = immutable.FilterByFieldFuncs(rec, newRecord, filterOption, filterBitmap)
				} else {
					newRecord = rec
				}

				if newRecord != nil && c.expRecord != nil {
					if !testRecsEqual(newRecord, c.expRecord) {
						t.Fatal("error result")
					}
				}
			})
		}
	}
}

func TestFilterByFiledWithFastPath2(t *testing.T) {
	msNames := []string{"cpu"}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_String, Name: "field1_string"},
		record.Field{Type: influx.Field_Type_Int, Name: "field2_int"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "field3_bool"},
		record.Field{Type: influx.Field_Type_Float, Name: "field4_float"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(schema,
		[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
		[]int{1, 0, 0, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
		[]int{1, 0, 0, 1}, []string{"ha", "hb", "hc", "hd"},
		[]int{1, 1, 1, 1}, []bool{true, true, false, false},
		[]int64{22, 21, 20, 19})
	minTime, maxTime := int64(0), int64(23)

	for nameIdx := range msNames {
		cases := []TestCase{
			{"FilterByTime01", 0, 20, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{0, 1100},
					[]int{0, 1}, []float64{1002.4, 0},
					[]int{0, 1}, []string{"hc", "hd"},
					[]int{1, 1}, []bool{false, false},
					[]int64{20, 19})},
			{"FilterByTime02", influxql.MinTime, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1, 1}, []int64{1000, 0, 0, 1100},
					[]int{1, 0, 0, 1}, []float64{1001.3, 1002.4, 1002.4, 0},
					[]int{1, 0, 0, 1}, []string{"ha", "hb", "hc", "hd"},
					[]int{1, 1, 1, 1}, []bool{true, true, false, false},
					[]int64{22, 21, 20, 19})},
			{"FilterByTime03", 20, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{1000, 0, 0},
					[]int{1, 0, 0}, []float64{1001.3, 1002.4, 1002.4},
					[]int{1, 0, 0}, []string{"ha", "hb", "hc"},
					[]int{1, 1, 1}, []bool{true, true, false},
					[]int64{22, 21, 20})},
			{"FilterByTime04", influxql.MinTime, 21, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"", nil, false,
				genRowRec(schema,
					[]int{1, 1, 1}, []int64{0, 0, 1100},
					[]int{0, 0, 1}, []float64{1002.4, 1002.4, 0},
					[]int{0, 0, 1}, []string{"hb", "hc", "hd"},
					[]int{1, 1, 1}, []bool{true, false, false},
					[]int64{21, 20, 19})},
			{"AllFieldLTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int <= 1000 AND field1_string <= 'hb' OR field4_float < 1002", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTCondition02", influxql.MinTime, influxql.MaxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int > 1100 AND field1_string < 'hb' OR field4_float <= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" (1000 < field2_int AND 'hb' < field1_string) OR 1002 < field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int <= 1100 AND field1_string <= 'hb' OR field4_float <= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldLTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1000 <= field2_int AND 'hb' <= field1_string OR  1002.4 <= field4_float", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldGTCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int > 1100 AND field1_string > 'hb' OR field4_float > 1002.4", nil, false, nil},
			{"AllFieldGTConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				" 1100 > field2_int AND 'hb' > field1_string OR 1002.4 > field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldGTECondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int >= 1100 AND field1_string >= 'hb' OR field4_float >= 1002.4", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
			{"AllFieldGTEConditionSwitchOps", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"1100 >= field2_int AND 'hb' >= field1_string OR 1002.4 >= field4_float", nil, false,
				genRowRec(schema,
					[]int{1, 1}, []int64{1000, 1100},
					[]int{1, 1}, []float64{1001.3, 0},
					[]int{1, 1}, []string{"ha", "hd"},
					[]int{1, 1}, []bool{true, false},
					[]int64{22, 19})},
			{"AllFieldEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int = 0 AND field1_string = 'hb' OR field4_float = 1002.4 AND field3_bool = true", nil, false,
				nil},
			{"AllFieldNEQCondition", minTime, maxTime, createFieldAux([]string{"field1_string", "field2_int", "field3_bool", "field4_float"}),
				"field2_int != 1000 AND field1_string != 'hd' OR field4_float != 1002.4 AND field3_bool != true", nil, false,
				genRowRec(schema,
					[]int{1}, []int64{1100},
					[]int{1}, []float64{0},
					[]int{1}, []string{"hd"},
					[]int{1}, []bool{false},
					[]int64{19})},
		}

		ascending := true
		for _, c := range cases {
			c := c
			t.Run(c.Name, func(t *testing.T) {
				opt := genQueryOpt(&c, msNames[nameIdx], ascending)
				querySchema := genQuerySchema(c.fieldAux, opt)
				var filterConditions []*influxql.VarRef
				var err error
				filterConditions, err = getFilterFieldsByExpr(querySchema.Options().GetCondition(), filterConditions[:0])
				if err != nil {
					t.Fatal(err)
				}
				queryCtx := &QueryCtx{}
				queryCtx.auxTags, queryCtx.schema = NewRecordSchema(querySchema, queryCtx.auxTags[:0], queryCtx.schema[:0], filterConditions, config.TSSTORE)
				if queryCtx.auxTags == nil && queryCtx.schema.Len() <= 1 {
					return
				}
				queryCtx.filterFieldsIdx = queryCtx.filterFieldsIdx[:0]
				queryCtx.filterTags = queryCtx.filterTags[:0]
				for _, f := range filterConditions {
					idx := queryCtx.schema.FieldIndex(f.Val)
					if idx >= 0 && f.Type != influxql.Unknown {
						queryCtx.filterFieldsIdx = append(queryCtx.filterFieldsIdx, idx)
					} else if f.Type != influxql.Unknown {
						queryCtx.filterTags = append(queryCtx.filterTags, f.Val)
					}
				}
				timeCond := binaryfilterfunc.GetTimeCondition(util.TimeRange{Min: opt.StartTime, Max: opt.EndTime}, queryCtx.schema, len(queryCtx.schema)-1)
				conFunctions, _ := binaryfilterfunc.NewCondition(timeCond, querySchema.Options().GetCondition(), queryCtx.schema, nil)

				// with time condition
				var newRecord *record.Record
				newRecord = record.NewRecordBuilder(rec.Schema)
				filterOption := &immutable.BaseFilterOptions{}
				filterOption.CondFunctions = conFunctions
				filterOption.RedIdxMap = make(map[int]struct{})
				filterBitmap := bitmap.NewFilterBitmap(conFunctions.NumFilter())
				if conFunctions != nil && conFunctions.HaveFilter() {
					newRecord = immutable.FilterByFieldFuncs(rec, newRecord, filterOption, filterBitmap)
				} else {
					newRecord = rec
				}

				if newRecord != nil && c.expRecord != nil {
					if !testRecsEqual(newRecord, c.expRecord) {
						t.Fatal("error result")
					}
				}
			})
		}
	}
}

func TestColumnStoreFlush(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	record := genRecord()

	msInfo := mutable.MsInfo{
		Name:   defaultMeasurementName,
		Schema: schema,
	}
	writeChunk := &mutable.WriteChunkForColumnStore{}
	writeChunk.WriteRec.SetWriteRec(record)
	msInfo.SetWriteChunk(writeChunk)
	sh.activeTbl.SetMsInfo(defaultMeasurementName, &msInfo)

	time.Sleep(time.Second * 1)
	// wait mem table flush
	sh.ForceFlush()

	err = closeShard(sh)
	require.NoError(t, err)
}

func TestColumnStoreFlushErr(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)

	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	storage := sh.storage.(*columnstoreImpl)
	for i := range storage.snapshotStatus {
		storage.snapshotStatus[i] = 1
	}
	sh.ForceFlush()
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestColumnStoreFlushErr2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)

	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	err = sh.indexBuilder.Close()
	if err != nil {
		t.Fatal(err)
	}
	sh.indexBuilder = nil
	sh.ForceFlush()
	err = sh.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteRecByColumnStore(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = eng.Close()
	}()

	sh := eng.DBPartitions["db0"][0].Shard(1).(*shard)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])

	record := genRecord()

	err = eng.WriteRec("db0", defaultMeasurementName, 0, 1, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	rec := msInfo.GetWriteChunk().WriteRec.GetRecord()
	if !testRecsEqual(rec, record) {
		t.Fatal("error result")
	}
}

func TestWriteRecByColumnStoreWithSchemaLess(t *testing.T) {
	dir := t.TempDir()
	eng, err := initEngine1(dir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = eng.Close()
	}()

	sh := eng.DBPartitions["db0"][0].Shard(1).(*shard)
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])

	oldRec := genRecord()
	sort.Sort(oldRec)
	err = eng.WriteRec("db0", defaultMeasurementName, 0, 1, oldRec, nil)
	if err != nil {
		t.Fatal(err)
	}

	newSchema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int1"},
		record.Field{Type: influx.Field_Type_Float, Name: "float1"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean1"},
		record.Field{Type: influx.Field_Type_String, Name: "string1"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	newRec := genRecord()
	sort.Sort(newRec)
	sort.Sort(newSchema)
	newRec.Schema = newSchema
	err = eng.WriteRec("db0", defaultMeasurementName, 0, 1, newRec, nil)
	if err != nil {
		t.Fatal(err)
	}
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	rec := msInfo.GetWriteChunk().WriteRec.GetRecord()
	resultRec := record.NewRecordBuilder(rec.Schemas())
	resultRec.AppendRec(oldRec, 0, oldRec.RowNums())
	resultRec.AppendRec(newRec, 0, newRec.RowNums())
	if !testRecsEqual(rec, resultRec) {
		t.Fatal("error result")
	}
}

func TestWriteDataByNewEngine(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	require.NoError(t, err)
	// wait mem table flush
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteDataByNewEngine2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)

	err = sh.WriteRows(rows, nil)
	if err == nil {
		t.Fatal("should return error mstInfo")
	}
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteDataByNewEngine3(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	require.NoError(t, err)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddSeqIDToCol(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	record := genRecord()
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])

	config.SetProductType("logkeeper")
	sort.Sort(record)
	if err = sh.WriteCols(defaultMeasurementName, record, nil); err != nil {
		t.Fatal(err)
	}

	config.SetProductType("csstore")
	record1 := genRecord()
	sort.Sort(record1)
	err = sh.WriteCols(defaultMeasurementName, record1, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddSeqIDToColV1(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	record := genRecord()
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	record1 := genRecord()
	sort.Sort(record1)
	err = sh.WriteCols(defaultMeasurementName, record1, nil)
	if err != nil {
		t.Fatal(err)
	}

	config.SetProductType("logkeeper")
	sort.Sort(record)
	if err = sh.WriteCols(defaultMeasurementName, record, nil); err != nil {
		t.Fatal(err)
	}
	config.SetProductType("csstore")
}

func TestLeftBound(t *testing.T) {
	nums := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	target := uint32(8)
	left := 8
	res := immutable.LeftBound(nums, target, left)
	require.Equal(t, res, 9)

	target = 11
	left = 9
	res = immutable.LeftBound(nums, target, left)
	require.Equal(t, res, 9)
}

// TestWriteDetachedData local bf is not exist
func TestWriteDetachedData(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(true)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

// TestWriteDetachedDataV2 local bf exist, and flush multi times
func TestWriteDetachedDataV2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	immutable.SetDetachedFlushEnabled(true)
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    sortKey,
			PrimaryKey: primaryKey,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	flushTimes := 3
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	for i := 0; i < flushTimes; i++ {
		mutable.SetWriteChunk(msInfo, rec)
		sh.commitSnapshot(sh.activeTbl)
	}

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteFullTextIndexV1(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(true)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{
			IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:       []uint32{uint32(index.BloomFilterFullText)},
			IndexList:  list,
			IndexOptions: []*influxql.IndexOptions{
				{
					Options: []*influxql.IndexOption{
						{Tokens: tokenizer.CONTENT_SPLITTER, TokensTable: tokenizer.CONTENT_SPLIT_TABLE, Tokenizers: "standard"},
					},
				},
			},
		},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteFullTextIndexV2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(true)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:      []uint32{uint32(index.BloomFilterFullText)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	flushTimes := 2
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	for i := 0; i < flushTimes; i++ {
		mutable.SetWriteChunk(msInfo, rec)
		sh.commitSnapshot(sh.activeTbl)
	}

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteFullTextIndexV3(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(false)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:      []uint32{uint32(index.BloomFilterFullText)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteAttachedFullTextIndexV1(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(false)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:      []uint32{uint32(index.BloomFilterFullText)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteAttachedFullTextIndexV2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	immutable.SetDetachedFlushEnabled(false)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{index.BloomFilterFullTextIndex},
			Oids:      []uint32{uint32(index.BloomFilterFullText)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	flushTimes := 2
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	for i := 0; i < flushTimes; i++ {
		mutable.SetWriteChunk(msInfo, rec)
		sh.commitSnapshot(sh.activeTbl)
	}

	require.Equal(t, 4*100, int(sh.rowCount))
	time.Sleep(time.Second * 1)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteDataByNewEngineWithSchemaLess(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, false, true, false, 1)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	require.NoError(t, err)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteDataByNewEngineWithTag(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	require.NoError(t, err)

	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	mutable.JoinWriteRec(sh.activeTbl, defaultMeasurementName)
	writeRec := msInfo.GetWriteChunk().WriteRec
	require.Equal(t, 9, writeRec.GetRecord().Len())
	// wait mem table flush
	sh.ForceFlush()

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteDataByNewEngineWithTag2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, false, true, false, 1)

	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	err = sh.WriteRows(rows, nil)
	require.NoError(t, err)

	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	mutable.MergeSchema(sh.activeTbl, defaultMeasurementName)
	mutable.JoinWriteRec(sh.activeTbl, defaultMeasurementName)
	writeRec := msInfo.GetWriteChunk().WriteRec
	require.Equal(t, 9, writeRec.GetRecord().Len())
	// wait mem table flush
	sh.ForceFlush()

	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAppendTagsFieldsToRecord(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])

	begin := time.Now().UnixNano()
	rows := []influx.Row{{
		Name: defaultMeasurementName,
		Tags: influx.PointTags{
			{Key: "a", Value: "T001"},
			{Key: "c", Value: "T001"},
			{Key: "g", Value: "T001"},
			{Key: "f", Value: "T001"},
			{Key: "d", Value: "T001"},
			{Key: "h", Value: "T001"},
		},
		Fields: influx.Fields{
			{Key: "b", Type: influx.Field_Type_Float, NumValue: 1.1},
			{Key: "d", Type: influx.Field_Type_Float, NumValue: 1.1},
			{Key: "e", Type: influx.Field_Type_Float, NumValue: 1.1},
			{Key: "a1", Type: influx.Field_Type_Float, NumValue: 1.1},
			{Key: "a2", Type: influx.Field_Type_Float, NumValue: 1.1},
			{Key: "c1", Type: influx.Field_Type_Float, NumValue: 1.1},
		},
		ShardKey:     nil,
		Timestamp:    begin,
		IndexKey:     nil,
		SeriesId:     0,
		IndexOptions: nil,
	}}
	err = writeData(sh, rows, false)
	if err != nil {
		t.Fatal(err)
	}
}

type QueryCtx struct {
	filterFieldsIdx []int
	filterTags      []string
	auxTags         []string
	schema          record.Schemas
}

type TestAggCase struct {
	Name               string            // testcase name
	startTime, endTime int64             // query time range
	fieldAux           []influxql.VarRef // fields to select
	fieldFilter        string            // field filter condition
	dims               []string          // group by dimensions
	ascending          bool              // order by time
	aggCall            []string
}

func genAggQuerySchema(fieldAux []influxql.VarRef, callField []influxql.Call, opt *query.ProcessorOptions) *executor.QuerySchema {
	var fields influxql.Fields
	var columnNames []string
	for i := range callField {
		f := &influxql.Field{
			Expr:  &callField[i],
			Alias: callField[i].Name,
		}
		fields = append(fields, f)
		columnNames = append(columnNames, callField[i].Name)
	}
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

func genAggQueryOpt(tc *TestAggCase, msName string, ascending bool, chunkSize int, interval time.Duration) *query.ProcessorOptions {
	var opt query.ProcessorOptions
	opt.Name = msName
	opt.Dimensions = tc.dims
	opt.Ascending = ascending
	opt.FieldAux = tc.fieldAux
	opt.MaxParallel = 1
	opt.ChunkSize = chunkSize
	opt.StartTime = tc.startTime
	opt.EndTime = tc.endTime
	opt.Interval = hybridqp.Interval{
		Duration: interval,
	}

	addFilterFieldCondition(tc.fieldFilter, &opt)

	return &opt
}

func genCall(fieldAux []influxql.VarRef, call string) []influxql.Call {
	field := make([]influxql.Call, 0, len(fieldAux))
	for i := range fieldAux {
		f := influxql.Call{
			Name: call,
			Args: []influxql.Expr{
				&fieldAux[i],
			},
		}
		field = append(field, f)
	}
	return field
}

func genOps(vaRefs []influxql.VarRef, calls []influxql.Call) []hybridqp.ExprOptions {
	ops := make([]hybridqp.ExprOptions, 0, len(calls))
	for i := range calls {
		ops = append(ops, hybridqp.ExprOptions{
			Expr: &calls[i],
			Ref:  vaRefs[i],
		})
	}
	return ops
}

func updateClusterCursor(cursors comm.KeyCursors, ops []hybridqp.ExprOptions, call string) {
	for _, c := range cursors {
		var callOps []*comm.CallOption
		for _, op := range ops {
			if call, ok := op.Expr.(*influxql.Call); ok {
				callOps = append(callOps, &comm.CallOption{Call: call, Ref: &influxql.VarRef{
					Val:  op.Ref.Val,
					Type: op.Ref.Type,
				}})
			}
		}
		schema := c.(*groupCursor).ctx.schema
		clusterCursor := c.(*groupCursor).tagSetCursors[0].(*AggTagSetCursor)
		if call == "count" {
			schemaCopy := schema.Copy()
			for i := 0; i < len(schemaCopy)-1; i++ {
				schemaCopy[i].Type = influx.Field_Type_Int
			}
			clusterCursor.SetSchema(schemaCopy)
			clusterCursor.aggOps = callOps
			clusterCursor.aggFunctionsInit()
			aggCursor := clusterCursor.baseCursorInfo.keyCursor.(*aggregateCursor)
			aggCursor.SetSchema(schema, schemaCopy, ops)
			aggCursor.input.(*fileLoopCursor).SetSchema(schema)
		} else {
			clusterCursor.SetSchema(schema)
			clusterCursor.aggOps = callOps
			clusterCursor.aggFunctionsInit()
			aggCursor := clusterCursor.baseCursorInfo.keyCursor.(*aggregateCursor)
			aggCursor.SetSchema(schema, schema, ops)
			aggCursor.input.(*fileLoopCursor).SetSchema(schema)
		}
	}
}

type aggResult map[string][]interface{}

func GenAggDataRecord(msNames []string, seriesNum, pointNumOfPerSeries int, interval time.Duration,
	tm time.Time, fullField, inc bool, fixBool bool, tv ...int) ([]influx.Row, int64, int64, *aggResult) {
	result := aggResult{}
	result = make(map[string][]interface{})
	result["first"] = make([]interface{}, 4)
	result["last"] = make([]interface{}, 4)
	result["sum"] = make([]interface{}, 4)
	result["min"] = make([]interface{}, 4)
	result["max"] = make([]interface{}, 4)
	result["count"] = make([]interface{}, 4)
	tm = tm.Truncate(time.Second)
	pts := make([]influx.Row, 0, seriesNum)
	names := msNames
	if len(msNames) == 0 {
		names = []string{defaultMeasurementName}
	}

	mms := func(i int) string {
		return names[i%len(names)]
	}

	var indexKeyPool []byte
	vInt, vFloat := int64(1), float64(1)
	tv1, tv2, tv3, tv4 := 1, 1, 1, 1
	for _, tgv := range tv {
		tv1 = tgv
	}
	var (
		intSum       int64
		float64Sum   float64
		intCount     int64
		floatCount   int64
		stringCount  int64
		booleanCount int64
	)
	for i := 0; i < seriesNum; i++ {
		fields := map[string]interface{}{
			"field2_int":    vInt,
			"field3_bool":   i%2 == 0,
			"field4_float":  vFloat,
			"field1_string": fmt.Sprintf("test-test-test-test-%d", i),
		}
		if fixBool {
			fields["field3_bool"] = (i%2 == 0)
		} else {
			fields["field3_bool"] = (rand.Int31n(100) > 50)
		}

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
				if result["min"][1] == nil || v.(int64) < result["min"][1].(int64) {
					result["min"][1] = v
				}
				if result["max"][1] == nil || v.(int64) > result["max"][1].(int64) {
					result["max"][1] = v
				}
				intSum += v.(int64)
				intCount += 1
			case float64:
				r.Fields[j].Type = influx.Field_Type_Float
				r.Fields[j].NumValue = v.(float64)
				if result["min"][3] == nil || v.(float64) < result["min"][3].(float64) {
					result["min"][3] = v
				}
				if result["max"][3] == nil || v.(float64) > result["max"][3].(float64) {
					result["max"][3] = v
				}
				float64Sum += r.Fields[j].NumValue
				floatCount += 1
			case string:
				r.Fields[j].Type = influx.Field_Type_String
				r.Fields[j].StrValue = v.(string)
				if result["first"][0] == nil || v.(string) > result["first"][0].(string) {
					result["first"][0] = v
				}
				stringCount += 1
			case bool:
				r.Fields[j].Type = influx.Field_Type_Boolean
				if v.(bool) == false {
					r.Fields[j].NumValue = 0
				} else {
					r.Fields[j].NumValue = 1
				}
				booleanCount += 1
			}
			j++
		}

		sort.Sort(&r.Fields)

		vInt++
		vFloat += 1
		tags := map[string]string{
			"tagkey1": fmt.Sprintf("tagvalue1_%d", tv1),
			"tagkey2": fmt.Sprintf("tagvalue2_%d", tv2),
			"tagkey3": fmt.Sprintf("tagvalue3_%d", tv3),
			"tagkey4": fmt.Sprintf("tagvalue4_%d", tv4),
		}

		// tags init
		r.Tags = make(influx.PointTags, len(tags))
		j = 0
		for k, v := range tags {
			r.Tags[j].Key = k
			r.Tags[j].Value = v
			j++
		}
		sort.Sort(&r.Tags)
		tv4++
		tv3++
		tv2++
		tv1++

		name := mms(i)
		r.Name = name
		r.Timestamp = tm.UnixNano()
		r.UnmarshalIndexKeys(indexKeyPool)
		r.ShardKey = r.IndexKey

		pts = append(pts, r)
	}
	if pointNumOfPerSeries > 1 {
		copyRs := copyIntervalStepPointRows(pts, pointNumOfPerSeries-1, interval, inc)
		pts = append(pts, copyRs...)
	}

	sort.Slice(pts, func(i, j int) bool {
		return pts[i].Timestamp < pts[j].Timestamp
	})
	result["min"][0] = result["first"][0]
	result["min"][2] = false
	result["max"][0] = result["first"][0]
	result["max"][2] = true
	result["first"] = result["max"]
	result["last"] = result["max"]
	result["sum"] = []interface{}{nil, intSum, nil, float64Sum}
	result["count"] = []interface{}{stringCount, intCount, booleanCount, floatCount}
	return pts, pts[0].Timestamp, pts[len(pts)-1].Timestamp, &result
}

func TestGroupCursorStartSpan(t *testing.T) {
	schema := &executor.QuerySchema{}
	cursor := &groupCursor{id: 1, tagSetCursors: nil, querySchema: schema}
	_, span := tracing.NewTrace("root")
	span.StartPP()

	sub := span.StartSpan("sub")
	sub.SetFields(fields.Fields{fields.String("pp", "10ms")})
	tracing.StartPP(sub)
	executor.EnableFileCursor(true)
	cursor.StartSpan(span)
}

type aggTagSetTestCase struct {
	opt              query.ProcessorOptions
	fileMinTIme      int64
	fileMaxTime      int64
	fileIntervalTime int64
	start            int64
	end              int64
	expectedStep     int
}

func writeOneRow(sh *shard, mst string, ts int64) error {
	rows := []influx.Row{{
		Name: mst,
		Tags: influx.PointTags{
			{Key: "tid", Value: "T001"},
		},
		Fields: influx.Fields{
			{Key: "value", Type: influx.Field_Type_Float, NumValue: 1.1},
		},
		ShardKey:     nil,
		Timestamp:    ts,
		IndexKey:     nil,
		SeriesId:     0,
		IndexOptions: nil,
	}}

	return writeData(sh, rows, false)
}

func swapMemTable(sh *shard, dir string) {
	sh.snapshotTbl = sh.activeTbl
	sh.activeTbl = mutable.NewMemTable(sh.engineType)
	sh.activeTbl.SetIdx(sh.skIdx)
}

func getFirstSid(table *mutable.MemTable, mst string) uint64 {
	msinfo, err := table.GetMsInfo(mst)
	if err != nil {
		return 0
	}

	sids := msinfo.GetAllSid()
	if len(sids) > 0 {
		return sids[0]
	}
	return 0
}

func TestLastFlushTime(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()

	var mst = "mst"
	for _, ts := range []int64{100, 90} {
		require.NoError(t, writeOneRow(sh, mst, ts))
	}

	sid := getFirstSid(sh.activeTbl, mst)
	require.NotEqual(t, int64(0), sid)

	swapMemTable(sh, dir)

	var assertLastFlushTime = func(mst string, sid uint64, exp int64) {
		got := sh.getLastFlushTime(mst, sid)
		require.Equal(t, exp, got)
	}

	assertLastFlushTime(mst, sid, 100)
	assertLastFlushTime("mst_not_exists", sid, int64(math.MinInt64))
	assertLastFlushTime(mst, sid+1000, int64(math.MinInt64))

	require.True(t, sh.immTables.FreeSequencer())
	assertLastFlushTime(mst, sid, int64(math.MaxInt64))

	rows, err := sh.immTables.GetRowCountsBySid(mst, sid)
	require.NoError(t, err)
	require.Equal(t, int64(0), rows)

	sh.activeTbl = sh.snapshotTbl
	sh.snapshotTbl = nil
	sh.ForceFlush()
	assertLastFlushTime("mst", sid, 100)
}

func TestFlushMstDeleted(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()

	var mst = "mst"
	for _, ts := range []int64{100, 90} {
		require.NoError(t, writeOneRow(sh, mst, ts))
	}

	sid := getFirstSid(sh.activeTbl, mst)
	require.NotEqual(t, int64(0), sid)

	swapMemTable(sh, dir)

	sh.setMstDeleting(mst)
	sh.activeTbl = sh.snapshotTbl
	sh.snapshotTbl = nil
	sh.ForceFlush()
	sh.clearMstDeleting(mst)
}

func TestGetValuesInMemTables(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()

	begin := time.Now().UnixNano()

	mst := "mst"
	memtables := mutable.MemTables{}

	var getValue = func(sid uint64, asc bool) *record.Record {
		return memtables.Values(mst, sid, util.TimeRange{Min: 0, Max: begin + 10}, record.Schemas{
			{Name: "value", Type: influx.Field_Type_Float},
			{Name: record.TimeField, Type: influx.Field_Type_Int},
		}, asc)
	}

	var assertValue = func(sid uint64, asc bool, expLen int) {
		rec := getValue(sid, asc)

		if expLen == 0 {
			require.Nil(t, rec)
			return
		}

		require.NotNil(t, rec)
		require.Equal(t, expLen, len(rec.Times()))
	}

	require.NoError(t, writeOneRow(sh, mst, begin))

	require.Equal(t, uint64(0), getFirstSid(sh.activeTbl, "mst_not_exists"))
	sid := getFirstSid(sh.activeTbl, mst)

	memtables.Init(sh.activeTbl, sh.snapshotTbl, true)
	assertValue(sid, true, 1)

	swapMemTable(sh, dir)

	memtables.Init(sh.activeTbl, sh.snapshotTbl, true)
	assertValue(sid, true, 1)
	require.NoError(t, writeOneRow(sh, mst, begin+1))

	assertValue(sid, true, 2)
	assertValue(sid, false, 2)
	assertValue(0, false, 0)

	sh.memDataReadEnabled = false
	assertValue(0, false, 0)
}

func TestWriteColsForTsstore(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	record := genRecord()

	if err := sh.WriteCols(defaultMeasurementName, record, nil); err != nil {
		require.Equal(t, err, errors.New("not implement yet"))
	}
}

func TestWriteWalForArrowFlightError(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()

	err = sh.writeWalForArrowFlight(nil)
	if err == nil {
		t.Fatal("too small bytes for record binary")
	}
}

func TestWriteColsForTsstoreWithShardClosed(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	record := genRecord()

	_ = closeShard(sh)
	if err := sh.WriteCols(defaultMeasurementName, record, nil); err == nil {
		t.Fatal("shard close can not write data")
	}
}

func TestWriteColsForColstore(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.COLUMNSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	record := genRecord()
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{Name: defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{SortKey: []string{},
			PrimaryKey: []string{}}}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])

	if err = sh.WriteCols(defaultMeasurementName, record, nil); err != nil {
		t.Fatal(err)
	}
	err = sh.WriteCols(defaultMeasurementName, nil, nil)
	if err == nil {
		t.Fatal("write rec can not be nil")
	}
	sh.ident.ReadOnly = true
	err = sh.WriteCols(defaultMeasurementName, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	sh.ident.ReadOnly = true
	DownSampleWriteDrop = false
	err = sh.WriteCols(defaultMeasurementName, record, nil)
	if err == nil {
		t.Fatal("can not write cols to downSampled shard")
	}
	DownSampleWriteDrop = true
}

func TestDownSampleRedo(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

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

	oFiles, _ := sh.immTables.GetTSSPFiles("mst", true)
	newFiles := []immutable.TSSPFile{oFiles.Files()[0]}
	oldFiles := []immutable.TSSPFile{oFiles.Files()[1]}
	_, err = sh.writeDownSampleInfo(msNames, [][]immutable.TSSPFile{oldFiles}, [][]immutable.TSSPFile{newFiles}, 0, 1)
	require.NoError(t, err)
	sh2 := NewShard(sh.dataPath, sh.walPath, sh.lock, sh.ident, sh.durationInfo, &meta.TimeRangeInfo{StartTime: sh.startTime, EndTime: sh.endTime}, DefaultEngineOption, config.TSSTORE)
	m := mockMetaClient()
	err = sh2.OpenAndEnable(m)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDownSampleSharCompact(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()
	sh.ident.DownSampleLevel = 1
	require.NoError(t, sh.Compact())
}

func TestFindTagIndex(t *testing.T) {
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}

	metaSchema := make(map[string]int32)
	for i := range schema {
		if schema[i].Name == "string" {
			metaSchema[schema[i].Name] = int32(influx.Field_Type_Tag)
			continue
		}
		metaSchema[schema[i].Name] = int32(schema[i].Type)
	}

	expRes := []int{3}
	res := findTagIndex(schema, metaSchema)

	if len(res) != len(expRes) {
		t.Fatal("find tag index error")
	}
	for i := range res {
		if res[i] != expRes[i] {
			t.Fatal("unexpected result")
		}
	}
}

func TestWriteIndexForLabelStore(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryIndex := sh.indexBuilder.GetPrimaryIndex()
	idx, _ := primaryIndex.(*tsi.MergeSetIndex)
	var writeIndexRequired bool
	var exist bool

	for i := 0; i < len(rows); i++ {
		if sh.closed.Closed() {
			t.Fatal("shard closed")
		}

		if !writeIndexRequired {
			exist, err = idx.IsTagKeyExist(rows[i])
			if err != nil {
				t.Fatal(err)
			}
			if !exist {
				writeIndexRequired = true
			}
		}
	}
	csIndexImpl, ok := idx.StorageIndex.(*tsi.CsIndexImpl)
	if !ok {
		t.Fatal("wrong index type")
	}
	if writeIndexRequired {
		for i := 0; i < len(rows); i++ {
			err = csIndexImpl.CreateIndexIfNotExistsByRow(idx, &rows[i])
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	sh.indexBuilder.Flush()

	for i := 0; i < len(rows); i++ {
		if sh.closed.Closed() {
			t.Fatal("shard closed")
		}

		exist, err = idx.IsTagKeyExist(rows[i])
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriteIndexForArrowFlight(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()

	// step2: write data
	//expRec := genRecord()
	tagCol := tsi.TagCol{}
	tagCol.Mst = []byte(defaultMeasurementName)
	tagCol.Key = []byte("string")
	tagCol.Val = []byte("hello")
	primaryIndex := sh.indexBuilder.GetPrimaryIndex()
	idx, _ := primaryIndex.(*tsi.MergeSetIndex)
	var writeIndexRequired bool
	var exist bool
	if sh.closed.Closed() {
		t.Fatal("shard closed")
	}

	if !writeIndexRequired {
		exist, err = idx.IsTagKeyExistByArrowFlight(&tagCol)
		if err != nil {
			t.Fatal(err)
		}
		if !exist {
			writeIndexRequired = true
		}
	}

	csIndexImpl, ok := idx.StorageIndex.(*tsi.CsIndexImpl)
	if !ok {
		t.Fatal("wrong index type")
	}
	if writeIndexRequired {
		err = csIndexImpl.CreateIndexIfNotExistsByCol(idx, &tagCol)
		if err != nil {
			t.Fatal(err)
		}
	}

	sh.indexBuilder.Flush()

	if sh.closed.Closed() {
		t.Fatal("shard closed")
	}

	exist, err = idx.IsTagKeyExistByArrowFlight(&tagCol)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteIndexForArrowFlight2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()

	rec := genRecord()
	tagIndex := []int{3}
	err = sh.indexBuilder.CreateIndexIfNotExistsByCol(rec, tagIndex, defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteIndexFail(t *testing.T) {
	dir := t.TempDir()
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	defer func() {
		_ = closeShard(sh)
	}()

	begin := time.Now().UnixNano()

	mstLen := 64 * 1024
	mst := make([]byte, mstLen)
	for i := 0; i < mstLen; i++ {
		mst[i] = 'a'
	}

	err = writeOneRow(sh, string(mst), begin)
	if !strings.Contains(err.Error(), "it looks like the item is too large") {
		t.Fatal("TestWriteIndexFail Fail")
	}
}

func TestWriteIndexForRow(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 20, 100, time.Second, st, true, true, false, 1)
	primaryIndex := sh.indexBuilder.GetPrimaryIndex()
	idx, _ := primaryIndex.(*tsi.MergeSetIndex)
	var isWriteIndexNeeded bool
	var exist bool

	for i := 0; i < len(rows); i++ {
		if !isWriteIndexNeeded {
			exist, err = idx.IsTagKeyExist(rows[i])
			if err != nil {
				t.Fatal(err)
			}
			if !exist {
				isWriteIndexNeeded = true
			}
		}
	}
	csIndexImpl, _ := idx.StorageIndex.(*tsi.CsIndexImpl)

	if isWriteIndexNeeded {
		for i := 0; i < len(rows); i++ {
			err = csIndexImpl.CreateIndexIfNotExistsByRow(idx, &rows[i])
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	sh.indexBuilder.Flush()

	for i := 0; i < len(rows); i++ {
		if sh.closed.Closed() {
			t.Fatal("shard closed")
		}

		exist, err = idx.IsTagKeyExist(rows[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInitDownSampleTaskNum(t *testing.T) {
	initMaxDownSampleParallelism(0)
	if maxDownSampleTaskNum != 1 {
		t.Fatal()
	}
	initMaxDownSampleParallelism(1)
	if maxDownSampleTaskNum != 1 {
		t.Fatal()
	}
}

func TestGetMstWriteCtx(t *testing.T) {
	ctx := getMstWriteCtx(time.Millisecond*10, config.TSSTORE)
	putMstWriteCtx(ctx)

	ctx = getMstWriteCtx(time.Millisecond*10, config.TSSTORE)
	time.Sleep(time.Millisecond * 20)
	putMstWriteCtx(ctx)
}

func TestGetMstWriteRecordCtx(t *testing.T) {
	ctx := getMstWriteRecordCtx(time.Millisecond*10, config.COLUMNSTORE)
	putMstWriteRecordCtx(ctx)

	ctx = getMstWriteRecordCtx(time.Millisecond*10, config.COLUMNSTORE)
	time.Sleep(time.Millisecond * 20)
	putMstWriteRecordCtx(ctx)
}

func TestInterEngine_DoShardMove_OrderFiles(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	syscontrol.SetHierarchicalStorageEnabled(true)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	syscontrol.SetHierarchicalStorageEnabled(false)

	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	err = closeShard(sh)
	require.NoError(t, err)

	// 1. both exist 00000001-0000-00000000.tssp and 00000001-0000-00000000.tssp.init
	srcFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp")
	dstFile1 := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp.init")
	fileops.CopyFile(srcFile, dstFile1)

	sh, err = createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	err = sh.doShardMove()
	require.NoError(t, err)
	err = closeShard(sh)
	require.NoError(t, err)

	// 2. exist only 00000001-0000-00000000.tssp.init, so there is no order and out of order files
	fileops.Remove(srcFile)
	sh, err = createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	err = sh.doShardMove()
	require.NoError(t, err)
	err = closeShard(sh)
	require.NoError(t, err)
}

func TestInterEngine_DoShardMove_OutOfOrderFiles(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	syscontrol.SetHierarchicalStorageEnabled(true)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	syscontrol.SetHierarchicalStorageEnabled(false)

	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	err = closeShard(sh)
	require.NoError(t, err)

	srcFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp")
	outOfOrderPath := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "out-of-order")
	fileops.MkdirAll(outOfOrderPath, 0750)
	dstFile2 := filepath.Join(outOfOrderPath, "00000002-0000-00000000.tssp")
	fileops.CopyFile(srcFile, dstFile2)

	sh, err = createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	err = sh.doShardMove()
	require.NoError(t, err)

	err = closeShard(sh)
	require.NoError(t, err)
}

/*
there are 3 files:
00000001-0000-00000000.tssp.init
00000002-0000-00000000.tssp
00000003-0000-00000000.tssp
00000001-0000-00000000.tssp.init just rename.
00000002-0000-00000000.tssp and 00000003-0000-00000000.tssp need to copy to obs and rename.
*/
func TestInterEngine_DoShardMove_BothFiles(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)

	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	err = closeShard(sh)
	require.NoError(t, err)

	srcFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp")
	dstFile1 := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp.init")
	dstFile2 := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000002-0000-00000000.tssp")
	dstFile3 := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000003-0000-00000000.tssp")
	fileops.CopyFile(srcFile, dstFile1)
	fileops.CopyFile(srcFile, dstFile2)
	fileops.CopyFile(srcFile, dstFile3)
	fileops.Remove(srcFile)

	sh, err = createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)
	err = sh.doShardMove()
	require.NoError(t, err)

	err = closeShard(sh)
	require.NoError(t, err)
}

func TestInterEngine_DoShardMove_Stop(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)

	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	err = closeShard(sh)
	require.NoError(t, err)

	srcFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp")
	dstFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp.init")
	fileops.CopyFile(srcFile, dstFile)

	sh, err = createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)

	sh.moveStop = make(chan struct{})
	close(sh.moveStop)
	err = sh.doShardMove()
	// check whether shard move is disabled
	require.Error(t, err)
	sh.moveStop = nil

	err = closeShard(sh)
	require.NoError(t, err)
}

func TestInterEngine_DoShardMove_Mock(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)

	file := MocTsspFile{
		path: "/tmp/openGemini",
	}
	mockobsName := "00000001-0000-00000000.tssp.init"

	err = sh.renameFileOnOBS(file, mockobsName)
	// check whether shard move is disabled
	require.Error(t, err)
	sh.moveStop = nil

	err = closeShard(sh)
	require.NoError(t, err)
}

func TestInterEngine_CopyFileRollBack(t *testing.T) {
	dir := t.TempDir()
	_ = os.RemoveAll(dir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir, config.TSSTORE)
	require.NoError(t, err)

	msNames := []string{"mst"}
	startTime := mustParseTime(time.RFC3339Nano, "2022-07-01T01:00:00Z")
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.endTime = mustParseTime(time.RFC3339Nano, "2022-07-08T01:00:00Z")
	time.Sleep(time.Second * 1)
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	err = closeShard(sh)
	require.NoError(t, err)

	existFile := filepath.Join(sh.dataPath, immutable.TsspDirName, "mst", "00000001-0000-00000000.tssp")
	sh.copyFileRollBack(existFile)

	sh.copyFileRollBack("")
}

func NewMockColumnStoreMstInfo() *meta2.MeasurementInfo {
	return &meta2.MeasurementInfo{
		Name: "cpu",
		Schema: map[string]int32{
			"field2_int":    influx.Field_Type_Int,
			"field3_bool":   influx.Field_Type_Boolean,
			"field4_float":  influx.Field_Type_Float,
			"field1_string": influx.Field_Type_String,
			"time":          influx.Field_Type_Int,
		},
		ColStoreInfo: &meta2.ColStoreInfo{
			PrimaryKey: []string{"time", "field1_string"},
			SortKey:    []string{"time", "field1_string"},
		},
		EngineType: config.COLUMNSTORE,
	}
}

func TestAggQueryOnlyInImmutable_NoEmpty_OneBoolMinMaxOp(t *testing.T) {
	testDir := t.TempDir()
	configs := []TestConfig{
		{50, 3, time.Second, false},
	}
	for _, conf := range configs {
		fileops.EnableMmapRead(false)
		executor.EnableFileCursor(true)
		if testing.Short() && conf.short {
			t.Skip("skipping test in short mode.")
		}
		// step1: clean env
		_ = os.RemoveAll(testDir)

		// step2: create shard
		sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.TSSTORE)
		if err != nil {
			t.Fatal(err)
		}

		// step3: write data, mem table row limit less than row cnt, query will get record from both mem table and immutable
		rows, minTime, maxTime, result := GenAggDataRecord([]string{"cpu"}, conf.seriesNum, conf.pointNumPerSeries, conf.interval, time.Now(), true, true, true)
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		err = writeData(sh, rows, true)
		if err != nil {
			t.Fatal(err)
		}
		// query data and judge
		cases := []TestAggCase{
			{"PartFieldFilter_single_column_bool_min", minTime, maxTime, createFieldAux([]string{"field3_bool"}), "", nil, true, []string{"min"}},
			{"PartFieldFilter_single_column_bool_max", minTime, maxTime, createFieldAux([]string{"field3_bool"}), "", nil, true, []string{"max"}},
		}
		chunkSize := []int{1, 2}
		timeOrder := []bool{true, false}
		for _, ascending := range timeOrder {
			for _, c := range cases {
				for _, size := range chunkSize {
					c := c
					ascending := ascending
					t.Run(c.Name, func(t *testing.T) {
						for i := range c.aggCall {
							opt := genAggQueryOpt(&c, "cpu", ascending, size, conf.interval)
							calls := genCall(c.fieldAux, c.aggCall[i])
							querySchema := genAggQuerySchema(c.fieldAux, calls, opt)
							ops := genOps(c.fieldAux, calls)
							cursors, err := sh.CreateCursor(context.Background(), querySchema)
							if err != nil {
								t.Fatal(err)
							}

							updateClusterCursor(cursors, ops, c.aggCall[i])
							// step5: loop all cursors to query data from shard
							// key is indexKey, value is Record
							m := genExpectRecordsMap(rows, querySchema)
							errs := make(chan error, len(cursors))
							checkAggQueryResultParallel(errs, cursors, m, ascending, c.aggCall[i], *result)
							close(errs)
							for i := 0; i < len(cursors); i++ {
								err = <-errs
								if err != nil {
									t.Fatal(err)
								}
							}
						}
					})
				}
			}

		}
		// step6: close shard
		err = closeShard(sh)
		if err != nil {
			t.Fatal(err)
		}
	}
	executor.EnableFileCursor(false)
}

type MockMetaClient struct {
	metaclient.MetaClient
}

func (client *MockMetaClient) ThermalShards(db string, start, end time.Duration) map[uint64]struct{} {
	//TODO implement me
	panic("implement me")
}

func (client *MockMetaClient) GetStreamInfosStore() map[string]*meta2.StreamInfo {
	//TODO implement me
	panic("implement me")
}

func (client *MockMetaClient) GetMeasurementInfoStore(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return nil, nil
}

func (client *MockMetaClient) GetMeasurementsInfoStore(dbName string, rpName string) (*meta2.MeasurementsInfo, error) {
	return nil, nil
}

func (client *MockMetaClient) OpenAtStore() {
	panic("implement me")
}

func (client *MockMetaClient) UpdateShardDownSampleInfo(Ident *meta2.ShardIdentifier) error {
	return nil
}

func mockMetaClient() *MockMetaClient {
	return &MockMetaClient{}
}

func (client *MockMetaClient) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
	panic("implement me")
}

func (client *MockMetaClient) CreateMeasurement(database string, retentionPolicy string, mst string, shardKey *meta2.ShardKeyInfo, indexR *influxql.IndexRelation,
	engineType config.EngineType, colStoreInfo *meta2.ColStoreInfo, _ []*proto2.FieldSchema, options *meta2.Options) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) AlterShardKey(database, retentionPolicy, mst string, shardKey *meta2.ShardKeyInfo) error {
	return nil
}
func (client *MockMetaClient) CreateDatabase(name string, enableTagArray bool, replicaN uint32, options *obs.ObsOptions) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateDatabaseWithRetentionPolicy(name string, spec *meta2.RetentionPolicySpec, shardKey *meta2.ShardKeyInfo, enableTagArray bool, replicaN uint32) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateRetentionPolicy(database string, spec *meta2.RetentionPolicySpec, makeDefault bool) (*meta2.RetentionPolicyInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}
func (client *MockMetaClient) CreateUser(name, password string, admin, rwuser bool) (meta2.User, error) {
	return nil, nil
}
func (client *MockMetaClient) Databases() map[string]*meta2.DatabaseInfo {
	return nil
}
func (client *MockMetaClient) Database(name string) (*meta2.DatabaseInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) DataNode(id uint64) (*meta2.DataNode, error) {
	return nil, nil
}
func (client *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, nil
}
func (client *MockMetaClient) DeleteDataNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DeleteMetaNode(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropShard(id uint64) error {
	return nil
}
func (client *MockMetaClient) DropDatabase(name string) error {
	return nil
}
func (client *MockMetaClient) DropRetentionPolicy(database, name string) error {
	return nil
}
func (client *MockMetaClient) DropSubscription(database, rp, name string) error {
	return nil
}
func (client *MockMetaClient) DropUser(name string) error {
	return nil
}
func (client *MockMetaClient) MetaNodes() ([]meta2.NodeInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) RetentionPolicy(database, name string) (rpi *meta2.RetentionPolicyInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) SetAdminPrivilege(username string, admin bool) error {
	return nil
}
func (client *MockMetaClient) SetPrivilege(username, database string, p originql.Privilege) error {
	return nil
}
func (client *MockMetaClient) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta2.ShardInfo, err error) {
	return nil, nil
}
func (client *MockMetaClient) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta2.ShardGroupInfo, err error) {
	return nil, nil
}

func (client *MockMetaClient) UpdateRetentionPolicy(database, name string, rpu *meta2.RetentionPolicyUpdate, makeDefault bool) error {
	return nil
}
func (client *MockMetaClient) UpdateUser(name, password string) error {
	return nil
}
func (client *MockMetaClient) UserPrivilege(username, database string) (*originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) UserPrivileges(username string) (map[string]originql.Privilege, error) {
	return nil, nil
}
func (client *MockMetaClient) Users() []meta2.UserInfo {
	return nil
}
func (client *MockMetaClient) MarkDatabaseDelete(name string) error {
	return nil
}
func (client *MockMetaClient) MarkRetentionPolicyDelete(database, name string) error {
	return nil
}
func (client *MockMetaClient) MarkMeasurementDelete(database, mst string) error {
	return nil
}
func (client *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	return nil, nil
}
func (client *MockMetaClient) DBRepGroups(database string) []meta2.ReplicaGroup {
	return nil
}
func (client *MockMetaClient) GetReplicaN(database string) (int, error) {
	return 1, nil
}
func (client *MockMetaClient) ShardOwner(shardID uint64) (database, policy string, sgi *meta2.ShardGroupInfo) {
	return "", "", nil
}
func (client *MockMetaClient) Measurement(database string, rpName string, mstName string) (*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Schema(database string, retentionPolicy string, mst string) (fields map[string]int32, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (client *MockMetaClient) GetMeasurements(m *influxql.Measurement) ([]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) TagKeys(database string) map[string]set.Set {
	return nil
}
func (client *MockMetaClient) FieldKeys(database string, ms influxql.Measurements) (map[string]map[string]int32, error) {
	return nil, nil
}
func (client *MockMetaClient) QueryTagKeys(database string, ms influxql.Measurements, cond influxql.Expr) (map[string]map[string]struct{}, error) {
	return nil, nil
}
func (client *MockMetaClient) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	return nil, nil
}
func (client *MockMetaClient) Measurements(database string, ms influxql.Measurements) ([]string, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowShards() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowShardGroups() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowSubscriptions() models.Rows {
	return nil
}
func (client *MockMetaClient) ShowRetentionPolicies(database string) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowContinuousQueries() (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) ShowCluster() models.Rows { return nil }
func (client *MockMetaClient) ShowClusterWithCondition(nodeType string, ID uint64) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) GetAliveShards(database string, sgi *meta2.ShardGroupInfo) []int {
	return nil
}

var adminUserExists bool = true

func (client *MockMetaClient) AdminUserExists() bool {
	return adminUserExists
}

var authenticateOk bool = true

func (client *MockMetaClient) Authenticate(username, password string) (u meta2.User, e error) {
	if authenticateOk {
		return nil, nil
	}
	return nil, meta2.ErrUserLocked
}

func (client *MockMetaClient) DropDownSamplePolicy(database, name string, dropAll bool) error {
	return nil
}

func (client *MockMetaClient) NewDownSamplePolicy(database, name string, info *meta2.DownSamplePolicyInfo) error {
	return nil
}

func (client *MockMetaClient) ShowDownSamplePolicies(database string) (models.Rows, error) {
	return nil, nil
}
func (client *MockMetaClient) UpdateUserInfo() {}

func (client *MockMetaClient) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta2.RpMeasurementsFieldsInfo, error) {
	return nil, nil
}

func (client *MockMetaClient) UpdateStreamMstSchema(database string, retentionPolicy string, mst string, stmt *influxql.SelectStatement) error {
	return nil
}

func (client *MockMetaClient) CreateStreamPolicy(info *meta.StreamInfo) error {
	return nil
}

func (client *MockMetaClient) ShowStreams(database string, showAll bool) (models.Rows, error) {
	return nil, nil
}

func (client *MockMetaClient) DropStream(name string) error {
	return nil
}

func (client *MockMetaClient) GetStreamInfos() map[string]*meta2.StreamInfo {
	return nil
}

func (client *MockMetaClient) GetDstStreamInfos(db, rp string, dstSis *[]*meta2.StreamInfo) bool {
	return false
}

func (mmc *MockMetaClient) GetAllMst(dbName string) []string {
	var msts []string
	msts = append(msts, "cpu")
	msts = append(msts, "mem")
	return msts
}

func (client *MockMetaClient) RetryRegisterQueryIDOffset(host string) (uint64, error) {
	return 0, nil
}

func TestDropMeasurementWhenReplayingWal(t *testing.T) {
	sh := &shard{stopDownSample: util.NewSignal()}
	sh.replayingWal = true
	require.EqualError(t, sh.DropMeasurement(context.Background(), "mst"), "async replay wal not finish")
}
