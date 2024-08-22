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
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/readcache"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	Init()
	SetMaxCompactor(cpu.GetCpuNum())
	SetMaxFullCompactor(1)
}

var (
	testTimeStart = time.Date(2021, 11, 1, 0, 0, 0, 0, time.UTC)
	timeInterval  = time.Second
)

type MinMax struct {
	min, max uint64
}

var schemaForColumnStore = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "primaryKey_float1", Type: influx.Field_Type_Float},
	{Name: "primaryKey_string1", Type: influx.Field_Type_String},
	{Name: "primaryKey_string2", Type: influx.Field_Type_String},
	{Name: "sortKey_int1", Type: influx.Field_Type_Int},
	{Name: "time", Type: influx.Field_Type_Int},
}

func genSortedKeyMap(sk []string) map[string]int {
	sortKeyMap := make(map[string]int)
	for i := 0; i < len(sk); i++ {
		for idx, field := range schemaForColumnStore {
			if sk[i] == field.Name {
				sortKeyMap[field.Name] = idx
			}
		}
	}
	return sortKeyMap
}

func genTestDataForColumnStore(id uint64, idCount int, rows int, startValue *float64, starTime *time.Time) (uint64, map[uint64]*record.Record) {
	tm := *starTime

	value := *startValue

	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schemaForColumnStore)
		f1Builder := b.Column(0) // float
		f2Builder := b.Column(1) // string
		f3Builder := b.Column(2) // float
		f4Builder := b.Column(3) // int
		f5Builder := b.Column(4) // float
		f6Builder := b.Column(5) // int
		f7Builder := b.Column(6) // bool
		f8Builder := b.Column(7) // string
		tmBuilder := b.Column(8) // timestamp
		for i := 1; i <= rows; i++ {
			f1Builder.AppendFloat(value)
			f2Builder.AppendInteger(int64(value))
			f3Builder.AppendBoolean(bv)
			f4Builder.AppendString(fmt.Sprintf("test-%f", value))
			f5Builder.AppendFloat(value)
			f6Builder.AppendString(fmt.Sprintf("test-%f", value+1.0))
			f7Builder.AppendString(fmt.Sprintf("test-%f", value+2.0))
			f8Builder.AppendInteger(int64(value))
			tmBuilder.AppendInteger(tm.UnixNano())

			tm = tm.Add(timeInterval)
			value += 1.0
			bv = !bv
		}

		*starTime = tm
		*startValue = value
		return b
	}

	data := make(map[uint64]*record.Record, idCount)
	for i := 0; i < idCount; i++ {
		rec := genRecFn()
		data[id] = rec
	}

	return id, data
}

func mergeForColumnStore(sk []string, sortKeyMap map[string]int, newRec, oldRec *record.Record) *record.Record {
	mergeRec := record.NewRecordBuilder(schemaForColumnStore)
	mergeRec.ReserveColumnRows(8192)
	newRecRows := newRec.RowNums()
	oldRecRows := oldRec.RowNums()

	posOld, posNew := 0, 0
	for posOld < oldRecRows && posNew < newRecRows {
	LooP:
		for oldIdx := 0; oldIdx < len(sk); oldIdx++ {
			switch oldRec.Schema[sortKeyMap[sk[oldIdx]]].Type {
			case influx.Field_Type_Int:
				col, isNil := oldRec.ColVals[sortKeyMap[sk[oldIdx]]].IntegerValue(posOld)
				if isNil {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
				newCol, isNil := newRec.ColVals[sortKeyMap[sk[oldIdx]]].IntegerValue(posNew)
				if isNil {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				}
				if col > newCol {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				} else if col < newCol {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
			case influx.Field_Type_Float:
				col, isNil := oldRec.ColVals[sortKeyMap[sk[oldIdx]]].FloatValue(posOld)
				if isNil {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
				newCol, isNil := newRec.ColVals[sortKeyMap[sk[oldIdx]]].FloatValue(posNew)
				if isNil {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				}
				if col > newCol {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				} else if col < newCol {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
			case influx.Field_Type_String:
				col, isNil := oldRec.ColVals[sortKeyMap[sk[oldIdx]]].StringValueSafe(posOld)
				if isNil {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
				newCol, isNil := newRec.ColVals[sortKeyMap[sk[oldIdx]]].StringValueSafe(posNew)
				if isNil {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				}
				if col > newCol {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				} else if col < newCol {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
			case influx.Field_Type_Boolean:
				col, isNil := oldRec.ColVals[sortKeyMap[sk[oldIdx]]].BooleanValue(posOld)
				if isNil {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
				newCol, isNil := newRec.ColVals[sortKeyMap[sk[oldIdx]]].BooleanValue(posNew)
				if isNil {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				}
				if col == newCol {
					break
				}
				if newCol == false {
					mergeRec.AppendRec(newRec, posNew, posNew+1)
					posNew++
					break LooP
				} else if col == false {
					mergeRec.AppendRec(oldRec, posOld, posOld+1)
					posOld++
					break LooP
				}
			}
			if oldIdx == oldRecRows-1 {
				mergeRec.AppendRec(newRec, posNew, posNew+1)
				mergeRec.AppendRec(oldRec, posOld, posOld+1)
				posOld++
				posNew++
			}
		}
	}
	if posOld < oldRecRows {
		mergeRec.AppendRec(oldRec, posOld, oldRecRows)
	} else if posNew < newRecRows {
		mergeRec.AppendRec(newRec, posNew, newRecRows)
	}
	return mergeRec
}

func writeIntoFile(msb *MsBuilder, tmp bool) error {
	f, err := msb.NewTSSPFile(tmp)
	if err != nil {
		panic(err)
	}
	if f != nil {
		msb.Files = append(msb.Files, f)
	}

	err = RenameTmpFiles(msb.Files)
	if err != nil {
		return err
	}
	return nil
}

func TestFullCompactForColumnStore(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	tier := uint64(util.Hot)
	recRows := 10000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_string1", "primaryKey_string2", "primaryKey_float1"}
	sortKey := []string{"primaryKey_string1", "primaryKey_string2", "primaryKey_float1", "sortKey_int1"}
	sort := []string{"primaryKey_string1", "primaryKey_string2", "primaryKey_float1", "time", "sortKey_int1"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}

	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey: primaryKey,
			SortKey:    sortKey,
		},
		Schema: &schema,
	}
	store.ImmTable.SetMstInfo("mst", &mstinfo)

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
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)

	needMerge := false
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
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
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}
	// get origin pkRec
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	msb.NewPKIndexWriter()

	fixRowsPerSegment := GenFixRowsPerSegment(oldRec, conf.maxRowsPerSegment)
	pkRec, pkMark, err := msb.pkIndexWriter.Build(oldRec, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, util.DefaultMaxRowsPerSegment4ColStore)
	if err != nil {
		t.Fatal(err)
	}

	if err = store.FullCompact(1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}

	dataFilePath := store.CSFiles["mst"].files[0].FileName()
	dataFileName := dataFilePath.String()
	indexFilePath := path.Join(store.path, "mst", colstore.AppendPKIndexSuffix(dataFileName))
	newPkInfos := store.PKFiles["mst"].GetPKInfos()
	if pkMark.GetFragmentCount() != newPkInfos[indexFilePath].GetMark().GetFragmentCount() {
		t.Fatal("FragmentCount is not equal")
	}
	if equal := checkPkRecord(pkRec, newPkInfos[indexFilePath].GetRec()); !equal {
		t.Fatal("pkRec is not equal")
	}
	check("mst", fids.files[0].Path(), oldRec)
}

func TestLevelCompactForColumnStore(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	tier := uint64(util.Hot)
	recRows := 10000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "field3_bool", "field2_int"}
	sortKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int"}
	sort := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "time", "sortKey_int1", "field3_bool", "field2_int"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey: primaryKey,
			SortKey:    sortKey,
		},
		Schema: &schema,
	}
	store.ImmTable.SetMstInfo("mst", &mstinfo)

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
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
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
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}
	// get origin pkRec
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	msb.NewPKIndexWriter()
	fixRowsPerSegment := GenFixRowsPerSegment(oldRec, conf.maxRowsPerSegment)
	pkRec, pkMark, err := msb.pkIndexWriter.Build(oldRec, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, util.DefaultMaxRowsPerSegment4ColStore)
	if err != nil {
		t.Fatal(err)
	}

	if err = store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}

	dataFilePath := store.CSFiles["mst"].files[0].FileName()
	dataFileName := dataFilePath.String()
	indexFilePath := path.Join(store.path, "mst", colstore.AppendPKIndexSuffix(dataFileName))
	newPkInfos := store.PKFiles["mst"].GetPKInfos()
	if pkMark.GetFragmentCount() != newPkInfos[indexFilePath].GetMark().GetFragmentCount() {
		t.Fatal("FragmentCount is not equal")
	}
	if equal := checkPkRecord(pkRec, newPkInfos[indexFilePath].GetRec()); !equal {
		t.Fatal("pkRec is not equal")
	}
	check("mst", fids.files[0].Path(), oldRec)
}

func TestLevelCompactForColumnStoreWithTimeCluster(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	tier := uint64(util.Hot)
	recRows := 10000
	lockPath := ""
	var tcDuration time.Duration = 60000000000

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "field3_bool", "field2_int", "time"}
	sortKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int", "time"}
	sort := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int", "time"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:          primaryKey,
			SortKey:             sortKey,
			TimeClusterDuration: tcDuration,
		},
		Schema: &schema,
	}
	store.ImmTable.SetMstInfo("mst", &mstinfo)

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
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
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
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}
	// get origin pkRec
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	msb.NewPKIndexWriter()
	fixRowsPerSegment := GenFixRowsPerSegment(oldRec, conf.maxRowsPerSegment)
	pkRec, pkMark, err := msb.pkIndexWriter.Build(oldRec, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, util.DefaultMaxRowsPerSegment4ColStore)
	if err != nil {
		t.Fatal(err)
	}

	if err = store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}

	dataFilePath := store.CSFiles["mst"].files[0].FileName()
	dataFileName := dataFilePath.String()
	indexFilePath := path.Join(store.path, "mst", colstore.AppendPKIndexSuffix(dataFileName))
	newPkInfos := store.PKFiles["mst"].GetPKInfos()
	if pkMark.GetFragmentCount() != newPkInfos[indexFilePath].GetMark().GetFragmentCount() {
		t.Fatal("FragmentCount is not equal")
	}
	if equal := checkPkRecordWithTimeCluster(pkRec, newPkInfos[indexFilePath].GetRec(), tcDuration); !equal {
		t.Fatal("pkRec is not equal")
	}
	check("mst", fids.files[0].Path(), oldRec)
}

func TestLevelCompactForColumnStoreWithSchemaLess(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	tier := uint64(util.Hot)
	recRows := 10000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"field1_float", "field2_int"}
	sortKey := []string{"field1_float", "field2_int"}
	sort := []string{"field1_float", "field2_int"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}

	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey: primaryKey,
			SortKey:    sortKey,
		},
		Schema: &schema,
	}
	store.ImmTable.SetMstInfo("mst", &mstinfo)

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
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)

	needMerge := false
	var ids uint64
	var data map[uint64]*record.Record
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
	for i := 0; i < filesN; i++ {
		if i == filesN-1 {
			_, data = genTestData(0, 1, recRows, &startValue, &tm)
		} else {
			ids, data = genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		}
		//ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
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
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}
	// get origin pkRec
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	msb.NewPKIndexWriter()
	fixRowsPerSegment := GenFixRowsPerSegment(oldRec, conf.maxRowsPerSegment)
	_, _, err := msb.pkIndexWriter.Build(oldRec, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, util.DefaultMaxRowsPerSegment4ColStore)
	if err != nil {
		t.Fatal(err)
	}
	conf.SetFilesLimit(1024)
	if err = store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	//expect split file
	if fids.Len() != 2 {
		t.Fatalf("exp 2 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}
}

func TestCompactSwitchFilesForColumnStore(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	conf.fileSizeLimit = 1 * 1024
	defer func() {
		conf.fileSizeLimit = util.DefaultFileSizeLimit
	}()
	tier := uint64(util.Hot)
	recRows := 8192
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "field3_bool", "field2_int"}
	sortKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int"}
	sort := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "time", "sortKey_int1", "field3_bool", "field2_int"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	store.ImmTable.(*csImmTableImpl).mstsInfo["mst"] = &meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey: primaryKey,
			SortKey:    sortKey,
		},
		Schema: &schema,
	}

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
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)

	needMerge := false
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
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
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}
	// get origin pkRec
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
	msb.NewPKIndexWriter()
	fixRowsPerSegment := GenFixRowsPerSegment(oldRec, conf.maxRowsPerSegment)
	_, _, err := msb.pkIndexWriter.Build(oldRec, pkSchema, fixRowsPerSegment, colstore.DefaultTCLocation, util.DefaultMaxRowsPerSegment4ColStore)
	if err != nil {
		t.Fatal(err)
	}

	if err = store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() <= 1 {
		t.Fatalf("exp more than 1 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}

	if fids.Len() != len(store.PKFiles["mst"].GetPKInfos()) {
		t.Fatalf("the number of data files and index files is not equal")
	}
}

func checkPkRecord(oldRec, newRec *record.Record) bool {
	oldV0 := oldRec.Column(0).StringValues(nil)
	oldV1 := oldRec.Column(1).StringValues(nil)
	oldV2 := oldRec.Column(2).FloatValues()

	v0 := newRec.Column(0).StringValues(nil)
	v1 := newRec.Column(1).StringValues(nil)
	v2 := newRec.Column(2).FloatValues()

	if !reflect.DeepEqual(oldV0, v0) {
		return false
	}

	if !reflect.DeepEqual(oldV1, v1) {
		return false
	}

	if !reflect.DeepEqual(oldV2, v2) {
		return false
	}
	return true
}

func checkPkRecordWithTimeCluster(oldRec, newRec *record.Record, tcDuration time.Duration) bool {
	oldvt := oldRec.TimeColumn()
	oldV0 := oldRec.Column(0).StringValues(nil)
	oldV1 := oldRec.Column(1).StringValues(nil)
	oldV2 := oldRec.Column(2).FloatValues()

	oldvtValues := oldvt.IntegerValues()
	for k, v := range oldvtValues {
		oldvtValues[k] = int64(time.Duration(v).Truncate(tcDuration))
	}

	vt := newRec.Column(0).IntegerValues()
	v0 := newRec.Column(1).StringValues(nil)
	v1 := newRec.Column(2).StringValues(nil)
	v2 := newRec.Column(3).FloatValues()

	if !reflect.DeepEqual(oldvtValues, vt) {
		return false
	}
	if !reflect.DeepEqual(oldV0, v0) {
		return false
	}

	if !reflect.DeepEqual(oldV1, v1) {
		return false
	}

	if !reflect.DeepEqual(oldV2, v2) {
		return false
	}
	return true
}

func checkPkRecordWithSchemaLess(oldRec, newRec *record.Record) bool {
	oldV0 := oldRec.Column(0).FloatValues()
	oldV1 := oldRec.Column(1).IntegerValues()

	v0 := newRec.Column(0).FloatValues()
	v1 := newRec.Column(1).IntegerValues()

	if !reflect.DeepEqual(oldV0, v0) {
		return false
	}

	if !reflect.DeepEqual(oldV1, v1) {
		return false
	}
	return true
}

func TestWriteMetaIndexForColumnStore(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	f := &FragmentIterators{}
	f.builder = NewMsBuilder(store.path, "cpu", store.lock, store.Conf, 1, fileName, *store.tier, nil, 1, config.TSSTORE, nil, 0)
	defer func() {
		f.builder.diskFileWriter.Close()
		f.builder.fd.Close()
	}()
	f.builder.Conf = conf
	f.builder.Conf.maxChunkMetaItemSize = 0
	f.builder.trailer.idCount = 1
	f.builder.trailer.minTime = 1
	f.builder.trailer.maxTime = -1
	f.builder.chunkBuilder.chunkMeta.timeRange = []SegmentRange{{0, 0}}
	err := f.writeMetaIndex(10, -999, 999)
	if err != nil {
		t.Fatal(err)
	}

}

func TestNewFragmentIteratorsPool(t *testing.T) {
	var n int
	n = 1
	p := NewFragmentIteratorsPool(n)
	if p == nil {
		t.Fatal("New Fragment Iterators Pool Failed")
	}
	n = 20
	p = NewFragmentIteratorsPool(n)
	if p == nil {
		t.Fatal("New Fragment Iterators Pool Failed")
	}
}

func TestPutFragmentIterators(t *testing.T) {
	for i := 0; i < cpu.GetCpuNum()+1; i++ {
		itr := FragmentIterators{
			ctx:               NewReadContext(true),
			colBuilder:        NewColumnBuilder(),
			itrs:              make([]*SortKeyIterator, 0, 8),
			SortKeyFileds:     make([]record.Field, 0, 16),
			RecordResult:      &record.Record{},
			TimeClusterResult: &record.Record{},
			schemaMap:         make(map[string]struct{}),
		}
		putFragmentIterators(&itr)
	}
}

func TestCompactionWriteMetaErr(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	sortKey := []string{"primaryKey_string1", "primaryKey_string2", "primaryKey_float1", "sortKey_int1"}
	store.ImmTable.(*csImmTableImpl).mstsInfo["mst"] = &meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey: sortKey,
		},
	}
	cm := make([]ColumnMeta, 1)
	cm[0] = ColumnMeta{name: "a", ty: influx.Field_Type_Int}
	fileItr := FileIterator{curtChunkMeta: &ChunkMeta{colMeta: cm}}
	filesItr := make(FileIterators, 0, 1)
	filesItr = append(filesItr, &fileItr)
	group := FilesInfo{name: "mst", compIts: filesItr}
	compItrs := getFragmentIterators()
	fileName := NewTSSPFileName(uint64(1), group.toLevel, 0, 0, true, store.lock)
	compItrs.builder = NewMsBuilder(store.path, compItrs.name, store.lock, store.Conf, 1, fileName, *store.tier, nil, 1, config.TSSTORE, nil, 0)
	indexName := compItrs.builder.fd.Name()[:len(compItrs.builder.fd.Name())-len(tmpFileSuffix)] + ".index.init"
	fd, err := fileops.OpenFile(indexName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fd.Close()
		compItrs.builder.fd.Close()
	}()
	tsspFileWriter := compItrs.builder.diskFileWriter.(*tsspFileWriter)
	cmw, ok := tsspFileWriter.cmw.(*indexWriter)
	require.True(t, ok)

	cmw.buf = cmw.buf[:1]
	cmw.name = "/"
	err = compItrs.writeMeta(&ChunkMeta{colMeta: cm}, 0, 1)
	if err == nil {
		t.Fatalf("writeMeta should open file error")
	}
}

func TestBlockCompactionPrepareForColumnStore(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 8192
	conf.FragmentsNumPerFlush = 1
	defer func() {
		conf.maxRowsPerSegment = util.DefaultMaxRowsPerSegment4ColStore
	}()
	tier := uint64(util.Hot)
	recRows := 10000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "field3_bool", "field2_int"}
	sortKey := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "sortKey_int1", "field3_bool", "field2_int"}
	sort := []string{"primaryKey_float1", "primaryKey_string1", "primaryKey_string2", "time", "sortKey_int1", "field3_bool", "field2_int"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: &schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	store.ImmTable.SetMstInfo("mst", &mstinfo)
	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas, indexRelation *influxql.IndexRelation) *record.Record {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}

		msb.NewIndexWriterBuilder(rec.Schema, *indexRelation)
		if len(msb.indexWriterBuilder.GetSkipIndexWriters()) > 0 {
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err := msb.writeSkipIndex(rec, fixRowsPerSegment); err != nil {
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

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
	needMerge := false
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		msb.NewPKIndexWriter()
		msb.NewIndexWriterBuilder(data[ids].Schema, mstinfo.IndexRelation)
		oldRec = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema, &mstinfo.IndexRelation)
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			t.Fatal(err)
		}
		fn := msb.Files[len(msb.Files)-1].Path()
		if err := RenameIndexFiles(fn, bfColumn); err != nil {
			t.Fatal(err)
		}
		store.AddTSSPFiles(msb.Name(), false, msb.Files...)
		for _, v := range data {
			recs = append(recs, v)
		}
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}
	group := store.ImmTable.LevelPlan(store, 0)[0]
	//group := FilesInfo{name: "mst", compIts: filesItr, oldFiles: fids.files}
	cLog, logEnd := logger.NewOperation(log, "Compaction test for column store", group.name)
	defer logEnd()
	lcLog := Log.NewLogger(errno.ModuleCompact).SetZapLogger(cLog)
	cs := store.ImmTable.(*csImmTableImpl)
	fi, err := store.ImmTable.NewFileIterators(store, group)
	if err != nil {
		t.Fatal(err)
	}
	fi.totalSegmentCount = uint64(logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterCntPerVerticalGorup)
	_, err = cs.NewFragmentIterators(store, fi, lcLog, &mstinfo)
	if err != nil {
		t.Fatal(err)
	}
}

// TestLevelBlockCompactForColumnStore total block count < limit(128)
func TestLevelBlockCompactForColumnStoreV1(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 20
	conf.FragmentsNumPerFlush = 1
	defer func() {
		conf.maxRowsPerSegment = util.DefaultMaxRowsPerSegment4ColStore
	}()
	tier := uint64(util.Hot)
	recRows := 1000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	sort := []string{"time"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: &schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	store.ImmTable.SetMstInfo("mst", &mstinfo)
	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas, indexRelation *influxql.IndexRelation) *record.Record {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		msb.NewIndexWriterBuilder(rec.Schema, *indexRelation)
		if len(msb.indexWriterBuilder.GetSkipIndexWriters()) > 0 {
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err := msb.writeSkipIndex(rec, fixRowsPerSegment); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
	needMerge := false
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		msb.NewPKIndexWriter()
		msb.NewIndexWriterBuilder(data[ids].Schema, mstinfo.IndexRelation)
		oldRec = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema, &mstinfo.IndexRelation)
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			t.Fatal(err)
		}
		fn := msb.Files[len(msb.Files)-1].Path()
		if err := RenameIndexFiles(fn, bfColumn); err != nil {
			t.Fatal(err)
		}
		store.AddTSSPFiles(msb.Name(), false, msb.Files...)
		for _, v := range data {
			recs = append(recs, v)
		}
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}

	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but got len:%d,fids:%v", fids.Len(), fids)
	}
}

// TestLevelBlockCompactForColumnStoreV2 total block count > limit(128) and continue executing 2 times compact
func TestLevelBlockCompactForColumnStoreV2(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 999999.0

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 20
	conf.FragmentsNumPerFlush = 1
	defer func() {
		conf.maxRowsPerSegment = util.DefaultMaxRowsPerSegment4ColStore
	}()
	tier := uint64(util.Hot)
	recRows := 1000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	sort := []string{"time"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: &schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	store.ImmTable.SetMstInfo("mst", &mstinfo)
	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas, indexRelation *influxql.IndexRelation) *record.Record {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}
		msb.NewIndexWriterBuilder(rec.Schema, *indexRelation)
		if len(msb.indexWriterBuilder.GetSkipIndexWriters()) > 0 {
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err := msb.writeSkipIndex(rec, fixRowsPerSegment); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0
	compactionTimes := 2
	filesN := LeveLMinGroupFiles[0] * compactionTimes
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
	needMerge := false
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		msb.NewPKIndexWriter()
		msb.NewIndexWriterBuilder(data[ids].Schema, mstinfo.IndexRelation)
		oldRec = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema, &mstinfo.IndexRelation)
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			t.Fatal(err)
		}
		fn := msb.Files[len(msb.Files)-1].Path()
		if err := RenameIndexFiles(fn, bfColumn); err != nil {
			t.Fatal(err)
		}
		store.AddTSSPFiles(msb.Name(), false, msb.Files...)
		for _, v := range data {
			recs = append(recs, v)
		}
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])

		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}

	for i := 0; i < compactionTimes; i++ {
		if err := store.LevelCompact(0, 1); err != nil {
			t.Fatal(err)
		}
		store.Wait()
	}
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}
}

func TestBlockCompactFileNameConflict(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 999999.0

	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 20
	conf.FragmentsNumPerFlush = 1
	conf.fileSizeLimit = 1 * 1024
	defer func() {
		conf.maxRowsPerSegment = util.DefaultMaxRowsPerSegment4ColStore
		conf.fileSizeLimit = util.DefaultFileSizeLimit
	}()

	tier := uint64(util.Hot)
	recRows := 1000
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()

	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	sort := []string{"time"}
	schema := make(meta.CleanSchema)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: &schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	store.ImmTable.SetMstInfo("mst", &mstinfo)
	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas, indexRelation *influxql.IndexRelation) *record.Record {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, util.DefaultMaxRowsPerSegment4ColStore); err != nil {
				t.Fatal(err)
			}
		}

		msb.NewIndexWriterBuilder(rec.Schema, *indexRelation)
		if len(msb.indexWriterBuilder.GetSkipIndexWriters()) > 0 {
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err := msb.writeSkipIndex(rec, fixRowsPerSegment); err != nil {
				t.Fatal(err)
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge
		}
		return rec
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0
	compactionTimes := 2
	filesN := LeveLMinGroupFiles[0] * compactionTimes
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		v, _ := store.ImmTable.(*csImmTableImpl).mstsInfo["mst"].Schema.GetTyp(pk[i])
		pkSchema[i] = record.Field{
			Type: int(v),
			Name: pk[i],
		}
	}
	needMerge := false
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		msb.NewPKIndexWriter()
		msb.NewIndexWriterBuilder(data[ids].Schema, mstinfo.IndexRelation)
		oldRec = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema, &mstinfo.IndexRelation)
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			t.Fatal(err)
		}
		fn := msb.Files[len(msb.Files)-1].Path()
		if err := RenameIndexFiles(fn, bfColumn); err != nil {
			t.Fatal(err)
		}
		store.AddTSSPFiles(msb.Name(), false, msb.Files...)
		for _, v := range data {
			recs = append(recs, v)
		}
		if msb.GetPKInfoNum() != 0 {
			for i, file := range msb.Files {
				dataFilePath := file.Path()
				indexFilePath := colstore.AppendPKIndexSuffix(RemoveTsspSuffix(dataFilePath))
				store.AddPKFile(msb.Name(), indexFilePath, msb.GetPKRecord(i), msb.GetPKMark(i), colstore.DefaultTCLocation)
			}
		}
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())
	files := store.CSFiles
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])

		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}

	for i := 0; i < compactionTimes; i++ {
		if err := store.LevelCompact(0, 1); err != nil {
			t.Fatal(err)
		}
		store.Wait()
	}
	files = store.CSFiles
	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}
}

func TestNextSingleFragmentError(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	curItrs := &SortKeyIterator{FileIterator: &FileIterator{}, segmentPostion: 1, position: 0}
	cm := make([]ColumnMeta, 1)
	curItrs.curtChunkMeta = &ChunkMeta{colMeta: cm}
	f := &FragmentIterators{}
	f.builder = NewMsBuilder(store.path, "cpu", store.lock, store.Conf, 1, fileName, *store.tier, nil, 1, config.COLUMNSTORE, nil, 0)
	f.builder.Conf = conf
	f.builder.Conf.maxRowsPerSegment = 1
	f.Conf = f.builder.Conf
	pkSchema := record.Schemas{}
	var startValue = 1.1
	tm := testTimeStart
	_, data := genTestData(1, 1, 10, &startValue, &tm)
	f.RecordResult = data[1]
	f.TimeClusterResult = data[1]
	curItrs.curRec = data[1]
	curItrs.sortKeyColums = []*record.ColVal{&data[1].ColVals[0]}
	var err error
	impl := &IteratorByRow{f: f}
	f.RecordResult, err = curItrs.NextSingleFragment(store, impl, pkSchema)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewDetachedMsBuilderError(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()
	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	obsOpts := &obs.ObsOptions{}
	_, err := NewDetachedMsBuilder(store.path, "cpu", store.lock, store.Conf, 1, fileName, *store.tier, nil, 1, config.COLUMNSTORE, obsOpts, nil, false)
	require.Equal(t, err, errors.New("endpoint is not set"))
}

var schema = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "time", Type: influx.Field_Type_Int},
}

func genTestData(id uint64, idCount int, rows int, startValue *float64, starTime *time.Time) ([]uint64, map[uint64]*record.Record) {
	tm := (*starTime).Add(timeInterval)

	value := *startValue

	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schema)
		f1Builder := b.Column(0) // float
		f2Builder := b.Column(1) // int
		f3Builder := b.Column(2) // bool
		f4Builder := b.Column(3) // string
		tmBuilder := b.Column(4) // timestamp
		for i := 1; i <= rows; i++ {
			f1Builder.AppendFloat(value)
			f2Builder.AppendInteger(int64(value))
			f3Builder.AppendBoolean(bv)
			f4Builder.AppendString(fmt.Sprintf("test-%f", value))
			tmBuilder.AppendInteger(tm.UnixNano())

			tm = tm.Add(timeInterval)
			value += 1.0
			bv = !bv
		}

		*starTime = tm
		*startValue = value
		return b
	}

	ids := make([]uint64, 0, idCount)
	data := make(map[uint64]*record.Record, idCount)
	for i := 0; i < idCount; i++ {
		rec := genRecFn()
		data[id] = rec
		ids = append(ids, id)
		id++
	}

	return ids, data
}

func TestMmsTables_LevelCompact_With_FileHandle_Optimize(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	store.CompactionEnable()

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
			merge.Merge(rec)
		}
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart

	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 1

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schema)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	for i := 0; i < filesN; i++ {
		ids, data := genTestData(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		write(ids, data, msb, oldRec)
		for _, v := range data {
			recs = append(recs, v)
		}
		store.AddTable(msb, true, false)
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())

	files := store.Order
	if len(files) != 1 {
		t.Fatalf("exp 4 file, but:%v", len(files))
	}
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}
	for _, f := range fids.files {
		err := f.FreeFileHandle()
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			t.Fatal("ref error")
		}
	}

	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}

	store.Wait()

	files = store.Order
	if len(files) != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", len(files))
	}

	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", fids)
	}

	check("mst", fids.files[0].Path(), oldRec)
}

func TestMmsTables_LevelCompact_1ID5Segment(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	store.CompactionEnable()

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
			merge.Merge(rec)
		}
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart

	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 1

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schema)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	for i := 0; i < filesN; i++ {
		ids, data := genTestData(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		write(ids, data, msb, oldRec)
		for _, v := range data {
			recs = append(recs, v)
		}
		store.AddTable(msb, true, false)
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())

	files := store.Order
	if len(files) != 1 {
		t.Fatalf("exp 4 file, but:%v", len(files))
	}
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
	}

	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}

	store.Wait()

	files = store.Order
	if len(files) != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", len(files))
	}

	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", fids)
	}

	check("mst", fids.files[0].Path(), oldRec)
}

func TestMmsTables_FullCompact(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	store.CompactionEnable()

	write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record) {
		for _, id := range ids {
			rec := data[id]
			err := msb.WriteData(id, rec)
			if err != nil {
				t.Fatal(err)
			}
			merge.Merge(rec)
		}
	}

	check := func(name string, fn string, orig *record.Record) {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readRec.Merge(rec)
		}

		oldV0 := orig.Column(0).FloatValues()
		oldV1 := orig.Column(1).IntegerValues()
		oldV2 := orig.Column(2).BooleanValues()
		oldV3 := orig.Column(3).StringValues(nil)
		oldTimes := orig.Times()
		v0 := readRec.Column(0).FloatValues()
		v1 := readRec.Column(1).IntegerValues()
		v2 := readRec.Column(2).BooleanValues()
		v3 := readRec.Column(3).StringValues(nil)
		times := readRec.Times()

		if !reflect.DeepEqual(oldTimes, times) {
			t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			t.Fatalf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
	}

	tm := testTimeStart

	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 1

	filesN := LeveLMinGroupFiles[0]
	oldRec := record.NewRecordBuilder(schema)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	for i := 0; i < filesN; i++ {
		ids, data := genTestData(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
		write(ids, data, msb, oldRec)
		for _, v := range data {
			recs = append(recs, v)
		}
		store.AddTable(msb, true, false)
	}

	tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())

	files := store.Order
	if len(files) != 1 {
		t.Fatalf("exp 4 file, but:%v", len(files))
	}
	fids, ok := files["mst"]
	if !ok || fids.Len() != filesN {
		t.Fatalf("mst not find")
	}

	for i, f := range fids.files {
		check("mst", f.Path(), recs[i])
	}

	if err := store.FullCompact(1); err != nil {
		t.Fatal(err)
	}

	store.Wait()

	files = store.Order
	if len(files) != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", len(files))
	}

	fids, ok = files["mst"]
	if !ok {
		t.Fatalf("mst not find")
	}

	if fids.Len() != 1 {
		t.Fatalf("exp 1 file after compact, but:%v", fids)
	}

	check("mst", fids.files[0].Path(), oldRec)
}

func TestMmsTables_LevelCompact_20ID10Segment(t *testing.T) {
	mergeFlags := []int32{util.NonStreamingCompact, util.StreamingCompact, util.AutoCompact}
	testCompDir := t.TempDir()
	lockPath := ""
	tm := testTimeStart

	for _, flag := range mergeFlags {
		_ = fileops.RemoveAll(testCompDir)
		cacheIns := readcache.GetReadMetaCacheIns()
		cacheIns.Purge()
		sig := interruptsignal.NewInterruptSignal()
		defer func() {
			sig.Close()
			_ = fileops.RemoveAll(testCompDir)
		}()

		var idMinMax, tmMinMax MinMax
		var startValue = 1.1

		SetMergeFlag4TsStore(flag)
		conf := NewTsStoreConfig()
		conf.maxRowsPerSegment = 100
		conf.maxSegmentLimit = 65535
		tier := uint64(util.Hot)
		recRows := conf.maxRowsPerSegment*9 + 1

		store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
		store.SetImmTableType(config.TSSTORE)
		defer store.Close()

		store.CompactionEnable()

		write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder) {
			for _, id := range ids {
				rec := data[id]
				err := msb.WriteData(id, rec)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		check := func(name string, fn string, ids []uint64, orig []*record.Record) {
			f := store.File(name, fn, true)
			contains, err := f.Contains(idMinMax.min)
			if err != nil || !contains {
				t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
			}

			midx, _ := f.MetaIndexAt(0)
			if midx == nil {
				t.Fatalf("meta index not find")
			}

			decs := NewReadContext(true)
			cms, err := f.ReadChunkMetaData(0, midx, nil, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}

			if len(cms) != int(midx.count) {
				t.Fatalf("len(cms): %v != midx.count: %v", len(cms), midx.count)
			}

			for i := range cms {
				cm := &cms[i]
				if cm.sid != ids[i] {
					t.Fatalf("chunkmeta id not eq, exp:%v, get:%v", ids[i], cm.sid)
				}

				readRec := record.NewRecordBuilder(schema)
				readRec.ReserveColumnRows(recRows * 4)
				rec := record.NewRecordBuilder(schema)
				rec.ReserveColumnRows(conf.maxRowsPerSegment)
				for j := range cm.timeMeta().entries {
					rec, err = f.ReadAt(cm, j, rec, decs, fileops.IO_PRIORITY_LOW_READ)
					if err != nil {
						t.Fatal(err)
					}
					readRec.Merge(rec)
				}

				oldV0 := orig[i].Column(0).FloatValues()
				oldV1 := orig[i].Column(1).IntegerValues()
				oldV2 := orig[i].Column(2).BooleanValues()
				oldV3 := orig[i].Column(3).StringValues(nil)
				oldTimes := orig[i].Times()
				v0 := readRec.Column(0).FloatValues()
				v1 := readRec.Column(1).IntegerValues()
				v2 := readRec.Column(2).BooleanValues()
				v3 := readRec.Column(3).StringValues(nil)
				times := readRec.Times()

				if !reflect.DeepEqual(oldTimes, times) {
					t.Fatalf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
				}

				if !reflect.DeepEqual(oldV0, v0) {
					t.Fatalf("int value not eq, \nexp:%v \nget:%v", oldV0, v0)
				}
				if !reflect.DeepEqual(oldV1, v1) {
					t.Fatalf("value not eq, \nexp:%v \nget:%v", oldV1, v1)
				}
				if !reflect.DeepEqual(oldV2, v2) {
					t.Fatalf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
				}

				if !reflect.DeepEqual(oldV3, v3) {
					t.Fatalf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
				}
			}
		}

		tmMinMax.min = uint64(tm.UnixNano())
		idMinMax.min = 1

		filesN := LeveLMinGroupFiles[0]
		idCount := 20
		oldMergeRec := make([]*record.Record, idCount)
		recs := make([][]*record.Record, filesN)
		var dataIds []uint64
		for i := 0; i < filesN; i++ {
			ids, data := genTestData(idMinMax.min, idCount, recRows, &startValue, &tm)
			fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
			msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 10, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
			write(ids, data, msb)

			for j, id := range ids {
				idRec := data[id]
				if oldMergeRec[j] == nil {
					oldMergeRec[j] = record.NewRecordBuilder(schema)
				}
				oldMergeRec[j].Merge(idRec)
				recs[i] = append(recs[i], idRec)
			}
			dataIds = ids
			store.AddTable(msb, true, false)
		}

		tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())

		files := store.Order
		if len(files) != 1 {
			t.Fatalf("exp 4 file, but:%v", len(files))
		}
		fids, ok := files["mst"]
		if !ok || fids.Len() != filesN {
			t.Fatalf("mst not find")
		}

		for i, f := range fids.files {
			check("mst", f.Path(), dataIds, recs[i])
		}

		if err := store.LevelCompact(0, 1); err != nil {
			t.Fatal(err)
		}

		store.Wait()

		files = store.Order
		if len(files) != 1 {
			t.Fatalf("exp 1 file after compact, but:%v", len(files))
		}

		fids, ok = files["mst"]
		if !ok {
			t.Fatalf("mst not find")
		}

		if fids.Len() != 1 {
			t.Fatalf("exp 1 file after compact, but:%v", fids)
		}

		check("mst", fids.files[0].Path(), dataIds, oldMergeRec)
		check("mst", fids.files[len(fids.files)-1].Path(), dataIds, oldMergeRec)
	}
}

func mustCloseTsspFiles(fiels []TSSPFile) {
	for _, f := range fiels {
		_ = f.Close()
	}
}

func mustCreateTsspFiles(path string, fileNames []string) []TSSPFile {
	_ = fileops.MkdirAll(path, 0750)
	files := make([]TSSPFile, 0, len(fileNames))
	lockPath := ""
	for i := range fileNames {
		name := filepath.Join(path, fileNames[i])
		pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
		fd, err := fileops.OpenFile(name, os.O_CREATE|os.O_RDWR, 0640, pri)
		if err != nil {
			mustCloseTsspFiles(files)
			panic(err)
		}
		dr := fileops.NewFileReader(fd, &lockPath)
		fileName := NewTSSPFileName(1, 0, 0, 0, true, &lockPath)
		f := &tsspFile{
			name: fileName,
			reader: &tsspFileReader{
				r:          dr,
				inMemBlock: NewMemReader(),
			},
			lock: &lockPath,
		}
		files = append(files, f)
	}

	return files
}

func filesInDir(dir string) []string {
	dirs, err := fileops.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	names := make([]string, 0, len(dirs))
	for _, d := range dirs {
		names = append(names, d.Name())
	}

	return names
}

func mustTouchFiles(path string, files []string) {
	_ = fileops.MkdirAll(path, 0750)
	for _, name := range files {
		fName := filepath.Join(path, name)
		fd, err := fileops.Create(fName, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
		if err != nil {
			panic(err)
		}
		_ = fd.Close()
	}
}

func TestCompactLog_Validate(t *testing.T) {
	testCompDir := t.TempDir()
	defer fileops.RemoveAll(testCompDir)
	info := &CompactedFileInfo{
		Name:    "mst1",
		IsOrder: true,
		OldFile: []string{
			"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
			"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		},
		NewFile: []string{"000000001-0001-0000.tssp.init"},
	}

	oldFiles := mustCreateTsspFiles(testCompDir, info.OldFile)
	newFiles := mustCreateTsspFiles(testCompDir, info.NewFile)
	defer func() {
		mustCloseTsspFiles(oldFiles)
		mustCloseTsspFiles(newFiles)
	}()
	lockPath := ""
	store := &MmsTables{lock: &lockPath}
	logFile, err := store.writeCompactedFileInfo(info.Name, oldFiles, newFiles, testCompDir, info.IsOrder)
	if err != nil {
		t.Fatal(err)
	}

	fi, _ := fileops.Stat(logFile)
	info1 := &CompactedFileInfo{}
	err = readCompactLogFile(logFile, info1)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(info, info1) {
		t.Fatalf("exp:%v, get:%v", info, info1)
	}

	_ = fileops.Truncate(logFile, fi.Size()-4)
	err = readCompactLogFile(logFile, info1)
	if err != ErrDirtyLog {
		t.Fatal(err)
	}
}

func TestCompactLog_AllNewFileExist1(t *testing.T) {
	testCompDir := t.TempDir()
	defer fileops.RemoveAll(testCompDir)

	allFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	expFiles := []string{
		"000000001-0001-0000.tssp", "000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	info := &CompactedFileInfo{
		Name:    "mst1",
		IsOrder: true,
		OldFile: allFiles[:4],
		NewFile: []string{"000000001-0001-0000.tssp.init"},
	}

	dir := filepath.Join(testCompDir, TsspDirName, "mst1")
	mustTouchFiles(dir, allFiles[4:])
	oldFiles := mustCreateTsspFiles(dir, info.OldFile)
	newFiles := mustCreateTsspFiles(dir, info.NewFile)
	defer func() {
		mustCloseTsspFiles(oldFiles)
		mustCloseTsspFiles(newFiles)
	}()

	lockPath := ""
	store := &MmsTables{lock: &lockPath}
	_, err := store.writeCompactedFileInfo(info.Name, oldFiles, newFiles, testCompDir, info.IsOrder)
	if err != nil {
		t.Fatal(err)
	}
	ctx := EventContext{mergeSet: &MockIndexMergeSet{func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, scheduler: nil}
	if err = recoverFile(testCompDir, &lockPath, config.TSSTORE, ctx); err != nil {
		t.Fatal(err)
	}

	files := filesInDir(dir)
	sort.Strings(expFiles)
	sort.Strings(files)
	if !reflect.DeepEqual(files, expFiles) {
		t.Fatalf("processLog error, exp:%v, get:%v", expFiles, files)
	}
}

func TestCompactLog_AllNewFileExist2(t *testing.T) {
	testCompDir := t.TempDir()
	defer fileops.RemoveAll(testCompDir)

	allFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	expFiles := []string{
		"000000001-0000-0000.tssp.init",
		"000000001-0001-0000.tssp", "000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	info := &CompactedFileInfo{
		Name:    "mst1",
		IsOrder: true,
		OldFile: allFiles[:4],
		NewFile: []string{"000000001-0001-0000.tssp.init"},
	}

	dir := filepath.Join(testCompDir, TsspDirName, "mst1")
	mustTouchFiles(dir, allFiles[4:])
	oldFiles := mustCreateTsspFiles(dir, info.OldFile)
	newFiles := mustCreateTsspFiles(dir, info.NewFile)
	defer func() {
		mustCloseTsspFiles(oldFiles)
		mustCloseTsspFiles(newFiles)
	}()

	lockPath := ""
	store := &MmsTables{lock: &lockPath}
	_, err := store.writeCompactedFileInfo(info.Name, oldFiles, newFiles, testCompDir, info.IsOrder)
	if err != nil {
		t.Fatal(err)
	}

	//rename 1 old file
	fName := filepath.Join(dir, info.OldFile[0])
	_ = fileops.RenameFile(fName, fName+tmpFileSuffix)
	//remove 1 old file
	fName = filepath.Join(dir, info.OldFile[1])
	_ = fileops.Remove(fName)
	ctx := EventContext{mergeSet: &MockIndexMergeSet{func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, scheduler: nil}
	if err = recoverFile(testCompDir, &lockPath, config.TSSTORE, ctx); err != nil {
		t.Fatal(err)
	}

	files := filesInDir(dir)
	sort.Strings(expFiles)
	sort.Strings(files)
	if !reflect.DeepEqual(files, expFiles) {
		t.Fatalf("processLog error, exp:%v, get:%v", expFiles, files)
	}
}

func TestCompactLog_NewFileNotExit1(t *testing.T) {
	testCompDir := t.TempDir()
	defer fileops.RemoveAll(testCompDir)

	allFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	expFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	info := &CompactedFileInfo{
		Name:    "mst1",
		IsOrder: true,
		OldFile: allFiles[:4],
		NewFile: []string{"000000001-0001-0000.tssp.init"},
	}

	dir := filepath.Join(testCompDir, TsspDirName, "mst1")
	mustTouchFiles(dir, allFiles[4:])
	oldFiles := mustCreateTsspFiles(dir, info.OldFile)
	newFiles := mustCreateTsspFiles(dir, info.NewFile)
	defer func() {
		mustCloseTsspFiles(oldFiles)
		mustCloseTsspFiles(newFiles)
	}()

	lockPath := ""
	store := &MmsTables{lock: &lockPath}
	_, err := store.writeCompactedFileInfo(info.Name, oldFiles, newFiles, testCompDir, info.IsOrder)
	if err != nil {
		t.Fatal(err)
	}

	//remove 1 new file
	fName := filepath.Join(dir, info.NewFile[0])
	_ = fileops.Remove(fName)
	// rename 1 old file
	fName = filepath.Join(dir, info.OldFile[0])
	_ = fileops.RenameFile(fName, fName+tmpFileSuffix)
	ctx := EventContext{mergeSet: &MockIndexMergeSet{func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, scheduler: nil}
	if err = recoverFile(testCompDir, &lockPath, config.TSSTORE, ctx); err != nil {
		t.Fatal(err)
	}

	files := filesInDir(dir)
	sort.Strings(expFiles)
	sort.Strings(files)
	if !reflect.DeepEqual(files, expFiles) {
		t.Fatalf("processLog error, exp:%v, get:%v", expFiles, files)
	}
}

func TestCompactLog_NewFileNotExit2(t *testing.T) {
	testCompDir := t.TempDir()
	defer fileops.RemoveAll(testCompDir)

	allFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
	}

	expFiles := []string{
		"000000001-0000-0000.tssp", "000000002-0000-0000.tssp",
		"000000003-0000-0000.tssp", "000000004-0000-0000.tssp",
		"000000005-0000-0000.tssp", "000000006-0000-0000.tssp",
		"000000002-0001-0000.tssp.init",
	}

	info := &CompactedFileInfo{
		Name:    "mst1",
		IsOrder: true,
		OldFile: allFiles[:4],
		NewFile: []string{"000000001-0001-0000.tssp.init", "000000002-0001-0000.tssp.init"},
	}

	dir := filepath.Join(testCompDir, TsspDirName, "mst1")
	mustTouchFiles(dir, allFiles[4:])
	oldFiles := mustCreateTsspFiles(dir, info.OldFile)
	newFiles := mustCreateTsspFiles(dir, info.NewFile)
	defer func() {
		mustCloseTsspFiles(oldFiles)
		mustCloseTsspFiles(newFiles)
	}()

	lockPath := ""
	store := &MmsTables{lock: &lockPath}
	_, err := store.writeCompactedFileInfo(info.Name, oldFiles, newFiles, testCompDir, info.IsOrder)
	if err != nil {
		t.Fatal(err)
	}

	//remove 1 new file
	fName := filepath.Join(dir, info.NewFile[0])
	_ = fileops.Remove(fName)
	// rename 1 old file
	fName = filepath.Join(dir, info.OldFile[0])
	_ = fileops.RenameFile(fName, fName+tmpFileSuffix)
	ctx := EventContext{mergeSet: &MockIndexMergeSet{func(sid uint64, buf []byte, condition influxql.Expr, callback func(key *influx.SeriesKey)) error {
		return nil
	}}, scheduler: nil}
	if err = recoverFile(testCompDir, &lockPath, config.TSSTORE, ctx); err != nil {
		t.Fatal(err)
	}

	files := filesInDir(dir)
	sort.Strings(expFiles)
	sort.Strings(files)
	if !reflect.DeepEqual(files, expFiles) {
		t.Fatalf("processLog error, exp:%v, get:%v", expFiles, files)
	}
}

func TestMmsTables_LevelCompact_SegmentLimit(t *testing.T) {
	type TestConfig struct {
		rowPerSegment  int
		segmentPerFile int
		fileSizeLimit  int64
		genRows        int
		mergeFlag      int32
	}

	confs := []TestConfig{
		{24, 7, util.DefaultFileSizeLimit, 24, util.NonStreamingCompact},
		{24, 7, util.DefaultFileSizeLimit, 24, util.StreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit / 2, 1500 * 10, util.NonStreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit / 2, 1500 * 10, util.StreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit / 2, 1500 * 10, util.AutoCompact},
	}
	testCompDir := t.TempDir()
	lockPath := ""
	for _, cf := range confs {
		_ = fileops.RemoveAll(testCompDir)
		cacheIns := readcache.GetReadMetaCacheIns()
		cacheIns.Purge()
		sig := interruptsignal.NewInterruptSignal()
		defer func() {
			sig.Close()
			_ = fileops.RemoveAll(testCompDir)
		}()

		var idMinMax, tmMinMax MinMax
		var startValue = 1.1

		SetMergeFlag4TsStore(cf.mergeFlag)
		recRows := cf.genRows
		conf := NewTsStoreConfig()
		conf.maxRowsPerSegment = cf.rowPerSegment
		conf.maxSegmentLimit = cf.segmentPerFile
		conf.fileSizeLimit = cf.fileSizeLimit
		tier := uint64(util.Hot)
		store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
		store.SetImmTableType(config.TSSTORE)

		store.CompactionEnable()

		write := func(ids []uint64, data map[uint64]*record.Record, msb *MsBuilder, merge map[uint64]*record.Record) {
			for _, id := range ids {
				rec := data[id]
				err := msb.WriteData(id, rec)
				if err != nil {
					t.Fatal(err)
				}
				r, ok := merge[id]
				if !ok {
					r = record.NewRecordBuilder(schema)
					r.Merge(rec)
					merge[id] = r
				} else {
					var rr record.Record
					rr.MergeRecord(rec, r)
					merge[id] = &rr
				}
			}
		}

		tm := testTimeStart
		tmMinMax.min = uint64(tm.UnixNano())
		idMinMax.min = 1

		filesN := LeveLMinGroupFiles[0]
		oldRec := make(map[uint64]*record.Record)

		recs := make([]*record.Record, 0, filesN)
		var ids []uint64
		var data map[uint64]*record.Record
		for i := 0; i < filesN; i++ {
			if cf.segmentPerFile == math.MaxUint16 && i == filesN-1 {
				ids, data = genTestData(idMinMax.min, 4, 100, &startValue, &tm)
			} else {
				ids, data = genTestData(idMinMax.min, 1, recRows, &startValue, &tm)
			}

			fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
			msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE, nil, 0)
			write(ids, data, msb, oldRec)
			for _, v := range data {
				recs = append(recs, v)
			}
			store.AddTable(msb, true, false)
		}

		tmMinMax.max = uint64(tm.UnixNano() - timeInterval.Nanoseconds())

		files := store.Order
		if len(files) != 1 {
			t.Fatalf("exp 4 file, but:%v", len(files))
		}
		fids, ok := files["mst"]
		if !ok || fids.Len() != filesN {
			t.Fatalf("mst not find")
		}

		if err := store.LevelCompact(0, 1); err != nil {
			t.Fatal(err)
		}

		store.Wait()

		files = store.Order
		fids, ok = files["mst"]
		if !ok {
			t.Fatalf("mst not find")
		}

		if fids.Len() != 2 {
			t.Fatalf("exp 2 file after compact, but:%v", fids)
		}

		if len(files) != 1 {
			t.Fatalf("exp 1 file after compact, but:%v", len(files))
		}

		read := make(map[uint64]*record.Record)
		if err := readOrderChunks("mst", store, read); err != nil {
			t.Fatal(err)
		}
		for id, rec := range oldRec {
			r, ok := read[id]
			if !ok {
				t.Fatalf("series id %v not find in file", id)
			}
			rStr := r.String()
			oStr := rec.String()
			if rStr != oStr {
				t.Fatalf("exp:%v, get:%v", oStr, rStr)
			}
		}
		_ = store.Close()
	}
}

func TestMmsTables_SetMaxCompactor(t *testing.T) {
	minConcurrentCompactions := 2
	maxConcurrentCompactions := 32

	concurrentCompactions := 4

	SetMaxCompactor(concurrentCompactions)
	if maxCompactor != concurrentCompactions {
		t.Fatal("set max concurrent compactions fail")
	}

	concurrentCompactions = 1
	SetMaxCompactor(concurrentCompactions)
	if maxCompactor != minConcurrentCompactions {
		t.Fatal("set max concurrent compactions fail")
	}

	concurrentCompactions = 64
	SetMaxCompactor(concurrentCompactions)
	if maxCompactor != maxConcurrentCompactions {
		t.Fatal("set max concurrent compactions fail")
	}
}

func TestMmsTables_SetMaxFullCompactor(t *testing.T) {
	concurrentCompactions := 4

	SetMaxCompactor(concurrentCompactions)
	if maxCompactor != concurrentCompactions {
		t.Fatal("set max concurrent compactions fail")
	}

	minFullConcurrentCompactions := 1

	fullConcurrentCompactions := 4

	SetMaxFullCompactor(fullConcurrentCompactions)
	if maxFullCompactor != concurrentCompactions/2 {
		t.Fatal("set max concurrent compactions fail")
	}

	fullConcurrentCompactions = 1
	SetMaxFullCompactor(fullConcurrentCompactions)
	if maxFullCompactor != minFullConcurrentCompactions {
		t.Fatal("set max concurrent compactions fail")
	}

	fullConcurrentCompactions = 64
	SetMaxFullCompactor(fullConcurrentCompactions)
	if maxFullCompactor != concurrentCompactions/2 {
		t.Fatal("set max concurrent compactions fail")
	}
}

func TestCompactRecovery(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, false, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	store.CompactionEnable()

	group := &CompactGroup{
		name:    "test_mem",
		shardId: 1,
		toLevel: 1,
		group:   []string{"1", "2", "3", "4"},
	}
	defer CompactRecovery(testCompDir, group)

	panic("compact panic")
}

func TestMergeRecovery(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, false, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	store.CompactionEnable()

	ctx := NewMergeContext("mst", 0)
	ctx.order.seq = append(ctx.order.seq, 1)
	defer MergeRecovery(testCompDir, "test_name", ctx)

	panic("merge panic")
}

func TestDisableCompAndMerge(t *testing.T) {
	mst := &MmsTables{
		inCompLock: sync.RWMutex{},
		scheduler:  scheduler.NewTaskScheduler(func(signal chan struct{}, onClose func()) {}, compLimiter),
	}
	mst.EnableCompAndMerge()
	mst.EnableCompAndMerge()
	require.True(t, mst.CompactionEnabled())

	mst.DisableCompAndMerge()
	mst.DisableCompAndMerge()
	require.False(t, mst.CompactionEnabled())
}

func TestGenFilePath(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	ObsOptions := &obs.ObsOptions{
		Enabled: true,
	}
	fileName := TSSPFileName{
		seq:   uint64(1),
		order: true,
	}
	_, _ = genFilePath(testCompDir, fileName, ObsOptions, true)
	_, _ = genFilePath(testCompDir, fileName, nil, false)
}

func TestFullyCompacted(t *testing.T) {
	m := &MmsTables{
		CSFiles: map[string]*TSSPFiles{"table1": &TSSPFiles{files: []TSSPFile{MocTsspFile{
			path: "/tmp/openGemini",
		}}}},
		stopCompMerge: make(chan struct{}),
	}
	// test for column store
	m.ImmTable = NewCsImmTableImpl()
	fullCompacted := m.ImmTable.FullyCompacted(m)
	assert.Equal(t, true, fullCompacted)
}

func TestGetRemoteSuffix(t *testing.T) {
	fileName := "0000001-000-000000.tssp"
	res := getRemoteSuffix(fileName)
	assert.Equal(t, "", res)

	fileName2 := "0000002-000-000000.tssp.obs"
	res2 := getRemoteSuffix(fileName2)
	assert.Equal(t, ".obs", res2)
}

func TestWriteMemoryBloomFilterData(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()

	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msBuilder := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.COLUMNSTORE, nil, 0)
	n := int64(2)
	buf := make([]byte, logstore.GetConstant(logstore.CurrentLogTokenizerVersion).FilterDataDiskSize*n)
	memBfData := [][]byte{buf}

	err := msBuilder.writeMemoryBloomFilterData(memBfData, nil, lockPath)
	assert.NoError(t, err)
	assert.Equal(t, n, msBuilder.localBFCount)

	err = msBuilder.writeMemoryBloomFilterData(memBfData, nil, lockPath)
	assert.NoError(t, err)
	assert.Equal(t, n*2, msBuilder.localBFCount)
}

func TestGetLocalBloomFilterData(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	defer store.Close()

	fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
	msBuilder := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.COLUMNSTORE, nil, 0)
	p := filepath.Join(msBuilder.Path, "full_text_bf.idx")
	skipIndexFilePaths := make([]string, 0, 1)
	skipIndexFilePaths = append(skipIndexFilePaths, p)
	msBuilder.localBFCount = 1
	_, err := msBuilder.getLocalBloomFilterData(skipIndexFilePaths)
	if err == nil {
		t.Fatal("should return no such file or dir")
	}
}
