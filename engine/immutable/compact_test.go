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
	"bytes"
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

	"github.com/openGemini/openGemini/engine/immutable/readcache"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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

var schema = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_string", Type: influx.Field_Type_String},
	{Name: "time", Type: influx.Field_Type_Int},
}

func genTestData(id uint64, idCount int, rows int, startValue *float64, starTime *time.Time) ([]uint64, map[uint64]*record.Record) {
	tm := *starTime

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
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
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
		defer f.Unref()
		defer UnrefFilesReader(f)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs)
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
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2)
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

	store.wg.Wait()

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
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
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
		defer f.Unref()
		defer f.UnrefFileReader()
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs)
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
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2)
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

	store.wg.Wait()

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
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	var idMinMax, tmMinMax MinMax
	var startValue = 1.1

	conf := NewConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*4 + 1
	lockPath := ""

	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
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
		defer f.Unref()
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			t.Fatalf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schema)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schema)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs)
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
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2)
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

	store.wg.Wait()

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
	mergeFlags := []int32{NonStreamingCompact, StreamingCompact, AutoCompact}
	testCompDir := t.TempDir()
	lockPath := ""
	for _, flag := range mergeFlags {
		_ = fileops.RemoveAll(testCompDir)
		cacheIns := readcache.GetReadCacheIns()
		cacheIns.Purge()
		sig := interruptsignal.NewInterruptSignal()
		defer func() {
			sig.Close()
			_ = fileops.RemoveAll(testCompDir)
		}()

		var idMinMax, tmMinMax MinMax
		var startValue = 1.1

		SegMergeFlag(flag)
		conf := NewConfig()
		conf.maxRowsPerSegment = 100
		conf.maxSegmentLimit = 65535
		tier := uint64(util.Hot)
		recRows := conf.maxRowsPerSegment*9 + 1

		store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
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
			defer f.Unref()
			defer f.UnrefFileReader()
			contains, err := f.Contains(idMinMax.min)
			if err != nil || !contains {
				t.Fatalf("show contain series id:%v, but not find, error:%v", idMinMax.min, err)
			}

			midx, _ := f.MetaIndexAt(0)
			if midx == nil {
				t.Fatalf("meta index not find")
			}

			decs := NewReadContext(true)
			cms, err := f.ReadChunkMetaData(0, midx, nil)
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
					rec, err = f.ReadAt(cm, j, rec, decs)
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

		tm := testTimeStart

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
			msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 10, fileName, store.Tier(), nil, 2)
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

		store.wg.Wait()

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
		dr := NewDiskFileReader(fd, &lockPath)
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
	if err = recoverFile(testCompDir, &lockPath); err != nil {
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
	_ = fileops.RenameFile(fName, fName+tmpTsspFileSuffix)
	//remove 1 old file
	fName = filepath.Join(dir, info.OldFile[1])
	_ = fileops.Remove(fName)
	if err = recoverFile(testCompDir, &lockPath); err != nil {
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
	_ = fileops.RenameFile(fName, fName+tmpTsspFileSuffix)
	if err = recoverFile(testCompDir, &lockPath); err != nil {
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
	_ = fileops.RenameFile(fName, fName+tmpTsspFileSuffix)
	if err = recoverFile(testCompDir, &lockPath); err != nil {
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
		{24, 7, defaultFileSizeLimit, 24, NonStreamingCompact},
		{24, 7, defaultFileSizeLimit, 24, StreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit, 1500 * 10, NonStreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit, 1500 * 10, StreamingCompact},
		{1000, math.MaxUint16, minFileSizeLimit, 1500 * 10, AutoCompact},
	}
	testCompDir := t.TempDir()
	lockPath := ""
	for _, cf := range confs {
		_ = fileops.RemoveAll(testCompDir)
		cacheIns := readcache.GetReadCacheIns()
		cacheIns.Purge()
		sig := interruptsignal.NewInterruptSignal()
		defer func() {
			sig.Close()
			_ = fileops.RemoveAll(testCompDir)
		}()

		var idMinMax, tmMinMax MinMax
		var startValue = 1.1

		SegMergeFlag(cf.mergeFlag)
		recRows := cf.genRows
		conf := NewConfig()
		conf.maxRowsPerSegment = cf.rowPerSegment
		conf.maxSegmentLimit = cf.segmentPerFile
		conf.fileSizeLimit = cf.fileSizeLimit
		tier := uint64(util.Hot)
		store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)

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
			msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 2)
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

		store.wg.Wait()

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

func TestDiskWriter(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "TestDiskWriter")
	_ = fileops.Remove(dir)
	defer fileops.RemoveAll(dir)
	_ = fileops.MkdirAll(dir, 0750)

	fd := &mockFile{
		NameFn: func() string {
			return name
		},
		CloseFn: func() error {
			return nil
		},
		WriteFn: func(p []byte) (n int, err error) {
			return 0, fmt.Errorf("write fail")
		},
	}

	var buf [128 * 1024]byte
	lockPath := ""
	dr := NewDiskWriter(fd, 1024, &lockPath)
	if _, err := dr.Write(buf[:32]); err != nil {
		t.Fatal(err)
	}

	// write fail
	if _, err := dr.Write(buf[:]); err == nil {
		t.Fatalf("diskwrite should be wirte fail")
	}

	// flush fail
	to := &bytes.Buffer{}
	_, err := dr.CopyTo(to)
	if err == nil {
		t.Fatalf("diskwrite should be copy fail")
	}

	// close fail
	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return fmt.Errorf("close fail") }
	dr.w.Reset(fd)
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err == nil || !strings.Contains(err.Error(), "close fail") {
		t.Fatalf("diskwriter should be cloase fail")
	}

	// open fail
	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return nil }
	dr.w.Reset(fd)
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err == nil {
		t.Fatalf("diskwriter should be open fail")
	}

	// copy fail
	fd1, err := fileops.OpenFile(name, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fd1.Write(buf[:])
	_ = fd1.Close()

	fd.WriteFn = func(p []byte) (n int, err error) { return len(p), nil }
	fd.CloseFn = func() error { return nil }
	dr.w.Reset(fd)
	dr.n = 0
	_, _ = dr.Write(buf[:])
	if _, err = dr.CopyTo(to); err != nil {
		t.Fatalf("diskwriter should be open fail")
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
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, false, conf)
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
	cacheIns := readcache.GetReadCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	conf := NewConfig()
	conf.maxRowsPerSegment = 100
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, false, conf)
	defer store.Close()

	store.CompactionEnable()

	ctx := NewMergeContext("mst")
	ctx.order.seq = append(ctx.order.seq, 1)
	defer MergeRecovery(testCompDir, "test_name", ctx)

	panic("merge panic")
}

func TestDisableCompAndMerge(t *testing.T) {
	mst := &MmsTables{inCompLock: sync.RWMutex{}}
	mst.EnableCompAndMerge()
	mst.EnableCompAndMerge()
	require.True(t, mst.CompactionEnabled())

	mst.DisableCompAndMerge()
	mst.DisableCompAndMerge()
	require.False(t, mst.CompactionEnabled())
}
