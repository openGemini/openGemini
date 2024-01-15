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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/rand"
	"github.com/openGemini/openGemini/lib/readcache"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFileWriter(t *testing.T) {
	testCompDir := t.TempDir()
	fn := filepath.Join(testCompDir, "fwrite.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer fileops.Remove(fn)
	fd, err := fileops.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := ""
	fw := newTsspFileWriter(fd, false, true, &lockPath)
	if fn != fw.Name() {
		t.Fatalf("invalid writer name")
	}

	if fw.GetFileWriter() == nil {
		t.Fatalf("invalid file writer")
	}

	n, err := fw.WriteData([]byte("test data"))
	if err != nil {
		t.Fatal(err)
	}

	if fw.DataSize() != int64(n) {
		t.Fatalf("write data fail")
	}

	fw.SwitchMetaBuffer()
	if err := fw.Close(); err != nil {
		t.Fatal(err)
	}

	_ = fd.Close()
}

func TestFileIterator(t *testing.T) {
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
	conf.maxSegmentLimit = 65535
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*9 + 1
	lockPath := ""
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

	tm := testTimeStart

	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 1

	filesN := 2
	idCount := 20
	oldMergeRec := make([]*record.Record, idCount)
	recs := make([][]*record.Record, filesN)

	for i := 0; i < filesN; i++ {
		ids, data := genTestData(idMinMax.min, idCount, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 10, fileName, store.Tier(), nil, 2, config.TSSTORE)
		write(ids, data, msb)

		for j, id := range ids {
			idRec := data[id]
			if oldMergeRec[j] == nil {
				oldMergeRec[j] = record.NewRecordBuilder(schema)
			}
			oldMergeRec[j].Merge(idRec)
			recs[i] = append(recs[i], idRec)
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

	names := make([]string, 0, filesN)
	for _, f := range fids.files {
		names = append(names, f.Path())
	}
	dropping := int64(0)
	group := CompactGroup{
		name:     "mst",
		shardId:  1,
		toLevel:  1,
		group:    names,
		dropping: &dropping,
	}

	fi, err := store.ImmTable.NewFileIterators(store, &group)
	if err != nil {
		t.Fatal(err)
	}

	if fi.avgChunkRows != recRows {
		t.Fatalf("avgChunkRows, exp:%v, but:%v", recRows, fi.avgChunkRows)
	}

	if fi.maxChunkRows != recRows {
		t.Fatalf("maxChunkRows, exp:%v, but:%v", recRows, fi.maxChunkRows)
	}

	SetMergeFlag4TsStore(AutoCompact)
	SetMergeFlag4TsStore(NonStreamingCompact)
	if NonStreamingCompaction(fi) != true {
		t.Fatalf("set NonStreamingCompact fail")
	}

	SetMergeFlag4TsStore(StreamingCompact)
	NonStreamingCompaction(fi)
	if NonStreamingCompaction(fi) != false {
		t.Fatalf("set StreamingCompact fail")
	}

	v := fi.compIts.MaxChunkRows()
	if v != recRows {
		t.Fatalf("MaxChunkRows, exp:%v, but:%v", recRows, v)
	}

	v = fi.compIts.AverageRows()
	if v != recRows {
		t.Fatalf("AverageRows, exp:%v, but:%v", recRows, v)
	}

	v = fi.compIts.MaxColumns()
	if v != 5 {
		t.Fatal("MaxColumns != 5")
	}

	for _, itr := range fi.compIts {
		cm := itr.curtChunkMeta
		seg := cm.colMeta[0].entries[0]
		_, err = itr.ReadData(seg.offset, seg.size)
		if err != nil {
			t.Fatal(err)
		}
	}

	fi.compIts.Close()
}

func TestFileIteratorError(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)

	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	conf.maxSegmentLimit = 65535
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)

	names := make([]string, 8)
	dropping := int64(0)
	group := CompactGroup{
		name:     "mst",
		shardId:  1,
		toLevel:  1,
		group:    names,
		dropping: &dropping,
	}
	store.Close()
	_, err := store.ImmTable.NewFileIterators(store, &group)
	require.Equal(t, err, errors.New("compact stopped"))
}

func TestMmsTables_LevelCompact_20ID10Segment_SegLimit(t *testing.T) {
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
	conf.maxRowsPerSegment = 40
	conf.maxSegmentLimit = 16
	conf.fileSizeLimit = defaultFileSizeLimit
	tier := uint64(util.Hot)
	recRows := conf.maxRowsPerSegment*2 + 1
	lockPath := ""
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

	check := func(name string, seq string, ids []uint64, orig []*record.Record) {
		f := store.File(name, seq, true)
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
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 10, fileName, store.Tier(), nil, 2, config.TSSTORE)
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

	for i, seq := range fids.files {
		check("mst", seq.Path(), dataIds, recs[i])
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

	if fids.Len() == 1 {
		t.Fatalf("exp >1 file after compact, but:1")
	}

	chunkOrderChunks := func() {
		fileRec := make(map[uint64]*record.Record)
		if err := readOrderChunks("mst", store, fileRec); err != nil {
			t.Fatal(err)
		}

		if len(dataIds) != len(fileRec) {
			t.Fatalf("read chunks fail, exp:%v, read:%v", len(dataIds), len(fileRec))
		}

		for i, id := range dataIds {
			orig := oldMergeRec[i]
			readRec, ok := fileRec[id]
			if !ok {
				t.Fatalf("chunk not find, id:%v", id)
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

	chunkOrderChunks()
}

func TestPreAggBuilder(t *testing.T) {
	builders := newPreAggBuilders()
	col := &record.ColVal{}

	intArr := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	floatArr := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	boolArr := []bool{true, false, true, false, true, false, true, false, true, false}
	strArr := []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10"}
	tmArr := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	arrs := []interface{}{intArr, floatArr, boolArr, strArr}
	for _, arr := range arrs {
		col.Init()
		switch ta := arr.(type) {
		case []int64:
			col.AppendIntegers(ta...)
			builders.intBuilder.addValues(col, tmArr)
		case []float64:
			col.AppendFloats(ta...)
			builders.floatBuilder.addValues(col, tmArr)
		case []bool:
			col.AppendBooleans(ta...)
			builders.boolBuilder.addValues(col, tmArr)
		case []string:
			col.AppendStrings(ta...)
			builders.stringBuilder.addValues(col, tmArr)
		}
	}

	builders.intBuilder.addMin(1, 0)
	builders.intBuilder.addMax(10, 11)
	builders.intBuilder.addSum(10)
	builders.intBuilder.addCount(10)

	builders.floatBuilder.addMin(1, 0)
	builders.floatBuilder.addMax(10, 11)
	builders.floatBuilder.addSum(10)
	builders.floatBuilder.addCount(10)

	builders.boolBuilder.addMin(1, 0)
	builders.boolBuilder.addMax(0, 11)
	builders.boolBuilder.addSum(10)
	builders.boolBuilder.addCount(10)

	builders.stringBuilder.addMin(1, 0)
	builders.stringBuilder.addMax(0, 11)
	builders.stringBuilder.addSum(10)
	builders.stringBuilder.addCount(10)

	builders.timeBuilder.addCount(10)
}

func TestColumnBuilder_EncodeColumn(t *testing.T) {
	var cm ChunkMeta
	var ref = record.Field{Name: "f1", Type: influx.Field_Type_Int}
	cb := NewColumnBuilder()
	ctx := NewReadContext(true)

	arrs := [][]int64{
		[]int64{},
		[]int64{1},
		[]int64{1, 2},
		[]int64{1, 2, 5, 3, 4, 9},
	}

	var col record.ColVal
	var cols [1]record.ColVal
	var dstCol record.ColVal

	for _, arr := range arrs {
		col.Init()
		if len(arr) > 0 {
			col.AppendIntegers(arr...)
		} else {
			col.AppendIntegerNull()
		}

		cm.resize(1, 1)
		cm.colMeta[0].name = ref.Name
		cm.colMeta[0].ty = byte(ref.Type)

		cb.cm = &cm
		cb.colMeta = &cm.colMeta[0]
		_ = cb.initEncoder(ref)
		cols[0] = col
		cb.data = cb.data[:0]
		if err := cb.encodeColumn(cols[:], nil, 0, ref); err != nil {
			t.Fatal(err)
		}

		dstCol.Init()
		if err := decodeColumnData(&ref, cb.data, &dstCol, ctx, false); err != nil {
			t.Fatal(err)
		}

		decArr := dstCol.IntegerValues()
		if len(arr) != len(decArr) {
			t.Fatal("len(arr) != len(decArr)")
		}

		if len(arr) > 0 && !reflect.DeepEqual(arr, decArr) {
			t.Fatalf("exp:%v, get:%v", arr, decArr)
		}
	}
}

func readOrderChunks(mst string, store *MmsTables, chunks map[uint64]*record.Record) error {
	fi := FilesInfo{}
	fi.name = mst
	files := store.tableFiles(mst, true)
	fi.dropping = &files.closing
	for _, f := range files.files {
		fi.oldFids = append(fi.oldFids, f.Path())
		f.Ref()
		f.RefFileReader()
		fi.oldFiles = append(fi.oldFiles, f)
		fItr := NewFileIterator(f, CLog)
		fi.compIts = append(fi.compIts, fItr)
		fi.estimateSize += int(f.FileSize())
	}

	itr := store.NewChunkIterators(fi)
	for itr.Len() > 0 {
		id, rec, err := itr.Next()
		if err != nil {
			CLog.Error("read data fail", zap.Error(err))
			return err
		}

		if rec == nil || id == 0 {
			break
		}

		nRec, ok := chunks[id]
		if !ok {
			nRec = rec.Clone()
			nRec.TimeColumn().AppendAll(rec.TimeColumn())
			chunks[id] = nRec
		} else {
			nRec.Merge(rec)
		}
	}
	return nil
}

func TestFileSizeExceedLimit(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	cacheIns := readcache.GetReadMetaCacheIns()
	cacheIns.Purge()
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()

	size := 2 * 1024 * 1024
	conf := NewTsStoreConfig()
	conf.SetFilesLimit(int64(size))
	conf.maxRowsPerSegment = 1000
	conf.maxSegmentLimit = 65535
	tier := uint64(util.Hot)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	rows := size/2 + 10
	startTime := time.Now().UnixNano()
	var startTimes []int64
	filesN := 9
	var id uint64 = 1
	oldData := make(map[uint64]*record.Record, filesN)
	oldIds := make([]uint64, 0, filesN)
	for i := 0; i < filesN; i++ {
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, size, config.TSSTORE)
		rec := genIntColumn(&startTime, rows, 2)
		if err := msb.WriteData(id, rec); err != nil {
			t.Fatal(err)
		}
		store.AddTable(msb, true, false)
		oldIds = append(oldIds, id)
		oldData[id] = rec
		startTimes = append(startTimes, startTime)
		id++
	}

	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()

	readRec := make(map[uint64]*record.Record, filesN)
	if err := readOrderChunks("mst", store, readRec); err != nil {
		t.Fatal(err)
	}

	if len(readRec) != len(oldData) {
		t.Fatalf("read chunk fail, exp:%v, get:%v", len(oldData), len(readRec))
	}

	compareRecFun := func(old, read *record.Record) {
		if old.RowNums() != read.RowNums() {
			t.Fatalf("raad chunk rows fail, exp:%v, get:%v", old.RowNums(), read.RowNums())
		}
		oldValues := old.Column(0).IntegerValues()
		readValues := read.Column(0).IntegerValues()
		if !reflect.DeepEqual(oldValues, readValues) {
			t.Fatalf("colum value: exp:%v, get:%v", oldValues, readValues)
		}

		oldTimes := old.Times()
		readTimes := read.Times()
		if !reflect.DeepEqual(oldTimes, readTimes) {
			t.Fatalf("times: exp:%v, get:%v", oldTimes, readTimes)
		}
	}

	for _, sid := range oldIds {
		old := oldData[sid]
		read := readRec[sid]
		compareRecFun(old, read)
	}
}

func genIntColumn(st *int64, rows int, interval int64) *record.Record {
	schema := record.Schemas{
		record.Field{Name: "f1", Type: influx.Field_Type_Int},
		record.Field{Name: record.TimeField, Type: influx.Field_Type_Int},
	}
	rec := record.NewRecordBuilder(schema)
	col := rec.Column(0)
	tCol := rec.TimeColumn()
	startTime := *st
	for i := 0; i < rows; i++ {
		v := rand.Int63n(1 << 60)
		col.AppendInteger(v)
		tCol.AppendInteger(startTime)
		if i > 0 && i%5 == 0 {
			startTime += interval + 1
		} else {
			startTime += interval
		}
	}
	*st = startTime

	return rec
}

func TestFileSizeExceedLimit1(t *testing.T) {
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
	conf.SetFilesLimit(1)
	tier := uint64(util.Warm)
	lockPath := ""
	store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
	store.SetImmTableType(config.TSSTORE)
	defer store.Close()

	rows := 30000
	startTime := time.Now().UnixNano()
	filesN := 8
	var id uint64 = 1
	oldData := make(map[uint64]*record.Record, filesN)
	oldIds := make([]uint64, 0, filesN)
	for i := 0; i < filesN; i++ {
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, "mst", &lockPath, conf, 1, fileName, store.Tier(), nil, 1024, config.TSSTORE)
		rec := genIntColumn(&startTime, rows, 2)
		if err := msb.WriteData(id, rec); err != nil {
			t.Fatal(err)
		}
		store.AddTable(msb, true, false)
		oRec, ok := oldData[id]
		if !ok {
			oldIds = append(oldIds, id)
			oldData[id] = rec
		} else {
			oRec.Merge(rec)
		}
	}

	SetMergeFlag4TsStore(StreamingCompact)
	if err := store.LevelCompact(0, 1); err != nil {
		t.Fatal(err)
	}
	store.Wait()

	readRec := make(map[uint64]*record.Record, filesN)
	if err := readOrderChunks("mst", store, readRec); err != nil {
		t.Fatal(err)
	}

	if len(readRec) != len(oldData) {
		t.Fatalf("read chunk fail, exp:%v, get:%v", len(oldData), len(readRec))
	}

	compareRecFun := func(old, read *record.Record) {
		if old.RowNums() != read.RowNums() {
			t.Fatalf("raad chunk rows fail, exp:%v, get:%v", old.RowNums(), read.RowNums())
		}
		oldValues := old.Column(0).IntegerValues()
		readValues := read.Column(0).IntegerValues()
		if !reflect.DeepEqual(oldValues, readValues) {
			t.Fatalf("colum value: exp:%v, get:%v", oldValues, readValues)
		}

		oldTimes := old.Times()
		readTimes := read.Times()
		if !reflect.DeepEqual(oldTimes, readTimes) {
			t.Fatalf("times: exp:%v, get:%v", oldTimes, readTimes)
		}
	}

	for _, sid := range oldIds {
		old := oldData[sid]
		read := readRec[sid]
		compareRecFun(old, read)
	}
}

func TestSplitColumn_panic(t *testing.T) {
	itr := &StreamIterators{
		col:  &record.ColVal{},
		Conf: NewTsStoreConfig(),
	}
	itr.Conf.SetMaxRowsPerSegment(16)
	for i := 0; i < 40; i++ {
		itr.col.AppendFloat(1)
	}

	var err string
	var split = func() {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Sprintf("%v", e)
			}
		}()

		itr.splitColumn(record.Field{Name: "foo", Type: influx.Field_Type_Float}, false)
	}
	split()
	require.Equal(t, "len(c.colSegs) != 2, got: 3", err)
}

func TestStreamWriteFile_WriteData_panic(t *testing.T) {
	sw := &StreamWriteFile{}
	sw.Conf = NewTsStoreConfig()
	sw.Conf.SetMaxRowsPerSegment(16)

	ref := record.Field{Name: "foo", Type: influx.Field_Type_Float}
	col := record.ColVal{}
	for i := 0; i < 20; i++ {
		col.AppendFloat(1)
	}

	require.EqualError(t, sw.WriteData(0, ref, record.ColVal{}, nil), "series id is 0")

	var err string
	var write = func() {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Sprintf("%v", e)
			}
		}()

		_ = sw.WriteData(100, ref, col, nil)
	}
	write()
	require.Equal(t, "col.Len=20 is greater than MaxRowsPerSegment=16", err)
}

func TestFileIterator_lessGroup(t *testing.T) {
	testCompDir := t.TempDir()
	conf := NewTsStoreConfig()
	conf.maxRowsPerSegment = 100
	conf.maxSegmentLimit = 65535
	tier := uint64(util.Hot)
	lockPath := ""

	var run = func(typ config.EngineType) {
		store := NewTableStore(testCompDir, &lockPath, &tier, true, conf)
		store.SetImmTableType(typ)
		defer store.Close()

		dropping := int64(0)
		group := CompactGroup{
			name:     "mst",
			shardId:  1,
			toLevel:  1,
			group:    []string{},
			dropping: &dropping,
		}
		_, err := store.ImmTable.NewFileIterators(store, &group)
		require.EqualError(t, err, "no enough files to do compact, iterator size: 0")
	}
	run(config.TSSTORE)
	run(config.COLUMNSTORE)
}
