/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"fmt"
	"path"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func writeData(testCompDir, mstName string) error {
	var idMinMax, tmMinMax MinMax
	var startValue = 999999.0
	conf := NewColumnStoreConfig()
	conf.maxRowsPerSegment = 20
	conf.FragmentsNumPerFlush = 3
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
	schema := make(map[string]int32)
	for i := range primaryKey {
		for j := range schemaForColumnStore {
			if primaryKey[i] == schemaForColumnStore[j].Name {
				schema[primaryKey[i]] = int32(schemaForColumnStore[j].Type)
			}
		}
	}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"primaryKey_string1", "primaryKey_string2"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstinfo := meta.MeasurementInfo{
		Name:       mstName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:     primaryKey,
			SortKey:        sortKey,
			CompactionType: config.BLOCK,
		},
		Schema: schema,
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}
	indexRelation := &influxql.IndexRelation{
		Oids:       []uint32{uint32(index.BloomFilter)},
		IndexNames: []string{"bloomfilter"},
	}

	store.ImmTable.SetMstInfo(mstName, &mstinfo)
	sortKeyMap := genSortedKeyMap(sort)
	write := func(ids uint64, data map[uint64]*record.Record, msb *MsBuilder, merge *record.Record,
		sortKeyMap map[string]int, primaryKey, sortKey []string, needMerge bool, pkSchema record.Schemas, indexRelation *influxql.IndexRelation) (*record.Record, error) {
		rec := data[ids]
		err := msb.WriteData(ids, rec)
		if err != nil {
			return nil, err
		}
		if len(pkSchema) != 0 {
			dataFilePath := msb.FileName.String()
			indexFilePath := path.Join(msb.Path, msb.msName, colstore.AppendPKIndexSuffix(dataFilePath))
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			if err = msb.writePrimaryIndex(rec, pkSchema, indexFilePath, *msb.lock, colstore.DefaultTCLocation, fixRowsPerSegment, DefaultMaxRowsPerSegment4ColStore); err != nil {
				return nil, err
			}
		}
		if indexRelation != nil && len(indexRelation.IndexNames) != 0 {
			dataFilePath := msb.FileName.String()
			fixRowsPerSegment := GenFixRowsPerSegment(rec, conf.maxRowsPerSegment)
			schemaIdx := logstore.GenSchemaIdxs(rec.Schema, &mstinfo.IndexRelation, false)
			if err := msb.writeSkipIndex(rec, schemaIdx, dataFilePath, *msb.lock, fixRowsPerSegment, false); err != nil {
				return nil, err
			}
		}
		if needMerge {
			merge = mergeForColumnStore(sort, sortKeyMap, rec, merge)
			return merge, nil
		}
		return rec, nil
	}

	check := func(name string, fn string, orig *record.Record) error {
		f := store.File(name, fn, true)
		contains, err := f.Contains(idMinMax.min)
		if err != nil || !contains {
			return err
		}

		midx, _ := f.MetaIndexAt(0)
		if midx == nil {
			return fmt.Errorf("meta index not find")
		}

		cm, err := f.ChunkMeta(midx.id, midx.offset, midx.size, midx.count, 0, nil, fileops.IO_PRIORITY_LOW_READ)
		if err != nil {
			return err
		}

		decs := NewReadContext(true)
		readRec := record.NewRecordBuilder(schemaForColumnStore)
		readRec.ReserveColumnRows(recRows * 4)
		rec := record.NewRecordBuilder(schemaForColumnStore)
		rec.ReserveColumnRows(conf.maxRowsPerSegment)
		for i := range cm.timeMeta().entries {
			rec, err = f.ReadAt(cm, i, rec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				return err
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
			return fmt.Errorf("time not eq, \nexp:%v \nget:%v", oldTimes, times)
		}

		if !reflect.DeepEqual(oldV0, v0) {
			return fmt.Errorf("flaot value not eq, \nexp:%v \nget:%v", oldV0, v0)
		}

		if !reflect.DeepEqual(oldV1, v1) {
			return fmt.Errorf("int value not eq, \nexp:%v \nget:%v", oldV1, v1)
		}

		if !reflect.DeepEqual(oldV2, v2) {
			return fmt.Errorf("bool value not eq, \nexp:%v \nget:%v", oldV2, v2)
		}

		if !reflect.DeepEqual(oldV3, v3) {
			return fmt.Errorf("string value not eq, \nexp:%v \nget:%v", oldV3, v3)
		}
		return nil
	}

	tm := testTimeStart
	tmMinMax.min = uint64(tm.UnixNano())
	idMinMax.min = 0
	compactionTimes := 2
	filesN := LeveLMinGroupFiles[0] * compactionTimes
	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * filesN)

	recs := make([]*record.Record, 0, filesN)
	pk := store.ImmTable.(*csImmTableImpl).mstsInfo[mstName].ColStoreInfo.PrimaryKey
	pkSchema := make([]record.Field, len(pk))
	for i := range pk {
		pkSchema[i] = record.Field{
			Type: int(store.ImmTable.(*csImmTableImpl).mstsInfo[mstName].Schema[pk[i]]),
			Name: pk[i],
		}
	}
	needMerge := false
	var err error
	for i := 0; i < filesN; i++ {
		ids, data := genTestDataForColumnStore(idMinMax.min, 1, recRows, &startValue, &tm)
		fileName := NewTSSPFileName(store.NextSequence(), 0, 0, 0, true, &lockPath)
		msb := NewMsBuilder(store.path, mstName, &lockPath, conf, 1, fileName, store.Tier(), nil, 2, config.TSSTORE)
		msb.NewPKIndexWriter()
		msb.NewSkipIndexWriter()
		oldRec, err = write(ids, data, msb, oldRec, sortKeyMap, primaryKey, sortKey, needMerge, pkSchema, indexRelation)
		if err != nil {
			return err
		}
		needMerge = true
		if err := writeIntoFile(msb, false); err != nil {
			return err
		}
		fn := msb.Files[len(msb.Files)-1].Path()
		if err := RenameIndexFiles(fn, bfColumn); err != nil {
			return err
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
	fids, ok := files[mstName]
	if !ok || fids.Len() != filesN {
		return fmt.Errorf("mst not found")
	}

	for i, f := range fids.files {
		err = check(mstName, f.Path(), recs[i])
		if err != nil {
			return err
		}

		fr := f.(*tsspFile).reader.(*tsspFileReader)
		if fr.ref != 0 {
			return fmt.Errorf("ref error")
		}
	}

	for i := 0; i < compactionTimes; i++ {
		if err := store.LevelCompact(0, 1); err != nil {
			return err
		}
		store.Wait()
	}
	files = store.CSFiles
	fids, ok = files[mstName]
	if !ok {
		return fmt.Errorf("mst not find")
	}
	return nil
}

func TestDetachedTSSPReader(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}

	p := path.Join(testCompDir, mstName)
	reader, _ := NewDetachedMetaIndexReader(p, nil)
	metaIndex, _ := reader.ReadMetaIndex([]int64{16, 56}, []int64{40, 40})
	decs := NewFileReaderContext(util.TimeRange{Min: 0, Max: 1635732519000000000}, schema, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ := NewTSSPFileDetachedReader(metaIndex, [][]int{[]int{0, 1, 2, 4, 30, 50, 200}, []int{0, 1, 2, 4, 30, 50, 200}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
	})
	totalRow := 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		totalRow += data.RowNums()
	}
	assert.Equal(t, 180, totalRow)
	treader.ResetBy(metaIndex, [][]int{[]int{0, 1, 2, 200}, []int{0, 2, 4, 30, 50, 200}}, decs)
	totalRow = 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		totalRow += data.RowNums()
	}
	assert.Equal(t, 100, totalRow)
	err = treader.Close()
	assert.Equal(t, true, IsInterfaceNil(err))
	option := query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
	}
	decs = NewFileReaderContext(util.TimeRange{Min: 1635724829000000000, Max: 1645724819000000000}, schema, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ = NewTSSPFileDetachedReader(metaIndex[:1], [][]int{[]int{0, 2, 200}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &option)
	totalRow = 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		totalRow += data.RowNums()
	}
	assert.Equal(t, 40, totalRow)

	decs = NewFileReaderContext(util.TimeRange{Min: 1635724829000000000, Max: 1645724819000000000}, schema, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ = NewTSSPFileDetachedReader(metaIndex[:1], [][]int{[]int{0, 2}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &option)
	totalRow = 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		totalRow += data.RowNums()
	}
	assert.Equal(t, 20, totalRow)

	schema1 := []record.Field{
		{Name: "time", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	decs = NewFileReaderContext(util.TimeRange{Min: 1635724829000000000, Max: 1645724819000000000}, schema1, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ = NewTSSPFileDetachedReader(metaIndex[:1], [][]int{[]int{0, 2}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &option)
	totalRow = 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		assert.Equal(t, data.RowNums(), data.ColVals[0].Len)
		totalRow += data.RowNums()
	}
	assert.Equal(t, 20, totalRow)
	decs = NewFileReaderContext(util.TimeRange{Min: 1635724829000000000, Max: 1645724819000000000}, schema1, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ = NewTSSPFileDetachedReader(metaIndex[:1], [][]int{[]int{0, 2}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &option)
	totalRow = 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		assert.Equal(t, data.RowNums(), data.ColVals[0].Len)
		totalRow += data.RowNums()
	}
}

func TestSeqFilterReader(t *testing.T) {
	testCompDir := t.TempDir()
	config.SetProductType("logkeeper")
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}

	p := path.Join(testCompDir, mstName)
	reader, _ := NewDetachedMetaIndexReader(p, nil)

	metaIndex, _ := reader.ReadMetaIndex([]int64{16, 56}, []int64{40, 40})
	option := query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
	}
	option.LogQueryCurrId = "1635724829000000000|9^^"
	option.Limit = 10
	schema3 := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	decs := NewFileReaderContext(util.TimeRange{Min: 1635724829000000000, Max: 1645724819000000000}, schema3, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	treader, _ := NewTSSPFileDetachedReader(metaIndex[:1], [][]int{[]int{0, 2}}, decs, sparseindex.NewOBSFilterPath("", p, nil), nil, true, &option)
	totalRow := 0
	for {
		data, _, _ := treader.Next()
		if data == nil {
			break
		}
		assert.Equal(t, data.RowNums(), data.ColVals[0].Len)
		totalRow += data.RowNums()
	}
	rec := &record.Record{}
	rec.SetSchema(schema3)
	rec.ColVals = make([]record.ColVal, len(schema3))
	rec.AppendTime(1635724829000000000)
	rec.ColVals[0].AppendInteger(1)
	rec.ColVals[1].AppendInteger(2)
	rec = treader.filterForLog(rec)
	assert.Equal(t, 1, rec.RowNums())

	option.LogQueryCurrId = "1635724829000000000|9^"
	err = treader.parseSeqId(&option)
	if IsInterfaceNil(err) {
		t.Errorf("get wrong result")
	}
	option.LogQueryCurrId = "^^"
	err = treader.parseSeqId(&option)
	if !IsInterfaceNil(err) {
		t.Errorf("get wrong result")
	}

	config.SetProductType("basic")
}
