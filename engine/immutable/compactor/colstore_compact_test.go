// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package compactor_test

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/immutable/compactor"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testTimeStart = time.Date(2021, 11, 1, 0, 0, 0, 0, time.UTC)
	timeInterval  = time.Second
)

var schemaForColumnStore = []record.Field{
	{Name: "field1_float", Type: influx.Field_Type_Float},
	{Name: "field2_int", Type: influx.Field_Type_Int},
	{Name: "field3_bool", Type: influx.Field_Type_Boolean},
	{Name: "field4_bf_string", Type: influx.Field_Type_String},
	{Name: "primaryKey_float1", Type: influx.Field_Type_Float},
	{Name: "primaryKey_string1", Type: influx.Field_Type_String},
	{Name: "primaryKey_string2", Type: influx.Field_Type_String},
	{Name: "sortKey_int1", Type: influx.Field_Type_Int},
	{Name: "time", Type: influx.Field_Type_Int},
}

func init() {
	config.GetStoreConfig().ColumnStore.CompactEnabled = true
	compactor.InitColStoreCompactor()
}

func genTestDataForColumnStore(rows int, pkFlag int, starTime *time.Time) *record.Record {
	tm := *starTime

	value := 1.1

	genRecFn := func() *record.Record {
		bv := false
		b := record.NewRecordBuilder(schemaForColumnStore)
		for i := 1; i <= rows; i++ {
			b.Column(0).AppendFloat(value)
			b.Column(1).AppendInteger(int64(value))
			b.Column(2).AppendBoolean(bv)
			b.Column(3).AppendString(fmt.Sprintf("test-%f", value))
			b.Column(4).AppendFloat(float64(i % 100))
			b.Column(5).AppendString(fmt.Sprintf("pk-string-%d-%d", pkFlag, i%10))
			b.Column(6).AppendString(fmt.Sprintf("pk-string-%d-%d", pkFlag, 1%10+1))
			b.Column(7).AppendInteger(int64(value))
			b.Column(8).AppendInteger(tm.UnixNano()) // timestamp

			tm = tm.Add(timeInterval)
			value += 1.0
			bv = !bv
		}

		*starTime = tm
		return b
	}

	return genRecFn()
}

func SortByRemainder10[T int64 | float64](a []T) {
	sort.Slice(a, func(i, j int) bool {
		numI := int(a[i])
		numJ := int(a[j])

		remI := numI % 10
		remJ := numJ % 10

		if remI != remJ {
			return remI < remJ
		}

		return a[i] < a[j]
	})
}

func genExprCol2Values() []bool {
	boolSlice := make([]bool, 1000)
	for group := 0; group < 5; group++ {
		start := group * 200

		for i := 0; i < 100; i++ {
			boolSlice[start+i] = true
		}

		for i := 100; i < 200; i++ {
			boolSlice[start+i] = false
		}
	}
	return boolSlice
}

func genExprCol3FromCol0(float64Slice []float64) []string {
	col3Values := make([]string, len(float64Slice))
	for i, val := range float64Slice {
		col3Values[i] = fmt.Sprintf("test-%f", val)
	}
	return col3Values
}

func genExprCo4FromCol0(float64Slice []float64) []float64 {
	col4Values := make([]float64, len(float64Slice))
	for i, val := range float64Slice {
		col4Values[i] = float64(int(val) % 100)
	}
	return col4Values
}

func defaultIdent() util.MeasurementIdent {
	ident := util.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	return ident
}

func clearMstInfo() {
	colstore.MstManagerIns().Clear()
}

func defaultBFIndexRelation() influxql.IndexRelation {
	list := make([]*influxql.IndexList, 1)
	list[0] = &influxql.IndexList{IList: []string{"field4_bf_string"}}

	return influxql.IndexRelation{
		IndexNames: []string{"bloom_filter"},
		Oids:       []uint32{uint32(index.BloomFilter)},
		IndexList:  list,
	}
}

func defaultColumnStoreMstInfo(pk, sk []string, tc time.Duration) *meta.MeasurementInfo {
	fields := make(meta.CleanSchema)

	for j := range schemaForColumnStore {
		fields[schemaForColumnStore[j].Name] = meta.SchemaVal{Typ: int8(schemaForColumnStore[j].Type)}
	}

	return &meta.MeasurementInfo{
		Name:       "mst",
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			PrimaryKey:          pk,
			SortKey:             append(pk, sk...),
			TimeClusterDuration: tc,
			Property: meta.MeasurementProperty{
				Unique:         false,
				PrimaryKeyType: meta.PrimaryKeyTypeCluster,
			},
		},
		Schema: &fields,
	}
}

func initColumnStoreMstInfo(mi *meta.MeasurementInfo) (*colstore.Measurement, bool) {
	ident := defaultIdent()
	colstore.MstManagerIns().Add(ident, mi)

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		return nil, false
	}

	return mst, true
}

func enableUnique(csInfo *meta.ColStoreInfo) {
	csInfo.PropertyKey = append(csInfo.PropertyKey, meta.PropertyKeyUniqueEnabled)
	csInfo.PropertyValue = append(csInfo.PropertyValue, "true")
	csInfo.BuildProperty()
}

func enableSparse(csInfo *meta.ColStoreInfo) {
	csInfo.PropertyKey = append(csInfo.PropertyKey, meta.PropertyKeyPKType)
	csInfo.PropertyValue = append(csInfo.PropertyValue, meta.PrimaryKeyTypeSparse)
	csInfo.BuildProperty()
}

func newColStore(dir string) *immutable.MmsTables {
	conf := immutable.NewColumnStoreConfig()
	tier := uint64(util.Hot)
	lockPath := ""

	store := immutable.NewTableStore(dir, &lockPath, &tier, true, conf)
	store.SetDbRp("db0", "rp0")
	store.SetImmTableType(config.COLUMNSTORE)
	store.CompactionEnable()
	return store
}

func createColStore(t *testing.T, mst *colstore.Measurement, filesN int) (*immutable.MmsTables, *immutable.TSSPFiles, func()) {
	store := newColStore(t.TempDir())

	err := writeDefaultData(store, mst, filesN)
	require.NoError(t, err)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, filesN, len(files.Files()))

	return store, files, func() {
		_ = store.Close()
	}
}

func writeDefaultData(store *immutable.MmsTables, mst *colstore.Measurement, fileNum int) error {
	recRows := 1000
	tm := testTimeStart

	oldRec := record.NewRecordBuilder(schemaForColumnStore)
	oldRec.ReserveColumnRows(recRows * fileNum)

	recs := make([]*record.Record, 0, fileNum)

	for i := 0; i < fileNum; i++ {
		rec := genTestDataForColumnStore(recRows, i%2, &tm)
		wa := mutable.NewWriteAttached(mst)
		wa.FlushRecord(store, rec)
		recs = append(recs, rec)
	}
	return nil
}

func itrTSSPFile(f immutable.TSSPFile, hook func(sid uint64, rec *record.Record)) {
	fi := immutable.NewFileIterator(f, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)

	for {
		if !itr.Next() {
			break
		}

		sid := itr.GetSeriesID()
		if sid == 0 {
			panic("series ID is zero")
		}
		rec := itr.GetRecord()
		record.CheckRecord(rec)

		hook(sid, rec)
	}
}

func itrTSSPFileBySegment(f immutable.TSSPFile, hook func(pk []byte, rec *record.Record) error) error {
	mi, err := f.MetaIndexAt(0)
	if err != nil {
		return err
	}

	metas, err := f.ReadChunkMetaData(0, mi, nil, fileops.IO_PRIORITY_HIGH)
	if err != nil {
		return err
	}

	if len(metas) != 1 {
		return fmt.Errorf("expected 1 chunk meta, found %d", len(metas))
	}
	colMetas := metas[0].GetColMeta()

	var rec record.Record
	for i := range colMetas {
		rec.Schema = append(rec.Schema, record.Field{
			Type: int(colMetas[i].Type()),
			Name: colMetas[i].Name()})
	}
	rec.ColVals = make([]record.ColVal, len(colMetas))

	sr := immutable.NewSegmentReader(immutable.NewFileIterator(f, logger.NewLogger(errno.ModuleCompact)))
	pkInfo := f.GetPkInfo()
	frs := pkInfo.GetMark().GetSegmentsFromFragmentRange()

	col := &record.ColVal{}

	for _, fr := range frs {
		rec.InitColVal(0, rec.Len())
		for j := fr.Start; j < fr.End; j++ {
			for i := range colMetas {
				cm := &colMetas[i]
				seg := cm.GetSegment(int(j))

				ref := &rec.Schema[i]
				err = sr.Read(seg, ref, col)
				if err != nil {
					return err
				}
				rec.ColVals[i].AppendColVal(col, ref.Type, 0, col.Len)
			}
		}

		record.CheckRecord(&rec)
		err = hook(nil, &rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func sequenceVerification(f immutable.TSSPFile, unique bool, skSchema record.Schemas) error {
	err := itrTSSPFileBySegment(f, func(pk []byte, rec *record.Record) error {
		for i := range rec.RowNums() - 1 {
			n := compareSortKey(rec, skSchema, i)
			if n > 0 || (unique && n == 0) {
				return fmt.Errorf("sequence verification failed")
			}
		}
		return nil
	})

	return err
}

func compareSortKey(rec *record.Record, skSchema record.Schemas, row int) int {
	var compareBool = func(a, b bool) int {
		if a && b {
			return 0
		}
		if a {
			return -1
		}
		return 1
	}
	var compareInt = func(col *record.ColVal) int {
		a, nilA := col.IntegerValue(row)
		b, nilB := col.IntegerValue(row + 1)

		if nilA || nilB {
			return compareBool(nilA, nilB)
		}
		return cmp.Compare(a, b)
	}
	var compareFloat = func(col *record.ColVal) int {
		a, nilA := col.FloatValue(row)
		b, nilB := col.FloatValue(row + 1)

		if nilA || nilB {
			return compareBool(nilA, nilB)
		}
		return cmp.Compare(a, b)
	}
	var compareString = func(col *record.ColVal) int {
		a, nilA := col.StringValue(row)
		b, nilB := col.StringValue(row + 1)

		if nilA || nilB {
			return compareBool(nilA, nilB)
		}
		return cmp.Compare(string(a), string(b))
	}

	n := 0
	for i := range skSchema {
		name := skSchema[i].Name
		if name == record.TimeField {
			n = compareInt(rec.TimeColumn())
		} else {
			j := rec.FieldIndexsFast(skSchema[i].Name)
			col := &rec.ColVals[j]
			switch skSchema[i].Type {
			case influx.Field_Type_Int:
				n = compareInt(col)
			case influx.Field_Type_Float:
				n = compareFloat(col)
			case influx.Field_Type_String:
				n = compareString(col)
			default:
				panic("unknown field type")
			}
		}
		if n != 0 {
			break
		}
	}
	return n
}

func TestColStoreCompactor_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	immutable.SetChunkMetaCompressMode(3)
	conf := immutable.GetColStoreConfig()

	t.Run("MaxRowsPerSegment=64", func(t *testing.T) {
		conf.SetMaxRowsPerSegment(64)
		runColStoreCompactor(t, mst, immutable.LeveLMinGroupFiles[0])
	})

	t.Run("MaxRowsPerSegment=default", func(t *testing.T) {
		conf.SetMaxRowsPerSegment(util.DefaultMaxRowsPerSegment4ColStore)
		runColStoreCompactor(t, mst, immutable.LeveLMinGroupFiles[0])
	})
}

func TestColStoreCompactorWithSkipIndex(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"sortKey_int1", "time"}
	defer clearMstInfo()

	mstInfo := defaultColumnStoreMstInfo(primaryKey, sortKey, 0)
	mstInfo.IndexRelation = defaultBFIndexRelation()

	mst, ok := initColumnStoreMstInfo(mstInfo)
	require.True(t, ok)

	var assertSkipIndexFiles = func(tb *immutable.MmsTables, files *immutable.TSSPFiles) {
		tb.Path()
		items, err := fileops.ReadDir(filepath.Join(tb.Path(), mst.MeasurementInfo().Name))
		require.NoError(t, err)
		n := 0
		for _, item := range items {
			if item.IsDir() {
				continue
			}
			n++
		}
		// 4 files: tssp, idx, bf, am
		require.Equal(t, 4, n)
	}
	var assertSkipIndexInfo = func(tb *immutable.MmsTables, files *immutable.TSSPFiles) {
		require.Equal(t, 1, len(files.Files()))
		// 2 files: bf, am
		require.Equal(t, 2, len(files.Files()[0].GetSkipIndexInfo()))

		schema := append(schemaForColumnStore[:2:2], record.Field{
			Type: influx.Field_Type_Int,
			Name: record.TimeField,
		})
		assertReadFile(t, files.Files()[0], schema)
	}

	runColStoreCompactor(t, mst, immutable.LeveLMinGroupFiles[0], assertSkipIndexFiles, assertSkipIndexInfo)
}

func TestColStoreCompactorMultipleLevels(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"sortKey_int1", "time"}
	defer clearMstInfo()

	mstInfo := defaultColumnStoreMstInfo(primaryKey, sortKey, 0)

	mst, ok := initColumnStoreMstInfo(mstInfo)
	require.True(t, ok)

	runColStoreCompactor(t, mst, immutable.LeveLMinGroupFiles[0]*8)
}

func TestColStoreCompactorWithTCDuration(t *testing.T) {
	primaryKey := []string{"time", "primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()

	mstInfo := defaultColumnStoreMstInfo(primaryKey, sortKey, time.Hour)

	mst, ok := initColumnStoreMstInfo(mstInfo)
	require.True(t, ok)

	runColStoreCompactor(t, mst, immutable.LeveLMinGroupFiles[0]*8)
}

func runColStoreCompactor(t *testing.T, mst *colstore.Measurement, filesN int, asserts ...func(*immutable.MmsTables, *immutable.TSSPFiles)) {
	store, _, release := createColStore(t, mst, filesN)
	defer release()

	schema := append(schemaForColumnStore[:2:2], record.Field{
		Type: influx.Field_Type_Int,
		Name: record.TimeField,
	})

	var compact = func(oldFiles []immutable.TSSPFile) {
		ident := util.NewMeasurementIdent("db0", "rp0")
		ident.SetName("mst")
		comp := immutable.NewColStoreCompactor(store.GetLockPath(), store.NewStreamWriteFile("mst"))
		newFiles, err := comp.Compact(ident, oldFiles)
		require.NoError(t, err)
		require.Equal(t, 1, len(newFiles))

		err = store.ReplaceFiles(ident.Name, oldFiles, newFiles, true)
		require.NoError(t, err)
	}

	for _, n := range immutable.LeveLMinGroupFiles {
		offset := 0
		for {
			files, ok := store.CSFiles["mst"]
			require.True(t, ok)
			require.True(t, len(files.Files()) > 0)

			if files.Len() < (n + offset) {
				break
			}

			var oldFiles []immutable.TSSPFile
			oldFiles = append(oldFiles, files.Files()[offset:offset+n]...)
			compact(oldFiles)
			offset++
		}
	}

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.True(t, len(files.Files()) > 0)

	assertReadFile(t, files.Files()[0], schema)
	pki := files.Files()[0].GetPkInfo()
	require.NotEmpty(t, pki)

	file := files.Files()[0]

	itrTSSPFile(file, func(sid uint64, rec *record.Record) {
		record.CheckRecord(rec)
		require.Equal(t, colstore.SeriesID, sid)
		require.Equal(t, filesN*1000, rec.RowNums())
	})

	for _, fn := range asserts {
		fn(store, files)
	}
}

func assertReadFile(t *testing.T, file immutable.TSSPFile, schema record.Schemas) {
	mi, err := file.MetaIndexAt(0)
	require.NoError(t, err)
	metas, err := file.ReadChunkMetaData(0, mi, nil, fileops.IO_PRIORITY_HIGH)
	require.NoError(t, err)
	require.Equal(t, 1, len(metas))

	rec := &record.Record{}
	rec.ResetWithSchema(schema)
	for i := range metas[0].SegmentCount() {
		rec, err = file.ReadAt(&metas[0], i, rec, immutable.NewReadContext(true), fileops.IO_PRIORITY_HIGH)
		require.NoError(t, err)
		record.CheckRecord(rec)
		require.True(t, rec.RowNums() > 0)
	}
}

func TestReadPKAtColStore(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	_, files, release := createColStore(t, mst, 1)
	defer release()

	testData := genTestDataForColumnStore(1000, 0, &testTimeStart)

	f := files.Files()[0]

	decs := immutable.NewReadContext(true)

	metaIndex, err := f.MetaIndexAt(0)
	require.NoError(t, err)

	chunkMeta, err := f.ReadChunkMetaData(0, metaIndex, nil, fileops.IO_PRIORITY_LOW_READ)
	require.NoError(t, err)

	readData := &record.Record{}
	readData.Schema = schemaForColumnStore
	readData.ColVals = make([]record.ColVal, len(schemaForColumnStore))

	pkMark := f.GetPkInfo().GetMark()
	segmentRanges := pkMark.GetSegmentsFromFragmentRange()
	for _, seg := range segmentRanges {
		for i := seg.Start; i < seg.End; i++ {
			tempRec := &record.Record{}
			tempRec.Schema = schemaForColumnStore
			tempRec.ColVals = make([]record.ColVal, len(schemaForColumnStore))
			tempRec, err = f.ReadAt(&chunkMeta[0], int(i), tempRec, decs, fileops.IO_PRIORITY_LOW_READ)
			if err != nil {
				t.Fatal(err)
			}
			readData.AppendRec(tempRec, 0, tempRec.RowNums())
		}
	}

	require.Equal(t, testData.RowNums(), readData.RowNums())

	// readData is order by pk
	testCol0 := testData.ColVals[0].FloatValues()
	SortByRemainder10(testCol0)
	assert.Equal(t, testCol0, readData.ColVals[0].FloatValues(), "the value of column 0 should be equal")

	testCol1 := testData.ColVals[1].IntegerValues()
	SortByRemainder10(testCol1)
	assert.Equal(t, testCol1, readData.ColVals[1].IntegerValues(), "the value of column 1 should be equal")

	testCol2 := genExprCol2Values()
	assert.Equal(t, testCol2, readData.ColVals[2].BooleanValues(), "the value of column 2 should be equal")

	assert.Equal(t, genExprCol3FromCol0(testCol0), readData.ColVals[3].StringValues(nil), "the value of column 3 should be equal")

	assert.Equal(t, genExprCo4FromCol0(testCol0), readData.ColVals[4].FloatValues(), "the value of column 4 should be equal")

	testCol5 := testData.ColVals[5].StringValues(nil)
	sort.Strings(testCol5)
	assert.Equal(t, testCol5, readData.ColVals[5].StringValues(nil), "the value of column 5 should be equal")

	assert.Equal(t, testData.ColVals[6], readData.ColVals[6], "the value of column 6 should be equal")

	testCol7 := testData.ColVals[7].IntegerValues()
	SortByRemainder10(testCol7)
	assert.Equal(t, testCol7, readData.ColVals[7].IntegerValues(), "the value of column 7 should be equal")
}

func TestInvalidPKInfo(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	_, files, release := createColStore(t, mst, immutable.LeveLMinGroupFiles[0])
	defer release()

	rec := &record.Record{}
	rec.ResetWithSchema(mst.PrimaryKey())
	file := files.Files()[0]
	file.SetPkInfo(colstore.NewPKInfo(rec, &fragment.IndexFragmentVariableImpl{}, "", -1))

	_, err := immutable.NewCSFileIterator(file, mst.TCDuration(), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid primary key")
}

func TestCheckPKRecord(t *testing.T) {
	rec := &record.Record{}
	rec.ResetWithSchema(schemaForColumnStore[:2:2])
	require.Error(t, compactor.CheckPKRecord(rec, 1))

	rec.ColVals[0].AppendFloat(1)
	rec.ColVals[1].AppendIntegerNull()
	require.Error(t, compactor.CheckPKRecord(rec, 1))
}

func TestUniqueKey_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1", "primaryKey_string2"}
	sortKey := []string{"field4_bf_string", "time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	enableUnique(mst.ColStoreInfo())
	require.True(t, mst.IsUniqueEnabled())

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 100000
	tm := testTimeStart

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	recDup := &record.Record{}
	recDup.ResetWithSchema(rec.Schema)
	recDup.AppendRec(rec, 0, rec.RowNums())
	recDup.AppendRec(rec, 0, rec.RowNums())

	wa := mutable.NewWriteAttached(mst)
	wa.FlushRecord(store, recDup)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	itrTSSPFile(files.Files()[0], func(sid uint64, rec *record.Record) {
		require.Equal(t, recRows, rec.RowNums())
	})
}

func TestUniqueKey_case2(t *testing.T) {
	primaryKey := []string{"primaryKey_string1", "primaryKey_string2"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	enableUnique(mst.ColStoreInfo())
	require.True(t, mst.IsUniqueEnabled())

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 2
	tm := testTimeStart

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	recDup := &record.Record{}
	recDup.ResetWithSchema(rec.Schema)
	recDup.AppendRec(rec, 0, rec.RowNums())

	col := &recDup.ColVals[0]
	col.Init()
	col.AppendFloatNull()
	col.AppendFloat(1.1)

	col = &rec.ColVals[0]
	col.Init()
	col.AppendFloat(1.2)
	col.AppendFloatNull()

	recDup.AppendRec(rec, 0, rec.RowNums())

	wa := mutable.NewWriteAttached(mst)
	wa.FlushRecord(store, recDup)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	itrTSSPFile(files.Files()[0], func(sid uint64, got *record.Record) {
		require.Equal(t, recRows, got.RowNums())
		require.Equal(t, []float64{1.2, 1.1}, got.ColVals[0].FloatValues())
	})
}

func TestDiffSchemaCompact_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 10
	tm := testTimeStart

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	rec2 := &record.Record{}
	rec2.ResetWithSchema(rec.Schema[2:])
	for i := 2; i < rec.Len(); i++ {
		rec2.ColVals[i-2].AppendColVal(&rec.ColVals[i], rec.Schema[i].Type, 0, rec.RowNums())
	}

	wa := mutable.NewWriteAttached(mst)
	wa.FlushRecord(store, rec)
	wa.FlushRecord(store, rec2)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 2, files.Len())

	require.NoError(t, compactFiles(store, files.Files()))

	files, ok = store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())
}

func TestUniqueKeyCompact_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"primaryKey_float1", "time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	enableUnique(mst.ColStoreInfo())
	require.True(t, mst.IsUniqueEnabled())

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 100000
	tm := testTimeStart

	ints := make([]int64, recRows)

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	var times []int64
	for i := range recRows {
		times = append(times, int64(1000000+i))
	}

	for i := range 6 {
		ints = ints[:0]
		for range recRows {
			ints = append(ints, int64(i))
		}

		rec.ColVals[1].Init()
		rec.ColVals[1].AppendIntegers(ints...)
		tmCol := rec.TimeColumn()
		tmCol.Init()

		sort.Slice(times, func(i, j int) bool {
			return rand.Float64() > 0.5
		})
		tmCol.AppendTimes(times)

		wa := mutable.NewWriteAttached(mst)
		wa.FlushRecord(store, rec)
	}
	rec.Schema = append(append(record.Schemas{}, rec.Schema[:1]...), rec.Schema[2:]...)
	rec.ColVals = append(rec.ColVals[:1], rec.ColVals[2:]...)
	for range 2 {
		wa := mutable.NewWriteAttached(mst)
		wa.FlushRecord(store, rec)
	}

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 8, files.Len())

	oldFiles := append([]immutable.TSSPFile{}, files.Files()...)
	oldFiles[1], oldFiles[2], oldFiles[3] = oldFiles[3], oldFiles[1], oldFiles[2]
	err := compactFiles(store, oldFiles)
	require.NoError(t, err)

	files, ok = store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	err = sequenceVerification(files.Files()[0], mst.IsUniqueEnabled(), mst.SortKey())
	require.NoError(t, err)
}

func TestCSConsumeIterator(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 1000
	tm := testTimeStart

	rec := genTestDataForColumnStore(recRows, 10, &tm)

	wa := mutable.NewWriteAttached(mst)
	wa.FlushRecord(store, rec)
	wa.FlushRecord(store, rec)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 2, files.Len())

	csFiles := files.Copy()

	var its []*immutable.CSFileIterator
	for i := 0; i < 2; i++ {
		it, err := immutable.NewCSFileIterator(csFiles[i], mst.TCDuration(), true)
		require.NoError(t, err)
		its = append(its, it)
	}

	itr := immutable.NewCSConsumeIterator(its)
	defer itr.Release()

	segmentCount := 0
	rowCount := 0

	for {
		rec, err := itr.Next()
		if err != nil {
			break
		}

		segmentCount++
		rowCount += rec.Rec.RowNums()
	}
	require.Equal(t, 20, segmentCount)
	require.Equal(t, 2000, rowCount)
}

func TestCompactChunkMetaTimeRange(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"primaryKey_float1", "time"}
	defer clearMstInfo()
	mst, ok := initColumnStoreMstInfo(defaultColumnStoreMstInfo(primaryKey, sortKey, 0))
	require.True(t, ok)

	enableUnique(mst.ColStoreInfo())
	require.True(t, mst.IsUniqueEnabled())

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 200
	tm := testTimeStart

	times := make([]int64, recRows)
	for i := range recRows {
		times[i] = int64(i + 1e9)
	}

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	for range 8 {
		rand.Shuffle(len(times), func(i, j int) {
			times[i], times[j] = times[j], times[i]
		})
		tmCol := rec.TimeColumn()
		tmCol.Init()
		tmCol.AppendTimes(times)
		wa := mutable.NewWriteAttached(mst)
		wa.FlushRecord(store, rec)
	}

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 8, files.Len())
	for i := range files.Files() {
		require.NoError(t, checkChunkMetaTimeRange(files.Files()[i]))
	}

	oldFiles := append([]immutable.TSSPFile{}, files.Files()...)
	oldFiles[1], oldFiles[2], oldFiles[3] = oldFiles[3], oldFiles[1], oldFiles[2]
	err := compactFiles(store, oldFiles)
	require.NoError(t, err)

	files, ok = store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())
	require.NoError(t, checkChunkMetaTimeRange(files.Files()[0]))
}

func checkChunkMetaTimeRange(f immutable.TSSPFile) error {
	fi := immutable.NewFileIterator(f, logger.NewLogger(errno.ModuleCompact))
	if !fi.NextChunkMeta() {
		return fmt.Errorf("invalid tssp file")
	}
	cm := fi.GetCurtChunkMeta()

	fileMinT, fileMaxT, err := f.MinMaxTime()
	if err != nil {
		return err
	}

	minT, maxT := cm.MinMaxTime()
	for i := range cm.SegmentCount() {
		minT = min(minT, cm.SegmentMinTime(i))
		maxT = max(maxT, cm.SegmentMaxTime(i))
	}
	if minT != fileMinT || maxT != fileMaxT {
		return fmt.Errorf("invalid min/max time range: min=%v max=%v; file min=%v max=%v", minT, maxT, fileMinT, fileMaxT)
	}

	return nil
}

func TestSparsePk_case1(t *testing.T) {
	primaryKey := []string{"primaryKey_string1"}
	sortKey := []string{"time"}
	defer clearMstInfo()
	mi := defaultColumnStoreMstInfo(primaryKey, sortKey, 0)
	enableSparse(mi.ColStoreInfo)
	enableUnique(mi.ColStoreInfo)

	mst, ok := initColumnStoreMstInfo(mi)
	require.True(t, ok)

	store := newColStore(t.TempDir())
	defer store.Close()

	recRows := 20
	fileCount := 64
	tm := testTimeStart

	rec := genTestDataForColumnStore(recRows, 10, &tm)
	times := make([]int64, recRows*2)
	for i := range times {
		times[i] = int64(i + 1e9)
	}

	wa := mutable.NewWriteAttached(mst)
	for i := range fileCount {
		start := i % (len(times) - recRows)

		tmCol := rec.TimeColumn()
		tmCol.Init()
		tmCol.AppendTimes(times[start : start+recRows])
		wa.FlushRecord(store, rec)
	}

	var compact = func(n int) {
		files, ok := store.CSFiles["mst"]
		require.True(t, ok)

		var groups = make([][]immutable.TSSPFile, 1+files.Len()/n)
		for i := range files.Len() {
			groups[i/n] = append(groups[i/n], files.Files()[i])
		}

		for i := range groups {
			if len(groups[i]) < 2 {
				continue
			}
			err := compactFiles(store, groups[i])
			require.NoError(t, err)
		}
	}

	compact(4)
	compact(4)
	compact(4)

	files, ok := store.CSFiles["mst"]
	require.True(t, ok)
	require.Equal(t, 1, files.Len())

	itrTSSPFile(files.Files()[0], func(sid uint64, rec *record.Record) {
		require.Equal(t, 300, rec.RowNums())
		require.True(t, rec.FieldIndexs(primaryKey[0]) > 0)
	})
}

func compactFiles(store *immutable.MmsTables, oldFiles []immutable.TSSPFile) error {
	oldFiles = append([]immutable.TSSPFile{}, oldFiles...)
	ident := util.NewMeasurementIdent("db0", "rp0")
	ident.SetName("mst")
	comp := immutable.NewColStoreCompactor(store.GetLockPath(), store.NewStreamWriteFile("mst"))
	newFiles, err := comp.Compact(ident, oldFiles)
	if err != nil {
		return err
	}
	if len(newFiles) != 1 {
		return fmt.Errorf("invalid compaction files")
	}

	err = store.ReplaceFiles(ident.Name, oldFiles, newFiles, true)
	return err
}
