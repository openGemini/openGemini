// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/binaryfilterfunc"
	"github.com/openGemini/openGemini/lib/bitmap"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_reserveColumnValue(t *testing.T) {
	mySchema := []record.Field{
		{Name: "field1_float", Type: influx.Field_Type_Float},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_bool", Type: influx.Field_Type_Boolean},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	rec := record.NewRecordBuilder(mySchema)
	rec.Column(0).AppendFloat(1)
	rec.Column(0).AppendFloatNull()
	rec.Column(1).AppendIntegerNull()
	rec.Column(1).AppendInteger(2)
	rec.Column(2).AppendBoolean(true)
	rec.Column(2).AppendBooleanNull()
	rec.Column(3).AppendString("a")
	rec.Column(3).AppendStringNull()
	rec.Column(4).AppendIntegers(10, 11)
	reserveColumnValue(&rec.Schema[0], rec.Column(0), 1)
	reserveColumnValue(&rec.Schema[1], rec.Column(1), 0)
	reserveColumnValue(&rec.Schema[2], rec.Column(2), 1)
	reserveColumnValue(&rec.Schema[3], rec.Column(3), 1)

	assert.Equal(t, rec.Column(0).Len, 1)
	assert.Equal(t, rec.Column(0).NilCount, 1)
	assert.Equal(t, rec.Column(1).Len, 1)
	assert.Equal(t, rec.Column(1).NilCount, 1)
	assert.Equal(t, rec.Column(2).Len, 1)
	assert.Equal(t, rec.Column(2).NilCount, 1)
	assert.Equal(t, rec.Column(3).Len, 1)
	assert.Equal(t, rec.Column(3).NilCount, 1)
}

func Test_AppendString(t *testing.T) {
	var bmCol = &record.ColVal{}
	col := &record.ColVal{}

	// test reverse 1 row
	bmCol.AppendString("aa")
	reverseStringValues(bmCol.Val, bmCol.Offset, col, bmCol)
	if col.Len != 1 {
		t.Fatal("reserve string column fail")
	}
	v, ok := bmCol.StringValueUnsafe(0)
	if ok || v != "aa" {
		t.Fatal("reserve string column fail")
	}

	// test reverse 1 nil row
	bmCol.Init()
	col.Init()
	bmCol.AppendStringNull()
	reverseStringValues(bmCol.Val, bmCol.Offset, col, bmCol)
	if col.Len != 1 && col.Len != col.NilCount {
		t.Fatal("reserve string column fail")
	}

	strArr := []string{"", "1aaa", "", "1bbb", "", "1ccc"}
	rArr := []string{"1ccc", "", "1bbb", "", "1aaa", ""}
	var offset []uint32
	var buf []byte
	for _, v := range strArr {
		offset = append(offset, uint32(len(buf)))
		buf = append(buf, v...)

		if len(v) == 0 {
			bmCol.AppendStringNull()
		} else {
			bmCol.AppendString(v)
		}
	}

	col.Init()
	reverseStringValues(buf, offset, col, bmCol)
	for i := range rArr {
		vi, ok := col.StringValueUnsafe(i)
		if ok && len(rArr[i]) != 0 {
			t.Fatalf("column(i) should ben nil, but:%v", rArr[i])
		}
		if vi != rArr[i] {
			t.Fatalf("exp:%v, get:%v", rArr[i], vi)
		}
	}
}

type MocTsspFile struct {
	TSSPFile
	path string
	err  error
}

func (m MocTsspFile) Path() string {
	return m.path
}

func (m MocTsspFile) Name() string {
	return ""
}

func (m MocTsspFile) FileName() TSSPFileName {
	return TSSPFileName{}
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
	return m.err
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

func (m MocTsspFile) MetaIndexAt(idx int) (*MetaIndex, error) {
	return nil, nil
}

func (m MocTsspFile) MetaIndex(id uint64, tr util.TimeRange) (int, *MetaIndex, error) {
	return 0, nil, nil
}

func (m MocTsspFile) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, ctx *ChunkMetaContext, ioPriority int) (*ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext, ioPriority int) (*record.Record, error) {
	return nil, nil
}

func (m MocTsspFile) ReadData(offset int64, size uint32, dst *[]byte, ioPriority int) ([]byte, error) {
	return nil, nil
}

func (m MocTsspFile) ReadChunkMetaData(metaIdx int, me *MetaIndex, dst []ChunkMeta, ioPriority int) ([]ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) CreateTime() int64 {
	return 0
}

func (m MocTsspFile) FileStat() *Trailer {
	return nil
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

func (m MocTsspFile) LoadIdTimes(p *IdTimePairs) error {
	return nil
}

func (m MocTsspFile) Rename(newName string) error {
	return nil
}

func (m MocTsspFile) Remove() error {
	return nil
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

func (m MocTsspFile) GetFileReaderRef() int64 {
	return 0
}

func (m MocTsspFile) ReOpen() error {
	return nil
}

func (m MocTsspFile) RenameOnObs(newName string, tmp bool, opt *obs.ObsOptions) error {
	return nil
}

func TestGetTsspFiles(t *testing.T) {
	m := &MmsTables{
		Order: map[string]*TSSPFiles{"table1": &TSSPFiles{files: []TSSPFile{MocTsspFile{
			path: "/tmp/openGemini",
		}}}},
	}
	f, _ := m.GetTSSPFiles("table1", true)
	if f.files[0].Path() != "/tmp/openGemini" {
		t.Fatal()
	}
}

func TestDecodeColumnData(t *testing.T) {
	timeCol := &record.ColVal{}
	timeCol.AppendIntegers(1, 2, 3)

	for _, typ := range []int{influx.Field_Type_Float, influx.Field_Type_Int, influx.Field_Type_Boolean} {
		col := &record.ColVal{}
		col.Bitmap = []byte{0, 0}
		col.BitMapOffset = 7
		ref := record.Field{Name: "foo", Type: typ}
		ctx := &ReadContext{
			coderCtx:  &encoding.CoderContext{},
			Ascending: false,
		}
		builder := ColumnBuilder{}
		builder.colMeta = &ColumnMeta{name: "foo", ty: uint8(typ), entries: make([]Segment, 1)}
		builder.coder = &encoding.CoderContext{}
		switch typ {
		case influx.Field_Type_Int:
			col.AppendIntegers(1, 2, 3)
			require.NoError(t, builder.encIntegerColumn([]record.ColVal{*timeCol}, []record.ColVal{*col}, 0))
		case influx.Field_Type_Float:
			col.AppendFloats(1, 2, 3)
			require.NoError(t, builder.encFloatColumn([]record.ColVal{*timeCol}, []record.ColVal{*col}, 0))
		case influx.Field_Type_Boolean:
			col.AppendBooleans(true, false, true)
			require.NoError(t, builder.encBooleanColumn([]record.ColVal{*timeCol}, []record.ColVal{*col}, 0))
		}

		other := &record.ColVal{}
		require.NoError(t, decodeColumnData(&ref, builder.data, other, ctx, true))
		require.Equal(t, []byte{7}, other.Bitmap)
	}
}

// fix #BUG2023110300631
func TestDecodeStringColumnDataOfTimeDesc1(t *testing.T) {
	timeCol := &record.ColVal{}
	timeCol.AppendIntegers(1, 2, 3)

	for _, typ := range []int{influx.Field_Type_String} {
		col := &record.ColVal{}
		col.Bitmap = make([]byte, 0)
		col.BitMapOffset = 0
		ref := record.Field{Name: "foo", Type: typ}
		ctx := &ReadContext{
			coderCtx:  &encoding.CoderContext{},
			Ascending: false,
		}
		builder := ColumnBuilder{}
		builder.colMeta = &ColumnMeta{name: "foo", ty: uint8(typ), entries: make([]Segment, 1)}
		builder.coder = &encoding.CoderContext{}
		switch typ {
		case influx.Field_Type_String:
			col.AppendStringNull()
			col.AppendString("abc")
			col.AppendStringNull()
			require.NoError(t, builder.encStringColumn([]record.ColVal{*timeCol}, []record.ColVal{*col}, 0))
		}

		other := &record.ColVal{}
		require.NoError(t, decodeColumnData(&ref, builder.data, other, ctx, true))
		require.Equal(t, []byte{'\x02'}, other.Bitmap)
	}
}

func preparePreAggBaseRec() *record.Record {
	s := []record.Field{
		{Name: "bps", Type: influx.Field_Type_Int},
		{Name: "direction", Type: influx.Field_Type_String},
		{Name: "campus", Type: influx.Field_Type_String},
		{Name: "net_export_name", Type: influx.Field_Type_String},
		{Name: "isp_as", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	rec := record.NewRecord(s, false)
	for i := 0; i < rec.ColNums(); i++ {
		switch rec.Schema.Field(i).Name {
		case "bps":
			for j := 0; j < 8192; j++ {
				rec.Column(i).AppendInteger(int64(j))
			}
		case "direction":
			for j := 0; j < 8192; j++ {
				if j < 4096 {
					rec.Column(i).AppendString("out")
				} else {
					rec.Column(i).AppendString("in")
				}
			}
		case "campus":
			for j := 0; j < 8192; j++ {
				if j < 2048 {
					rec.Column(i).AppendString("")
				} else if j < 4096 {
					rec.Column(i).AppendString("广州1")
				} else if j < 6144 {
					rec.Column(i).AppendString("上海1")
				} else {
					rec.Column(i).AppendString("苏州1")
				}
			}
		case "net_export_name":
			for j := 0; j < 8192; j++ {
				if j < 2048 {
					rec.Column(i).AppendString("")
				} else if j < 4096 {
					rec.Column(i).AppendString("华南-广州_PNI_广州移动")
				} else if j < 6144 {
					rec.Column(i).AppendString("华东-上海_PNI_上海联通")
				} else {
					rec.Column(i).AppendString("华东-苏州_PNI_苏州电信")
				}
			}
		case "isp_as":
			for j := 0; j < 8192; j++ {
				if j < 2048 {
					rec.Column(i).AppendString("1234")
				} else if j < 4096 {
					rec.Column(i).AppendString("1235")
				} else if j < 6144 {
					rec.Column(i).AppendString("1236")
				} else {
					rec.Column(i).AppendString("1237")
				}
			}
		case "time":
			for j := 0; j < 8192; j++ {
				if j < 4091 {
					rec.Column(i).AppendInteger(int64(1695461186000000000))
				} else {
					rec.Column(i).AppendInteger(int64(1695461486000000000))
				}
			}
		}
	}
	return rec
}

func BenchmarkFilterByFieldFuncsForMemReuse(b *testing.B) {
	_ = "time >= 1695461186000000000 and time <= 1695461486000000000 and direction = 'out' and campus = '广州1' and net_export_name = '华南-广州_PNI_广州移动'"
	rec := preparePreAggBaseRec()
	filterRec := record.NewRecord(rec.Schema, false)
	timeCond := binaryfilterfunc.GetTimeCondition(util.TimeRange{Min: 1695461186000000000, Max: 1695461486000000000}, rec.Schema, len(rec.Schema)-1)
	condition := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.AND,
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "direction"}, RHS: &influxql.StringLiteral{Val: "out"}},
			LHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "campus"}, RHS: &influxql.StringLiteral{Val: "广州1"}}},
		RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "net_export_name"}, RHS: &influxql.StringLiteral{Val: "华南-广州_PNI_广州移动"}},
	}
	condFunctions, _ := binaryfilterfunc.NewCondition(timeCond, condition, rec.Schema, nil)
	filterOption := &BaseFilterOptions{
		CondFunctions: condFunctions,
		RedIdxMap:     map[int]struct{}{},
	}
	filterBitMap := bitmap.NewFilterBitmap(filterOption.CondFunctions.NumFilter())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterByFieldFuncs(rec, filterRec, filterOption, filterBitMap)
		filterBitMap.Reset()
		filterRec.Reuse()
	}
}

func BenchmarkFilterByFieldFuncsForProjection(b *testing.B) {
	_ = "time >= 1695461186000000000 and time <= 1695461486000000000 and direction = 'out' and campus = '广州1' and net_export_name = '华南-广州_PNI_广州移动'"
	rec := preparePreAggBaseRec()
	timeCond := binaryfilterfunc.GetTimeCondition(util.TimeRange{Min: 1695461186000000000, Max: 1695461486000000000}, rec.Schema, len(rec.Schema)-1)
	condition := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.AND,
			RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "direction"}, RHS: &influxql.StringLiteral{Val: "out"}},
			LHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "campus"}, RHS: &influxql.StringLiteral{Val: "广州1"}}},
		RHS: &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "net_export_name"}, RHS: &influxql.StringLiteral{Val: "华南-广州_PNI_广州移动"}},
	}
	condFunctions, _ := binaryfilterfunc.NewCondition(timeCond, condition, rec.Schema, nil)
	filterOption := &BaseFilterOptions{
		CondFunctions: condFunctions,
		RedIdxMap:     map[int]struct{}{1: {}, 2: {}, 3: {}},
	}
	filterRec := record.NewRecord([]record.Field{
		{Name: "bps", Type: influx.Field_Type_Int},
		{Name: "isp_as", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}, false)
	filterBitMap := bitmap.NewFilterBitmap(filterOption.CondFunctions.NumFilter())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterByFieldFuncs(rec, filterRec, filterOption, filterBitMap)
		filterBitMap.Reset()
		filterRec.Reuse()
	}
}

func BenchmarkGenRecByReserveIds(b *testing.B) {
	_ = "select sum(bps) as bps from pre_agg_base " +
		"where time >= 1695461186000000000 and time <= 1695461486000000000 and direction = 'out' and campus = '广州1' and net_export_name = '华南-广州_PNI_广州移动'" +
		"group by isp_as " +
		"order by bps desc " +
		"limit 100"
	rec := preparePreAggBaseRec()
	filterRec := record.NewRecord([]record.Field{
		{Name: "bps", Type: influx.Field_Type_Int},
		{Name: "direction", Type: influx.Field_Type_String},
		{Name: "campus", Type: influx.Field_Type_String},
		{Name: "net_export_name", Type: influx.Field_Type_String},
		{Name: "isp_as", Type: influx.Field_Type_String},
		{Name: "time", Type: influx.Field_Type_Int},
	}, false)
	RedIdxMap := map[int]struct{}{1: {}, 2: {}, 3: {}}
	var reserveId []int
	for i := 0; i < 8092; i++ {
		if i%2 == 0 {
			reserveId = append(reserveId, i)
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GenRecByReserveIds(rec, filterRec, reserveId, RedIdxMap)
		filterRec.Reuse()
	}
}

func BenchmarkGenRecByRowNumbers(b *testing.B) {
	rec := preparePreAggBaseRec()
	filterRec := record.NewRecord(rec.Schema, false)
	var reserveId []int
	for i := 0; i < 8092; i++ {
		if i%2 == 0 {
			reserveId = append(reserveId, i)
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		genRecByRowNumbers(rec, filterRec, reserveId, nil, nil)
		filterRec.Reuse()
	}
}

type colValCodec struct {
	timeCol record.ColVal
	dataCol record.ColVal
	other   record.ColVal

	typ uint8
	buf []byte
}

func (codec *colValCodec) setTyp(typ uint8) {
	codec.typ = typ
}

func (codec *colValCodec) encode(t *testing.T) {
	builder := NewColumnBuilder()
	builder.colMeta = &ColumnMeta{}
	builder.colMeta.growEntry()

	timeCols := []record.ColVal{codec.timeCol}
	dataCols := []record.ColVal{codec.dataCol}

	ref := record.Field{
		Type: int(codec.typ),
		Name: "",
	}

	data, err := builder.encode(ref, dataCols, timeCols, 0)
	require.NoError(t, err)
	codec.buf = data
}

func (codec *colValCodec) decode(t *testing.T) {
	ref := &record.Field{
		Type: int(codec.typ),
		Name: "",
	}

	ctx := NewReadContext(true)
	err := decodeColumnData(ref, codec.buf, &codec.other, ctx, true)
	require.NoError(t, err)
	require.Equal(t, codec.dataCol, codec.other)
}

func (codec *colValCodec) assertEncodeDataSize(t *testing.T, expSize int) {
	require.Equal(t, expSize, len(codec.buf))
}

// ColVal.Len = 1
func TestEncodeOneRowMode(t *testing.T) {
	codec := &colValCodec{}
	codec.setTyp(encoding.BlockInteger)

	codec.timeCol.AppendIntegers(1)
	codec.dataCol.AppendIntegers(2)
	codec.encode(t)
	codec.assertEncodeDataSize(t, 9)

	other := &record.ColVal{}
	DecodeColumnOfOneValue(codec.buf[1:], other, influx.Field_Type_Int)
	require.Equal(t, 1, other.Len)
	require.Equal(t, 0, other.NilCount)
	require.Equal(t, 8, len(other.Val))

	codec.dataCol.Init()
	codec.dataCol.AppendIntegerNull()
	codec.encode(t)
	codec.assertEncodeDataSize(t, 5)

	var s = ""
	for i := 0; i < 50; i++ {
		s += "aaa"
	}
	codec.dataCol.Init()
	codec.dataCol.AppendString(s)
	codec.encode(t)
	require.True(t, len(codec.buf) < 151)
}

// ColVal.NilCount = 0
func TestEncodeFullMode(t *testing.T) {
	codec := &colValCodec{}

	codec.setTyp(encoding.BlockInteger)
	codec.timeCol.AppendIntegers(1, 2, 3)
	codec.dataCol.AppendIntegers(2, 3, 4)
	codec.encode(t)
	require.Equal(t, uint8(encoding.BlockIntegerFull), codec.buf[0])
	codec.decode(t)

	codec.setTyp(encoding.BlockString)
	codec.dataCol.Init()
	codec.dataCol.AppendStrings("aaa", "bbb", "ccc")
	codec.encode(t)
	require.Equal(t, uint8(encoding.BlockStringFull), codec.buf[0])
	codec.decode(t)
}

// ColVal.NilCount == ColVal.Len
func TestEncodeEmptyMode(t *testing.T) {
	codec := &colValCodec{}

	codec.setTyp(encoding.BlockInteger)
	codec.timeCol.AppendIntegers(1, 2, 3)
	codec.dataCol.AppendIntegerNulls(3)
	codec.encode(t)
	require.Equal(t, uint8(encoding.BlockIntegerEmpty), codec.buf[0])
	codec.decode(t)

	codec.setTyp(encoding.BlockString)
	codec.dataCol.Init()
	codec.dataCol.AppendStringNulls(3)
	codec.encode(t)
	require.Equal(t, uint8(encoding.BlockStringEmpty), codec.buf[0])
	codec.decode(t)
}

func TestUpgrade(t *testing.T) {
	codec := &colValCodec{}

	codec.setTyp(encoding.BlockInteger)
	codec.timeCol.AppendIntegers(1, 2, 3, 4)
	codec.dataCol.AppendIntegers(1, 2, 3)
	codec.dataCol.AppendIntegerNull()
	codec.encode(t)

	col := &record.ColVal{}
	ctx := NewReadContext(true)
	require.NoError(t, appendTimeColumnData(codec.buf, col, ctx, false))
	require.Equal(t, 1, len(col.Bitmap))
}

func TestEncodeColumn_error(t *testing.T) {
	codec := &colValCodec{}
	codec.setTyp(encoding.BlockInteger)
	codec.timeCol.AppendIntegers(1, 2, 3, 4)
	codec.dataCol.AppendIntegers(1, 2, 3)

	builder := NewColumnBuilder()
	builder.colMeta = &ColumnMeta{}
	builder.colMeta.growEntry()

	timeCols := []record.ColVal{codec.timeCol}

	ref := record.Field{
		Type: int(codec.typ),
		Name: "foo",
	}

	err := func() (err error) {
		defer func() {
			e := recover()
			if e != nil {
				err = fmt.Errorf("%v", e)
			}
		}()

		_, err = builder.encode(ref, nil, timeCols, 0)
		return err
	}()

	require.NotEmpty(t, err)
}

func TestDecodeColumnHeader_error(t *testing.T) {
	col := &record.ColVal{}
	col.AppendFloat(1)

	var invalidTyp uint8 = 100

	buf := EncodeColumnHeader(col, nil, invalidTyp)
	_, _, err := DecodeColumnHeader(col, buf, encoding.BlockInteger)

	exp := fmt.Sprintf("type(%d) in table not eq select type(%d)", invalidTyp, encoding.BlockInteger)
	require.EqualError(t, err, exp)
}

func preparePreAggBaseRec1() *record.Record {
	s := []record.Field{
		{Name: "bps", Type: influx.Field_Type_Int},
		{Name: "direction", Type: influx.Field_Type_String},
		{Name: "isgood", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}
	rec := record.NewRecord(s, false)
	for i := 0; i < rec.ColNums(); i++ {

		switch rec.Schema.Field(i).Name {
		case "bps":
			rec.Column(i).AppendIntegerNull()
			for j := 0; j < 10; j++ {
				rec.Column(i).AppendInteger(int64(j))
			}
		case "direction":
			rec.Column(i).AppendStringNull()
			for j := 0; j < 10; j++ {
				if j < 5 {
					rec.Column(i).AppendString("out")
				} else {
					rec.Column(i).AppendString("in")
				}
			}
		case "isgood":
			rec.Column(i).AppendBooleanNull()
			for j := 0; j < 10; j++ {
				if j < 5 {
					rec.Column(i).AppendBoolean(false)
				} else {
					rec.Column(i).AppendBoolean(true)
				}
			}
		case "time":
			for j := 0; j < 10; j++ {
				if j < 5 {
					rec.Column(i).AppendInteger(int64(1695461186000000000))
				} else {
					rec.Column(i).AppendInteger(int64(1695461486000000000))
				}
			}
		}
	}
	return rec
}

func TestFilterByField(t *testing.T) {
	rec := preparePreAggBaseRec1()
	filterRec := record.NewRecord(rec.Schema, false)
	filterOption := &BaseFilterOptions{
		CondFunctions: &binaryfilterfunc.ConditionImpl{},
		FieldsIdx:     []int{0, 1, 2},
		RedIdxMap:     map[int]struct{}{},
		FiltersMap:    make(map[string]*influxql.FilterMapValue),
	}
	filterBitMap := bitmap.NewFilterBitmap(filterOption.CondFunctions.NumFilter())
	condition := &influxql.BinaryExpr{Op: influxql.EQ, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result := FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	condition = &influxql.BinaryExpr{Op: influxql.NEQ, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result = FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	condition = &influxql.BinaryExpr{Op: influxql.LT, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result = FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	condition = &influxql.BinaryExpr{Op: influxql.LTE, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result = FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	condition = &influxql.BinaryExpr{Op: influxql.GT, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result = FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	condition = &influxql.BinaryExpr{Op: influxql.GTE, LHS: &influxql.VarRef{Val: "bps"}, RHS: &influxql.IntegerLiteral{Val: 5}}
	result = FilterByField(rec, filterRec, filterOption, condition, nil, nil, filterBitMap, nil)
	result.Reset()
	filterBitMap.Reset()
	filterRec.Reuse()
}
