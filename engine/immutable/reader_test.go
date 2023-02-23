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
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
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
	path string
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

func (m MocTsspFile) MetaIndexAt(idx int) (*MetaIndex, error) {
	return nil, nil
}

func (m MocTsspFile) MetaIndex(id uint64, tr record.TimeRange) (int, *MetaIndex, error) {
	return 0, nil, nil
}

func (m MocTsspFile) ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int, dst *ChunkMeta, buffer *[]byte) (*ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) Read(id uint64, tr record.TimeRange, dst *record.Record) (*record.Record, error) {
	return nil, nil
}

func (m MocTsspFile) ReadAt(cm *ChunkMeta, segment int, dst *record.Record, decs *ReadContext) (*record.Record, error) {
	return nil, nil
}

func (m MocTsspFile) ChunkAt(index int) (*ChunkMeta, error) {
	return nil, nil
}

func (m MocTsspFile) ReadData(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	return nil, nil
}

func (m MocTsspFile) ReadChunkMetaData(metaIdx int, me *MetaIndex, dst []ChunkMeta) ([]ChunkMeta, error) {
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

func (m MocTsspFile) ContainsByTime(tr record.TimeRange) (bool, error) {
	return false, nil
}

func (m MocTsspFile) ContainsValue(id uint64, tr record.TimeRange) (bool, error) {
	return false, nil
}

func (m MocTsspFile) MinMaxTime() (int64, int64, error) {
	return 0, 0, nil
}

func (m MocTsspFile) Delete(ids []int64) error {
	return nil
}

func (m MocTsspFile) DeleteRange(ids []int64, min, max int64) error {
	return nil
}

func (m MocTsspFile) HasTombstones() bool {
	return false
}

func (m MocTsspFile) TombstoneFiles() []TombstoneFile {
	return nil
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
			coderCtx:  &CoderContext{},
			Ascending: false,
		}
		builder := ColumnBuilder{}
		builder.colMeta = &ColumnMeta{name: "foo", ty: uint8(typ), entries: make([]Segment, 1)}
		builder.coder = &CoderContext{}

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
