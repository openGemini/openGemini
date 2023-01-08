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
