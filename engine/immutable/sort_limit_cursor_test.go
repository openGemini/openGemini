/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	query2 "github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestSortLimitCursor(t *testing.T) {
	option := &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "time", Ascending: true,
			},
			&influxql.SortField{
				Name: record.SeqIDField, Ascending: true,
			},
		},
	}

	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "field5_bool", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	sortLimit := NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{nil}, []error{nil}), 0)
	defer sortLimit.Close()
	data, _, _ := sortLimit.Next()
	if data != nil {
		t.Errorf("get wrong data")
	}

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{nil}, []error{fmt.Errorf("wrong")}), 0)
	_, _, err := sortLimit.Next()
	if err == nil {
		t.Errorf("get wrong data")
	}

	r := record.NewRecord(s, false)
	for i := 0; i < 10; i++ {
		r.ColVals[0].AppendInteger(int64(i))
		r.ColVals[1].AppendInteger(int64(i))
		r.ColVals[2].AppendFloat(float64(i))
		r.ColVals[3].AppendString(strconv.Itoa(i))
		r.ColVals[4].AppendBoolean(true)
		r.AppendTime(int64(i))
	}

	r.ColVals[0].AppendInteger(1)

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{r}, []error{nil}), 0)
	data, _, _ = sortLimit.Next()
	if data.RowNums() != 3 || data.Time(0) != 7 {
		t.Errorf("get wrong data")
	}

	option = &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "time", Ascending: true,
			},
			&influxql.SortField{
				Name: record.SeqIDField, Ascending: true,
			},
		},
		Ascending: true,
	}

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{r}, []error{nil}), 0)
	data, _, _ = sortLimit.Next()
	if data.RowNums() != 3 || data.Time(0) != 0 {
		t.Errorf("get wrong data")
	}

	option = &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "field4_string", Ascending: true,
			},
		},
		Ascending: true,
	}

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{r}, []error{nil}), 0)
	data, _, _ = sortLimit.Next()
	if data.RowNums() != 3 || data.Time(0) != 0 {
		t.Errorf("get wrong data")
	}

	option = &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "field3_float", Ascending: true,
			},
		},
		Ascending: true,
	}

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{r}, []error{nil}), 0)
	data, _, _ = sortLimit.Next()
	if data.RowNums() != 3 || data.Time(0) != 0 {
		t.Errorf("get wrong data")
	}
}

func TestSortLimitCursorFiledNil(t *testing.T) {
	option := &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "time", Ascending: true,
			},
			&influxql.SortField{
				Name: record.SeqIDField, Ascending: true,
			},
		},
	}

	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "field5_bool", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	sortLimit := NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{nil}, []error{nil}), 0)
	defer sortLimit.Close()
	data, _, _ := sortLimit.Next()
	if data != nil {
		t.Errorf("get wrong data")
	}

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{nil}, []error{fmt.Errorf("wrong")}), 0)
	_, _, err := sortLimit.Next()
	if err == nil {
		t.Errorf("get wrong data")
	}

	r := record.NewRecord(s, false)
	r.ColVals[0].AppendInteger(0)
	r.ColVals[1].AppendInteger(0)
	r.ColVals[2].AppendFloat(1)
	r.ColVals[3].AppendString("test")
	r.ColVals[4].AppendBoolean(true)
	r.AppendTime(0)

	r.ColVals[0].AppendIntegerNull()
	r.ColVals[1].AppendInteger(0)
	r.ColVals[2].AppendFloat(1)
	r.ColVals[3].AppendString("test")
	r.ColVals[4].AppendBoolean(true)
	r.AppendTime(0)

	sortLimit = NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{r}, []error{nil}), 0)
	data, _, _ = sortLimit.Next()
	if data.RowNums() != 2 || data.Time(0) != 0 {
		t.Errorf("get wrong data")
	}
}

func TestSortLimitCursorFiledAbort(t *testing.T) {
	option := &query2.ProcessorOptions{
		QueryId: 1,
		Query:   "select * from mst",
		Limit:   3,
		SortFields: influxql.SortFields{
			&influxql.SortField{
				Name: "time", Ascending: true,
			},
			&influxql.SortField{
				Name: record.SeqIDField, Ascending: true,
			},
		},
	}

	s := []record.Field{
		{Name: record.SeqIDField, Type: influx.Field_Type_Int},
		{Name: "field2_int", Type: influx.Field_Type_Int},
		{Name: "field3_float", Type: influx.Field_Type_Float},
		{Name: "field4_string", Type: influx.Field_Type_String},
		{Name: "field5_bool", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	sortLimit := NewSortLimitCursor(option, s, NewMockCursor([]*record.Record{nil}, []error{nil}), 0)
	sortLimit.Close()
	data, _, _ := sortLimit.Next()
	if data != nil {
		t.Errorf("get wrong data")
	}
}

type MockCursor struct {
	index int
	data  []*record.Record
	err   []error
}

func NewMockCursor(data []*record.Record, err []error) *MockCursor {
	return &MockCursor{
		data: data,
		err:  err,
	}
}

func (t *MockCursor) Name() string {
	return "MockCursor"
}

func (t *MockCursor) StartSpan(span *tracing.Span) {
}

func (t *MockCursor) EndSpan() {
}

func (t *MockCursor) SinkPlan(plan hybridqp.QueryNode) {
}

func (t *MockCursor) GetSchema() record.Schemas {
	return nil
}

func (t *MockCursor) Next() (*record.Record, comm.SeriesInfoIntf, error) {
	var d *record.Record
	var err error
	if t.index < len(t.data) {
		d = t.data[t.index]
		err = t.err[t.index]
	}
	t.index += 1
	return d, nil, err
}

func (t *MockCursor) Close() error {
	return nil
}

func (t *MockCursor) SetOps(ops []*comm.CallOption) {

}

func (t *MockCursor) NextAggData() (*record.Record, *comm.FileInfo, error) {
	return nil, nil, nil
}

func (t *MockCursor) UpdateTime(time int64) {
}
