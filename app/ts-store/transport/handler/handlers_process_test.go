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

package handler

import (
	"fmt"
	"path"
	"testing"

	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/msgservice"
	data "github.com/openGemini/openGemini/lib/msgservice/data"
	internal "github.com/openGemini/openGemini/lib/msgservice/data"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestProcessDDL(t *testing.T) {
	condition := "tk1=tv1"
	errStr := processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return errno.NewError(errno.PtNotFound)
	})

	err := msgservice.NormalizeError(errStr)
	assert.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return fmt.Errorf("shard not found")
	})

	err = msgservice.NormalizeError(errStr)
	assert.Equal(t, true, err.Error() == "shard not found")

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return nil
	})

	err = msgservice.NormalizeError(errStr)
	assert.Equal(t, true, err == nil)
}

func TestProcessShowTagValues(t *testing.T) {

	db := path.Join(dataPath, "db0")
	pts := []uint32{1}

	returnErrDB := "test_return_error"

	for _, testcase := range []struct {
		Name                 string
		ShowTagValuesRequest *internal.ShowTagValuesRequest
		Want                 *errno.Error
	}{
		{
			Name: "show tag values with order by 1",
			ShowTagValuesRequest: &internal.ShowTagValuesRequest{
				Db:    &db,
				PtIDs: pts,
				Exact: proto.Bool(false),
			},
			Want: nil,
		},
		{
			Name: "show tag values with order by 2",
			ShowTagValuesRequest: &internal.ShowTagValuesRequest{
				Db:    &db,
				PtIDs: pts,
			},
			Want: nil,
		},
		{
			Name: "show tag values without order by 1",
			ShowTagValuesRequest: &internal.ShowTagValuesRequest{
				Db:    &db,
				PtIDs: pts,
				Exact: proto.Bool(true),
			},
			Want: nil,
		},
		{
			Name: "show tag values without order by ref DBPT fail",
			ShowTagValuesRequest: &internal.ShowTagValuesRequest{
				Db:    &returnErrDB,
				PtIDs: pts,
				Exact: proto.Bool(true),
			},
			Want: errno.NewError(errno.DBPTClosed, "pt", 1),
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			h := NewHandler(msgservice.ShowTagValuesRequestMessage)
			msg := &msgservice.ShowTagValuesRequest{}
			msg.Db = testcase.ShowTagValuesRequest.Db
			msg.PtIDs = testcase.ShowTagValuesRequest.PtIDs
			msg.Exact = testcase.ShowTagValuesRequest.Exact
			require.NoError(t, h.SetMessage(msg))

			s := &storage.Storage{}
			s.SetEngine(&MockEngine{})
			h.SetStore(s)
			rsp, _ := h.Process()
			response, ok := rsp.(*msgservice.ShowTagValuesResponse)
			if !ok {
				t.Fatal("response type is invalid")
			}

			if testcase.Want == nil {
				assert.Equal(t, nil, msgservice.NormalizeError(response.Err))
			} else {
				assert.Equal(t, testcase.Want.Errno(), msgservice.NormalizeError(response.Err).(*errno.Error).Errno())
			}

			response.Err = nil
		})
	}
}

type MockEngine struct {
	engine.Engine
	itrs       []record.Iterator
	isColStore bool
}

func (e *MockEngine) TagKeys(_ string, _ []uint32, _ [][]byte, _ influxql.Expr, _ influxql.TimeRange) ([]string, error) {
	return []string{"mst,tag1,tag2,tag3", "mst2,tag1,tag2,tag3"}, nil
}

func (e *MockEngine) SeriesKeys(_ string, _ []uint32, _ [][]byte, _ influxql.Expr, _ influxql.TimeRange) ([]string, error) {
	return nil, nil
}

func (e *MockEngine) GetShardSplitPoints(_ string, _ uint32, _ uint64, _ []int64) ([]string, error) {
	return nil, nil
}

func (e *MockEngine) SendRaftMessage(database string, ptId uint64, msg raftpb.Message) error {
	return nil
}

func (e *MockEngine) CreateDDLBasePlans(planType hybridqp.DDLType, db string, ptIDs []uint32, tr *influxql.TimeRange) engine.DDLBasePlans {
	plan := &MockDDLPlans{
		planType: planType,
		ExecuteFn: func(mstKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (interface{}, error) {
			if planType == hybridqp.ShowTagValues {
				return influxql.TablesTagSets{}, nil
			}
			return []string{}, nil
		},
		StopFn:    func() {},
		AddPlanFn: func(interface{}) {},
	}
	return plan
}

func (e *MockEngine) DbPTRef(db string, ptId uint32, read bool) error {
	if db == "test_return_error" {
		return errno.NewError(errno.DBPTClosed, "pt", 1)
	}
	return nil
}

func (e *MockEngine) DbPTUnref(db string, ptId uint32, read bool) {}

func (e *MockEngine) GetLastIndex(db string, ptId uint32) (uint64, error) {
	if ptId == 1 {
		return 1, nil
	}
	return 0, fmt.Errorf("get err")
}

func (e *MockEngine) CreateConsumeIterator(ident util.MeasurementIdent, pts []uint32, opt *query.ProcessorOptions) ([]record.Iterator, func()) {
	return e.itrs, func() {}
}

func (e *MockEngine) SetIterator(sidCnt int, schema record.Schemas) {
	e.itrs = []record.Iterator{NewMockIterator(sidCnt, schema)}
}

func (e *MockEngine) IsColStore(_, _, _ string) bool {
	return e.isColStore
}

func (e *MockEngine) GetShardIDs(_ string, _ uint32, _ *influxql.TimeRange) ([]uint64, error) {
	return nil, nil
}

func (e *MockEngine) RowCount(_ string, _ uint32, _ []uint64, _ hybridqp.Catalog, _ bool) (int64, error) {
	return 1, nil
}

func (e *MockEngine) GetColStorePK(_, _, _ string) (record.Schemas, bool) {
	return nil, e.isColStore
}

type MockIterator struct {
	init   bool
	sidCnt int
	schema record.Schemas
}

func NewMockIterator(sidCnt int, schema record.Schemas) *MockIterator {
	return &MockIterator{sidCnt: sidCnt, schema: schema}
}

func (m *MockIterator) Next() (*record.ConsumeRecord, error) {
	if !m.init {
		m.init = true
		return &record.ConsumeRecord{
			Rec: buildRecord(m.schema),
		}, nil
	}
	return nil, nil
}

func (m *MockIterator) Release() {
}

func (m *MockIterator) SidCnt() int {
	return m.sidCnt
}

func buildRecord(schema record.Schemas) *record.Record {
	rec := record.NewRecord(schema, false)
	for i, field := range rec.Schema {
		switch field.Type {
		case influx.Field_Type_Int:
			rec.ColVals[i].AppendIntegers(1, 2, 3, 4)
		case influx.Field_Type_Float:
			rec.ColVals[i].AppendFloats(1.0, 2.0, 3.0, 4.0)
		case influx.Field_Type_String:
			rec.ColVals[i].AppendStrings("a", "b", "c", "d")
		case influx.Field_Type_Boolean:
			rec.ColVals[i].AppendBooleans(true, false, true, false)
		}
	}
	return rec
}

type MockDDLPlans struct {
	ExecuteFn func(tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (interface{}, error)
	StopFn    func()
	AddPlanFn func(interface{})
	planType  hybridqp.DDLType
}

func (p *MockDDLPlans) Execute(mstKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (interface{}, error) {
	return p.ExecuteFn(mstKeys, condition, tr, limit)
}

func (p *MockDDLPlans) Stop() {
	p.StopFn()
}

func (p *MockDDLPlans) AddPlan(plan interface{}) {
	p.AddPlanFn(plan)
}

func TestProcessGetShardSplitPoints(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)

	h := NewHandler(msgservice.GetShardSplitPointsRequestMessage)
	if err := h.SetMessage(&msgservice.GetShardSplitPointsRequest{
		GetShardSplitPointsRequest: internal.GetShardSplitPointsRequest{
			DB:   &db,
			PtID: &pt,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.GetShardSplitPointsResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}

func TestProcessSeriesKeys(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pts := []uint32{1}
	ms := []string{"cpu"}

	h := NewHandler(msgservice.SeriesKeysRequestMessage)
	if err := h.SetMessage(&msgservice.SeriesKeysRequest{
		SeriesKeysRequest: internal.SeriesKeysRequest{
			Db:           &db,
			PtIDs:        pts,
			Measurements: ms,
		},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.SeriesKeysResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}

func TestProcessRaftMessages(t *testing.T) {
	h := NewHandler(msgservice.RaftMessagesRequestMessage)
	if err := h.SetMessage(&msgservice.RaftMessagesRequest{
		Database:    "test",
		PtId:        1,
		RaftMessage: raftpb.Message{},
	}); err != nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.RaftMessagesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Empty(t, response.GetErrMsg())
}

func TestProcessShowSeries(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pts := []uint32{1}

	returnErrDB := "test_return_error"

	for _, testcase := range []struct {
		Name              string
		ShowSeriesRequest *internal.SeriesKeysRequest
		Want              *errno.Error
	}{
		{
			Name: "show series",
			ShowSeriesRequest: &internal.SeriesKeysRequest{
				Db:    &db,
				PtIDs: pts,
				Exact: proto.Bool(false),
			},
			Want: nil,
		},
		{
			Name: "show /hint/ series",
			ShowSeriesRequest: &internal.SeriesKeysRequest{
				Db:    &db,
				PtIDs: pts,
				Exact: proto.Bool(true),
			},
			Want: nil,
		},
		{
			Name: "show series DBPT fail",
			ShowSeriesRequest: &internal.SeriesKeysRequest{
				Db:    &returnErrDB,
				PtIDs: pts,
				Exact: proto.Bool(false),
			},
			Want: errno.NewError(errno.DBPTClosed, "pt", 1),
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			h := NewHandler(msgservice.SeriesKeysRequestMessage)
			msg := &msgservice.SeriesKeysRequest{}
			msg.Db = testcase.ShowSeriesRequest.Db
			msg.PtIDs = testcase.ShowSeriesRequest.PtIDs
			msg.Exact = testcase.ShowSeriesRequest.Exact
			require.NoError(t, h.SetMessage(msg))

			s := &storage.Storage{}
			s.SetEngine(&MockEngine{})
			h.SetStore(s)
			rsp, _ := h.Process()
			response, ok := rsp.(*msgservice.SeriesKeysResponse)
			if !ok {
				t.Fatal("response type is invalid")
			}
			assert.Equal(t, nil, msgservice.NormalizeError(response.Err))
			response.Err = nil
		})
	}
}

func TestProcessShowLastIndex(t *testing.T) {
	h := NewHandler(msgservice.ShowLastIndexRequestMessage)
	req := &msgservice.ShowLastIndexRequest{}
	req.Database = proto.String("db0")
	req.PtIds = []uint32{1}
	if err := h.SetMessage(req); err != nil {
		t.Fatal(err)
	}
	req1 := &msgservice.SeriesCardinalityRequest{}
	if err := h.SetMessage(req1); err == nil {
		t.Fatal(err)
	}

	s := &storage.Storage{}
	s.SetEngine(&MockEngine{})
	h.SetStore(s)

	rsp, _ := h.Process()
	response, ok := rsp.(*msgservice.ShowLastIndexResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Empty(t, response.Error())

	req.PtIds = []uint32{2}
	rsp, _ = h.Process()
	response, ok = rsp.(*msgservice.ShowLastIndexResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Equal(t, response.Error().Error(), "get err")
}

func TestConvertOrCondIntoExpr(t *testing.T) {
	// Test case 1: Empty condition list
	testEmptyCondition(t)

	// Test case 2: Single condition
	testSingleCondition(t)

	// Test case 3: Multiple conditions
	testMultipleConditions(t)
}

func testEmptyCondition(t *testing.T) {
	// Test when condition list is empty
	expr, err := ConvertOrCondIntoExpr(nil)
	require.NoError(t, err)
	if expr != nil {
		t.Errorf("Expected nil expression for empty condition list, got %v", expr)
	}
	cond := []*data.EqCond{nil}
	_, err = ConvertOrCondIntoExpr(cond)
	require.NoError(t, err)
}

func testSingleCondition(t *testing.T) {
	// Test with a single condition
	cond := []*data.EqCond{{
		Key: proto.String("key"),
		Val: proto.String("value"),
		Typ: proto.Uint32(uint32(influxql.Tag)),
		Op:  proto.Int64(int64(influxql.EQ)),
	}}
	expr, err := ConvertOrCondIntoExpr(cond)
	require.NoError(t, err)
	if expr == nil {
		t.Error("Expected non-nil expression for single condition, got nil")
	}
	// Add assertions to verify the structure of the expression
	require.Equal(t, expr.String(), "\"key\"::tag = 'value'")
}

func testMultipleConditions(t *testing.T) {
	// Test with multiple conditions
	cond := []*data.EqCond{{
		Key: proto.String("key1"),
		Val: proto.String("value1"),
		Typ: proto.Uint32(uint32(influxql.Tag)),
		Op:  proto.Int64(int64(influxql.EQ)),
	}, {
		Key: proto.String("key2"),
		Val: proto.String("value1"),
		Typ: proto.Uint32(uint32(influxql.String)),
		Op:  proto.Int64(int64(influxql.EQ)),
	}, {
		Key: proto.String("key3"),
		Val: proto.String("10.1"),
		Typ: proto.Uint32(uint32(influxql.Float)),
		Op:  proto.Int64(int64(influxql.EQ)),
	}, {
		Key: proto.String("key4"),
		Val: proto.String("10"),
		Typ: proto.Uint32(uint32(influxql.Integer)),
		Op:  proto.Int64(int64(influxql.EQ)),
	}, {
		Key: proto.String("key5"),
		Val: proto.String("true"),
		Typ: proto.Uint32(uint32(influxql.Boolean)),
		Op:  proto.Int64(int64(influxql.EQ)),
	},
	}
	expr, err := ConvertOrCondIntoExpr(cond)
	require.NoError(t, err)
	if expr == nil {
		t.Error("Expected non-nil expression for multiple conditions, got nil")
	}
	// Add assertions to verify the structure of the expression
	require.Equal(t, "key5::boolean = true OR key4::integer = 10 OR key3::float = 10.1 OR key2::string = 'value1' OR key1::tag = 'value1'", expr.String())
}

func TestSmarterQuery_IsOnlyPKFilter(t *testing.T) {
	type fields struct {
		req *msgservice.SmarterQueryRequest
	}
	type args struct {
		pk record.Schemas
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "no pk",
			fields: fields{
				req: &msgservice.SmarterQueryRequest{},
			},
			args: args{
				pk: record.Schemas{},
			},
			want: false,
		},
		{
			name: "field",
			fields: fields{
				req: &msgservice.SmarterQueryRequest{
					SmarterQueryRequest: internal.SmarterQueryRequest{
						Condition: []*data.EqCond{{
							Key: proto.String("key1"),
							Val: proto.String("value"),
							Typ: proto.Uint32(uint32(influxql.Tag)),
							Op:  proto.Int64(int64(influxql.EQ)),
						}},
					},
				},
			},
			args: args{
				pk: record.Schemas{{Type: influx.Field_Type_Tag, Name: "key"}},
			},
			want: false,
		},
		{
			name: "only pk",
			fields: fields{
				req: &msgservice.SmarterQueryRequest{
					SmarterQueryRequest: internal.SmarterQueryRequest{
						Condition: []*data.EqCond{{
							Key: proto.String("key"),
							Val: proto.String("value"),
							Typ: proto.Uint32(uint32(influxql.Tag)),
							Op:  proto.Int64(int64(influxql.EQ)),
						}},
					},
				},
			},
			args: args{
				pk: record.Schemas{{Type: influx.Field_Type_Tag, Name: "key"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &SmarterQuery{
				req: tt.fields.req,
			}
			assert.Equalf(t, tt.want, h.IsOnlyPKFilter(tt.args.pk), "IsOnlyPKFilter(%v)", tt.args.pk)
		})
	}
}
