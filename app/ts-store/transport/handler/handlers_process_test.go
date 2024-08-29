// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	internal "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestProcessDDL(t *testing.T) {
	condition := "tk1=tv1"
	errStr := processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return errno.NewError(errno.PtNotFound)
	})

	err := netstorage.NormalizeError(errStr)
	assert.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return fmt.Errorf("shard not found")
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err.Error() == "shard not found")

	errStr = processDDL(&condition, func(expr influxql.Expr, tr influxql.TimeRange) error {
		return nil
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err == nil)
}

func TestProcessShowTagValues(t *testing.T) {

	db := path.Join(dataPath, "db0")
	pts := []uint32{1}

	returnErrDB := "test_return_error"

	for _, testcase := range []struct {
		Name                 string
		ShowTagValuesRequest internal.ShowTagValuesRequest
		Want                 *errno.Error
	}{
		{
			Name: "show tag values with order by 1",
			ShowTagValuesRequest: internal.ShowTagValuesRequest{
				Db:       &db,
				PtIDs:    pts,
				Disorder: proto.Bool(false),
			},
			Want: nil,
		},
		{
			Name: "show tag values with order by 2",
			ShowTagValuesRequest: internal.ShowTagValuesRequest{
				Db:    &db,
				PtIDs: pts,
			},
			Want: nil,
		},
		{
			Name: "show tag values without order by 1",
			ShowTagValuesRequest: internal.ShowTagValuesRequest{
				Db:       &db,
				PtIDs:    pts,
				Disorder: proto.Bool(true),
			},
			Want: nil,
		},
		{
			Name: "show tag values without order by ref DBPT fail",
			ShowTagValuesRequest: internal.ShowTagValuesRequest{
				Db:       &returnErrDB,
				PtIDs:    pts,
				Disorder: proto.Bool(true),
			},
			Want: errno.NewError(errno.DBPTClosed, "pt", 1),
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			h := newHandler(netstorage.ShowTagValuesRequestMessage)
			if err := h.SetMessage(&netstorage.ShowTagValuesRequest{
				ShowTagValuesRequest: testcase.ShowTagValuesRequest,
			}); err != nil {
				t.Fatal(err)
			}

			s := &storage.Storage{}
			s.SetEngine(&MockEngine{})
			h.SetStore(s)
			rsp, _ := h.Process()
			response, ok := rsp.(*netstorage.ShowTagValuesResponse)
			if !ok {
				t.Fatal("response type is invalid")
			}

			if testcase.Want == nil {
				assert.Equal(t, nil, netstorage.NormalizeError(response.Err))
			} else {
				assert.Equal(t, testcase.Want.Errno(), netstorage.NormalizeError(response.Err).(*errno.Error).Errno())
			}

			response.Err = nil
		})
	}
}

type MockEngine struct {
	netstorage.Engine
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

func (e *MockEngine) CreateShowTagValuesPlan(db string, ptIDs []uint32, tr *influxql.TimeRange) netstorage.ShowTagValuesPlan {
	plan := &MockShowTagValuesPlan{
		ExecuteFn: func(tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (netstorage.TablesTagSets, error) {
			return netstorage.TablesTagSets{}, nil
		},
		StopFn: func() {},
	}
	return plan
}

func (e *MockEngine) DbPTRef(db string, ptId uint32) error {
	fmt.Printf(db)
	if db == "test_return_error" {
		return errno.NewError(errno.DBPTClosed, "pt", 1)
	}
	return nil
}

func (e *MockEngine) DbPTUnref(db string, ptId uint32) {}

type MockShowTagValuesPlan struct {
	ExecuteFn func(tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (netstorage.TablesTagSets, error)
	StopFn    func()
}

func (p *MockShowTagValuesPlan) Execute(tagKeys map[string][][]byte, condition influxql.Expr, tr util.TimeRange, limit int) (netstorage.TablesTagSets, error) {
	return p.ExecuteFn(tagKeys, condition, tr, limit)
}

func (p *MockShowTagValuesPlan) Stop() {
	p.StopFn()
}

func TestProcessGetShardSplitPoints(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)

	h := newHandler(netstorage.GetShardSplitPointsRequestMessage)
	if err := h.SetMessage(&netstorage.GetShardSplitPointsRequest{
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
	response, ok := rsp.(*netstorage.GetShardSplitPointsResponse)
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

	h := newHandler(netstorage.SeriesKeysRequestMessage)
	if err := h.SetMessage(&netstorage.SeriesKeysRequest{
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
	response, ok := rsp.(*netstorage.SeriesKeysResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil
}

func TestProcessRaftMessages(t *testing.T) {
	h := newHandler(netstorage.RaftMessagesRequestMessage)
	if err := h.SetMessage(&netstorage.RaftMessagesRequest{
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
	response, ok := rsp.(*netstorage.RaftMessagesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Empty(t, response.GetErrMsg())
}
