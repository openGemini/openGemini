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

package handler

import (
	"fmt"
	"path"
	"sort"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-store/storage"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/netstorage"
	internal "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dataPath = "/tmp/test_data"

type handlerItem struct {
	typ uint8
	msg codec.BinaryCodec
}

type mockCodec struct {
}

func (r *mockCodec) MarshalBinary() ([]byte, error) {
	return []byte{1}, nil
}

func (r *mockCodec) UnmarshalBinary(buf []byte) error {
	return nil
}

func TestBaseHandler_SetMessage(t *testing.T) {
	var items = []handlerItem{
		{
			typ: netstorage.SeriesKeysRequestMessage,
			msg: &netstorage.SeriesKeysRequest{},
		},
		{
			typ: netstorage.SeriesExactCardinalityRequestMessage,
			msg: &netstorage.SeriesExactCardinalityRequest{},
		},
		{
			typ: netstorage.SeriesCardinalityRequestMessage,
			msg: &netstorage.SeriesCardinalityRequest{},
		},
		{
			typ: netstorage.ShowTagValuesRequestMessage,
			msg: &netstorage.ShowTagValuesRequest{},
		},
		{
			typ: netstorage.ShowTagValuesCardinalityRequestMessage,
			msg: &netstorage.ShowTagValuesCardinalityRequest{},
		},
		{
			typ: netstorage.GetShardSplitPointsRequestMessage,
			msg: &netstorage.GetShardSplitPointsRequest{},
		},
		{
			typ: netstorage.DeleteRequestMessage,
			msg: &netstorage.DeleteRequest{},
		},
		{
			typ: netstorage.CreateDataBaseRequestMessage,
			msg: &netstorage.CreateDataBaseRequest{},
		},
		{
			typ: netstorage.ShowTagKeysRequestMessage,
			msg: &netstorage.ShowTagKeysRequest{},
		},
	}

	for _, item := range items {
		h := newHandler(item.typ)
		require.NoError(t, h.SetMessage(item.msg))
		require.NotNil(t, h.SetMessage(&mockCodec{}))
	}
}

func TestCreateDataBase_Process(t *testing.T) {
	db := path.Join(dataPath, "db0")
	pt := uint32(1)
	rp := "rp0"

	h := newHandler(netstorage.CreateDataBaseRequestMessage)
	if err := h.SetMessage(&netstorage.CreateDataBaseRequest{
		CreateDataBaseRequest: internal.CreateDataBaseRequest{
			Db: &db,
			Pt: &pt,
			Rp: &rp,
		},
	}); err != nil {
		t.Fatal(err)
	}

	h.SetStore(&storage.Storage{})

	rsp, _ := h.Process()
	response, ok := rsp.(*netstorage.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NotNil(t, response.GetErr())
	response.Err = nil

	fileops.MkdirAll(dataPath, 0750)
	defer fileops.RemoveAll(dataPath)
	rsp, _ = h.Process()
	response, ok = rsp.(*netstorage.CreateDataBaseResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.Nil(t, response.Error())
}

var clientIDs = []uint64{1000, 2000, 3000}
var mockQueriesNum = 10

type mockQuery struct {
	id   int
	info *netstorage.QueryExeInfo
}

func (m *mockQuery) Abort() {}

func (m *mockQuery) GetQueryExeInfo() *netstorage.QueryExeInfo {
	return m.info
}

func generateMockQueryExeInfos(clientID uint64, n int) []mockQuery {
	res := make([]mockQuery, mockQueriesNum)
	for i := 0; i < n; i++ {
		q := mockQuery{id: i, info: &netstorage.QueryExeInfo{
			QueryID:   clientID + uint64(i),
			Stmt:      fmt.Sprintf("select * from mst%d\n", i),
			Database:  fmt.Sprintf("db%d", i),
			BeginTime: int64(i * 10000000),
			RunState:  netstorage.Running,
		}}
		res[i] = q
	}
	return res
}

func TestShowQueries_Process(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	except := make([]*netstorage.QueryExeInfo, 0)

	for _, cid := range clientIDs {
		// generate mock infos for all clients
		queries := generateMockQueryExeInfos(cid, mockQueriesNum)
		for i, mQuery := range queries {
			qm := query.NewManager(cid)

			qid := mQuery.GetQueryExeInfo().QueryID

			qm.Add(qid, &queries[i])
			except = append(except, mQuery.GetQueryExeInfo())
		}
	}

	// Simulate show queries, get the queryInfos
	h := newHandler(netstorage.ShowQueriesRequestMessage)
	if err := h.SetMessage(&netstorage.ShowQueriesRequest{}); err != nil {
		t.Fatal(err)
	}
	h.SetStore(&storage.Storage{})

	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}

	response, ok := rsp.(*netstorage.ShowQueriesResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}

	res := response.QueryExeInfos
	// sort res and except to assert
	sort.Slice(res, func(i, j int) bool {
		return res[i].QueryID > res[j].QueryID
	})
	sort.Slice(except, func(i, j int) bool {
		return except[i].QueryID > except[j].QueryID
	})

	for i := range except {
		assert.Equal(t, except[i].QueryID, res[i].QueryID)
		assert.Equal(t, except[i].Stmt, res[i].Stmt)
		assert.Equal(t, except[i].Database, res[i].Database)
	}

	assert.Equal(t, len(clientIDs)*mockQueriesNum, len(res))
}

func TestKillQuery_Process(t *testing.T) {
	abortedQID := clientIDs[0] + 1

	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	h := newHandler(netstorage.KillQueryRequestMessage)
	req := netstorage.KillQueryRequest{}
	req.QueryID = proto.Uint64(abortedQID)
	if err := h.SetMessage(&req); err != nil {
		t.Fatal(err)
	}
	h.SetStore(&storage.Storage{})

	for _, cid := range clientIDs {
		// generate mock infos for all clients
		queries := generateMockQueryExeInfos(cid, mockQueriesNum)
		for i, mQuery := range queries {
			qm := query.NewManager(cid)

			qid := mQuery.GetQueryExeInfo().QueryID

			qm.Add(qid, &queries[i])
		}
	}

	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}
	response, ok := rsp.(*netstorage.KillQueryResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NoError(t, response.Error())
	assert.Equal(t, true, query.NewManager(clientIDs[0]).Aborted(abortedQID))
}

func TestShowTagKeys_Process(t *testing.T) {
	db := path.Join(dataPath, "db0")
	condition := "tag1=tagkey"

	h := newHandler(netstorage.ShowTagKeysRequestMessage)
	req := netstorage.ShowTagKeysRequest{}
	req.Db = &db
	req.Condition = &condition
	req.PtIDs = []uint32{1}
	req.Measurements = []string{"mst"}
	if err := h.SetMessage(&req); err != nil {
		t.Fatal(err)
	}
	st := storage.Storage{}
	st.SetEngine(&MockEngine{})
	h.SetStore(&st)
	rsp, err := h.Process()
	if err != nil {
		t.Fatal(err)
	}
	response, ok := rsp.(*netstorage.ShowTagKeysResponse)
	if !ok {
		t.Fatal("response type is invalid")
	}
	assert.NoError(t, response.Error())
}
