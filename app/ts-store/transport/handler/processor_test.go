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
	"sort"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/app/ts-store/transport/query"
	"github.com/openGemini/openGemini/engine/executor/spdy"
	"github.com/openGemini/openGemini/lib/netstorage"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/assert"
)

var clientIDs = []uint64{1000, 2000, 3000}
var mockQueriesNum = 10

type mockQuery struct {
	id     int
	status *netdata.QueryExeInfo
}

func (m *mockQuery) Abort() {

}

func (m *mockQuery) GetQueryExeInfo() *netdata.QueryExeInfo {
	return m.status
}

func generateMockQueryExeInfos(clientID uint64, n int) []mockQuery {
	res := make([]mockQuery, mockQueriesNum)
	for i := 0; i < n; i++ {
		q := mockQuery{id: i, status: &netdata.QueryExeInfo{
			QueryID:  proto.Uint64(clientID + uint64(i)),
			Stmt:     proto.String(fmt.Sprintf("select * from mst%d\n", i)),
			Database: proto.String(fmt.Sprintf("db%d", i)),
			Duration: proto.Int64(int64(i * 10000000)),
			IsKilled: proto.Bool(true),
		}}
		res[i] = q
	}
	return res
}

func TestShowQueriesProcessor_Handle(t *testing.T) {
	resp := &EmptyResponser{}
	resp.session = spdy.NewMultiplexedSession(spdy.DefaultConfiguration(), nil, 0)

	except := make([]*netdata.QueryExeInfo, 0)

	for _, cid := range clientIDs {
		// generate mock infos for all clients
		queries := generateMockQueryExeInfos(cid, mockQueriesNum)
		for i, mQuery := range queries {
			qm := query.NewManager(cid)

			qid := mQuery.GetQueryExeInfo().GetQueryID()

			qm.Add(qid, &queries[i])
			except = append(except, mQuery.GetQueryExeInfo())
		}
	}

	// Simulate show queries, get the queryInfos
	p := NewShowQueriesProcessor()
	err := p.Handle(resp, &netstorage.ShowQueriesRequest{})

	assert.NoError(t, err)

	res := resp.Data.(*netstorage.ShowQueriesResponse).QueryExeInfos

	// sort res and except to assert
	sort.Slice(res, func(i, j int) bool {
		return res[i].GetQueryID() > res[j].GetQueryID()
	})
	sort.Slice(except, func(i, j int) bool {
		return except[i].GetQueryID() > except[j].GetQueryID()
	})

	for i := range except {
		assert.Equal(t, except[i].GetQueryID(), res[i].GetQueryID())
		assert.Equal(t, except[i].GetStmt(), res[i].GetStmt())
		assert.Equal(t, except[i].GetDatabase(), res[i].GetDatabase())
	}

	assert.Equal(t, len(clientIDs)*mockQueriesNum, len(res))
}
