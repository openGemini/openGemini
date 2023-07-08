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

package query

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	netdata "github.com/openGemini/openGemini/lib/netstorage/data"
	"github.com/stretchr/testify/assert"
)

var clientIDs = []uint64{1000, 2000, 3000}
var mockQueriesNum = 10

func TestManager_Abort(t *testing.T) {
	var seq uint64 = 10
	query := &mockQuery{id: 10}

	qm := NewManager(clientIDs[0])
	qm.Add(seq, query)

	assert.Equal(t, false, qm.Aborted(seq))

	qm.SetAbortedExpire(time.Second * 2)
	qm.Abort(seq)

	assert.Equal(t, true, qm.Aborted(seq))

	time.Sleep(time.Second)
	qm.cleanAbort()
	assert.Equal(t, 1, len(qm.aborted), "clean abort failed")

	time.Sleep(time.Second)
	qm.cleanAbort()
	assert.Equal(t, 0, len(qm.aborted), "clean abort failed")
}

func TestManager_Query(t *testing.T) {
	var seq uint64 = 10
	query := &mockQuery{id: 10}

	qm := NewManager(clientIDs[0])
	qm.Add(seq, query)
	assert.Equal(t, query, qm.Get(seq))

	qm.Finish(seq)

	var nilQuery IQuery
	assert.Equal(t, nilQuery, qm.Get(seq))
}

type mockQuery struct {
	id   int
	info *netdata.QueryExeInfo
}

func (m *mockQuery) Abort() {

}

func (m *mockQuery) GetQueryExeInfo() *netdata.QueryExeInfo {
	return m.info
}

func GenerateMockQueries(clientID uint64, n int) []mockQuery {
	res := make([]mockQuery, mockQueriesNum)
	for i := 0; i < n; i++ {
		q := mockQuery{id: i, info: &netdata.QueryExeInfo{
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

func TestManager_GetAll(t *testing.T) {
	// generate mock infos for one client
	queries := GenerateMockQueries(clientIDs[0], mockQueriesNum)
	except := make([]*netdata.QueryExeInfo, 0)

	for i := range queries {
		NewManager(clientIDs[0]).Add(uint64(i), &queries[i])
		except = append(except, queries[i].GetQueryExeInfo())
	}

	res := NewManager(clientIDs[0]).GetAll()

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
	assert.Equal(t, mockQueriesNum, len(res))
}

func TestVisitManagers(t *testing.T) {
	count := 0
	testFn := func(manager *Manager) {
		for range manager.items {
			// for every item in every qm, count++
			count++
		}
	}
	// generate len(clientIDs) * mockQueriesNum queryInfos
	for _, cID := range clientIDs {
		queries := GenerateMockQueries(cID, mockQueriesNum)
		for i := range queries {
			NewManager(cID).Add(uint64(i), &queries[i])
		}
	}

	VisitManagers(testFn)
	assert.Equal(t, len(clientIDs)*mockQueriesNum, count)
}
