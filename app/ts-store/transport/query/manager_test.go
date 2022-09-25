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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var clientID uint64 = 1001

func TestManager_Abort(t *testing.T) {
	var seq uint64 = 10
	query := &mockQuery{id: 10}

	qm := NewManager(clientID)
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

	qm := NewManager(clientID)
	qm.Add(seq, query)
	assert.Equal(t, query, qm.Get(seq))

	qm.Finish(seq)

	var nilQuery IQuery
	assert.Equal(t, nilQuery, qm.Get(seq))
}

type mockQuery struct {
	id int
}

func (m *mockQuery) Abort() {

}
