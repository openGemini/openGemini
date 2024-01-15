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

package coordinator_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/coordinator"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
)

func TestMetaExecutor(t *testing.T) {
	me := coordinator.NewMetaExecutor()
	me.MetaClient = &MockMetaClient{}
	defer me.Close()

	me.SetTimeOut(time.Second)
	err := me.EachDBNodes("db_not_exists", func(nodeID uint64, pts []uint32, hasErr *bool) error { return nil })
	assert.True(t, errno.Equal(err, errno.DatabaseNotFound))

	err = me.EachDBNodes("dbpt_not_exists", func(nodeID uint64, pts []uint32, hasErr *bool) error { return nil })
	assert.True(t, errno.Equal(err, errno.DatabaseNotFound))

	err = me.EachDBNodes("db0", func(nodeID uint64, pts []uint32, hasErr *bool) error { return nil })
	assert.NoError(t, err)

}

type MockMetaClient struct {
	metaclient.Client
}

func (c *MockMetaClient) GetNodePtsMap(database string) (map[uint64][]uint32, error) {
	if database == "dbpt_not_exists" {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}
	return nil, nil
}

func (c *MockMetaClient) Database(name string) (*meta2.DatabaseInfo, error) {
	if name == "db_not_exists" {
		return nil, errno.NewError(errno.DatabaseNotFound, name)
	}
	return &meta2.DatabaseInfo{}, nil
}

func (c *MockMetaClient) DBPtView(database string) (meta2.DBPtInfos, error) {
	if database == "dbpt_not_exists" {
		return nil, errno.NewError(errno.DatabaseNotFound, database)
	}
	ret := meta2.DBPtInfos{}
	ret = append(ret, meta2.PtInfo{
		Owner:  meta2.PtOwner{NodeID: 1},
		Status: 0,
		PtId:   1,
	})

	return ret, nil
}

func (c *MockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return []meta2.DataNode{
		{NodeInfo: meta2.NodeInfo{
			ID:      1,
			Host:    "",
			RPCAddr: "",
			TCPHost: "",
			Status:  0,
		}},
		{NodeInfo: meta2.NodeInfo{
			ID:      2,
			Host:    "",
			RPCAddr: "",
			TCPHost: "",
			Status:  0,
		}},
	}, nil
}
