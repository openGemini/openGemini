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

package syscontrol

import (
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/sysconfig"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

type mockMetaClient struct {
	meta.MetaClient
}

func (mockMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return []meta2.DataNode{
		{
			NodeInfo: meta2.NodeInfo{
				ID:   0,
				Host: "127.0.0.1:8400",
			},
		},
		{
			NodeInfo: meta2.NodeInfo{
				ID:   1,
				Host: "127.0.0.2:8400",
			},
		},
	}, nil
}

func (mockMetaClient) SendSysCtrlToMeta(mod string, param map[string]string) (map[string]string, error) {
	return nil, nil
}

type mockErrMetaClient struct {
	meta.MetaClient
}

func (mockErrMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, fmt.Errorf("no datanode")
}

type mockStorage struct {
	netstorage.Storage
}

func (mockStorage) SendSysCtrlOnNode(nodID uint64, req netstorage.SysCtrlRequest) (map[string]string, error) {
	return nil, nil
}

func TestProcessRequest_Readonly(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("readonly")
	req.SetParam(map[string]string{
		"switchon": "true",
		"allnodes": "y",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "\n\tsuccess")
	sb.Reset()

	req.SetParam(map[string]string{
		"allnodes": "y",
	})
	require.Error(t, ProcessRequest(req, &sb))
	sb.Reset()
}

func TestProcessRequest_LogRows(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("log_rows")

	req.SetParam(map[string]string{
		"switchon": "true",
	})
	var sb strings.Builder
	require.EqualError(t, ProcessRequest(req, &sb), "rules can not be empty")
	sb.Reset()

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst, tk1=tv1",
	})
	require.EqualError(t, ProcessRequest(req, &sb), "rules can not contains space")
	sb.Reset()

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst,tk1=tv1",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()

	req.SetParam(map[string]string{
		"switchon": "false",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()
}

func TestProcessRequest_ForceBroadcastQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("force_broadcast_query")
	req.SetParam(map[string]string{
		"enabled": "1",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "\n\tsuccess")
	assert.Equal(t, sysconfig.EnableForceBroadcastQuery, int64(1))
	sb.Reset()

	req.SetParam(map[string]string{
		"enabled": "0",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	assert.Equal(t, sysconfig.EnableForceBroadcastQuery, int64(0))
	sb.Reset()
}

func TestProcessRequest_ForceBroadcastQueryInvalid(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("force_broadcast_query")
	req.SetParam(map[string]string{
		"enable": "1",
	})
	var sb strings.Builder
	require.Error(t, ProcessRequest(req, &sb))
	sb.Reset()

	var req2 netstorage.SysCtrlRequest
	req2.SetParam(map[string]string{
		"enabled": "2",
	})
	require.Error(t, ProcessRequest(req2, &sb))
	sb.Reset()
}

func TestQuerySeriesLimit(t *testing.T) {
	SetQuerySeriesLimit(123)
	limit := GetQuerySeriesLimit()
	assert.Equal(t, limit, 123)
}

func TestSetTimeFilterProtection(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("time_filter_protection")
	req.SetParam(map[string]string{
		"enabled": "true",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()

	SetTimeFilterProtection(false)
	enable := GetTimeFilterProtection()
	assert.Equal(t, enable, false)
}

func TestSetTimeFilterProtectionInvalid(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("time_filter_protection")
	req.SetParam(map[string]string{
		"enabed": "true",
	})
	var sb strings.Builder
	require.Error(t, ProcessRequest(req, &sb))
	sb.Reset()
}

type mockQueryStorage struct {
	netstorage.Storage
}

func (mockQueryStorage) SendQueryRequestOnNode(nodeID uint64, req netstorage.SysCtrlRequest) (map[string]string, error) {
	return map[string]string{
		"db0-rp0-pt0": `[{}]`,
	}, nil
}

func Test_handleQueryShardStatus(t *testing.T) {
	// case Not support query request
	res, err := ProcessQueryRequest("test", nil)
	assert.Equal(t, err.Error(), "not support query mod test")

	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockQueryStorage{}
	param := map[string]string{
		"db": "db0",
	}
	res, err = ProcessQueryRequest(QueryShardStatus, param)
	assert.NoError(t, err)
	assert.Equal(t, res, `{"db0-rp0-pt0":[{}]}`)
}

func TestProcessRequest_compactionEn(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("compen")
	req.SetParam(map[string]string{
		"allnodes": "y",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "127.0.0.1:8400: success,")
	require.Contains(t, sb.String(), "127.0.0.2:8400: success,")
	sb.Reset()

	req.SetParam(map[string]string{
		"host": "127.0.0.1",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "127.0.0.1:8400: success,")
	sb.Reset()
}

func Test_handleNodeInterruptQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var sb strings.Builder
	var req netstorage.SysCtrlRequest
	req.SetMod("interruptquery")
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	assert.Equal(t, ProcessRequest(req, &sb), nil)
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	assert.Equal(t, ProcessRequest(req, &sb), nil)
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	SysCtrl.MetaClient = nil
	assert.NotEqual(t, ProcessRequest(req, &sb), nil)
	SysCtrl.MetaClient = &mockErrMetaClient{}
	assert.NotEqual(t, ProcessRequest(req, &sb), nil)
}

func Test_handleUpperMemUsePct(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var sb strings.Builder
	var req netstorage.SysCtrlRequest
	req.SetMod("uppermemusepct")
	req.SetParam(map[string]string{
		"limit": "99",
	})
	assert.Equal(t, ProcessRequest(req, &sb), nil)
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	assert.Equal(t, ProcessRequest(req, &sb), nil)
	req.SetParam(map[string]string{
		"limit": "99",
	})
	SysCtrl.MetaClient = nil
	assert.NotEqual(t, ProcessRequest(req, &sb), nil)
	SysCtrl.MetaClient = &mockErrMetaClient{}
	assert.NotEqual(t, ProcessRequest(req, &sb), nil)
	SetUpperMemUsePct(0)
}

func TestParallelQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("parallelbatch")
	req.SetParam(map[string]string{
		"enabled": "true",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()

	req.SetMod("parallelbatch")
	req.SetParam(map[string]string{
		"enabled": "false",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
	sb.Reset()
}

func Test_Failpoint(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	SysCtrl.NetStore = &mockStorage{}
	var req netstorage.SysCtrlRequest
	req.SetMod("failpoint")
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	var sb strings.Builder
	require.NoError(t, ProcessRequest(req, &sb))
	require.Contains(t, sb.String(), "success")
}
