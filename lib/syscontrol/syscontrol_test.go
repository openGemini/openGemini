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

package syscontrol

import (
	"strings"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
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
	assert.Equal(t, executor.EnableForceBroadcastQuery, int64(1))
	sb.Reset()

	req.SetParam(map[string]string{
		"enabled": "0",
	})
	require.NoError(t, ProcessRequest(req, &sb))
	assert.Equal(t, executor.EnableForceBroadcastQuery, int64(0))
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
