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

package syscontrol_test

import (
	"bufio"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/backup"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/syscontrol"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMetaClient struct {
	meta.MetaClient
}

var SysCtrl = syscontrol.SysCtrl

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
	return map[string]string{"127.0.0.1": "success"}, nil
}

func (mockMetaClient) SendBackupToMeta(currentServer int, mod string, param map[string]string) string {
	return ""
}

func (mockMetaClient) MetaServers() []string {
	return []string{"127.0.0.1:8901"}
}

type mockErrMetaClient struct {
	meta.MetaClient
}

func (mockErrMetaClient) DataNodes() ([]meta2.DataNode, error) {
	return nil, fmt.Errorf("no datanode")
}

func TestProcessRequest_Readonly(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("readonly")
	req.SetParam(map[string]string{
		"switchon": "true",
		"allnodes": "y",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "\n\tsuccess")
	nw.Reset(httptest.NewRecorder())

	req.SetParam(map[string]string{
		"allnodes": "y",
	})
	require.Error(t, syscontrol.ProcessRequest(req, nw))
}

func TestProcessRequest_EnableReadonlyWrite(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("enable_readonly_write")
	req.SetParam(map[string]string{"switchon": "true"})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	t.Run("success", func(t *testing.T) {
		require.NoError(t, syscontrol.ProcessRequest(req, nw))
		nw.Flush()
		require.Equal(t, "\n\tsuccess", w.Body.String())
		nw.Reset(httptest.NewRecorder())
	})
	t.Run("failed", func(t *testing.T) {
		req.SetParam(map[string]string{"allnodes": "y"})
		require.Error(t, syscontrol.ProcessRequest(req, nw))
		nw.Reset(httptest.NewRecorder())
	})
}

func TestProcessRequest_LogRows(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("log_rows")

	req.SetParam(map[string]string{
		"switchon": "true",
	})

	nw := bufio.NewWriter(httptest.NewRecorder())
	require.EqualError(t, syscontrol.ProcessRequest(req, nw), "rules can not be empty")

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst, tk1=tv1",
	})
	nw = bufio.NewWriter(httptest.NewRecorder())
	require.EqualError(t, syscontrol.ProcessRequest(req, nw), "rules can not contains space")
	//sb.Reset()

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst",
	})
	w := httptest.NewRecorder()
	nw = bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")

	req.SetParam(map[string]string{
		"switchon": "true",
		"rules":    "mst,tk1=tv1",
	})
	w = httptest.NewRecorder()
	nw = bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")

	req.SetParam(map[string]string{
		"switchon": "false",
	})
	w = httptest.NewRecorder()
	nw = bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")

}

func TestProcessRequest_ForceBroadcastQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("force_broadcast_query")
	req.SetParam(map[string]string{
		"enabled": "1",
	})

	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "\n\tsuccess")
	assert.Equal(t, sysconfig.EnableForceBroadcastQuery, int64(1))

	req.SetParam(map[string]string{
		"enabled": "0",
	})

	nw = bufio.NewWriter(httptest.NewRecorder())
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	assert.Equal(t, sysconfig.EnableForceBroadcastQuery, int64(0))
}

func TestProcessRequest_ForceBroadcastQueryInvalid(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("force_broadcast_query")
	req.SetParam(map[string]string{
		"enable": "1",
	})
	nw := bufio.NewWriter(httptest.NewRecorder())
	require.Error(t, syscontrol.ProcessRequest(req, nw))

	var req2 msgservice.SysCtrlRequest
	req2.SetParam(map[string]string{
		"enabled": "2",
	})
	nw = bufio.NewWriter(httptest.NewRecorder())
	require.Error(t, syscontrol.ProcessRequest(req2, nw))
}

func TestQuerySeriesLimit(t *testing.T) {
	syscontrol.SetQuerySeriesLimit(123)
	limit := syscontrol.GetQuerySeriesLimit()
	assert.Equal(t, limit, 123)
}

func TestSetTimeFilterProtection(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("time_filter_protection")
	req.SetParam(map[string]string{
		"enabled": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")

	syscontrol.SetTimeFilterProtection(false)
	enable := syscontrol.GetTimeFilterProtection()
	assert.Equal(t, enable, false)
}

func TestSetTimeFilterProtectionInvalid(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("time_filter_protection")
	req.SetParam(map[string]string{
		"enabed": "true",
	})
	nw := bufio.NewWriter(httptest.NewRecorder())
	require.Error(t, syscontrol.ProcessRequest(req, nw))
}

func Test_handleQueryShardStatus(t *testing.T) {
	// case Not support query request

	_, err := syscontrol.ProcessQueryRequest("test", nil)
	assert.Equal(t, err.Error(), "not support query mod test")

	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendQueryRequestOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return map[string]string{
			"db0-rp0-pt0": `[{}]`,
		}, nil
	})

	defer patches.Reset()
	param := map[string]string{
		"db": "db0",
	}
	res, err := syscontrol.ProcessQueryRequest(syscontrol.QueryShardStatus, param)
	assert.NoError(t, err)
	assert.Equal(t, res, `{"db0-rp0-pt0":[{}]}`)
}

func TestProcessRequest_compactionEn(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("compen")
	req.SetParam(map[string]string{
		"allnodes": "y",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "127.0.0.1:8400: success,")
	require.Contains(t, w.Body.String(), "127.0.0.2:8400: success,")

	req.SetParam(map[string]string{
		"host": "127.0.0.1",
	})
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "127.0.0.1:8400: success,")
}

func Test_handleNodeInterruptQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	nw := bufio.NewWriter(httptest.NewRecorder())
	var req msgservice.SysCtrlRequest
	req.SetMod("interruptquery")
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	assert.Equal(t, strings.Contains(syscontrol.ProcessRequest(req, nw).Error(), "no parameter found"), true)
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	SysCtrl.MetaClient = nil
	assert.NotEqual(t, syscontrol.ProcessRequest(req, nw), nil)
	SysCtrl.MetaClient = &mockErrMetaClient{}
	assert.NotEqual(t, syscontrol.ProcessRequest(req, nw), nil)
}

func Test_handleUpperMemUsePct(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	nw := bufio.NewWriter(httptest.NewRecorder())
	var req msgservice.SysCtrlRequest
	req.SetMod("uppermemusepct")
	req.SetParam(map[string]string{
		"limit": "99",
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	req.SetParam(map[string]string{
		"unkown": "true",
	})
	assert.Equal(t, strings.Contains(syscontrol.ProcessRequest(req, nw).Error(), "no parameter found"), true)
	req.SetParam(map[string]string{
		"limit": "99",
	})
	SysCtrl.MetaClient = nil
	assert.NotEqual(t, syscontrol.ProcessRequest(req, nw), nil)
	SysCtrl.MetaClient = &mockErrMetaClient{}
	assert.NotEqual(t, syscontrol.ProcessRequest(req, nw), nil)
	sysconfig.SetUpperMemPct(0)
}

func TestParallelQuery(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("parallelbatch")
	req.SetParam(map[string]string{
		"enabled": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")

	req.SetMod("parallelbatch")
	req.SetParam(map[string]string{
		"enabled": "false",
	})
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
}

func Test_Failpoint(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("failpoint")
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
}

func Test_ProcessBackup(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod(syscontrol.Backup)
	req.SetParam(map[string]string{
		"isNode":         "true",
		backup.DataBases: "prom",
		"backupMeta":     "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessBackup(req, nw, []string{"127.0.0.1"}))
	nw.Flush()
	require.Contains(t, w.Body.String(), "")
}

func Test_ProcessBackup2(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod(syscontrol.Backup)
	req.SetParam(map[string]string{
		"isNode":     "false",
		"backupMeta": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessBackup(req, nw, []string{"127.0.0.1"}))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
}

func Test_ProcessBackupStatus(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod(syscontrol.BackupStatus)
	nw := bufio.NewWriter(httptest.NewRecorder())
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
}

func TestSendSysCtrlOnNodeErr(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return []uint64{1}, nil
	})

	_, err := sysctrl.SendSysCtrlOnNode(1, req)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.SysCtrlResponse, got: []uint64")
}

func TestSendSysCtrlOnNodeErr2(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return errors.New("InitWithNodeID Error")
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return []uint64{1}, nil
	})

	_, err := sysctrl.SendSysCtrlOnNode(1, req)
	assert.EqualError(t, err, "InitWithNodeID Error")
}

func TestSendSysCtrlOnNodeErr3(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return []uint64{1}, errors.New("SysCtrl Error")
	})

	_, err := sysctrl.SendSysCtrlOnNode(1, req)
	assert.EqualError(t, err, "SysCtrl Error")
}

func TestSendSysCtrlOnNode(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "GetNode", func(_ *msgservice.Requester) *meta2.DataNode {
		return &meta2.DataNode{
			NodeInfo: meta2.NodeInfo{
				ID:      0,
				TCPHost: "127.0.0.1:8400",
			},
		}
	})

	_, err := sysctrl.SendSysCtrlOnNode(1, req)
	assert.NoError(t, err)
}

func TestSendQueryRequestOnNodeErr(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return []uint64{1}, nil
	})

	_, err := sysctrl.SendQueryRequestOnNode(1, req)
	assert.EqualError(t, err, "invalid data type, exp: *msgservice.SysCtrlResponse, got: []uint64")
}

func TestSendQueryRequestOnNode(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})

	_, err := sysctrl.SendQueryRequestOnNode(1, req)
	assert.NoError(t, err)
}

func TestSendQueryRequestOnNodeErr2(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return errors.New("InitWithNodeID Error")
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})

	_, err := sysctrl.SendQueryRequestOnNode(1, req)
	assert.EqualError(t, err, "InitWithNodeID Error")
}

func TestSendQueryRequestOnNodeErr3(t *testing.T) {
	sysctrl := syscontrol.NewSysControl()
	sysctrl.MetaClient = mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("backup")
	req.SetParam(map[string]string{
		"isNode": "true",
	})
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, errors.New("SysCtrl Error")
	})

	_, err := sysctrl.SendQueryRequestOnNode(1, req)
	assert.EqualError(t, err, "SysCtrl Error")
}

func Test_ShardGroupTimezone(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})

	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("shard_group_timezone")
	req.SetParam(map[string]string{
		"timezone1": "UTC",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.Equal(t, syscontrol.ProcessRequest(req, nw).Error(), "no parameter found")

	req.SetParam(map[string]string{
		"timezone": "/UTC",
	})
	assert.NotEmpty(t, syscontrol.ProcessRequest(req, nw))
	req.SetParam(map[string]string{
		"timezone": "UTC",
	})
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
}

// curl -i -XPOST 'http://127.0.0.1:8635/debug/ctrl?mod=db_num_limit&limit=3'
func Test_DatabaseNumLimit(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("db_num_limit")
	req.SetParam(map[string]string{
		"limit": "10",
	})

	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessRequest(req, nw))
	nw.Flush()
	require.Contains(t, w.Body.String(), "\n\tsuccess")
	assert.Equal(t, syscontrol.DatabaseNumLimit, int32(10))
}

func Test_DatabaseNumLimitFail(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("db_num_limit")
	req.SetParam(map[string]string{
		"limit": "true",
	})

	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.Error(t, syscontrol.ProcessRequest(req, nw))
}

func TestUpdateAkSk_failed(t *testing.T) {
	convey.Convey("tests for update obs ak sk", t, func() {
		var req msgservice.SysCtrlRequest
		req.SetMod(syscontrol.ObsAkSk)

		w := httptest.NewRecorder()
		nw := bufio.NewWriter(w)
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
			return nil
		})
		patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
			return []uint64{1}, nil
		})

		convey.Convey("check failed", func() {
			req.SetParam(map[string]string{
				"ak": "",
			})
			err := syscontrol.ProcessRequest(req, nw)
			convey.ShouldNotBeNil(err)
		})

		convey.Convey("broadcastCmdToStore failed", func() {
			req.SetParam(map[string]string{
				"ak": "",
				"sk": "",
			})
			err := syscontrol.ProcessRequest(req, nw)
			convey.ShouldNotBeNil(err)
		})
	})
}

func TestObsHostName(t *testing.T) {
	convey.Convey("tests for update obs hostname", t, func() {
		var req msgservice.SysCtrlRequest
		req.SetMod(syscontrol.ObsHostName)
		w := httptest.NewRecorder()
		nw := bufio.NewWriter(w)
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
			return nil
		})
		patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
			return []uint64{1}, nil
		})

		convey.Convey("check failed", func() {
			req.SetParam(map[string]string{
				"test": "",
			})
			err := syscontrol.ProcessRequest(req, nw)
			convey.ShouldNotBeNil(err)
		})

		convey.Convey("broadcastCmdToStore failed", func() {
			req.SetParam(map[string]string{
				"hostname": "",
			})
			err := syscontrol.ProcessRequest(req, nw)
			convey.ShouldNotBeNil(err)
		})
	})
}

func TestQuerytimeout(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("querytimeout")
	req.SetParam(map[string]string{
		"limit": "4s",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
}

func TestQuerytimeoutError(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("querytimeout")
	req.SetParam(map[string]string{
		"limit": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.Error(t, syscontrol.ProcessRequest(req, nw))
}

func TestSlowquery(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("slowquery")
	req.SetParam(map[string]string{
		"limit": "4s",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
}

func TestSlowqueryError(t *testing.T) {
	var req msgservice.SysCtrlRequest
	req.SetMod("slowquery")
	req.SetParam(map[string]string{
		"limit": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.Error(t, syscontrol.ProcessRequest(req, nw))
}

func Test_LastRowCache(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetLocalBindAddress("127.0.0.1:8086")
	req.SetLocalDomain("sqlstore_sql_node_1")
	req.SetMod(syscontrol.LastRowCache)
	req.SetParam(map[string]string{
		syscontrol.LastRowCacheMaxCost:       "777",
		syscontrol.LastRowCache:              "1",
		syscontrol.LastRowCacheMetricsEnable: "1",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
	nw.Reset(httptest.NewRecorder())
}

func TestCheckUpdateParquetTaskParamValid(t *testing.T) {
	convey.Convey("check parquet task", t, func() {
		params := map[string]string{syscontrol.ParquetBatchSize: "1", "1": "2"}
		err := syscontrol.CheckUpdateParquetTaskParamValid(params)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_LastRowCache_AllNodes(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetLocalBindAddress("127.0.0.1:8086")
	req.SetLocalDomain("sqlstore_sql_node_1")
	req.SetMod(syscontrol.LastRowCache)
	req.SetParam(map[string]string{
		syscontrol.LastRowCacheMaxCost:       "777",
		syscontrol.LastRowCache:              "1",
		syscontrol.LastRowCacheMetricsEnable: "1",
		"allnodes":                           "y",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
	nw.Reset(httptest.NewRecorder())
}

func Test_LastRowCache_AllNodes_2(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetLocalBindAddress("127.0.0.3:8086")
	req.SetLocalDomain("sqlstore_sql_node_3")
	req.SetMod(syscontrol.LastRowCache)
	req.SetParam(map[string]string{
		syscontrol.LastRowCacheMaxCost:       "777",
		syscontrol.LastRowCache:              "1",
		syscontrol.LastRowCacheMetricsEnable: "1",
		"allnodes":                           "y",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
	nw.Reset(httptest.NewRecorder())
}

func Test_ForceFlushDuration_OK(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	var req msgservice.SysCtrlRequest
	req.SetMod("force_flush_duration")
	req.SetParam(map[string]string{
		"duration": "10m",
	})
	req.SetLocalBindAddress("127.0.0.3:8086")
	req.SetLocalDomain("sqlstore_sql_node_3")
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod((*msgservice.Requester)(nil), "InitWithNodeID", func(_ *msgservice.Requester) error {
		return nil
	})
	patches.ApplyMethod((*msgservice.Requester)(nil), "SysCtrl", func(_ *msgservice.Requester) (interface{}, error) {
		return &msgservice.SysCtrlResponse{}, nil
	})
	assert.Equal(t, syscontrol.ProcessRequest(req, nw), nil)
	nw.Flush()
	require.Contains(t, w.Body.String(), "success")
	nw.Reset(httptest.NewRecorder())
}

func TestWalEnabled(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("walenabled")
	req.SetParam(map[string]string{
		"switchon": "true",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	assert.NoError(t, syscontrol.ProcessRequest(req, nw))
}

func TestWalEnabledFalse(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("walenabled")
	req.SetParam(map[string]string{
		"switchon": "false",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	assert.NoError(t, syscontrol.ProcessRequest(req, nw))
}

func TestDataCacheEnable(t *testing.T) {
	SysCtrl.MetaClient = &mockMetaClient{}
	patches := gomonkey.ApplyMethod(SysCtrl, "SendSysCtrlOnNode", func(_ *syscontrol.SysControl) (map[string]string, error) {
		return nil, nil
	})
	defer patches.Reset()
	var req msgservice.SysCtrlRequest
	req.SetMod("datacacheenable")
	req.SetParam(map[string]string{
		"enabled": "1",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	assert.NoError(t, syscontrol.ProcessRequest(req, nw))

	req.SetParam(map[string]string{
		"enabled": "2",
	})
	assert.Error(t, syscontrol.ProcessRequest(req, nw))
}
