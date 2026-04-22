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

func (mockMetaClient) SendBackupToMeta(mod string, param map[string]string) (map[string]string, error) {
	return nil, nil
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
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessBackup(req, nw, "127.0.0.1"))
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
		"isNode": "false",
	})
	w := httptest.NewRecorder()
	nw := bufio.NewWriter(w)
	require.NoError(t, syscontrol.ProcessBackup(req, nw, "127.0.0.1"))
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
