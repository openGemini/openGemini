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

package metaclient

import (
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/logger"
	"github.com/openGemini/openGemini/open_src/github.com/hashicorp/serf/serf"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type FuncExecutor struct {
	fns []func()
	err error
}

func (e *FuncExecutor) Add(fn func()) {
	e.fns = append(e.fns, fn)
}

func (e *FuncExecutor) Execute() error {
	for _, fn := range e.fns {
		fn()
		if e.err != nil {
			return e.err
		}
	}
	return nil
}

func mockMetaData() (meta.Data, error) {
	exec := &FuncExecutor{}

	data := meta.Data{PtNumPerNode: 1, ClusterPtNum: 1}
	meta.DataLogger = logger.New(os.Stderr)
	dataNodes := []meta.DataNode{{NodeInfo: meta.NodeInfo{ID: 1, Host: "127.0.0.1:8086", TCPHost: "127.0.0.1:8188", Status: serf.StatusAlive}},
		{NodeInfo: meta.NodeInfo{ID: 2, Host: "127.0.0.2:8086", TCPHost: "127.0.0.2:8188", Status: serf.StatusAlive}}}
	data.DataNodes = dataNodes

	exec.Add(func() {
		exec.err = data.CreateDatabase("foo", nil, nil)
	})

	exec.Add(func() {
		exec.err = data.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{
			Name:     "bar",
			ReplicaN: 1,
			Duration: 24 * time.Hour,
		}, false)
	})

	var rp *meta.RetentionPolicyInfo

	exec.Add(func() {
		rp, exec.err = data.RetentionPolicy("foo", "bar")

		if rp == nil {
			exec.err = fmt.Errorf("creation of retention policy failed")
		}
	})

	exec.Add(func() {
		exec.err = data.CreateMeasurement("foo", "bar", "cpu",
			&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, nil)
	})

	exec.Add(func() {
		exec.err = data.CreateMeasurement("foo", "bar", "cpu",
			&proto2.ShardKeyInfo{ShardKey: []string{"hostName", "location"}, Type: proto.String(influxql.RANGE)}, nil)
	})

	exec.Add(func() {
		exec.err = data.UpdateSchema("foo", "bar", "cpu", []*proto2.FieldSchema{
			{
				FieldName: proto.String("host_node"),
				FieldType: proto.Int32(influx.Field_Type_Tag),
			},
			{
				FieldName: proto.String("server_node"),
				FieldType: proto.Int32(influx.Field_Type_Tag),
			},
			{
				FieldName: proto.String("year"),
				FieldType: proto.Int32(influx.Field_Type_Tag),
			},
		})
	})

	err := exec.Execute()
	if err != nil {
		return meta.Data{}, err
	}

	return data, nil
}

func TestQueryTagKeys(t *testing.T) {
	data, err := mockMetaData()
	if err != nil {
		t.Fatalf(err.Error())
	}
	mc := &Client{cacheData: &data}

	ms := influxql.Measurements{}
	ms = append(ms, &influxql.Measurement{
		Database:          "foo",
		RetentionPolicy:   "bar",
		Name:              "cpu",
		Regex:             nil,
		IsTarget:          false,
		SystemIterator:    "",
		IsSystemStatement: false,
	})

	var ret map[string]map[string]struct{}

	ret, _ = mc.QueryTagKeys("foo", ms, &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "_tagKey"},
		RHS: &influxql.StringLiteral{Val: "year"},
	})

	if _, ok := ret["cpu"]; !ok {
		t.Fatalf("tag keys query failed. exp: contain cpu")
	}
	if _, ok := ret["cpu"]["year"]; !ok {
		t.Fatalf("tag keys query failed. exp: contain year")
	}

	reg, err := regexp.Compile(".*node")
	if err != nil {
		t.Fatalf(err.Error())
	}
	ret, _ = mc.QueryTagKeys("foo", ms, &influxql.BinaryExpr{
		Op:  influxql.EQREGEX,
		LHS: &influxql.VarRef{Val: "_tagKey"},
		RHS: &influxql.RegexLiteral{Val: reg},
	})

	if _, ok := ret["cpu"]["host_node"]; !ok {
		t.Fatalf("tag keys query failed. exp: contain host_node")
	}
	if _, ok := ret["cpu"]["server_node"]; !ok {
		t.Fatalf("tag keys query failed. exp: contain server_node")
	}
}
