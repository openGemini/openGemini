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
package downsample

import (
	"errors"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

func TestDownSampleQuerySchemaGen(t *testing.T) {
	mstInfos := &meta.RpMeasurementsFieldsInfo{
		MeasurementInfos: []*meta.MeasurementFieldsInfo{
			{
				MstName: "mst1",
				TypeFields: []*meta.MeasurementTypeFields{
					{
						Type:   int64(influxql.Float),
						Fields: []string{"f1", "f2"},
					},
					{
						Type:   int64(influxql.Integer),
						Fields: []string{"f3", "f4"},
					},
				},
			},
			{
				MstName: "mst2",
				TypeFields: []*meta.MeasurementTypeFields{
					{
						Type:   int64(influxql.Float),
						Fields: []string{"f1", "f2"},
					},
					{
						Type:   int64(influxql.Integer),
						Fields: []string{"f3", "f4"},
					},
				},
			},
		},
	}
	policy := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max", "mean"},
				DataType: int64(influxql.Integer),
			},
			{
				AggOps:   []string{"min", "max", "mean"},
				DataType: int64(influxql.Float),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
			{
				SampleInterval: 10 * time.Hour,
				TimeInterval:   250 * time.Second,
				WaterMark:      10 * time.Hour,
			},
		},
		Duration: 2400 * time.Hour,
	}
	info1 := &meta.ShardDownSamplePolicyInfo{
		DbName:                "test",
		RpName:                "rp0",
		DownSamplePolicyLevel: 2,
		Ident: &meta.ShardIdentifier{
			DownSampleLevel: 0,
		},
	}
	schemas := downSampleQuerySchemaGen(info1, mstInfos, policy)
	if len(schemas) != 2 {
		t.Fatal("unexpected schemas length, expect: 2")
	}
	info2 := &meta.ShardDownSamplePolicyInfo{
		DbName:                "test",
		RpName:                "rp0",
		DownSamplePolicyLevel: 1,
		Ident: &meta.ShardIdentifier{
			DownSampleLevel: 0,
		},
	}
	schemasLevel1 := downSampleQuerySchemaGen(info2, mstInfos, policy)
	if len(schemasLevel1) != 1 {
		t.Fatal("unexpected schemas length, expect: 2")
	}
	info3 := &meta.ShardDownSamplePolicyInfo{
		DbName:                "test",
		RpName:                "rp0",
		DownSamplePolicyLevel: 2,
		Ident: &meta.ShardIdentifier{
			DownSampleLevel: 1,
		},
	}
	schemasLevel2 := downSampleQuerySchemaGen(info3, mstInfos, policy)
	if len(schemasLevel2) != 2 {
		t.Fatal("unexpected schemas length, expect: 2")
	}
}

func TestMocService(t *testing.T) {
	s := &Service{}
	policy := &meta.DownSamplePolicyInfo{
		Calls: []*meta.DownSampleOperators{
			{
				AggOps:   []string{"min", "max", "mean"},
				DataType: int64(influxql.Integer),
			},
			{
				AggOps:   []string{"min", "max", "mean"},
				DataType: int64(influxql.Float),
			},
		},
		DownSamplePolicies: []*meta.DownSamplePolicy{
			{
				SampleInterval: time.Hour,
				TimeInterval:   25 * time.Second,
				WaterMark:      time.Hour,
			},
			{
				SampleInterval: 10 * time.Hour,
				TimeInterval:   250 * time.Second,
				WaterMark:      10 * time.Hour,
			},
		},
		Duration: 2400 * time.Hour,
	}
	policies := &meta.DownSamplePoliciesInfoWithDbRp{
		Infos: []*meta.DownSamplePolicyInfoWithDbRp{
			{
				Info:   policy,
				DbName: "db0",
				RpName: "rp0",
			},
		},
	}
	s.MetaClient = NewMocMetaClient(policies)
	mstInfo := &meta.MeasurementFieldsInfo{
		MstName: "mst",
		TypeFields: []*meta.MeasurementTypeFields{
			{
				Type:   int64(influxql.Float),
				Fields: []string{"f1", "f2"}},
		},
	}
	s.MetaClient.(*mocMetaClient).AddMeasurementInfos(mstInfo, "db0", "rp0")
	s.Engine = NewMocEngine([]*meta.ShardDownSamplePolicyInfo{
		{DbName: "db0", RpName: "rp0", ShardId: 1, PtId: 1, TaskID: 2, DownSamplePolicyLevel: 1, Ident: &meta.ShardIdentifier{
			ShardID: 1, ShardGroupID: 1, Policy: "rp0", OwnerDb: "db0", OwnerPt: 1, ShardType: "hash", ReadOnly: true, EngineType: uint32(config.TSSTORE)}},
	})
	s.Engine.(*mocEngine).DownSamplePolicies["db0"+"."+"rp0"] = &meta.StoreDownSamplePolicy{
		Info: policy,
	}
	s.Logger = logger.NewLogger(errno.ModuleUnknown).With(zap.String("service", "downsample"))
	s.MetaClient.(*mocMetaClient).returnFunc = s.MetaClient.(*mocMetaClient).GetMstInfoWithInRpNormal
	s.handle()
	s.MetaClient.(*mocMetaClient).returnFunc = s.MetaClient.(*mocMetaClient).GetMstInfoWithInRpError
	s.handle()
}

type mocEngine struct {
	DownSamplePolicies map[string]*meta.StoreDownSamplePolicy
	infos              []*meta.ShardDownSamplePolicyInfo
}

func NewMocEngine(infos []*meta.ShardDownSamplePolicyInfo) *mocEngine {
	return &mocEngine{
		infos:              infos,
		DownSamplePolicies: make(map[string]*meta.StoreDownSamplePolicy),
	}
}

func (m *mocEngine) StartDownSampleTask(sdsp *meta.ShardDownSamplePolicyInfo, schema []hybridqp.Catalog, log *zap.Logger, meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) error {
	return nil
}

func (m *mocEngine) UpdateStoreDownSamplePolicies(info *meta.DownSamplePolicyInfo, ident string) {
}

func (m *mocEngine) GetShardDownSamplePolicyInfos(meta interface {
	UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error
}) ([]*meta.ShardDownSamplePolicyInfo, error) {
	return m.infos, nil
}

func (m *mocEngine) GetDownSamplePolicy(key string) *meta.StoreDownSamplePolicy {
	return m.DownSamplePolicies[key]
}

func (m *mocEngine) UpdateDownSampleInfo(policies *meta.DownSamplePoliciesInfoWithDbRp) {

}

type mocMetaClient struct {
	infos           *meta.DownSamplePoliciesInfoWithDbRp
	measurementInfo map[string]DbMap
	returnFunc      func(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error)
}

type DbMap struct {
	m map[string][]*meta.MeasurementFieldsInfo
}

func NewMocMetaClient(infos *meta.DownSamplePoliciesInfoWithDbRp) *mocMetaClient {
	return &mocMetaClient{
		infos:           infos,
		measurementInfo: make(map[string]DbMap),
	}
}

func (m *mocMetaClient) AddMeasurementInfos(info *meta.MeasurementFieldsInfo, db, rp string) {
	database, ok := m.measurementInfo["1"]
	if !ok {
		rpMap := make(map[string][]*meta.MeasurementFieldsInfo)
		slice := make([]*meta.MeasurementFieldsInfo, 0, 0)
		slice = append(slice, info)
		rpMap[rp] = slice
		m.measurementInfo[db] = DbMap{m: rpMap}
	} else if rm, ok := database.m[rp]; !ok {
		slice := make([]*meta.MeasurementFieldsInfo, 0, 0)
		slice = append(slice, info)
		database.m[rp] = slice
	} else if ok {
		rm = append(rm, info)
	}
}

func (m *mocMetaClient) GetDownSamplePolicies() (*meta.DownSamplePoliciesInfoWithDbRp, error) {
	return m.infos, nil
}

func (m *mocMetaClient) GetMstInfoWithInRp(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error) {
	return m.returnFunc(dbName, rpName, dataTypes)
}

func (m *mocMetaClient) GetMstInfoWithInRpNormal(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error) {
	r := &meta.RpMeasurementsFieldsInfo{
		MeasurementInfos: []*meta.MeasurementFieldsInfo{},
	}
	database, ok := m.measurementInfo[dbName]
	if !ok {
		return nil, nil
	}
	rp, ok := database.m[rpName]
	if !ok {
		return nil, nil
	}
	for i := range rp {
		info := &meta.MeasurementFieldsInfo{MstName: rp[i].MstName}
		info.TypeFields = make([]*meta.MeasurementTypeFields, 0, 0)
		for j := range dataTypes {
			for _, t := range rp[i].TypeFields {
				if t.Type == dataTypes[j] {
					info.TypeFields = append(info.TypeFields, t)
					break
				}
			}
		}
		r.MeasurementInfos = append(r.MeasurementInfos, info)
	}
	return r, nil
}

func (m *mocMetaClient) GetMstInfoWithInRpError(dbName, rpName string, dataTypes []int64) (*meta.RpMeasurementsFieldsInfo, error) {
	return nil, errors.New("empty info")
}

func (m *mocMetaClient) UpdateShardDownSampleInfo(Ident *meta.ShardIdentifier) error {
	return nil
}

func TestRewriteDownSampleField(t *testing.T) {
	varef := &influxql.VarRef{
		Val:  "field1",
		Type: influxql.Integer,
	}
	call1 := &influxql.Call{
		Name: "min",
		Args: []influxql.Expr{varef},
	}
	call2 := &influxql.Call{
		Name: "max",
		Args: []influxql.Expr{call1},
	}
	rewriteField(call1, "min")
	rewriteField(call2, "max")
}
