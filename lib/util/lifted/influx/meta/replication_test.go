// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"testing"

	"github.com/openGemini/openGemini/lib/logger"
	assert1 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// repN=1, nodeN=3, ptPerNode=1
func Test_CreateDBReplication1(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 1,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
			},
		},
		PtNumPerNode: 1,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := CreateDBRGFns[NodeHard](d, "testDB", 1)
	assert1.NoError(t, err)
	assert1.Equal(t, 3, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// repN=1, nodeN=3, ptPerNode=2
func Test_CreateDBReplication2(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 1,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := CreateDBRGFns[NodeHard](d, "testDB", 1)
	assert1.NoError(t, err)
	assert1.Equal(t, 6, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// repN=3, nodeN=1, ptPerNode=2
func Test_CreateDBReplication3(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	assert1.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// repN=3, nodeN=3, ptPerNode=2
func Test_CreateDBReplication4(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	assert1.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// repN=3, nodeN=5, ptPerNode=2
func Test_CreateDBReplication5(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 4,
				},
			},
			{
				NodeInfo: NodeInfo{
					ID: 5,
				},
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  6,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  7,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  8,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  9,
					Owner: PtOwner{NodeID: 5},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	assert1.NoError(t, err)
	assert1.Equal(t, 4, len(d.ReplicaGroups["testDB"]))
	for i := 0; i < 2; i++ {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
	for i := 2; i < 4; i++ {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i+4), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// rg0{pt0, pt2, pt4}, rg1{pt1, pt3, pt5} -> rg0{pt0, pt2, pt4}, rg1{pt1, pt3, pt5}, rg2{pt6}, rg3{pt7}
func Test_NodeHardChooseRG1(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	data := &Data{
		PtNumPerNode: 2,
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
					RGID:  0,
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
					RGID:  1,
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
					RGID:  0,
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
					RGID:  1,
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
					RGID:  0,
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
					RGID:  1,
				},
				{
					PtId:  6,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  7,
					Owner: PtOwner{NodeID: 4},
				},
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": {
				{
					ID:         0,
					MasterPtID: 0,
					Status:     Health,
				},
				{
					ID:         1,
					MasterPtID: 1,
					Status:     Health,
				},
			},
		},
	}
	newNode := DataNode{NodeInfo: NodeInfo{ID: 4}}
	NodeHardChooseRG(data, "db0", &newNode, 3)
	assert1.Equal(t, uint32(2), data.PtView["db0"][6].RGID)
	assert1.Equal(t, uint32(3), data.PtView["db0"][7].RGID)
	assert1.Equal(t, 4, len(data.ReplicaGroups["db0"]))
	assert1.Equal(t, uint32(6), data.ReplicaGroups["db0"][2].MasterPtID)
	assert1.Equal(t, uint32(7), data.ReplicaGroups["db0"][3].MasterPtID)
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][2].Status)
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][3].Status)
}

// rg0{pt0}, rg1{pt1} -> rg0{pt0, pt2}, rg1{pt1, pt3}
func Test_NodeHardChooseRG2(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	data := &Data{
		PtNumPerNode: 2,
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
					RGID:  0,
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
					RGID:  1,
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": {
				{
					ID:         0,
					MasterPtID: 0,
					Status:     UnFull,
				},
				{
					ID:         1,
					MasterPtID: 1,
					Status:     UnFull,
				},
			},
		},
	}
	newNode := DataNode{NodeInfo: NodeInfo{ID: 2}}
	NodeHardChooseRG(data, "db0", &newNode, 3)
	assert1.Equal(t, uint32(0), data.PtView["db0"][2].RGID)
	assert1.Equal(t, uint32(1), data.PtView["db0"][3].RGID)
	assert1.Equal(t, 1, len(data.ReplicaGroups["db0"][0].Peers))
	assert1.Equal(t, 1, len(data.ReplicaGroups["db0"][1].Peers))
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][0].Status)
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][1].Status)
}

// rg0{pt0, pt2}, rg1{pt1, pt3} -> rg0{pt0, pt2, pt4}, rg1{pt1, pt3, pt5}
func Test_NodeHardChooseRG3(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	data := &Data{
		PtNumPerNode: 2,
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
					RGID:  0,
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
					RGID:  1,
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
					RGID:  0,
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
					RGID:  1,
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": {
				{
					ID:         0,
					MasterPtID: 0,
					Status:     UnFull,
					Peers:      []Peer{Peer{ID: 2, PtRole: Slave}},
				},
				{
					ID:         1,
					MasterPtID: 1,
					Status:     UnFull,
					Peers:      []Peer{Peer{ID: 3, PtRole: Slave}},
				},
			},
		},
	}
	newNode := DataNode{NodeInfo: NodeInfo{ID: 3}}
	NodeHardChooseRG(data, "db0", &newNode, 3)
	assert1.Equal(t, uint32(0), data.PtView["db0"][4].RGID)
	assert1.Equal(t, uint32(1), data.PtView["db0"][5].RGID)
	assert1.Equal(t, SubHealth, data.ReplicaGroups["db0"][0].Status)
	assert1.Equal(t, SubHealth, data.ReplicaGroups["db0"][1].Status)
}

// -> rg0{pt0}, rg1{pt1}
func Test_NodeHardChooseRG4(t *testing.T) {
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	data := &Data{
		PtNumPerNode: 2,
		Databases: map[string]*DatabaseInfo{
			"db0": {
				Name:     "db0",
				ReplicaN: 3,
			},
		},
		PtView: map[string]DBPtInfos{
			"db0": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
					RGID:  0,
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
					RGID:  1,
				},
			},
		},
		ReplicaGroups: map[string][]ReplicaGroup{
			"db0": {},
		},
	}
	newNode := DataNode{NodeInfo: NodeInfo{ID: 1}}
	NodeHardChooseRG(data, "db0", &newNode, 3)
	assert1.Equal(t, uint32(0), data.PtView["db0"][0].RGID)
	assert1.Equal(t, uint32(1), data.PtView["db0"][1].RGID)
	assert1.Equal(t, uint32(0), data.ReplicaGroups["db0"][0].MasterPtID)
	assert1.Equal(t, uint32(1), data.ReplicaGroups["db0"][1].MasterPtID)
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][0].Status)
	assert1.Equal(t, UnFull, data.ReplicaGroups["db0"][1].Status)
}

// AzHard , repN=1, nodeN=3(3Az), ptPerNode=1
func TestCreateReplication(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 1,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az3",
			},
		},
		PtNumPerNode: 1,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}

	err := AzHardCreateDBRG(d, "testDB", 1)
	assert1.NoError(t, err)
	assert1.Equal(t, 3, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=1, nodeN=3(3Az), ptPerNode=2
func TestCreateReplication2(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 1,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az3",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := AzHardCreateDBRG(d, "testDB", 1)
	assert1.NoError(t, err)
	assert1.Equal(t, 6, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=3, nodeN=1(1Az), ptPerNode=2
func TestCreateReplication3(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=3, nodeN=2(1Az), ptPerNode=2
func TestCreateReplication4(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az1",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 4, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=3, nodeN=2(2Az), ptPerNode=2
func TestCreateReplication5(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=3, nodeN=3(3Az), ptPerNode=2
func TestCreateReplication6(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az3",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
		assert1.Equal(t, uint32(i), d.ReplicaGroups["testDB"][i].MasterPtID)
	}
}

// AzHard , repN=3, nodeN=3(2Az), ptPerNode=2
func TestCreateReplication7(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az1",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 4, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
	}
}

// AzHard , repN=3, nodeN=4(2Az), ptPerNode=2
func TestCreateReplication8(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 4,
				},
				Az: "az2",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  6,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  7,
					Owner: PtOwner{NodeID: 4},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 6, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, UnFull, d.ReplicaGroups["testDB"][i].Status)
	}
}

// AzHard , repN=3, nodeN=6(3Az), ptPerNode=2
func TestCreateReplication9(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az3",
			},
			{
				NodeInfo: NodeInfo{
					ID: 4,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 5,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 6,
				},
				Az: "az3",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  6,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  7,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  8,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  9,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  10,
					Owner: PtOwner{NodeID: 6},
				},
				{
					PtId:  11,
					Owner: PtOwner{NodeID: 6},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 4, len(d.ReplicaGroups["testDB"]))
	for i := range d.ReplicaGroups["testDB"] {
		assert1.Equal(t, SubHealth, d.ReplicaGroups["testDB"][i].Status)
	}
}

// AzHard , repN=3, nodeN=6(3Az), ptPerNode=2
func TestCreateReplication10(t *testing.T) {
	// setType as AzHard
	SetRepDisPolicy(uint8(1))
	DataLogger = logger.GetLogger().With(zap.String("service", "data"))
	d := &Data{
		Databases: map[string]*DatabaseInfo{
			"testDB": &DatabaseInfo{
				Name:     "testDB",
				ReplicaN: 3,
			},
		},
		DataNodes: []DataNode{
			{
				NodeInfo: NodeInfo{
					ID: 1,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 2,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 3,
				},
				Az: "az3",
			},
			{
				NodeInfo: NodeInfo{
					ID: 4,
				},
				Az: "az1",
			},
			{
				NodeInfo: NodeInfo{
					ID: 5,
				},
				Az: "az2",
			},
			{
				NodeInfo: NodeInfo{
					ID: 6,
				},
				Az: "az1",
			},
		},
		PtNumPerNode: 2,
		PtView: map[string]DBPtInfos{
			"testDB": {
				{
					PtId:  0,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  1,
					Owner: PtOwner{NodeID: 1},
				},
				{
					PtId:  2,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 2},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 3},
				},
				{
					PtId:  6,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  7,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  8,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  9,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  10,
					Owner: PtOwner{NodeID: 6},
				},
				{
					PtId:  11,
					Owner: PtOwner{NodeID: 6},
				},
			},
		},
	}
	err := d.CreateDBReplication("testDB", 3)
	require.NoError(t, err)
	assert1.Equal(t, 6, len(d.ReplicaGroups["testDB"]))
	statusSet := make(map[RGStatus]int32)
	for _, group := range d.ReplicaGroups["testDB"] {
		statusSet[group.Status]++
	}

	assert1.Equal(t, statusSet[SubHealth], int32(2))
	assert1.Equal(t, statusSet[UnFull], int32(4))
}
