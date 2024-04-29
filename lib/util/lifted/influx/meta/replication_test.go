/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package meta

import (
	"testing"

	"github.com/openGemini/openGemini/lib/logger"
	assert1 "github.com/stretchr/testify/assert"
)

func Test_CreateReplication(t *testing.T) {
	DataLogger = logger.GetLogger()
	d := &Data{
		PtNumPerNode: 2,
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
				{
					PtId:  3,
					Owner: PtOwner{NodeID: 4},
				},
				{
					PtId:  4,
					Owner: PtOwner{NodeID: 5},
				},
				{
					PtId:  5,
					Owner: PtOwner{NodeID: 6},
				},
			},
		},
	}
	err := d.CreateReplication("testDB", 1)
	assert1.NoError(t, err)

	err = d.CreateReplication("testDB", 3)
	assert1.NoError(t, err)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"]))
	assert1.Equal(t, uint32(1), d.ReplicaGroups["testDB"][1].ID)
	assert1.Equal(t, uint32(3), d.ReplicaGroups["testDB"][1].MasterPtID)
	assert1.Equal(t, 2, len(d.ReplicaGroups["testDB"][1].Peers))
	assert1.Equal(t, uint32(4), d.ReplicaGroups["testDB"][1].Peers[0].ID)
	assert1.Equal(t, uint32(5), d.ReplicaGroups["testDB"][1].Peers[1].ID)
}
