// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package executor_test

import (
	"encoding/json"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func mockGetTimeGraph() string {
	jsonNodeData := `{
	"data": {
	  "resultUid": "eadee230-db0a-40df-887c-9958e1d872df",
	  "metadata": {
		"region": "global",
		"timestamp": "1724360595038",
		"topokeys": [
		  "resource0",
		  "communication0",
		  "application0"
		],
		"stitching_points": [
		  {
			"point_type": [
			  "communication0.ECS",
			  "resource0.ECS"
			],
			"point_uid": "0f3baaf1-1a66-486e-938f-5257247a23c4"
		  },
		  {
			"point_type": [
			  "resource0.deploymentunit001",
			  "application0.deployment001"
			],
			"point_uid": "eadee230-db0a-40df-887c-9958e1d872df "
		  }
		]
	  },
	  "graph": {
		"vertex": [
		  {
			"uid": "VM1",
			"metadata": {
			  "kind": "node",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "VM2",
			"metadata": {
			  "kind": "node",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "VM3",
			"metadata": {
			  "kind": "node",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "VM4",
			"metadata": {
			  "kind": "node",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "VM5",
			"metadata": {
			  "kind": "node",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "ELB",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress1",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress2",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Service1",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Service2",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Service3",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "Service4",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "rds1",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  },
		  {
			"uid": "rds2",
			"metadata": {
			  "kind": "pod",
			  "region": "cn-north-1",
			  "tags": {
				"application": "RDS",
				"deploymentunit": "DBS-open-apiserver-for-rds"
			  }
			}
		  }
		],
		"edges": [
		  {
			"uid": "ELB_source0::::VM1_source0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "source0",
			  "sourceUid": "ELB",
			  "targetTopokey": "source0",
			  "targetUid": "VM1",
			  "tags": {}
			}
		  },
		  {
			"uid": "Nginx-ingress1_source0::::VM2_source0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "source0",
			  "sourceUid": "Nginx-ingress1",
			  "targetTopokey": "source0",
			  "targetUid": "VM2",
			  "tags": {}
			}
		  },
		  {
			"uid": "Nginx-ingress2_communication0::::VM3_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Nginx-ingress2",
			  "targetTopokey": "communication0",
			  "targetUid": "VM3",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service1_communication0::::VM2_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service1",
			  "targetTopokey": "communication0",
			  "targetUid": "VM2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service2_communication0::::VM2_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service2",
			  "targetTopokey": "communication0",
			  "targetUid": "VM2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service3_communication0::::VM3_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service3",
			  "targetTopokey": "communication0",
			  "targetUid": "VM3",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service4_communication0::::VM3_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service4",
			  "targetTopokey": "communication0",
			  "targetUid": "VM3",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "rds1_communication0::::VM4_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "rds1",
			  "targetTopokey": "communication0",
			  "targetUid": "VM4",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "rds2_communication0::::VM5_communication0",
			"metadata": {
			  "kind": "LOCATE",
			  "sourceTopokey": "communication0",
			  "sourceUid": "rds2",
			  "targetTopokey": "communication0",
			  "targetUid": "VM5",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "ELB_communication0::::Nginx-ingress1_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "ELB",
			  "targetTopokey": "communication0",
			  "targetUid": "Nginx-ingress1",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "ELB_communication0::::Nginx-ingress2_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "ELB",
			  "targetTopokey": "communication0",
			  "targetUid": "Nginx-ingress2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress1_communication0::::Service1_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Nginx-ingress1",
			  "targetTopokey": "communication0",
			  "targetUid": "Service1",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress1_communication0::::Service2_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Nginx-ingress1",
			  "targetTopokey": "communication0",
			  "targetUid": "Service2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress2_communication0::::Service3_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Nginx-ingress2",
			  "targetTopokey": "communication0",
			  "targetUid": "Service3",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Nginx-ingress2_communication0::::Service4_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Nginx-ingress2",
			  "targetTopokey": "communication0",
			  "targetUid": "Service4",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service1_communication0::::rds1_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service1",
			  "targetTopokey": "communication0",
			  "targetUid": "rds1",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service2_communication0::::rds1_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service2",
			  "targetTopokey": "communication0",
			  "targetUid": "rds1",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service3_communication0::::rds2_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service3",
			  "targetTopokey": "communication0",
			  "targetUid": "rds2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  },
		  {
			"uid": "Service4_communication0::::rds2_communication0",
			"metadata": {
			  "kind": "communication",
			  "sourceTopokey": "communication0",
			  "sourceUid": "Service4",
			  "targetTopokey": "communication0",
			  "targetUid": "rds2",
			  "tags": {
				"avg_bindwith": "4kb/s"
			  }
			}
		  }
		]
	  }
	}
  }`
	return jsonNodeData
}

var algoParamErr string = `{
  "HopCount1": 2,
  "BFSNarrow1": false,
  "task1": {
    "task_id": "111",
    "cloud_service": "CCE",
    "metadata": {
      "name": "xxxx",
      "status": "xxxx",
      "scene": "xxx",
      "creator": "xxx",
      "core_entity_name": "ELB",
      "core_entity_id": "ELB",
      "create_time": "xxx",
      "start_time": "xxx",
      "update_time": "xxx",
      "end_time": 1111111,
      "annotations": {
        "description": "xxxx"
      }
    }
  }
}`

var algoParam string = `{
    "HopCount": 2,
    "BFSNarrow": false,
    "task": {
      "task_id": "111",
      "cloud_service": "CCE",
      "metadata": {
        "name": "xxxx",
        "status": "xxxx",
        "scene": "xxx",
        "creator": "xxx",
        "core_entity_name": "ELB",
        "core_entity_id": "ELB",
        "create_time": "xxx",
        "start_time": "xxx",
        "update_time": "xxx",
        "end_time": 1111111,
        "annotations": {
          "description": "xxxx"
        }
      }
    }
  }`

var id []string = []string{
	"anomaly-001", "alarm-001", "anomaly-002", "anomaly-003", "event-001",
}

var name []string = []string{
	"CPU-突增", "CPU-突增", "CPU-突增", "流量过载", "流量过载",
}

var level []int64 = []int64{
	3, 1, 1, 1, 2,
}

var ruleid []string = []string{
	"xxx", "xxx", "xxx", "xxx", "xxx",
}

var entityid []string = []string{
	"ELB", "Nginx-ingress1", "Service1", "Service1", "VM1",
}

var entityid2 []string = []string{
	"ELB", "ELB", "Service1", "Service1", "ELB",
}

var typ []string = []string{
	"anomaly", "alarm", "alarm", "alarm", "event",
}

var annotation []string = []string{
	`{
        "description": "xxx",
        "timestamps": [1712455020000, 1712455080000, 1712455140000],
        "value": [80,90,85],
        "index":[15,16,17]
    }`,
	`{
        "description": "xxx",
        "start_time": 1712411288000,
        "end_time": 1712443688000
    }`,
	`{
        "description": "xxx",
        "start_time": 1712440088000,
        "end_time": 1712443688
    }`,
	`{
        "description": "xxx",
        "start_time": 1712454488000,
        "end_time": ""
    }`,
	`{
        "description": "xxx",
        "start_time": 1712454600000,
        "end_time": 1712454960000,
        "create_time": 1712454600000
    }`,
}

func TestRCA(t *testing.T) {
	subTopo := executor.NewGraph()
	subTopo.CreateGraph(mockGetTimeGraph())

	var algoParams executor.AlgoParam
	err := json.Unmarshal([]byte(algoParam), &algoParams)
	if err != nil {
		t.Error("unknow algo_params")
	}

	var algoParamsErr executor.AlgoParam
	err = json.Unmarshal([]byte(algoParamErr), &algoParamsErr)
	if err != nil {
		t.Error("unknow algo_params")
	}

	ts := []int64{1, 2, 3, 4, 5}
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value0", Type: influxql.String},
		influxql.VarRef{Val: "value1", Type: influxql.String},
		influxql.VarRef{Val: "value2", Type: influxql.Integer},
		influxql.VarRef{Val: "value3", Type: influxql.String},
		influxql.VarRef{Val: "value4", Type: influxql.String},
		influxql.VarRef{Val: "value5", Type: influxql.String},
		influxql.VarRef{Val: "value6", Type: influxql.String},
	)
	chunk := executor.NewChunkBuilder(rowDataType).NewChunk("rca")
	chunk.AppendTimes(ts)

	chunk.Column(0).AppendStringValues(id)
	chunk.Column(0).AppendColumnTimes(ts)
	chunk.Column(0).AppendManyNotNil(5)

	chunk.Column(1).AppendStringValues(name)
	chunk.Column(1).AppendColumnTimes(ts)
	chunk.Column(1).AppendManyNotNil(5)

	chunk.Column(2).AppendIntegerValues(level)
	chunk.Column(2).AppendColumnTimes(ts)
	chunk.Column(2).AppendManyNotNil(5)

	chunk.Column(3).AppendStringValues(ruleid)
	chunk.Column(3).AppendColumnTimes(ts)
	chunk.Column(3).AppendManyNotNil(5)

	chunk.Column(4).AppendStringValues(entityid)
	chunk.Column(4).AppendColumnTimes(ts)
	chunk.Column(4).AppendManyNotNil(5)

	chunk.Column(5).AppendStringValues(typ)
	chunk.Column(5).AppendColumnTimes(ts)
	chunk.Column(5).AppendManyNotNil(5)

	chunk.Column(6).AppendStringValues(annotation)
	chunk.Column(6).AppendColumnTimes(ts)
	chunk.Column(6).AppendManyNotNil(5)

	colMap := map[string]int{
		executor.ID:          0,
		executor.Name:        1,
		executor.Level:       2,
		executor.RuleID:      3,
		executor.EntityID:    4,
		executor.Type:        5,
		executor.Annotations: 6,
	}

	expNodes := []string{
		"ELB",
		"rds1",
		"VM1",
		"VM2",
		"VM3",
		"VM4",
		"Nginx-ingress1",
		"Nginx-ingress2",
		"Service1",
		"Service2",
		"Service3",
		"Service4",
	}
	expEdges := []string{
		"ELB_source0::::VM1_source0",
		"ELB_communication0::::Nginx-ingress1_communication0",
		"ELB_communication0::::Nginx-ingress2_communication0",
		"Nginx-ingress1_source0::::VM2_source0",
		"Nginx-ingress1_communication0::::Service1_communication0",
		"Nginx-ingress1_communication0::::Service2_communication0",
		"Nginx-ingress2_communication0::::Service3_communication0",
		"Nginx-ingress2_communication0::::Service4_communication0",
		"Nginx-ingress2_communication0::::VM3_communication0",
		"Service1_communication0::::VM2_communication0",
		"Service1_communication0::::rds1_communication0",
		"Service2_communication0::::VM2_communication0",
		"Service2_communication0::::rds1_communication0",
		"Service3_communication0::::VM3_communication0",
		"Service4_communication0::::VM3_communication0",
		"rds1_communication0::::VM4_communication0",
	}

	chunks := []executor.Chunk{chunk}
	_, err = executor.FaultDemarcation(chunks, subTopo, algoParamsErr, colMap)
	assert.EqualError(t, err, "RCA Error: meta not found in algoParams")

	check := func(t *testing.T, ret *executor.Graph, expNodes []string, expEdges []string) {
		if len(ret.Nodes) != len(expNodes) {
			t.Error("ret error from FaultDemarcation")
		}
		for _, uid := range expNodes {
			if _, ok := ret.Nodes[uid]; !ok {
				t.Error("ret error from FaultDemarcation", uid)
			}
		}

		if len(ret.Edges) != len(expEdges) {
			t.Error("ret error from FaultDemarcation")
		}
		for _, uid := range expEdges {
			if _, ok := ret.Edges[uid]; !ok {
				t.Error("ret error from FaultDemarcation", uid)
			}
		}
	}

	ret, err := executor.FaultDemarcation(chunks, subTopo, algoParams, colMap)
	assert.NoError(t, err)
	check(t, ret, expNodes, expEdges)

	chunk.Column(4).Reset()
	chunk.Column(4).AppendStringValues(entityid2)
	chunk.Column(4).AppendColumnTimes(ts)
	chunk.Column(4).AppendManyNotNil(5)

	ret, err = executor.FaultDemarcation(chunks, subTopo, algoParams, colMap)
	assert.NoError(t, err)
	check(t, ret, expNodes, expEdges)
}
