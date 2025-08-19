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

package executor

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/smartystreets/goconvey/convey"
)

func TestGraph(t *testing.T) {
	graph := &Graph{
		Nodes: make(map[string]GraphNode),
		Edges: make(map[string]GraphEdge),
	}
	// JSON data
	jsonNodeData := `{
    "data": {
      	"resultUid": "eadee230-db0a-40df-887c-9958e1d872df",
		"metadata": {
      		"region": "cn-north-1",
      		"timestamp": "1724360595038",
			"topokeys": [ 
				"source0",
				"source1"
    		]
		},
     	 "graph": {
          	"vertex": [
				{
				  "uid": "vm_a6a7f584",
				  "metadata": {
					"kind": "Node",
					"region": "cn-north-1", 
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "vm_a23fewfq1",
				  "metadata": {
					"kind": "Node",
					"region": "cn-north-1", 
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_e253075e",
				  "metadata": {
					"kind": "Pod",
					"region": "cn-north-1", 
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_9a254377",
				  "metadata": {
					"kind": "Pod",
					"region": "cn-north-1", 
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_3h87d980",
				  "metadata": {
					"kind": "Pod",
					"region": "cn-north-1", 
					"tags": { "1": "1" }
				  }
				}
            ],
			"edges": [
				{
				  "uid": "pod_e253075e_source0::::vm_a6a7f584_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "pod_e253075e",
					"targetTopokey": "source0",
					"targetUid": "vm_a6a7f584",
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_9a254377_source0::::vm_a6a7f584_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "pod_9a254377",
					"targetTopokey": "source0",
					"targetUid": "vm_a6a7f584",
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "vm_a6a7f584_source0::::vm_a23fewfq1_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "vm_a6a7f584",
					"targetTopokey": "source0",
					"targetUid": "vm_a23fewfq1",
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_e253075e_source0::::pod_3h87d980_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "pod_e253075e",
					"targetTopokey": "source0",
					"targetUid": "pod_3h87d980",
					"tags": { "1": "1" }
				  }
				},
				{
				  "uid": "pod_3h87d980_source0::::vm_a23fewfq1_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "pod_3h87d980",
					"targetTopokey": "source0",
					"targetUid": "vm_a23fewfq1",
					"tags": { "1": "1" }
				  }
				}
          ]
      }
    }
  }`

	success, _ := graph.CreateGraph(jsonNodeData)
	nodeInfo := graph.GetNodeInfo("vm_a6a7f584")
	edgeInfo := graph.GetEdgeInfo("pod_e253075e_source0::::vm_a6a7f584_source0")
	subgraph, err := graph.MultiHopFilter("vm_a6a7f584", 1, nil, nil)
	assertEqual(t, true, success)
	assertEqual(t, err, nil)

	expectNodeResult := &GraphNode{
		Uid: "vm_a6a7f584",
		MetaData: NodeMetaData{
			Kind:   "Node",
			Region: "Node",
			Tags:   map[string]string{"1": "1"},
		},
		OutEdges: []string{"vm_a6a7f584_vm_a23fewfq1"},
		InEdges:  []string{"pod_e253075e_vm_a6a7f584", "pod_9a254377_vm_a6a7f584"},
	}

	expectEdgeResult := &GraphEdge{
		Uid: "pod_e253075e_source0::::vm_a6a7f584_source0",
		MetaData: EdgeMetaData{
			Kind:          "LOCATE",
			SourceTopoKey: "source0",
			SourceUid:     "pod_e253075e",
			TargetTopoKey: "source0",
			TargetUid:     "vm_a6a7f584",
			Tags:          map[string]string{"1": "1"},
		},
	}

	expectSubgraphResult := &Graph{
		Nodes: map[string]GraphNode{
			"vm_a6a7f584": {
				Uid: "vm_a6a7f584",
				MetaData: NodeMetaData{
					Kind:   "Node",
					Region: "cn-north-1",
					Tags:   map[string]string{"1": "1"},
				},
				OutEdges: []string{"vm_a6a7f584_vm_a23fewfq1"},
				InEdges:  []string{"pod_e253075e_vm_a6a7f584", "pod_9a254377_vm_a6a6a7f584"},
			},
			"vm_a23fewfq1": {
				Uid: "vm_a23fewfq1",
				MetaData: NodeMetaData{
					Kind:   "Node",
					Region: "cn-north-1",
					Tags:   map[string]string{"1": "1"},
				},
				OutEdges: []string{},
				InEdges:  []string{"vm_a6a7f584_vm_a23fewfq1", "pod_3h87d980_vm_a23fewfq1"},
			},
			"pod_e253075e": {
				Uid: "pod_e253075e",
				MetaData: NodeMetaData{
					Kind:   "Pod",
					Region: "cn-north-1",
					Tags:   map[string]string{"1": "1"},
				},
				OutEdges: []string{"pod_e253075e_pod_3h87d980", "pod_e253075e_vm_a6a7f584"},
				InEdges:  []string{},
			},
			"pod_9a254377": {
				Uid: "pod_9a254377",
				MetaData: NodeMetaData{
					Kind:   "Pod",
					Region: "cn-north-1",
					Tags:   map[string]string{"1": "1"},
				},
				OutEdges: []string{"pod_9a254377_vm_a6a7f584"},
				InEdges:  []string{},
			},
		},
		Edges: map[string]GraphEdge{
			"vm_a6a7f584_source0::::vm_a23fewfq1_source0": {
				Uid: "vm_a6a7f584_source0::::vm_a23fewfq1_source0",
				MetaData: EdgeMetaData{
					Kind:          "LOCATE",
					SourceTopoKey: "source0",
					SourceUid:     "vm_a6a7f584",
					TargetTopoKey: "source0",
					TargetUid:     "vm_a23fewfq1",
					Tags:          map[string]string{"1": "1"},
				},
			},
			"pod_e253075e_source0::::vm_a6a7f584_source0": {
				Uid: "pod_e253075e_source0::::vm_a6a7f584_source0",
				MetaData: EdgeMetaData{
					Kind:          "LOCATE",
					SourceTopoKey: "source0",
					SourceUid:     "pod_e253075e",
					TargetTopoKey: "source0",
					TargetUid:     "vm_a6a7f584",
					Tags:          map[string]string{"1": "1"},
				},
			},
			"pod_9a254377_source0::::vm_a6a7f584_source0": {
				Uid: "pod_9a254377_source0::::vm_a6a7f584_source0",
				MetaData: EdgeMetaData{
					Kind:          "LOCATE",
					SourceTopoKey: "source0",
					SourceUid:     "pod_9a254377",
					TargetTopoKey: "source0",
					TargetUid:     "vm_a6a7f584",
					Tags:          map[string]string{"1": "1"},
				},
			},
		},
	}

	assertEqual(t, expectNodeResult.MetaData.Kind, nodeInfo.MetaData.Kind)
	assertEqual(t, expectNodeResult.Uid, nodeInfo.Uid)
	assertEqual(t, len(expectNodeResult.OutEdges), len(nodeInfo.OutEdges))
	assertEqual(t, len(expectNodeResult.InEdges), len(nodeInfo.InEdges))
	assertEqual(t, expectEdgeResult.MetaData.TargetUid, edgeInfo.MetaData.TargetUid)
	assertEqual(t, expectEdgeResult.MetaData.SourceUid, edgeInfo.MetaData.SourceUid)

	if len(subgraph.Nodes) != len(expectSubgraphResult.Nodes) {
		t.Errorf("Subgraph Nodes length mismatch: expected %d, got %d", len(expectSubgraphResult.Nodes), len(subgraph.Nodes))
	} else {
		for uid, expectedNode := range expectSubgraphResult.Nodes {
			actualNode, ok := subgraph.Nodes[uid]
			if !ok {
				t.Errorf("Node %s not found in subgraph", uid)
				continue
			}
			assertEqual(t, expectedNode.MetaData.Kind, actualNode.MetaData.Kind)
			assertEqual(t, expectedNode.Uid, actualNode.Uid)
			assertEqual(t, len(expectedNode.OutEdges), len(actualNode.OutEdges))
			assertEqual(t, len(expectedNode.InEdges), len(actualNode.InEdges))
		}
	}

	if len(subgraph.Edges) != len(expectSubgraphResult.Edges) {
		t.Errorf("Subgraph Edges length mismatch: expected %d, got %d", len(expectSubgraphResult.Edges), len(subgraph.Edges))
	} else {
		for edgeId, expectedEdge := range expectSubgraphResult.Edges {
			actualEdge, ok := subgraph.Edges[edgeId]
			if !ok {
				t.Errorf("Edge %s not found in subgraph", edgeId)
				continue
			}
			assertEqual(t, expectedEdge.MetaData.TargetUid, actualEdge.MetaData.TargetUid)
			assertEqual(t, expectedEdge.MetaData.SourceUid, actualEdge.MetaData.SourceUid)
		}
	}
}

func TestGraphBranch(t *testing.T) {
	convey.Convey("sourceNode does not exist", t, func() {
		graph := &Graph{
			Nodes: make(map[string]GraphNode),
			Edges: make(map[string]GraphEdge),
		}

		jsonNodeData := `{
        "data": {
		   "graph": {
			  "vertex": [
				{
				  "uid": "vm_a6a7f584",
				  "metadata": {
					"kind": "Node",
					"region": "cn-north-1",
					"tags": { "1": "1" }
				  }
				}
			  ],
			  "edges": [
				  {
				  "uid": "pod_e253075e_source0::::vm_a6a7f584_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourceTopokey": "source0",
					"sourceUid": "pod_e253075e",
					"targetTopokey": "source0",
					"targetUid": "vm_a6a7f584",
					"tags": { "1": "1" }
				  }
              }
           ]
        }
      }
    }`
		success, err := graph.CreateGraph(jsonNodeData)
		assertEqual(t, false, success)
		assertEqual(t, "this edge's sourceNode does not exist", err.Error())
	})
	convey.Convey("targetNode does not exist", t, func() {
		graph := &Graph{
			Nodes: make(map[string]GraphNode),
			Edges: make(map[string]GraphEdge),
		}

		jsonNodeData1 := `{
        "data": {
		   "graph": {
			  "vertex": [
				{
				  "uid": "pod_e253075e",
				  "metadata": {
					"kind": "Pod",
					"region": "cn-north-", 
					"tags": { "1": "1" }
				  }
				}
			  ],
			  "edges": [
				  {
				  "uid": "pod_e253075e_source0::::vm_a6a7f584_source0",
				  "metadata": {
					"kind": "LOCATE",
					"sourcetopokey": "source0",
					"sourceuid": "pod_e253075e",
					"targetTopokey": "source0",
					"targetUid": "vm_a6a7f584",
					"tags": { "1": "1" }
				  }
			  }]
            }
           }
        }`
		success, err := graph.CreateGraph(jsonNodeData1)
		assertEqual(t, false, success)
		assertEqual(t, "this edge's targetNode does not exist", err.Error())
	})

	convey.Convey("error parsing JSON", t, func() {
		graph := &Graph{
			Nodes: make(map[string]GraphNode),
			Edges: make(map[string]GraphEdge),
		}

		jsonNodeData2 := `{
        "data": {
		   "graph": {
			  "vertex": [
				{
				  "uid": "pod_e253075e",
				  "metadata": {
					"kind": "Pod",
					"region": "cn-north-", 
					"tags": { "1": "1" }
				  }
				}
			  ],
			  "edges": [
				  {
				  "uid": "pod_e253075e_source0::::vm_a6a7f584_source0",
				  "metadata": {
				  }
			  ]
           }
         }
        }`
		success, err := graph.CreateGraph(jsonNodeData2)
		assertEqual(t, false, success)
		assertEqual(t, "error parsing JSON", err.Error())
	})

}

func TestCheckFilterCondition(t *testing.T) {
	//use mock data
	graph := &Graph{
		Nodes: make(map[string]GraphNode),
		Edges: make(map[string]GraphEdge),
	}
	graph.CreateGraph(mockGetTimeGraph())
	convey.Convey("node common condition", t, func() {
		sql := "graph 2 'Nginx-ingress2' node kind = 'Pod' and uid = 'Service3' or uid = 'rds2' or uid = 'ELB'"
		stmt := getStmt(t, sql)
		subGraph, err := graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 4, len(subGraph.Nodes))
		assertEqual(t, 3, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Nginx-ingress2' node namepod = 'podService3' or namepod = 'pod-system'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 3, len(subGraph.Nodes))
		assertEqual(t, 2, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Nginx-ingress2' node kind != 'Pod' and uid !='vm3'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 1, len(subGraph.Nodes))
		assertEqual(t, 0, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Nginx-ingress2' node namepod != 'podService3'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 7, len(subGraph.Nodes))
		assertEqual(t, 7, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 10 'Nginx-ingress2'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 14, len(subGraph.Nodes))
		assertEqual(t, 19, len(subGraph.Edges))
		assertEqual(t, nil, err)

	})

	convey.Convey("edge common condition", t, func() {
		sql := "graph 2 'Nginx-ingress2' edge kind = 'communication'"
		stmt := getStmt(t, sql)
		subGraph, err := graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 6, len(subGraph.Nodes))
		assertEqual(t, 6, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Service4' edge kind != 'communication'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 4, len(subGraph.Nodes))
		assertEqual(t, 3, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Service3' edge edgeprop = 'edge-com0' or edgeprop = 'edge-com1'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 4, len(subGraph.Nodes))
		assertEqual(t, 3, len(subGraph.Edges))
		assertEqual(t, nil, err)

		sql = "graph 2 'Service3' edge kind != 'communication' or edgeprop != 'edge-com1'"
		stmt = getStmt(t, sql)
		subGraph, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 5, len(subGraph.Nodes))
		assertEqual(t, 6, len(subGraph.Edges))
		assertEqual(t, nil, err)

	})

	convey.Convey("error condition", t, func() {
		sql := "graph 2 'Service4' edge 'kind' = 'communication'"
		stmt := getStmt(t, sql)
		_, err := graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, "unsupported edge or node filter condition syntax", err.Error())

		sql = "graph 2 'Nginx-ingress2' node namepod = podService3"
		stmt = getStmt(t, sql)
		_, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, "unsupported edge or node filter condition syntax", err.Error())

		sql = "graph 2 'Nginx-ingress2' node kinds > 'podService3'"
		stmt = getStmt(t, sql)
		_, err = graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, "unsupported operator: >", err.Error())
	})

	convey.Convey("complex condition", t, func() {
		sql := "graph 2 'Service3' node 'Nginx-ingress2' = uid or uid = 'vm3' or uid = 'rds2' and ('pod-system' = namepod or namespace = 'kube-system' or namedb = 'db3') edge 'LOCATE' = kind"
		stmt := getStmt(t, sql)
		subGraph, err := graph.MultiHopFilter(stmt.StartNodeId, stmt.HopNum, stmt.NodeCondition, stmt.EdgeCondition)
		assertEqual(t, 3, len(subGraph.Nodes))
		assertEqual(t, 2, len(subGraph.Edges))
		assertEqual(t, nil, err)
	})

}

func getStmt(t *testing.T, sql string) *influxql.GraphStatement {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}
	stmt := q.Statements[0].(*influxql.GraphStatement)
	return stmt
}

func BenchmarkMultiHopFilter(b *testing.B) {
	graphData2w := generateGraphData(20000, 1)
	data2w, _ := json.Marshal(graphData2w)
	graphData200w := generateGraphData(20000, 100)
	data200w, _ := json.Marshal(graphData200w)
	graph := NewGraph()

	nodeCondition := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val:   Kind,
			Alias: "",
		},
		RHS: &influxql.StringLiteral{
			Val: "Pod",
		},
		ReturnBool: false,
	}

	edgeCondition := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val:   Kind,
			Alias: "",
		},
		RHS: &influxql.StringLiteral{
			Val: "LOCATE",
		},
		ReturnBool: false,
	}

	edgePropCondition := &influxql.BinaryExpr{
		Op: influxql.EQ,
		LHS: &influxql.VarRef{
			Val:   "namedb",
			Alias: "",
		},
		RHS: &influxql.StringLiteral{
			Val: "orientprop0",
		},
		ReturnBool: false,
	}

	b.Run("nodenum:2w,edgenum:2w,hopNum:3,CondExist:no", func(b *testing.B) {
		graph.CreateGraph(string(data2w))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			subGraph, err := graph.MultiHopFilter("vertex-338", 3, nil, nil)
			if subGraph == nil || err != nil {
				b.Fatal("nil subGraph is not allowed", err)
			}
		}
	})

	b.Run("nodenum:2w,edgenum:2w,hopNum:3,CondExist:yes", func(b *testing.B) {
		graph.CreateGraph(string(data2w))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			subGraph, err := graph.MultiHopFilter("vertex-17760", 3, nodeCondition, edgeCondition)
			if subGraph == nil || err != nil {
				b.Fatal("nil subGraph is not allowed", err)
			}
		}
	})

	b.Run("nodenum:2w,edgenum:200w,hopNum:3,CondExist:no", func(b *testing.B) {
		graph.CreateGraph(string(data200w))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			subGraph, err := graph.MultiHopFilter("vertex-269", 3, nil, nil)
			if subGraph == nil || err != nil {
				b.Fatal("nil subGraph is not allowed", err)
			}
		}
	})

	b.Run("nodenum:2w,edgenum:200w,hopNum:3,CondExist:yes", func(b *testing.B) {
		graph.CreateGraph(string(data200w))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			subGraph, err := graph.MultiHopFilter("vertex-12310", 3, nodeCondition, edgeCondition)
			if subGraph == nil || err != nil {
				b.Fatal("nil subGraph is not allowed", err)
			}
		}
	})

	b.Run("nodenum:2w,edgenum:200w,hopNum:3,PropCondExist:yes", func(b *testing.B) {
		graph.CreateGraph(string(data200w))

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			subGraph, err := graph.MultiHopFilter("vertex-123", 3, nil, edgePropCondition)
			if subGraph == nil || err != nil {
				b.Fatal("nil subGraph is not allowed", err)
			}
		}
	})
}

// ensure num of per node's out-edge less than nodeNum
func generateGraphData(nodeNum int, edgesPerNode int) Response {
	var (
		NODE_KINDS = []string{"Node", "Pod"}
		EDGE_KINDS = []string{"LOCATE", "communication"}
	)

	PROPERTY_TEMPLATE := map[string]string{
		"namedb":    "orientprop0",
		"proptest1": "orientprop1",
		"proptest2": "orientprop2",
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	nodes := make([]GraphNode, nodeNum)
	nodeUIDs := make([]string, nodeNum)
	for i := 0; i < nodeNum; i++ {
		kind := NODE_KINDS[rng.Intn(len(NODE_KINDS))]
		uid := fmt.Sprintf("vertex-%d", i)

		nodeProperties := make(map[string]string, len(PROPERTY_TEMPLATE))
		for k, v := range PROPERTY_TEMPLATE {
			nodeProperties[k] = v
		}

		for k := range nodeProperties {
			if rng.Float64() > 0.3 {
				nodeProperties[k] = fmt.Sprintf("nodeprop%d", rng.Intn(100)+1)
			}
		}

		nodes[i] = GraphNode{
			Uid: uid,
			MetaData: NodeMetaData{
				Kind:   kind,
				Region: "cn-north-1",
				Tags:   nodeProperties,
			},
			OutEdges: []string{},
			InEdges:  []string{},
		}
		nodeUIDs[i] = uid
	}

	edges := make([]GraphEdge, edgesPerNode*nodeNum)
	for i := 0; i < nodeNum; i++ {
		sourceUID := nodeUIDs[i]
		seen := make(map[string]struct{})
		for j := 0; j < edgesPerNode; j++ {
			var targetUID string
			for {
				targetUID = nodeUIDs[rng.Intn(nodeNum)]
				_, exist := seen[targetUID]
				if targetUID != sourceUID && !exist {
					seen[targetUID] = struct{}{}
					break
				}
			}
			kind := EDGE_KINDS[rng.Intn(len(EDGE_KINDS))]

			edgeProperties := make(map[string]string, len(PROPERTY_TEMPLATE))
			for k, v := range PROPERTY_TEMPLATE {
				edgeProperties[k] = v
			}

			for k := range edgeProperties {
				if rng.Float64() > 0.3 {
					edgeProperties[k] = fmt.Sprintf("edgeprop%d", rng.Intn(100)+1)
				}
			}

			uid := fmt.Sprintf("%s_source0::::%s_source0", sourceUID, targetUID)
			edges[i*edgesPerNode+j] = GraphEdge{
				Uid: uid,
				MetaData: EdgeMetaData{
					Kind:          kind,
					SourceTopoKey: "source0",
					SourceUid:     sourceUID,
					TargetTopoKey: "source0",
					TargetUid:     targetUID,
					Tags:          edgeProperties,
				},
			}
		}
	}

	data := Response{
		Data: GraphData{
			ResultUid: "eadee230-db0a-40df-887c-9958e1d872df",
			MetaData: MetaData{
				Region:    "cn-north-1",
				Timestamp: "1724360595038",
				Topokeys:  []string{"source1", "source0"},
			},
			Graph: TopoInfo{
				Vertex: nodes,
				Edges:  edges,
			},
		},
	}
	return data
}
