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
	"container/list"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

type GraphNode struct {
	Uid      string       `json:"uid"`
	MetaData NodeMetaData `json:"metadata"`
	OutEdges []string
	InEdges  []string
}

type NodeMetaData struct {
	Kind   string            `json:"kind"`
	Region string            `json:"region"`
	Tags   map[string]string `json:"tags"`
}

type GraphEdge struct {
	Uid      string       `json:"uid"`
	MetaData EdgeMetaData `json:"metadata"`
}

type EdgeMetaData struct {
	Kind          string            `json:"kind"`
	SourceTopoKey string            `json:"sourceTopoKey"`
	SourceUid     string            `json:"sourceUid"`
	TargetTopoKey string            `json:"targetTopoKey"`
	TargetUid     string            `json:"targetUid"`
	Tags          map[string]string `json:"tags"`
}

type Property struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GraphData struct {
	ResultUid string   `json:"resultUid"`
	MetaData  MetaData `json:"metadata"`
	Graph     TopoInfo `json:"graph"`
}

type TopoInfo struct {
	Vertex []GraphNode `json:"vertex"`
	Edges  []GraphEdge `json:"edges"`
}

type Response struct {
	Data GraphData `json:"data"`
}

type MetaData struct {
	Region    string   `json:"region"`
	Timestamp string   `json:"timestamp"`
	Topokeys  []string `json:"topokeys"`
}

type Graph struct {
	Nodes map[string]GraphNode
	Edges map[string]GraphEdge
}

const (
	Kind     string = "kind"
	Uid      string = "uid"
	IncomeOp string = "income"
	OutGoOp  string = "outgo"
)

type IGraph interface {
	GetNodeInfo(id string) *GraphNode
	GetEdgeInfo(id string) *GraphEdge
	BatchInsertNodes(graphData GraphData) bool
	BatchInsertEdges(graphData GraphData) (bool, error)
	CreateGraph(jsonGraphData string) (bool, error)
	MultiHopFilter(startNodeId string, hopNum int, nodeCondition influxql.Expr, edgeCondition influxql.Expr) (*Graph, error)
	GraphToRows() models.Rows
}

func NewGraph() *Graph {
	return &Graph{
		Nodes: make(map[string]GraphNode),
		Edges: make(map[string]GraphEdge),
	}
}

func (G *Graph) GetNodeInfo(id string) *GraphNode {
	if node, ok := G.Nodes[id]; ok {
		return &node
	}
	return nil
}

func (G *Graph) GetEdgeInfo(id string) *GraphEdge {
	if edge, ok := G.Edges[id]; ok {
		return &edge
	}
	return nil
}

func (G *Graph) BatchInsertNodes(graphData GraphData) bool {
	for _, node := range graphData.Graph.Vertex {
		G.Nodes[node.Uid] = node
	}
	return true
}

func (G *Graph) BatchInsertEdges(graphData GraphData) (bool, error) {
	for _, edge := range graphData.Graph.Edges {
		G.Edges[edge.Uid] = edge
	}
	for edgeId, edge := range G.Edges {
		sourceNode, ok := G.Nodes[edge.MetaData.SourceUid]
		if !ok {
			// todo: handle the condition where the source node or target node does not exist
			return false, errors.New("this edge's sourceNode does not exist")
		}
		sourceNode.OutEdges = append(sourceNode.OutEdges, edgeId)
		G.Nodes[edge.MetaData.SourceUid] = sourceNode
		targetNode, ok := G.Nodes[edge.MetaData.TargetUid]
		if !ok {
			return false, errors.New("this edge's targetNode does not exist")
		}
		targetNode.InEdges = append(targetNode.InEdges, edgeId)
		G.Nodes[edge.MetaData.TargetUid] = targetNode
	}
	return true, nil
}

func (G *Graph) CreateGraph(jsonGraphData string) (bool, error) {
	var resp Response
	err := json.Unmarshal([]byte(jsonGraphData), &resp)
	if err != nil {
		return false, errors.New("error parsing JSON")
	}
	if !G.BatchInsertNodes(resp.Data) {
		return false, nil
	}
	if ok, err := G.BatchInsertEdges(resp.Data); err != nil || !ok {
		return false, err
	}
	return true, nil
}

func (G *Graph) MultiHopFilter(startNodeId string, hopNum int, nodeCondition influxql.Expr, edgeCondition influxql.Expr) (*Graph, error) {
	startNode, ok := G.Nodes[startNodeId]
	if !ok {
		return nil, fmt.Errorf("MultiHopFilter startNodeId not found %s", startNodeId)
	}

	edgesBySource := make(map[string][]GraphEdge)
	edgesByTarget := make(map[string][]GraphEdge)
	for _, edge := range G.Edges {
		edgesBySource[edge.MetaData.SourceUid] = append(edgesBySource[edge.MetaData.SourceUid], edge)
		edgesByTarget[edge.MetaData.TargetUid] = append(edgesByTarget[edge.MetaData.TargetUid], edge)
	}

	visited := make(map[string]struct{})
	queue := list.New()
	queue.PushBack(startNode)
	visited[startNodeId] = struct{}{}

	subgraph := &Graph{
		Nodes: make(map[string]GraphNode),
		Edges: make(map[string]GraphEdge),
	}
	subgraph.Nodes[startNodeId] = startNode

	for queue.Len() > 0 && hopNum > 0 {
		levelSize := queue.Len()
		for i := 0; i < levelSize; i++ {
			current, ok := queue.Remove(queue.Front()).(GraphNode)
			if !ok {
				return nil, errors.New("error: queue.Remove(queue.Front()) is not of type Node")
			}

			// check outgoing edges
			outGoingEdges, ok := edgesBySource[current.Uid]
			if ok && outGoingEdges != nil {
				_, err := G.processEdges(outGoingEdges, subgraph, &visited, queue, nodeCondition, edgeCondition, OutGoOp)
				if err != nil {
					return nil, err
				}
			}

			// check incoming edges
			inComingEdges, ok := edgesByTarget[current.Uid]
			if !ok || inComingEdges == nil {
				continue
			}
			_, err := G.processEdges(inComingEdges, subgraph, &visited, queue, nodeCondition, edgeCondition, IncomeOp)
			if err != nil {
				return nil, err
			}
		}

		hopNum--
		if len(visited) == len(G.Nodes) {
			break
		}
	}
	return subgraph, nil
}

func (G *Graph) processEdges(edges []GraphEdge, subgraph *Graph, visited *map[string]struct{}, queue *list.List, nodeCondition influxql.Expr, edgeCondition influxql.Expr, hopDir string) (*Graph, error) {
	for _, edge := range edges {
		isMatch, err := G.isMatchQueryConditions(nodeCondition, edgeCondition, edge, hopDir)
		if err != nil {
			return nil, err
		}
		if !isMatch {
			continue
		}
		var curNode GraphNode
		var curUid string
		if hopDir == IncomeOp {
			curUid = edge.MetaData.SourceUid
			if _, exists := G.Nodes[curUid]; !exists {
				return nil, fmt.Errorf("topo data received from api is incorrect")
			}
			curNode = G.Nodes[curUid]
		} else {
			curUid = edge.MetaData.TargetUid
			if _, exists := G.Nodes[curUid]; !exists {
				return nil, fmt.Errorf("topo data received from api is incorrect")
			}
			curNode = G.Nodes[curUid]
		}
		subgraph.Edges[edge.Uid] = edge
		if _, exists := (*visited)[curUid]; exists {
			continue
		}
		subgraph.Nodes[curNode.Uid] = curNode
		(*visited)[curNode.Uid] = struct{}{}
		queue.PushBack(curNode)
	}
	return subgraph, nil
}

func (G *Graph) isMatchQueryConditions(nodeCondition influxql.Expr, edgeCondition influxql.Expr, edge GraphEdge, hopDir string) (bool, error) {
	isNodeMatch, err := G.isFilterConditionMatch(nodeCondition, edge, hopDir, G.checkNodeFilterCondition)
	if err != nil {
		return false, err
	}
	isEdgeMatch, err := G.isFilterConditionMatch(edgeCondition, edge, hopDir, G.checkEdgeFilterCondition)
	if err != nil {
		return false, err
	}
	isMatch := isNodeMatch && isEdgeMatch
	return isMatch, nil
}

func (G *Graph) isFilterConditionMatch(Cond influxql.Expr, edge GraphEdge, hopDir string, handler func(left string, right string, edge GraphEdge, op influxql.Token, hopDir string) (bool, error)) (bool, error) {
	checkField := func(expr influxql.Expr) (bool, error) {

		binaryExpr, ok := expr.(*influxql.BinaryExpr)
		if !ok {
			return false, errno.NewError(errno.ConvertToBinaryExprFailed, expr)
		}

		var varRef *influxql.VarRef
		var literal *influxql.StringLiteral
		switch binaryExpr.Op {
		case influxql.EQ, influxql.NEQ:
			if varRef, ok = binaryExpr.LHS.(*influxql.VarRef); !ok {
				if varRef, ok = binaryExpr.RHS.(*influxql.VarRef); !ok {
					return false, errors.New("unsupported edge or node filter condition syntax")
				}
			}
			if literal, ok = binaryExpr.RHS.(*influxql.StringLiteral); !ok {
				if literal, ok = binaryExpr.LHS.(*influxql.StringLiteral); !ok {
					return false, errors.New("unsupported edge or node filter condition syntax")
				}
			}
			return handler(varRef.Val, literal.Val, edge, binaryExpr.Op, hopDir)
		default:
			return false, fmt.Errorf("unsupported operator: %s", binaryExpr.Op)
		}
	}
	return G.checkCondition(Cond, checkField)
}

func (G *Graph) checkEdgeFilterCondition(varRef string, literal string, edge GraphEdge, op influxql.Token, hopDir string) (bool, error) {
	switch varRef {
	case Kind:
		return (op == influxql.EQ && edge.MetaData.Kind == literal) || (op == influxql.NEQ && edge.MetaData.Kind != literal), nil
	default:
		return G.checkEdgeProperties(varRef, literal, edge, op)
	}
}

func (G *Graph) checkNodeFilterCondition(varRef string, literal string, edge GraphEdge, op influxql.Token, hopDir string) (bool, error) {
	var nodeUid string
	if hopDir == IncomeOp {
		nodeUid = edge.MetaData.SourceUid
	} else {
		nodeUid = edge.MetaData.TargetUid
	}
	switch varRef {
	case Kind:
		return (op == influxql.EQ && G.Nodes[nodeUid].MetaData.Kind == literal) || (op == influxql.NEQ && G.Nodes[nodeUid].MetaData.Kind != literal), nil
	case Uid:
		return (op == influxql.EQ && nodeUid == literal) || (op == influxql.NEQ && nodeUid != literal), nil
	default:
		return G.checkNodeProperties(varRef, literal, nodeUid, op)
	}
}

func (G *Graph) checkNodeProperties(key string, val string, nodeUid string, op influxql.Token) (bool, error) {
	nodeProp, ok := G.Nodes[nodeUid]
	if !ok {
		return false, errors.New("the nodeUid not found")
	}
	for k, v := range nodeProp.MetaData.Tags {
		if k == key {
			return (op == influxql.EQ && v == val) || (op == influxql.NEQ && v != val), nil
		}
	}
	if op == influxql.NEQ {
		return true, nil
	}
	return false, nil
}

func (G *Graph) checkEdgeProperties(key string, val string, edge GraphEdge, op influxql.Token) (bool, error) {
	for k, v := range edge.MetaData.Tags {
		if k == key {
			return (op == influxql.EQ && v == val) || (op == influxql.NEQ && v != val), nil
		}
	}
	if op == influxql.NEQ {
		return true, nil
	}
	return false, nil
}

func (G *Graph) checkCondition(expr influxql.Expr, checkField func(influxql.Expr) (bool, error)) (bool, error) {
	if expr == nil {
		return true, nil
	}

	if binaryExpr, ok := expr.(*influxql.BinaryExpr); ok {
		switch binaryExpr.Op {
		case influxql.EQ, influxql.NEQ:
			return checkField(expr)
		case influxql.OR:
			leftResult, err := G.checkCondition(binaryExpr.LHS, checkField)
			if err != nil {
				return false, err
			}
			if leftResult {
				return true, nil
			}
			rightResult, err := G.checkCondition(binaryExpr.RHS, checkField)
			if err != nil {
				return false, err
			}
			return rightResult, nil
		case influxql.AND:
			leftResult, err := G.checkCondition(binaryExpr.LHS, checkField)
			if err != nil {
				return false, err
			}
			if !leftResult {
				return false, nil
			}
			rightResult, err := G.checkCondition(binaryExpr.RHS, checkField)
			if err != nil {
				return false, err
			}
			return rightResult, nil
		default:
			return false, fmt.Errorf("unsupported operator: %s", binaryExpr.Op)
		}
	}
	// handle ParenExpr
	if parenExpr, ok := expr.(*influxql.ParenExpr); ok {
		return G.checkCondition(parenExpr.Expr, checkField)
	}

	return checkField(expr)
}

func (G *Graph) GraphToRows() models.Rows {
	nodeRow := &models.Row{Columns: []string{"Uid", "MetaData"}}
	for _, node := range G.Nodes {
		nodeRow.Values = append(nodeRow.Values, []interface{}{node.Uid, node.MetaData})
	}

	edgeRow := &models.Row{Columns: []string{"Uid", "MetaData"}}
	for _, edge := range G.Edges {
		edgeRow.Values = append(edgeRow.Values, []interface{}{edge.Uid, edge.MetaData})
	}
	return models.Rows{nodeRow, edgeRow}
}

func (G *Graph) addToBufMap(bufMap map[interface{}]struct{}) {
	for _, v := range G.Nodes {
		(bufMap)[v.Uid] = struct{}{}
	}
}

func mockGetTimeGraph() string {
	jsonNodeData := `{
    "data": {
		  "resultUid": "eadee230-db0a-40df-887c-9958e1d872df",
		  "metadata": {
			"region": "global",
			"timestamp": "1753080098056",
			"topokeys": ["source0", "source1"]
		  },
		  "graph": {
			"vertex": [
			  {
				"uid": "vm1",
				"metadata": {
				  "kind": "Node",
				  "region": "cn-north-1",
				  "tags": { "namedb": "db1", "prop1": "value1" }
				}
			  },
			  {
				"uid": "vm2",
				"metadata": {
				  "kind": "Node",
				  "region": "cn-north-1",
				  "tags": { "namedb": "db2", "propvm2": "valuevm2" }
				}
			  },
			  {
				"uid": "vm3",
				"metadata": {
				  "kind": "Node",
				  "region": "cn-east-1",
				  "tags": { "namedb": "db3", "namedbtest": "db3test" }
				}
			  },
			  {
				"uid": "vm4",
				"metadata": {
				  "kind": "Node",
				  "region": "cn-east-2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "vm5",
				"metadata": {
				  "kind": "Node",
				  "region": "cn-south-1",
				  "tags": { "namedb": "db5" }
				}
			  },
			  {
				"uid": "ELB",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-south-2",
				  "tags": { "1": "1", "namedb": "db3" }
				}
			  },
			  {
				"uid": "Nginx-ingress1",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-south-3",
				  "tags": { "namespace": "kube-system", "propingress1": "valueingress1" }
				}
			  },
			  {
				"uid": "Nginx-ingress2",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-east-3",
				  "tags": { "1": "1", "namedb": "db3" }
				}
			  },
			  {
				"uid": "Service1",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-east-1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service2",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-east-2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service3",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-east-2",
				  "tags": { "k1": "1", "namepod": "podService3" }
				}
			  },
			  {
				"uid": "Service4",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-west-2",
				  "tags": { "1": "1", "namepod": "podService6" }
				}
			  },
			  {
				"uid": "rds1",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-west-2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "rds2",
				"metadata": {
				  "kind": "Pod",
				  "region": "cn-west-2",
				  "tags": { "proprds2": "valuerds2", "namepod": "pod-system" }
				}
			  }
			],
			"edges": [
			  {
				"uid": "ELB_source0::::vm1_source0",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "ELB",
				  "targetTopokey": "source0",
				  "targetUid": "vm1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Nginx-ingress1_source1::::vm2_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source1",
				  "sourceUid": "Nginx-ingress1",
				  "targetTopokey": "source1",
				  "targetUid": "vm2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Nginx-ingress2_source0::::vm3_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "Nginx-ingress2",
				  "targetTopokey": "source1",
				  "targetUid": "vm3",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service1_source0::::vm2_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service1",
				  "targetTopokey": "source1",
				  "targetUid": "vm2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service2_source0::::vm2_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service2",
				  "targetTopokey": "source1",
				  "targetUid": "vm2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service3_source0::::vm3_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service3",
				  "targetTopokey": "source1",
				  "targetUid": "vm3",
				  "tags": { "1": "1", "edgeprop": "edge-com2" }
				}
			  },
			  {
				"uid": "Service4_source0::::vm3_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service4",
				  "targetTopokey": "source1",
				  "targetUid": "vm3",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "rds1_source0::::vm4_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "rds1",
				  "targetTopokey": "source1",
				  "targetUid": "vm4",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "rds2_source0::::vm5_source1",
				"metadata": {
				  "kind": "LOCATE",
				  "sourceTopokey": "source0",
				  "sourceUid": "rds2",
				  "targetTopokey": "source1",
				  "targetUid": "vm5",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "ELB_source0::::Nginx-ingress1_source0",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "ELB",
				  "targetTopokey": "source0",
				  "targetUid": "Nginx-ingress1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "ELB_source1::::Nginx-ingress2_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source1",
				  "sourceUid": "ELB",
				  "targetTopokey": "source1",
				  "targetUid": "Nginx-ingress2",
				  "tags": { "1": "1", "edgeprop": "edge-com0" }
				}
			  },
			  {
				"uid": "Nginx-ingress1_source0::::Service1_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Nginx-ingress1",
				  "targetTopokey": "source1",
				  "targetUid": "Service1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Nginx-ingress1_source0::::Service2_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Nginx-ingress1",
				  "targetTopokey": "source1",
				  "targetUid": "Service2",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Nginx-ingress2_source1::::Service3_source0",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source1",
				  "sourceUid": "Nginx-ingress2",
				  "targetTopokey": "source0",
				  "targetUid": "Service3",
				  "tags": { "edgeprop": "edge-com0", "1": "1" }
				}
			  },
			  {
				"uid": "Nginx-ingress2_source0::::Service4_source0",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Nginx-ingress2",
				  "targetTopokey": "source0",
				  "targetUid": "Service4",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service1_source0::::rds1_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service1",
				  "targetTopokey": "source1",
				  "targetUid": "rds1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service2_source0::::rds1_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service2",
				  "targetTopokey": "source1",
				  "targetUid": "rds1",
				  "tags": { "1": "1" }
				}
			  },
			  {
				"uid": "Service3_source0::::rds2_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source0",
				  "sourceUid": "Service3",
				  "targetTopokey": "source1",
				  "targetUid": "rds2",
				  "tags": { "1": "1", "edgeprop": "edge-com1" }
				}
			  },
			  {
				"uid": "Service4_source1::::rds2_source1",
				"metadata": {
				  "kind": "communication",
				  "sourceTopokey": "source1",
				  "sourceUid": "Service4",
				  "targetTopokey": "source1",
				  "targetUid": "rds2",
				  "tags": { "1": "1", "edgeprop": "edge-com2" }
				}
			  }
			]
		  }
      }
   }`
	return jsonNodeData
}
