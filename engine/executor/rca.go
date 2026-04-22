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
	"errors"
	"sort"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"go.uber.org/zap"
)

const (
	ID          = "id"
	Name        = "name"
	Level       = "level"
	RuleID      = "rule_id"
	EntityID    = "entity_id"
	Type        = "type"
	Annotations = "annotations"
)

// RCA assumes that the following fields exist in AnomalyEvent.Annotations.
const (
	// anomaly
	ANOMALY    = "anomaly"    // event type
	Timestamps = "timestamps" // required field

	// alarm
	ALARM = "alarm" // event type

	// event
	EVENT     = "event"       // event type
	CreatedTS = "create_time" // required field

	// alarm & event
	StartTS = "start_time" // optional field for EVENT, required field for ALARM
	EndTS   = "end_time"   // optional
)

type AlgoParam struct {
	HopCount  int                    `json:"hop_count"`
	BFSNarrow bool                   `json:"bfs_narrow"`
	Task      map[string]interface{} `json:"task"`
}

// RCA assumes that the following fields exist in AlgoParam.Task.
const (
	META       = "metadata"
	CoreEntity = "core_entity_id"
)

func isWithinTSRange(targetTS int64, sortedTSList []int64, closeHourMS int64) bool {
	// Search for the nearest timestamp.
	pos := sort.Search(len(sortedTSList), func(i int) bool {
		return sortedTSList[i] >= targetTS
	})

	// Check the current & previous position.
	for _, i := range []int{pos, pos - 1} {
		if i >= 0 && i < len(sortedTSList) {
			if hybridqp.Abs(targetTS-sortedTSList[i]) <= closeHourMS {
				return true
			}
		}
	}
	return false
}

func isAnomaly(anomalyTS []int64, curEntityID string, chunks []Chunk, colMap map[string]int) (bool, error) {
	const (
		halfHourMs = 30 * 60 * 1000
		twoHourMs  = 120 * 60 * 1000
	)

	for _, chunk := range chunks {
		columns := chunk.Columns()
		idCol := columns[colMap[ID]]
		entityIDCol := columns[colMap[EntityID]]
		typeCol := columns[colMap[Type]]
		annotationsCol := columns[colMap[Annotations]]
		for i := 0; i < idCol.Length(); i++ {
			if entityIDCol.StringValue(i) != curEntityID {
				continue
			}
			tmp := annotationsCol.StringValue(i)
			annotations := make(map[string]interface{})
			err := json.Unmarshal([]byte(tmp), &annotations)
			if err != nil {
				log.Error("RCA Error: unable to unmarshal annotations", zap.Error(err))
				return false, err
			}
			switch typeCol.StringValue(i) {
			case ANOMALY:
				timestamps, ok := annotations[Timestamps]
				if !ok {
					return false, errors.New("RCA Error: timestamps not found in annotations")
				}
				for _, ts := range timestamps.([]interface{}) {
					if isWithinTSRange(int64(ts.(float64)), anomalyTS, halfHourMs) {
						return true, nil
					}
				}
			case ALARM:
				ts, ok := annotations[StartTS]
				if !ok {
					return false, errors.New("RCA Error: fired timestamp not found in annotations")
				}
				_, ok = annotations[EndTS]
				if ok {
					if isWithinTSRange(int64(ts.(float64)), anomalyTS, halfHourMs) {
						return true, nil
					}
				} else {
					if isWithinTSRange(int64(ts.(float64)), anomalyTS, twoHourMs) {
						return true, nil
					}
				}
			case EVENT:
				ts, ok := annotations[CreatedTS]
				if !ok {
					return false, errors.New("RCA Error: created timestamp not found in annotations")
				}

				tsEnd, okEnd := annotations[EndTS]
				tsStart, okStart := annotations[StartTS]
				if okEnd {
					if isWithinTSRange(int64(tsEnd.(float64)), anomalyTS, halfHourMs) {
						return true, nil
					}
				} else if okStart {
					if isWithinTSRange(int64(tsStart.(float64)), anomalyTS, twoHourMs) {
						return true, nil
					}
				} else {
					if isWithinTSRange(int64(ts.(float64)), anomalyTS, twoHourMs) {
						return true, nil
					}
				}
			}
		}
	}

	return false, nil
}

func FaultDemarcation(chunks []Chunk, subTopo *Graph, algoParams AlgoParam, colMap map[string]int) (graph *Graph, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("RCA Error: FaultDemarcation failed")
			err = errors.New("RCA Error: FaultDemarcation failed")
		}
	}()
	tmp, ok := algoParams.Task[META]
	if !ok {
		return nil, errors.New("RCA Error: meta not found in algoParams")
	}
	taskMeta, ok := tmp.(map[string]interface{})
	if !ok {
		return nil, errors.New("RCA Error: meta format error in algoParams")
	}

	tmp, ok = taskMeta[CoreEntity]
	if !ok {
		return nil, errors.New("RCA Error: core entity not found in task meta")
	}
	coreEntityID, ok := tmp.(string)
	if !ok {
		return nil, errors.New("RCA Error: core entity type error in task meta")
	}

	BFSHopCount := algoParams.HopCount
	if BFSHopCount == 0 {
		BFSHopCount = 2
	}
	BFSNarrow := algoParams.BFSNarrow

	// Extract anomaly timestamps.
	coreAnomalyTS, err := extractCoreAnomalyTimestamps(chunks, colMap, coreEntityID, taskMeta)
	if err != nil {
		return nil, err
	}

	// Build graph index.
	nodeIdx, sourceEdgeIdx, targetEdgeIdx := buildGraphIndices(subTopo)

	edgeList := make([]GraphEdge, 0, len(sourceEdgeIdx)*2)
	existedEdgeID := make(map[string]struct{}, len(sourceEdgeIdx)*2)
	visitedNodes := make(map[string]struct{}, len(nodeIdx))
	visitedNodes[coreEntityID] = struct{}{}
	nodeQueue := []string{coreEntityID}
	nodeList := make([]GraphNode, 0, len(nodeIdx))
	if nodes, ok := nodeIdx[coreEntityID]; ok {
		nodeList = append(nodeList, nodes...)
	}
	idx := 0

	for idx < len(nodeQueue) {
		curEntityID := nodeQueue[idx]
		anomaly, err := isAnomaly(coreAnomalyTS, curEntityID, chunks, colMap)
		if err != nil {
			return nil, err
		}
		if !anomaly {
			idx += 1
			continue
		}
		tmpVisited := make(map[string]struct{})
		tmpVisited[curEntityID] = struct{}{}
		tmpNodeIDList := []string{curEntityID}
		tmpHopCount := []int{0}
		tmpIdx := 0
		for tmpIdx < len(tmpNodeIDList) {
			tmpEntityID := tmpNodeIDList[tmpIdx]
			tmpSourceID, oks := sourceEdgeIdx[tmpEntityID]
			tmpTargetID, okt := targetEdgeIdx[tmpEntityID]
			if oks {
				for _, tmpCase := range tmpSourceID {
					// edge.uid == SourceUid_SourceTopoKey::::TargetUid_TargetTopoKey
					meta := tmpCase.MetaData
					edgeUid := meta.SourceUid + "_" + meta.SourceTopoKey +
						"::::" + meta.TargetUid + "_" + meta.TargetTopoKey
					_, inExistedEdge := existedEdgeID[edgeUid]
					_, inNode := visitedNodes[meta.TargetUid]
					_, inTmpNode := tmpVisited[meta.TargetUid]
					if !inExistedEdge && (inNode || inTmpNode) {
						existedEdgeID[edgeUid] = struct{}{}
						edgeList = append(edgeList, tmpCase)
					}
					if tmpHopCount[tmpIdx] < BFSHopCount && !inTmpNode {
						tmpVisited[meta.TargetUid] = struct{}{}
						tmpNodeIDList = append(tmpNodeIDList, meta.TargetUid)
						tmpHopCount = append(tmpHopCount, tmpHopCount[tmpIdx]+1)
					}
				}
			}
			if okt {
				for _, tmpCase := range tmpTargetID {
					// edge.uid == SourceUid_SourceTopoKey::::TargetUid_TargetTopoKey
					meta := tmpCase.MetaData
					edgeUid := meta.SourceUid + "_" + meta.SourceTopoKey +
						"::::" + meta.TargetUid + "_" + meta.TargetTopoKey
					_, inExistedEdge := existedEdgeID[edgeUid]
					_, inNode := visitedNodes[meta.SourceUid]
					_, inTmpNode := tmpVisited[meta.SourceUid]
					if !inExistedEdge && (inNode || inTmpNode) {
						existedEdgeID[edgeUid] = struct{}{}
						edgeList = append(edgeList, tmpCase)
					}
					if tmpHopCount[tmpIdx] < BFSHopCount && !inTmpNode {
						tmpVisited[meta.SourceUid] = struct{}{}
						tmpNodeIDList = append(tmpNodeIDList, meta.SourceUid)
						tmpHopCount = append(tmpHopCount, tmpHopCount[tmpIdx]+1)
					}
				}
			}
			tmpIdx += 1
		}
		for tmpNode := range tmpVisited {
			if _, ok := visitedNodes[tmpNode]; !ok {
				if n, ok := nodeIdx[tmpNode]; ok {
					nodeList = append(nodeList, n...)
				}
				visitedNodes[tmpNode] = struct{}{}
				nodeQueue = append(nodeQueue, tmpNode)
			}
		}
		if BFSNarrow {
			BFSHopCount = 1
		}
		idx += 1
	}

	return buildGraph(nodeList, edgeList), nil
}

func buildGraph(nodeList []GraphNode, edgeList []GraphEdge) *Graph {
	nodes := make(map[string]GraphNode, len(nodeList))
	edges := make(map[string]GraphEdge, len(edgeList))
	for _, node := range nodeList {
		nodes[node.Uid] = node
	}
	for _, edge := range edgeList {
		edges[edge.Uid] = edge
	}
	return &Graph{Nodes: nodes, Edges: edges}
}

func extractCoreAnomalyTimestamps(chunks []Chunk, colMap map[string]int, coreEntityID string, taskMeta map[string]interface{}) ([]int64, error) {
	var coreAnomalyTS []int64
	found := false

	for _, chunk := range chunks {
		columns := chunk.Columns()
		idCol := columns[colMap[ID]]
		entityIDCol := columns[colMap[EntityID]]
		typeCol := columns[colMap[Type]]
		annotationsCol := columns[colMap[Annotations]]

		for i := 0; i < idCol.Length(); i++ {
			if entityIDCol.StringValue(i) != coreEntityID {
				continue
			}
			found = true
			tmp := annotationsCol.StringValue(i)
			var annotations map[string]interface{}
			if err := json.Unmarshal([]byte(tmp), &annotations); err != nil {
				log.Error("RCA Error: unmarshal annotations failed",
					zap.Error(err),
					zap.String("entityID", coreEntityID))
				continue
			}

			switch typeCol.StringValue(i) {
			case ANOMALY:
				timestamps, ok := annotations[Timestamps]
				if !ok {
					return nil, errors.New("RCA Error: timestamps not found in annotations")
				}
				for _, ts := range timestamps.([]interface{}) {
					coreAnomalyTS = append(coreAnomalyTS, int64(ts.(float64)))
				}
			case ALARM:
				timestamp, ok := annotations[StartTS]
				if !ok {
					return nil, errors.New("RCA Error: fired timestamp not found in annotations")
				}
				coreAnomalyTS = append(coreAnomalyTS, int64(timestamp.(float64)))
			case EVENT:
				timestamp, ok := annotations[StartTS]
				if !ok {
					timestamp, ok = annotations[CreatedTS]
					if !ok {
						return nil, errors.New("RCA Error: created timestamp not found in annotations")
					}
				}
				coreAnomalyTS = append(coreAnomalyTS, int64(timestamp.(float64)))
			}
		}
	}

	if !found || len(coreAnomalyTS) == 0 {
		if endTime, ok := taskMeta[EndTS].(float64); ok {
			coreAnomalyTS = append(coreAnomalyTS, int64(endTime))
		} else {
			return nil, errors.New("no valid timestamps found and end time not available")
		}
	}

	return coreAnomalyTS, nil
}

func buildGraphIndices(subTopo *Graph) (
	nodeIdx map[string][]GraphNode,
	sourceEdgeIdx map[string][]GraphEdge,
	targetEdgeIdx map[string][]GraphEdge,
) {
	nodeIdx = make(map[string][]GraphNode, len(subTopo.Nodes))
	sourceEdgeIdx = make(map[string][]GraphEdge, len(subTopo.Edges))
	targetEdgeIdx = make(map[string][]GraphEdge, len(subTopo.Edges))

	for _, node := range subTopo.Nodes {
		nodeIdx[node.Uid] = append(nodeIdx[node.Uid], node)
	}

	for _, edge := range subTopo.Edges {
		sourceEdgeIdx[edge.MetaData.SourceUid] = append(sourceEdgeIdx[edge.MetaData.SourceUid], edge)
		targetEdgeIdx[edge.MetaData.TargetUid] = append(targetEdgeIdx[edge.MetaData.TargetUid], edge)
	}

	return nodeIdx, sourceEdgeIdx, targetEdgeIdx
}
