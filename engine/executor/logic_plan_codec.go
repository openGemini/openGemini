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

package executor

import (
	"encoding/binary"
	"fmt"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/query"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
	"google.golang.org/protobuf/proto"
)

func UnmarshalBinary(buf []byte, schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	pb := &internal.QueryNode{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return nil, err
	}

	err, node := UnmarshalBinaryNode(pb, schema)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func marshalNodes(nodes ...hybridqp.QueryNode) ([][]byte, error) {
	plansBuf := make([][]byte, 0, len(nodes))

	for _, p := range nodes {
		if p == nil {
			continue
		}
		buf, err := MarshalBinary(p)
		if err != nil {
			return nil, err
		}

		plansBuf = append(plansBuf, buf)
	}

	return plansBuf, nil
}

func unmarshalNodes(plansBuf [][]byte, schema hybridqp.Catalog) ([]hybridqp.QueryNode, error) {
	if len(plansBuf) == 0 {
		return nil, nil
	}

	plans := make([]hybridqp.QueryNode, 0, len(plansBuf))
	for _, buf := range plansBuf {
		pb := &internal.QueryNode{}
		if err := proto.Unmarshal(buf, pb); err != nil {
			return nil, err
		}

		err, node := UnmarshalBinaryNode(pb, schema)
		if err != nil {
			return nil, err
		}

		plans = append(plans, node)
	}

	return plans, nil
}

func Marshal(plan LogicalPlaner, extMarshal func(pb *internal.QueryNode), nodes ...hybridqp.QueryNode) ([]byte, error) {
	pb := &internal.QueryNode{
		Name: plan.LogicPlanType(),
		Rt:   nil,
		Ops:  nil,
	}

	var err error
	pb.Inputs, err = marshalNodes(nodes...)
	if err != nil {
		return nil, err
	}

	if extMarshal != nil {
		extMarshal(pb)
	}

	return proto.Marshal(pb)
}

func (v *HeuVertex) SetInputs(_ []hybridqp.QueryNode) {

}

// MarshalQueryNode
// The schema of all nodes are the same, only one codec is required.
// |8 byte schema size|schema buffer|node buffer|
func MarshalQueryNode(node hybridqp.QueryNode) ([]byte, error) {
	buf := bufferpool.Get()
	schema, err := proto.Marshal(query.EncodeQuerySchema(node.Schema()))
	if err != nil {
		return nil, err
	}
	nodeBuf, err := MarshalBinary(node)
	if err != nil {
		return nil, err
	}

	buf = bufferpool.Resize(buf, record.Uint64SizeBytes)
	binary.BigEndian.PutUint64(buf[:record.Uint64SizeBytes], uint64(len(schema)))
	buf = append(buf, schema...)
	buf = append(buf, nodeBuf...)
	return buf, nil
}

func (p *LogicalLimit) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalLimit
}

func (p *LogicalExchange) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalExchange
}

func (p *LogicalIndexScan) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalIndexScan
}

func (p *LogicalAggregate) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalAggregate
}

func (p *LogicalMerge) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalMerge
}

func (p *LogicalSortMerge) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSortMerge
}

func (p *LogicalFilter) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalFilter
}

func (p *LogicalDedupe) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalDedupe
}

func (p *LogicalInterval) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalInterval
}

func (p *LogicalSeries) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSeries
}

func (p *LogicalReader) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalReader
}

func (p *LogicalTagSubset) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalTagSubset
}

func (p *LogicalFill) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalFill
}

func (p *LogicalAlign) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalAlign
}

func (p *LogicalMst) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalMst
}

func (p *LogicalProject) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalProject
}

func (p *LogicalSlidingWindow) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSlidingWindow
}

func (p *LogicalFilterBlank) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalFilterBlank
}

func (p *LogicalHttpSender) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalHttpSender
}

func (p *LogicalFullJoin) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalFullJoin
}

func (p *LogicalWriteIntoStorage) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalWriteIntoStorage
}

func (p *LogicalSequenceAggregate) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSequenceAggregate
}

func (p *LogicalSplitGroup) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSplitGroup
}

func (p *LogicalHoltWinters) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalHoltWinters
}

func (p *LogicalSortAppend) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSortAppend
}

func (p *LogicalSubQuery) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalSubQuery
}

func (p *LogicalGroupBy) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalGroupBy
}

func (p *LogicalOrderBy) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalOrderBy
}

func (p *LogicalHttpSenderHint) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalHttpSenderHint
}

func (p *LogicalTarget) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalTarget
}

func (p *LogicalDummyShard) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalDummyShard
}

func (p *LogicalTSSPScan) LogicPlanType() internal.LogicPlanType {
	return internal.LogicPlanType_LogicalTSSPScan
}

func (p *LogicalLimit) String() string {
	return "LogicalLimit"
}

func (p *LogicalExchange) String() string {
	return "LogicalExchange"
}

func (p *LogicalIndexScan) String() string {
	return "LogicalIndexScan"
}

func (p *LogicalAggregate) String() string {
	return "LogicalAggregate"
}

func (p *LogicalMerge) String() string {
	return "LogicalMerge"
}

func (p *LogicalSortMerge) String() string {
	return "LogicalSortMerge"
}

func (p *LogicalFilter) String() string {
	return "LogicalFilter"
}

func (p *LogicalDedupe) String() string {
	return "LogicalDedupe"
}

func (p *LogicalInterval) String() string {
	return "LogicalInterval"
}

func (p *LogicalSeries) String() string {
	return "LogicalSeries"
}

func (p *LogicalReader) String() string {
	return "LogicalReader"
}

func (p *LogicalTagSubset) String() string {
	return "LogicalTagSubset"
}

func (p *LogicalFill) String() string {
	return "LogicalFill"
}

func (p *LogicalAlign) String() string {
	return "LogicalAlign"
}

func (p *LogicalMst) String() string {
	return "LogicalMst"
}

func (p *LogicalProject) String() string {
	return "LogicalProject"
}

func (p *LogicalSlidingWindow) String() string {
	return "LogicalSlidingWindow"
}

func (p *LogicalFilterBlank) String() string {
	return "LogicalFilterBlank"
}

func (p *LogicalHttpSender) String() string {
	return "LogicalHttpSender"
}

func (p *LogicalFullJoin) String() string {
	return "LogicalFullJoin"
}

func (p *LogicalWriteIntoStorage) String() string {
	return "LogicalWriteIntoStorage"
}

func (p *LogicalSequenceAggregate) String() string {
	return "LogicalSequenceAggregate"
}

func (p *LogicalSplitGroup) String() string {
	return "LogicalSplitGroup"
}

func (p *LogicalHoltWinters) String() string {
	return "LogicalHoltWinters"
}

func (p *LogicalSortAppend) String() string {
	return "LogicalSortAppend"
}

func (p *LogicalSubQuery) String() string {
	return "LogicalSubQuery"
}

func (p *LogicalGroupBy) String() string {
	return "LogicalGroupBy"
}

func (p *LogicalOrderBy) String() string {
	return "LogicalOrderBy"
}

func (p *LogicalHttpSenderHint) String() string {
	return "LogicalHttpSenderHint"
}

func (p *LogicalTarget) String() string {
	return "LogicalTarget"
}

func (p *LogicalDummyShard) String() string {
	return "LogicalDummyShard"
}

func (p *LogicalTSSPScan) String() string {
	return "LogicalTSSPScan"
}

func MarshalBinary(q hybridqp.QueryNode) ([]byte, error) {
	switch p := q.(type) {
	case *HeuVertex:
		return nil, nil
	case *LogicalExchange:
		return Marshal(p, func(pb *internal.QueryNode) {
			pb.Exchange = uint32(p.eType)<<8 | uint32(p.eRole)
		}, p.inputs...)
	case *LogicalLimit:
		return Marshal(p, func(pb *internal.QueryNode) {
			pb.Limit = int64(p.LimitPara.Limit)
			pb.Offset = int64(p.LimitPara.Offset)
			pb.LimitType = int64(p.LimitPara.LimitType)
		}, p.inputs...)
	case *LogicalIndexScan:
		return Marshal(p, nil, p.inputs...)
	case *LogicalAggregate:
		aggType := internal.AggType_Normal
		if p.isCountDistinct {
			aggType = internal.AggType_CountDistinct
		} else if p.aggType == tagSetAgg {
			aggType = internal.AggType_TagSet
		}
		return Marshal(p, func(pb *internal.QueryNode) {
			pb.AggType = aggType
		}, p.inputs...)
	case *LogicalMerge:
		return Marshal(p, nil, p.inputs...)
	case *LogicalSortMerge:
		return Marshal(p, nil, p.inputs...)
	case *LogicalFilter:
		return Marshal(p, nil, p.inputs...)
	case *LogicalDedupe:
		return Marshal(p, nil, p.inputs...)
	case *LogicalInterval:
		return Marshal(p, nil, p.inputs...)
	case *LogicalSeries:
		return Marshal(p, nil, p.inputs...)
	case *LogicalReader:
		return Marshal(p, nil, p.inputs...)
	case *LogicalTagSubset:
		return Marshal(p, nil, p.inputs...)
	case *LogicalFill:
		return Marshal(p, nil, p.inputs...)
	case *LogicalAlign:
		return Marshal(p, nil, p.inputs...)
	case *LogicalMst:
		return Marshal(p, nil, p.inputs...)
	case *LogicalProject:
		return Marshal(p, nil, p.inputs...)
	case *LogicalSlidingWindow:
		return Marshal(p, nil, p.inputs...)
	default:
		panic(fmt.Sprintf("unsupoorted type %t", p))
	}
}

func UnmarshalBinaryNode(pb *internal.QueryNode, schema hybridqp.Catalog) (error, hybridqp.QueryNode) {
	var nodes []hybridqp.QueryNode
	var err error
	if len(pb.Inputs) != 0 {
		nodes, err = unmarshalNodes(pb.Inputs, schema)
		if err != nil {
			return err, nil
		}
	}

	switch pb.Name {
	case internal.LogicPlanType_LogicalExchange:
		if len(nodes) == 1 {
			eType := ExchangeType(pb.Exchange >> 8 & 0xff)
			eRole := ExchangeRole(pb.Exchange & 0xff)
			node := NewLogicalExchange(nodes[0], eType, []hybridqp.Trait{}, schema)
			if eRole == PRODUCER_ROLE {
				node.ToProducer()
			}
			return nil, node
		}
	case internal.LogicPlanType_LogicalLimit:
		if len(nodes) == 1 {
			limitPara := LimitTransformParameters{
				Limit:     int(pb.Limit),
				Offset:    int(pb.Offset),
				LimitType: hybridqp.LimitType(pb.LimitType),
			}
			node := NewLogicalLimit(nodes[0], schema, limitPara)
			return nil, node
		}
	case internal.LogicPlanType_LogicalIndexScan:
		if len(nodes) == 1 {
			return nil, NewLogicalIndexScan(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalAggregate:
		if len(nodes) == 1 {
			switch pb.AggType {
			case internal.AggType_Normal:
				return nil, NewLogicalAggregate(nodes[0], schema)
			case internal.AggType_CountDistinct:
				return nil, NewCountDistinctAggregate(nodes[0], schema)
			case internal.AggType_TagSet:
				return nil, NewLogicalTagSetAggregate(nodes[0], schema)
			}
		}
	case internal.LogicPlanType_LogicalMerge:
		return nil, NewLogicalMerge(nodes, schema)
	case internal.LogicPlanType_LogicalSortMerge:
		return nil, NewLogicalSortMerge(nodes, schema)
	case internal.LogicPlanType_LogicalFilter:
		if len(nodes) == 1 {
			return nil, NewLogicalFilter(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalDedupe:
		if len(nodes) == 1 {
			return nil, NewLogicalDedupe(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalInterval:
		if len(nodes) == 1 {
			return nil, NewLogicalInterval(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalSeries:
		return nil, NewLogicalSeries(schema)
	case internal.LogicPlanType_LogicalReader:
		if len(nodes) == 1 {
			return nil, NewLogicalReader(nodes[0], schema)
		} else if len(nodes) == 0 {
			return nil, NewLogicalReader(nil, schema)
		}
	case internal.LogicPlanType_LogicalTagSubset:
		if len(nodes) == 1 {
			return nil, NewLogicalTagSubset(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalFill:
		if len(nodes) == 1 {
			return nil, NewLogicalFill(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalAlign:
		if len(nodes) == 1 {
			return nil, NewLogicalAlign(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalMst:
		// unused
	case internal.LogicPlanType_LogicalProject:
		if len(nodes) == 1 {
			return nil, NewLogicalProject(nodes[0], schema)
		}
	case internal.LogicPlanType_LogicalSlidingWindow:
		if len(nodes) == 1 {
			return nil, NewLogicalSlidingWindow(nodes[0], schema)
		}
	default:
		panic(fmt.Sprintf("unsupoorted type %v", pb.Name))
	}
	panic("UnmarshalBinaryNode fail")
}

func UnmarshalQueryNode(buf []byte) (hybridqp.QueryNode, error) {
	if len(buf) < record.Uint64SizeBytes {
		return nil, errno.NewError(errno.ShortBufferSize, record.Uint64SizeBytes, len(buf))
	}

	schemaSize := binary.BigEndian.Uint64(buf[:record.Uint64SizeBytes])
	buf = buf[record.Uint64SizeBytes:]

	if uint64(len(buf)) < schemaSize {
		return nil, errno.NewError(errno.ShortBufferSize, schemaSize, len(buf))
	}

	schemaPb := &internal.QuerySchema{}
	if err := proto.Unmarshal(buf[:schemaSize], schemaPb); err != nil {
		return nil, err
	}

	schema, err := query.DecodeQuerySchema(schemaPb)
	if err != nil {
		return nil, err
	}

	node, err := UnmarshalBinary(buf[schemaSize:], schema)
	if err != nil {
		return nil, err
	}

	planner := BuildHeuristicPlannerForStore()
	planner.SetRoot(node)
	best := planner.FindBestExp()

	return best, nil
}

type SetSchemaVisitor struct {
	schema hybridqp.Catalog
}

func (v *SetSchemaVisitor) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	node.SetSchema(v.schema)
	return v
}
