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
	"reflect"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/query"
	internal "github.com/openGemini/openGemini/open_src/influx/query/proto"
	"google.golang.org/protobuf/proto"
)

//go:generate tmpl -data=@logical_plan_tmpldata logical_plan_codec.gen.go.tmpl

const (
	inputsNumberFlagNull = iota
	inputsNumberFlagOne
	inputsNumberFlagN
)

var logicPlanNewHandler = map[string]func() hybridqp.QueryNode{
	reflect.TypeOf(&LogicalExchange{}).String(): func() hybridqp.QueryNode {
		return &LogicalExchange{}
	},
	reflect.TypeOf(&LogicalLimit{}).String(): func() hybridqp.QueryNode {
		return &LogicalLimit{}
	},
	reflect.TypeOf(&LogicalIndexScan{}).String(): func() hybridqp.QueryNode {
		return &LogicalIndexScan{}
	},
}

func NewLogicPlanByName(name string) hybridqp.QueryNode {
	fn, ok := logicPlanNewHandler[name]
	if !ok {
		return nil
	}

	return fn()
}

type QueryNodeCodec struct {
	extMarshal   func(pb *internal.QueryNode)
	extUnmarshal func(pb *internal.QueryNode)
}

func (c *QueryNodeCodec) UnmarshalBinary(buf []byte) (hybridqp.QueryNode, error) {
	pb := &internal.QueryNode{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return nil, err
	}

	node := NewLogicPlanByName(pb.Name)
	if node == nil {
		return nil, errno.NewError(errno.LogicPlanNotInit, pb.Name)
	}

	if err := node.UnmarshalBinary(pb); err != nil {
		return nil, err
	}

	return node, nil
}

func (c *QueryNodeCodec) marshalNodes(nodes ...hybridqp.QueryNode) ([][]byte, error) {
	plansBuf := make([][]byte, 0, len(nodes))

	for _, p := range nodes {
		if p == nil {
			continue
		}
		buf, err := p.MarshalBinary()
		if err != nil {
			return nil, err
		}

		plansBuf = append(plansBuf, buf)
	}

	return plansBuf, nil
}

func (c *QueryNodeCodec) unmarshalNodes(plansBuf [][]byte) ([]hybridqp.QueryNode, error) {
	if len(plansBuf) == 0 {
		return nil, nil
	}

	plans := make([]hybridqp.QueryNode, 0, len(plansBuf))
	for _, buf := range plansBuf {
		pb := &internal.QueryNode{}
		if err := proto.Unmarshal(buf, pb); err != nil {
			return nil, err
		}

		p := NewLogicPlanByName(pb.Name)
		if p == nil {
			return nil, errno.NewError(errno.NotSupportUnmarshal, pb.Name)
		}

		if err := p.UnmarshalBinary(pb); err != nil {
			return nil, err
		}

		plans = append(plans, p)
	}

	return plans, nil
}

func (c *QueryNodeCodec) Marshal(plan LogicalPlan, nodes ...hybridqp.QueryNode) ([]byte, error) {
	pb := &internal.QueryNode{
		Name: reflect.TypeOf(plan).String(),
		Rt:   nil,
		Ops:  nil,
	}
	if plan.RowDataType() != nil {
		pb.Rt = plan.RowDataType().Marshal()
	}
	if ops := plan.RowExprOptions(); ops != nil {
		pb.Ops = make([]*internal.ExprOptions, 0, len(ops))
		for i := range ops {
			pb.Ops = append(pb.Ops, ops[i].Marshal())
		}
	}

	var err error
	pb.Inputs, err = c.marshalNodes(nodes...)
	if err != nil {
		return nil, err
	}

	if c.extMarshal != nil {
		c.extMarshal(pb)
	}

	return proto.Marshal(pb)
}

func (c *QueryNodeCodec) Unmarshal(plan *LogicalPlanBase, child hybridqp.QueryNode, pb *internal.QueryNode, flag int) error {
	var err error

	if pb.Rt != nil {
		plan.rt = &hybridqp.RowDataTypeImpl{}
		err = plan.rt.Unmarshal(pb.Rt)
		if err != nil {
			return err
		}
	}

	if pb.Ops != nil {
		plan.ops = make([]hybridqp.ExprOptions, len(pb.Ops))
		for i, e := range pb.Ops {
			err = plan.ops[i].Unmarshal(e)
			if err != nil {
				return err
			}
		}
	}

	if c.extUnmarshal != nil {
		c.extUnmarshal(pb)
	}

	if flag == inputsNumberFlagNull {
		return nil
	}

	nodes, err := c.unmarshalNodes(pb.Inputs)
	if err != nil {
		return err
	}

	child.SetInputs(nodes)
	return nil
}

func (p *LogicalPlanBase) SetInputs(_ []hybridqp.QueryNode) {

}

func (p *LogicalPlanBase) SetSchema(schema hybridqp.Catalog) {
	p.schema = schema
}

func (p *LogicalPlanBase) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (p *LogicalPlanBase) UnmarshalBinary(_ *internal.QueryNode) error {
	return nil
}

func (v *HeuVertex) SetInputs(_ []hybridqp.QueryNode) {

}

func (v *HeuVertex) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (v *HeuVertex) UnmarshalBinary(_ *internal.QueryNode) error {
	return nil
}

func (p *LogicalExchange) SetInputs(inputs []hybridqp.QueryNode) {
	p.input = inputs[0]
}

func (p *LogicalExchange) MarshalBinary() ([]byte, error) {
	codec := &QueryNodeCodec{}
	codec.extMarshal = func(pb *internal.QueryNode) {
		pb.Exchange = uint32(p.eType)<<8 | uint32(p.eRole)
	}

	return codec.Marshal(p, p.input)
}

func (p *LogicalExchange) UnmarshalBinary(pb *internal.QueryNode) error {
	codec := &QueryNodeCodec{}
	codec.extUnmarshal = func(pb *internal.QueryNode) {
		p.eType = ExchangeType(pb.Exchange >> 8 & 0xff)
		p.eRole = ExchangeRole(pb.Exchange & 0xff)
	}

	return codec.Unmarshal(&p.LogicalPlanBase, p, pb, inputsNumberFlagOne)
}

func (p *LogicalLimit) SetInputs(inputs []hybridqp.QueryNode) {
	p.input = inputs[0]
}

func (p *LogicalLimit) MarshalBinary() ([]byte, error) {
	codec := &QueryNodeCodec{}
	codec.extMarshal = func(pb *internal.QueryNode) {
		pb.Limit = int64(p.LimitPara.Limit)
		pb.Offset = int64(p.LimitPara.Offset)
		pb.LimitType = int64(p.LimitPara.LimitType)
	}

	return codec.Marshal(p, p.input)
}

func (p *LogicalLimit) UnmarshalBinary(pb *internal.QueryNode) error {
	codec := &QueryNodeCodec{}
	codec.extUnmarshal = func(pb *internal.QueryNode) {
		p.LimitPara.Limit = int(pb.Limit)
		p.LimitPara.Offset = int(pb.Offset)
		p.LimitPara.LimitType = hybridqp.LimitType(pb.LimitType)
	}

	return codec.Unmarshal(&p.LogicalPlanBase, p, pb, inputsNumberFlagOne)
}

func (p *LogicalIndexScan) SetInputs(inputs []hybridqp.QueryNode) {
	p.input = inputs[0]
}

func (p *LogicalIndexScan) MarshalBinary() ([]byte, error) {
	codec := &QueryNodeCodec{}

	return codec.Marshal(p, p.input)
}

func (p *LogicalIndexScan) UnmarshalBinary(pb *internal.QueryNode) error {
	codec := &QueryNodeCodec{}

	return codec.Unmarshal(&p.LogicalPlanBase, p, pb, inputsNumberFlagOne)
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
	nodeBuf, err := node.MarshalBinary()
	if err != nil {
		return nil, err
	}

	buf = bufferpool.Resize(buf, record.Uint64SizeBytes)
	binary.BigEndian.PutUint64(buf[:record.Uint64SizeBytes], uint64(len(schema)))
	buf = append(buf, schema...)
	buf = append(buf, nodeBuf...)
	return buf, nil
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

	node, err := (&QueryNodeCodec{}).UnmarshalBinary(buf[schemaSize:])
	if err != nil {
		return nil, err
	}

	hybridqp.WalkQueryNodeInPreOrder(&SetSchemaVisitor{
		schema: schema,
	}, node)

	return node, nil
}

type SetSchemaVisitor struct {
	schema hybridqp.Catalog
}

func (v *SetSchemaVisitor) Visit(node hybridqp.QueryNode) hybridqp.QueryNodeVisitor {
	node.SetSchema(v.schema)
	return v
}
