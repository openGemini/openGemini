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

package meta

import (
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type MeasurementInfo struct {
	Name           string
	ShardKeys      []ShardKeyInfo
	Schema         map[string]int32
	IndexRelations []IndexRelation
	MarkDeleted    bool
}

func (msti *MeasurementInfo) walkSchema(fn func(fieldName string, fieldType int32)) {
	for fieldName := range msti.Schema {
		fn(fieldName, msti.Schema[fieldName])
	}
}

func (msti *MeasurementInfo) GetShardKey(ID uint64) *ShardKeyInfo {
	for i := len(msti.ShardKeys) - 1; i >= 0; i-- {
		if msti.ShardKeys[i].ShardGroup <= ID {
			return &msti.ShardKeys[i]
		}
	}
	return nil
}

func (msti *MeasurementInfo) marshal() *proto2.MeasurementInfo {
	pb := &proto2.MeasurementInfo{
		Name:        proto.String(msti.Name),
		MarkDeleted: proto.Bool(msti.MarkDeleted),
	}

	if msti.ShardKeys != nil {
		pb.ShardKeys = make([]*proto2.ShardKeyInfo, len(msti.ShardKeys))
		for i := range msti.ShardKeys {
			pb.ShardKeys[i] = msti.ShardKeys[i].Marshal()
		}
	}

	if msti.Schema != nil {
		pb.Schema = make(map[string]int32, len(msti.Schema))
		for n, t := range msti.Schema {
			pb.Schema[n] = t
		}
	}

	if len(msti.IndexRelations) > 0 {
		pb.IndexRelations = make([]*proto2.IndexRelation, len(msti.IndexRelations))
		for i := range msti.IndexRelations {
			pb.IndexRelations[i] = msti.IndexRelations[i].Marshal()
		}
	}

	return pb
}

func (msti *MeasurementInfo) unmarshal(pb *proto2.MeasurementInfo) {
	msti.Name = pb.GetName()
	msti.MarkDeleted = pb.GetMarkDeleted()
	if pb.GetShardKeys() != nil {
		msti.ShardKeys = make([]ShardKeyInfo, len(pb.GetShardKeys()))
		for i := range pb.GetShardKeys() {
			msti.ShardKeys[i].unmarshal(pb.GetShardKeys()[i])
		}
	}

	if len(pb.GetSchema()) > 0 {
		msti.Schema = make(map[string]int32, len(pb.GetSchema()))
	}

	for name, t := range pb.GetSchema() {
		msti.Schema[name] = t
	}

	msti.IndexRelations = make([]IndexRelation, len(pb.GetIndexRelations()))
	for i, indexR := range pb.GetIndexRelations() {
		msti.IndexRelations[i].unmarshal(indexR)
	}
}

func (msti MeasurementInfo) clone() *MeasurementInfo {
	other := msti
	other.Schema = msti.cloneSchema()
	if msti.ShardKeys == nil {
		return &other
	}
	other.ShardKeys = make([]ShardKeyInfo, len(msti.ShardKeys))
	for i := range msti.ShardKeys {
		other.ShardKeys[i] = msti.ShardKeys[i].clone()
	}

	return &other
}

func (msti MeasurementInfo) cloneSchema() map[string]int32 {
	if msti.Schema == nil {
		return nil
	}

	schema := make(map[string]int32, len(msti.Schema))
	for name, info := range msti.Schema {
		schema[name] = info
	}
	return schema
}

func (msti MeasurementInfo) FieldKeys(ret map[string]map[string]int32) {
	for key := range msti.Schema {
		if msti.Schema[key] == influx.Field_Type_Tag {
			continue
		}
		ret[msti.Name][key] = msti.Schema[key]
	}
}

func (msti MeasurementInfo) MatchTagKeys(cond influxql.Expr, ret map[string]map[string]struct{}) {
	for key, typ := range msti.Schema {
		if typ != influx.Field_Type_Tag {
			continue
		}
		valMap := map[string]interface{}{
			"_tagKey": key,
			"_name":   msti.Name,
		}
		if cond == nil || influxql.EvalBool(cond, valMap) {
			ret[msti.Name][key] = struct{}{}
		}
	}
}

type ShardKeyInfo struct {
	ShardKey   []string
	Type       string
	ShardGroup uint64
}

func (ski *ShardKeyInfo) EqualsToAnother(other *ShardKeyInfo) bool {
	if len(ski.ShardKey) != len(other.ShardKey) {
		return false
	}

	if ski.Type != other.Type {
		return false
	}

	// shardKey is sorted
	for i := range ski.ShardKey {
		if ski.ShardKey[i] != other.ShardKey[i] {
			return false
		}
	}

	return true
}

func (ski *ShardKeyInfo) Marshal() *proto2.ShardKeyInfo {
	pb := &proto2.ShardKeyInfo{ShardKey: ski.ShardKey, Type: proto.String(ski.Type)}
	if ski.ShardGroup > 0 {
		pb.SgID = proto.Uint64(ski.ShardGroup)
	}
	return pb
}

func (ski *ShardKeyInfo) unmarshal(pb *proto2.ShardKeyInfo) {
	ski.ShardKey = pb.GetShardKey()
	ski.Type = pb.GetType()
	if pb.GetSgID() > 0 {
		ski.ShardGroup = pb.GetSgID()
	}
}

func (ski ShardKeyInfo) clone() ShardKeyInfo {
	if ski.ShardKey == nil {
		return ski
	}

	shardKey := make([]string, len(ski.ShardKey))
	for i := range ski.ShardKey {
		shardKey[i] = ski.ShardKey[i]
	}

	ski.ShardKey = shardKey
	return ski
}

type IndexRelation struct {
	Rid        uint32
	Oids       []uint32
	IndexNames []string
	IndexList  []*IndexList
}

type IndexList struct {
	IList []string
}

func (indexR *IndexRelation) Marshal() *proto2.IndexRelation {
	pb := &proto2.IndexRelation{Rid: proto.Uint32(indexR.Rid),
		Oid:       indexR.Oids,
		IndexName: indexR.IndexNames}

	pb.IndexLists = make([]*proto2.IndexList, len(indexR.IndexList))
	for i, IList := range indexR.IndexList {
		indexList := &proto2.IndexList{
			IList: IList.IList,
		}
		pb.IndexLists[i] = indexList
	}
	return pb
}

func (indexR *IndexRelation) unmarshal(pb *proto2.IndexRelation) {
	indexR.Rid = pb.GetRid()
	indexR.Oids = pb.GetOid()
	indexR.IndexNames = pb.GetIndexName()
	indexLists := pb.GetIndexLists()
	indexR.IndexList = make([]*IndexList, len(indexLists))
	for i, iList := range indexLists {
		indexR.IndexList[i] = &IndexList{
			IList: iList.GetIList(),
		}
	}
}

func (msti *MeasurementInfo) ContainIndexRelation(ID uint64) bool {
	return true
}

func (msti *MeasurementInfo) GetIndexRelationIndexList() []IndexRelation {
	return msti.IndexRelations
}
