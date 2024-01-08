package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go

2022.01.23 Add TargetShards to find shards in one query
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/sysconfig"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

// ShardGroupInfo represents metadata about a shard group. The DeletedAt field is important
// because it makes it clear that a ShardGroup has been marked as deleted, and allow the system
// to be sure that a ShardGroup is not simply missing. If the DeletedAt is set, the system can
// safely delete any associated shards.
type ShardGroupInfo struct {
	ID          uint64
	StartTime   time.Time
	EndTime     time.Time
	DeletedAt   time.Time
	Shards      []ShardInfo
	TruncatedAt time.Time
	EngineType  config.EngineType
	Version     uint32
}

func (sgi *ShardGroupInfo) canDelete() bool {
	for i := range sgi.Shards {
		if !sgi.Shards[i].MarkDelete {
			return false
		}
	}
	return true
}

func (sgi *ShardGroupInfo) walkShards(fn func(sh *ShardInfo)) {
	for i := range sgi.Shards {
		fn(&sgi.Shards[i])
	}
}

func (sgi ShardGroupInfo) getShardsAndSeriesKeyForHintQuery(tagsGroup *influx.PointTags, aliveShardIdxes []int, mstName string, ski *ShardKeyInfo) ([]ShardInfo, []byte) {
	shards := make([]ShardInfo, 0, len(sgi.Shards))
	sort.Sort(tagsGroup)
	r := influx.Row{Name: mstName, Tags: *tagsGroup}
	r.UnmarshalIndexKeys(nil)
	r.UnmarshalShardKeyByTag(nil)
	if len(ski.ShardKey) > 0 {
		r.ShardKey = r.ShardKey[len(mstName)+1:]
	}
	shard := sgi.ShardFor(HashID(r.ShardKey), aliveShardIdxes)
	shards = append(shards, *shard)
	// Force the query to be broadcast
	if sysconfig.GetEnableForceBroadcastQuery() == sysconfig.OnForceBroadcastQuery {
		return sgi.genShardInfosByIndex(aliveShardIdxes), r.IndexKey
	}
	return shards, r.IndexKey
}

func (sgi ShardGroupInfo) TargetShardsHintQuery(mst *MeasurementInfo, ski *ShardKeyInfo, condition influxql.Expr, opt *query.SelectOptions, aliveShardIdxes []int) ([]ShardInfo, []byte) {
	tagsGroup := getConditionTags(condition, mst.Schema)
	if len(tagsGroup) != 1 {
		return sgi.genShardInfosByIndex(aliveShardIdxes), nil
	}

	// it's used for specific series of the hint query
	if opt.HintType == hybridqp.SpecificSeriesQuery {
		var tagCount int
		for key := range mst.Schema {
			if mst.Schema[key] == influx.Field_Type_Tag {
				tagCount++
			}
		}
		// check whether the query contains the all tags of the measurement
		if tagCount != len(*tagsGroup[0]) {
			return sgi.genShardInfosByIndex(aliveShardIdxes), nil
		}

		// check whether the query's tagKey matches the schema's tagKey
		for i := 0; i < tagCount; i++ {
			if _, ok := mst.Schema[(*tagsGroup[0])[i].Key]; !ok {
				return sgi.genShardInfosByIndex(aliveShardIdxes), nil
			}
		}
	}

	// it's used for specific or full series of the hint query
	return sgi.getShardsAndSeriesKeyForHintQuery(tagsGroup[0], aliveShardIdxes, mst.Name, ski)

}

func (sgi *ShardGroupInfo) genShardInfosByIndex(aliveShardIdxes []int) []ShardInfo {
	shards := make([]ShardInfo, 0, len(sgi.Shards))
	for i := range aliveShardIdxes {
		shards = append(shards, sgi.Shards[aliveShardIdxes[i]])
	}
	return shards
}

func (sgi ShardGroupInfo) TargetShards(mst *MeasurementInfo, ski *ShardKeyInfo, condition influxql.Expr, aliveShardIdxes []int) []ShardInfo {
	if ski == nil || ski.ShardKey == nil || (ski.Type == HASH && condition == nil) {
		return sgi.genShardInfosByIndex(aliveShardIdxes)
	}
	tagsGroup := getConditionTags(condition, mst.Schema)
	if len(tagsGroup) == 0 {
		return sgi.genShardInfosByIndex(aliveShardIdxes)
	}

	// Force the query to be broadcast
	if sysconfig.GetEnableForceBroadcastQuery() == sysconfig.OnForceBroadcastQuery {
		return sgi.genShardInfosByIndex(aliveShardIdxes)
	}
	var shardKeyAndValue []byte
	shards := make([]ShardInfo, 0, len(sgi.Shards))
	shardKeyAndValue = append(shardKeyAndValue, mst.Name...)
	for tagGroupIdx := range tagsGroup {
		sort.Sort(tagsGroup[tagGroupIdx])
		i, j := 0, 0
		for i < len(ski.ShardKey) && j < len(*tagsGroup[tagGroupIdx]) {
			cmp := strings.Compare(ski.ShardKey[i], (*tagsGroup[tagGroupIdx])[j].Key)
			if cmp < 0 {
				break
			}
			if cmp == 0 {
				shardKeyAndValue = append(shardKeyAndValue, ","...)
				shardKeyAndValue = append(shardKeyAndValue, (*tagsGroup[tagGroupIdx])[j].Key...)
				shardKeyAndValue = append(shardKeyAndValue, "="...)
				shardKeyAndValue = append(shardKeyAndValue, (*tagsGroup[tagGroupIdx])[j].Value...)
				i++
			}
			j++
		}

		if ski.Type == RANGE {
			for i := range sgi.Shards {
				if sgi.Shards[i].ContainPrefix(string(shardKeyAndValue)) {
					shards = append(shards, sgi.Shards[i])
				}
			}
			continue
		}

		if i < len(ski.ShardKey) {
			return sgi.genShardInfosByIndex(aliveShardIdxes)
		}

		shard := sgi.ShardFor(HashID(shardKeyAndValue[len(mst.Name)+1:]), aliveShardIdxes)
		if shard == nil {
			continue
		}
		shards = append(shards, *shard)
	}
	return shards
}

func getConditionTags(condition influxql.Expr, schema map[string]int32) []*influx.PointTags {
	if condition == nil {
		return nil
	}

	switch expr := condition.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND:
			ltags := getConditionTags(expr.LHS, schema)
			rtags := getConditionTags(expr.RHS, schema)

			if ltags == nil {
				return rtags
			}
			for i := range ltags {
				for j := range rtags {
					for ti := range *rtags[j] {
						if v, ok := schema[(*rtags[j])[ti].Key]; ok && v == influx.Field_Type_Tag {
							*ltags[i] = append(*ltags[i], (*rtags[j])[ti])
						}
					}
				}
			}
			return ltags
		case influxql.OR:
			ltags := getConditionTags(expr.LHS, schema)
			rtags := getConditionTags(expr.RHS, schema)
			if ltags == nil {
				return rtags
			}
			if rtags == nil {
				return ltags
			}
			return append(ltags, rtags...)
		case influxql.EQ:
			if tag := conditionTagsByBinary(expr, schema); tag != nil {
				return []*influx.PointTags{{*tag}}
			}
			return nil
		}
	default:
		return nil
	}
	return nil
}

func conditionTagsByBinary(n *influxql.BinaryExpr, schema map[string]int32) *influx.Tag {
	if isTimeCondition(n) {
		return nil
	}
	// Retrieve the variable reference from the correct side of the expression.
	key, ok := n.LHS.(*influxql.VarRef)
	value := n.RHS
	if !ok {
		return nil
	}

	switch value := value.(type) {
	case *influxql.StringLiteral:
		if v, ok := schema[key.Val]; ok && v == influx.Field_Type_Tag {
			return &influx.Tag{Key: key.Val, Value: value.Val}
		}
		return nil
	default:
		return nil
	}
}

func isTimeCondition(expr influxql.Expr) bool {
	switch expr := expr.(type) {
	case *influxql.BinaryExpr:
		key, ok := expr.LHS.(*influxql.VarRef)
		if !ok {
			return false
		}
		if strings.ToLower(key.Val) != "time" {
			return false
		}
		// Create iterator based on value type.
		switch expr.RHS.(type) {
		case *influxql.IntegerLiteral, *influxql.TimeLiteral, *influxql.StringLiteral:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (sgi ShardGroupInfo) EachShards(fn func(s *ShardInfo)) {
	for i := range sgi.Shards {
		fn(&sgi.Shards[i])
	}
}

// ShardGroupInfos implements sort.Interface on []ShardGroupInfo, based
// on the StartTime field.
type ShardGroupInfos []ShardGroupInfo

// Len implements sort.Interface.
func (a ShardGroupInfos) Len() int { return len(a) }

// Swap implements sort.Interface.
func (a ShardGroupInfos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface.
func (a ShardGroupInfos) Less(i, j int) bool {
	iEnd := a[i].EndTime
	if a[i].Truncated() {
		iEnd = a[i].TruncatedAt
	}

	jEnd := a[j].EndTime
	if a[j].Truncated() {
		jEnd = a[j].TruncatedAt
	}

	if iEnd.Equal(jEnd) {
		return a[i].StartTime.Before(a[j].StartTime)
	}

	return iEnd.Before(jEnd)
}

func (sgi *ShardGroupInfo) Shard(id uint64) *ShardInfo {
	for i := range sgi.Shards {
		if sgi.Shards[i].ID == id {
			return &sgi.Shards[i]
		}
	}
	return nil
}

// Contains returns true iif StartTime <= t < EndTime.
func (sgi *ShardGroupInfo) Contains(t time.Time) bool {
	return !t.Before(sgi.StartTime) && t.Before(sgi.EndTime)
}

// Overlaps returns whether the shard group contains data for the time range between min and max
func (sgi *ShardGroupInfo) Overlaps(min, max time.Time) bool {
	return !sgi.StartTime.After(max) && sgi.EndTime.After(min)
}

// Deleted returns whether this ShardGroup has been deleted.
func (sgi *ShardGroupInfo) Deleted() bool {
	return !sgi.DeletedAt.IsZero()
}

// Truncated returns true if this ShardGroup has been truncated (no new writes).
func (sgi *ShardGroupInfo) Truncated() bool {
	return !sgi.TruncatedAt.IsZero()
}

// clone returns a deep copy of sgi.
func (sgi ShardGroupInfo) clone() ShardGroupInfo {
	other := sgi

	if sgi.Shards != nil {
		other.Shards = make([]ShardInfo, len(sgi.Shards))
		for i := range sgi.Shards {
			other.Shards[i] = sgi.Shards[i].clone()
		}
	}

	return other
}

func (sgi *ShardGroupInfo) DestShard(shardKey string) *ShardInfo {
	for i := range sgi.Shards {
		if sgi.Shards[i].Contain(shardKey) {
			return &sgi.Shards[i]
		}
	}
	return nil
}

// ShardFor returns the ShardInfo for a Point hash.
func (sgi *ShardGroupInfo) ShardFor(hash uint64, aliveShardIdxes []int) *ShardInfo {
	// hash mod all shards in shard-storage and replication policy
	if len(aliveShardIdxes) == 0 {
		return nil
	}
	return &sgi.Shards[aliveShardIdxes[hash%uint64(len(aliveShardIdxes))]]
}

// marshal serializes to a protobuf representation.
func (sgi *ShardGroupInfo) marshal() *proto2.ShardGroupInfo {
	pb := &proto2.ShardGroupInfo{
		ID:         proto.Uint64(sgi.ID),
		StartTime:  proto.Int64(MarshalTime(sgi.StartTime)),
		EndTime:    proto.Int64(MarshalTime(sgi.EndTime)),
		DeletedAt:  proto.Int64(MarshalTime(sgi.DeletedAt)),
		EngineType: proto.Uint32(uint32(sgi.EngineType)),
		Version:    proto.Uint32(sgi.Version),
	}

	if !sgi.TruncatedAt.IsZero() {
		pb.TruncatedAt = proto.Int64(MarshalTime(sgi.TruncatedAt))
	}

	pb.Shards = make([]*proto2.ShardInfo, len(sgi.Shards))
	for i := range sgi.Shards {
		pb.Shards[i] = sgi.Shards[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (sgi *ShardGroupInfo) unmarshal(pb *proto2.ShardGroupInfo) {
	sgi.ID = pb.GetID()
	// todo mjq why if else
	if i := pb.GetStartTime(); i == 0 {
		sgi.StartTime = time.Unix(0, 0).UTC()
	} else {
		sgi.StartTime = UnmarshalTime(i)
	}
	if i := pb.GetEndTime(); i == 0 {
		sgi.EndTime = time.Unix(0, 0).UTC()
	} else {
		sgi.EndTime = UnmarshalTime(i)
	}
	sgi.DeletedAt = UnmarshalTime(pb.GetDeletedAt())
	sgi.EngineType = config.EngineType(pb.GetEngineType())

	if pb != nil && pb.TruncatedAt != nil {
		sgi.TruncatedAt = UnmarshalTime(pb.GetTruncatedAt())
	}

	if len(pb.GetShards()) > 0 {
		sgi.Shards = make([]ShardInfo, len(pb.GetShards()))
		for i, x := range pb.GetShards() {
			sgi.Shards[i].unmarshal(x)
		}
	}

	sgi.Version = pb.GetVersion()
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID              uint64
	Owners          []uint32 // pt group for replications.
	Min             string
	Max             string
	Tier            uint64
	IndexID         uint64
	DownSampleID    uint64
	DownSampleLevel int64
	ReadOnly        bool
	MarkDelete      bool
}

func (si ShardInfo) Contain(shardKey string) bool {
	gtMin := strings.Compare(si.Min, shardKey) <= 0
	ltMax := si.Max == ""
	ltMax = ltMax || strings.Compare(shardKey, si.Max) < 0
	return gtMin && ltMax
}

func (si ShardInfo) ContainPrefix(prefix string) bool {
	prefixLen := len(prefix)
	gtMin := si.Min == ""
	if len(si.Min) > prefixLen {
		gtMin = strings.Compare(si.Min[:prefixLen], prefix) <= 0
	} else {
		gtMin = gtMin || strings.Compare(si.Min, prefix) <= 0
	}

	ltMax := si.Max == "" || strings.Compare(prefix, si.Max) < 0
	return gtMin && ltMax
}

// clone returns a deep copy of si.
func (si ShardInfo) clone() ShardInfo {
	other := si

	other.Owners = make([]uint32, len(si.Owners))
	for i := range si.Owners {
		other.Owners[i] = si.Owners[i]
	}

	return other
}

// marshal serializes to a protobuf representation.
func (si ShardInfo) marshal() *proto2.ShardInfo {
	pb := &proto2.ShardInfo{
		ID:              proto.Uint64(si.ID),
		Min:             proto.String(si.Min),
		Max:             proto.String(si.Max),
		Tier:            proto.Uint64(si.Tier),
		IndexID:         proto.Uint64(si.IndexID),
		DownSampleLevel: proto.Int64(si.DownSampleLevel),
		DownSampleID:    proto.Uint64(si.DownSampleID),
		ReadOnly:        proto.Bool(si.ReadOnly),
		MarkDelete:      proto.Bool(si.MarkDelete),
	}
	pb.OwnerIDs = make([]uint32, len(si.Owners))
	for i := range si.Owners {
		pb.OwnerIDs[i] = si.Owners[i]
	}

	return pb
}

// UnmarshalBinary decodes the object from a binary format.
func (si *ShardInfo) UnmarshalBinary(buf []byte) error {
	var pb proto2.ShardInfo
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	si.unmarshal(&pb)
	return nil
}

// unmarshal deserializes from a protobuf representation.
func (si *ShardInfo) unmarshal(pb *proto2.ShardInfo) {
	si.ID = pb.GetID()
	si.Min = pb.GetMin()
	si.Max = pb.GetMax()
	si.Tier = pb.GetTier()
	si.IndexID = pb.GetIndexID()
	si.DownSampleLevel = pb.GetDownSampleLevel()
	si.DownSampleID = pb.GetDownSampleID()
	si.ReadOnly = pb.GetReadOnly()
	si.MarkDelete = pb.GetMarkDelete()

	si.Owners = make([]uint32, len(pb.GetOwnerIDs()))
	for i, x := range pb.GetOwnerIDs() {
		si.Owners[i] = uint32(x)
	}
}

// ShardOwner represents a node that owns a shard.
type ShardOwner struct {
	NodeID uint64
}

// clone returns a deep copy of so.
func (so ShardOwner) clone() ShardOwner {
	return so
}

// marshal serializes to a protobuf representation.
func (so ShardOwner) marshal() *proto2.ShardOwner {
	return &proto2.ShardOwner{
		NodeID: proto.Uint64(so.NodeID),
	}
}

// unmarshal deserializes from a protobuf representation.
func (so *ShardOwner) unmarshal(pb *proto2.ShardOwner) {
	so.NodeID = pb.GetNodeID()
}

func HashID(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func TierToString(tier uint64) string {
	switch tier {
	case util.Hot:
		return "hot"
	case util.Warm:
		return "warm"
	case util.Cold:
		return "cold"
	case util.Moving:
		return "moving"
	}
	return "unknown"
}

func StringToTier(tier string) uint64 {
	switch tier {
	case "HOT":
		return util.Hot
	case "WARM":
		return util.Warm
	case "COLD":
		return util.Cold
	case "MOVING":
		return util.Moving
	default:
		return util.TierBegin
	}
}
