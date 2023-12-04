package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go

2022.01.23 Change RetentionPolicies struct from slice to map
Add PTInfo to present partition of database
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"sort"

	"github.com/gogo/protobuf/proto"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

// DatabaseInfo represents information about a database in the system.
type DatabaseInfo struct {
	Name                   string
	DefaultRetentionPolicy string
	RetentionPolicies      map[string]*RetentionPolicyInfo
	MarkDeleted            bool
	ShardKey               ShardKeyInfo
	EnableTagArray         bool
	ReplicaN               int
	ContinuousQueries      map[string]*ContinuousQueryInfo // {"cqName": *ContinuousQueryInfo}
	Options                *ObsOptions
}

func NewDatabase(name string) *DatabaseInfo {
	return &DatabaseInfo{
		Name:        name,
		MarkDeleted: false,
	}
}

func (di *DatabaseInfo) GetRetentionPolicy(name string) (*RetentionPolicyInfo, error) {
	rpi := di.RetentionPolicy(name)
	if rpi == nil {
		return nil, ErrRetentionPolicyNotFound(name)
	}
	if rpi.MarkDeleted {
		return nil, ErrRetentionPolicyIsBeingDelete
	}
	return rpi, nil
}

// RetentionPolicy returns a retention policy by name.
func (di *DatabaseInfo) RetentionPolicy(name string) *RetentionPolicyInfo {
	if name == "" {
		if di.DefaultRetentionPolicy == "" {
			return nil
		}
		name = di.DefaultRetentionPolicy
	}

	return di.RetentionPolicies[name]
}

func (di *DatabaseInfo) checkUpdateRetentionPolicyName(name string, updateName *string) error {
	if updateName == nil || *updateName == name {
		return nil
	}
	if di.RetentionPolicy(*updateName) != nil {
		return ErrRetentionPolicyExists
	}
	return nil
}

// ShardInfos returns a list of all shards' info for the database.
func (di DatabaseInfo) ShardInfos() []ShardInfo {
	shards := map[uint64]*ShardInfo{}
	for i := range di.RetentionPolicies {
		for j := range di.RetentionPolicies[i].ShardGroups {
			sg := di.RetentionPolicies[i].ShardGroups[j]
			// Skip deleted shard groups
			if sg.Deleted() {
				continue
			}
			for k := range sg.Shards {
				si := &di.RetentionPolicies[i].ShardGroups[j].Shards[k]
				shards[si.ID] = si
			}
		}
	}

	infos := make([]ShardInfo, 0, len(shards))
	for _, info := range shards {
		infos = append(infos, *info)
	}

	return infos
}

// clone returns a deep copy of di.
func (di DatabaseInfo) clone() *DatabaseInfo {
	other := di

	if di.RetentionPolicies != nil {
		other.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
		for _, rp := range di.RetentionPolicies {
			other.RetentionPolicies[rp.Name] = rp.Clone()
		}
	}

	if di.ContinuousQueries != nil {
		other.ContinuousQueries = make(map[string]*ContinuousQueryInfo)
		for _, cq := range di.ContinuousQueries {
			other.ContinuousQueries[cq.Name] = cq.Clone()
		}
	}

	if di.Options != nil {
		options := *di.Options
		other.Options = &options
	}

	return &other
}

// marshal serializes to a protobuf representation.
func (di DatabaseInfo) marshal() *proto2.DatabaseInfo {
	pb := &proto2.DatabaseInfo{}
	pb.Name = proto.String(di.Name)
	pb.DefaultRetentionPolicy = proto.String(di.DefaultRetentionPolicy)

	pb.RetentionPolicies = make([]*proto2.RetentionPolicyInfo, len(di.RetentionPolicies))
	i := 0
	for _, rp := range di.RetentionPolicies {
		pb.RetentionPolicies[i] = rp.Marshal()
		i++
	}

	pb.ContinuousQueries = make([]*proto2.ContinuousQueryInfo, len(di.ContinuousQueries))
	i = 0
	for _, cq := range di.ContinuousQueries {
		pb.ContinuousQueries[i] = cq.Marshal()
		i++
	}

	pb.MarkDeleted = proto.Bool(di.MarkDeleted)
	if di.ShardKey.ShardKey != nil {
		pb.ShardKey = di.ShardKey.Marshal()
	}
	pb.EnableTagArray = proto.Bool(di.EnableTagArray)
	pb.ReplicaN = proto.Int64(int64(di.ReplicaN))
	if di.Options != nil {
		pb.Options = di.Options.Marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (di *DatabaseInfo) unmarshal(pb *proto2.DatabaseInfo) {
	di.Name = pb.GetName()
	di.DefaultRetentionPolicy = pb.GetDefaultRetentionPolicy()

	if len(pb.GetRetentionPolicies()) > 0 {
		di.RetentionPolicies = make(map[string]*RetentionPolicyInfo)
		for _, x := range pb.GetRetentionPolicies() {
			rp := &RetentionPolicyInfo{}
			rp.unmarshal(x)
			di.RetentionPolicies[rp.Name] = rp
		}
	}

	if len(pb.GetContinuousQueries()) > 0 {
		di.ContinuousQueries = make(map[string]*ContinuousQueryInfo)
		for _, x := range pb.GetContinuousQueries() {
			cq := &ContinuousQueryInfo{}
			cq.unmarshal(x)
			di.ContinuousQueries[cq.Name] = cq
		}
	}

	di.MarkDeleted = pb.GetMarkDeleted()
	if pb.ShardKey != nil {
		di.ShardKey.unmarshal(pb.GetShardKey())
	}
	di.EnableTagArray = pb.GetEnableTagArray()
	di.ReplicaN = int(pb.GetReplicaN())
	if di.ReplicaN == 0 {
		di.ReplicaN = 1
	}
	if pb.GetOptions() != nil {
		di.Options = &ObsOptions{}
		di.Options.Unmarshal(pb.GetOptions())
	}
}

type PtOwner struct {
	NodeID uint64
}

// clone returns a deep copy of so.
func (po PtOwner) clone() PtOwner {
	return po
}

// marshal serializes to a protobuf representation.
func (po PtOwner) marshal() *proto2.PtOwner {
	return &proto2.PtOwner{
		NodeID: proto.Uint64(po.NodeID),
	}
}

// unmarshal deserializes from a protobuf representation.
func (po *PtOwner) unmarshal(pb *proto2.PtOwner) {
	po.NodeID = pb.GetNodeID()
}

type DBPtInfos []PtInfo

type PtStatus uint32

const (
	Online PtStatus = iota
	PrepareOffload
	PrepareAssign
	Offline
	RollbackPrepareOffload
	RollbackPrepareAssign
	Disabled
)

type PtInfo struct {
	Owner  PtOwner
	Status PtStatus
	PtId   uint32
	Ver    uint64
	RGID   uint32
}

func GetNodeDBPts(pi DBPtInfos, nodeId uint64) []uint32 {
	pts := make([]uint32, 0, len(pi))
	for _, ptInfo := range pi {
		if ptInfo.Owner.NodeID == nodeId {
			pts = append(pts, ptInfo.PtId)
		}
	}
	return pts
}

func (pi *PtInfo) Marshal() *proto2.PtInfo {
	pb := &proto2.PtInfo{
		Status: proto.Uint32(uint32(pi.Status)),
		PtId:   proto.Uint32(pi.PtId),
		Ver:    proto.Uint64(pi.Ver),
		RGID:   proto.Uint32(pi.RGID),
	}
	pb.Owner = pi.Owner.marshal()
	return pb
}

func (pi *PtInfo) unmarshal(pb *proto2.PtInfo) {
	pi.Status = PtStatus(pb.GetStatus())
	pi.Owner.unmarshal(pb.Owner)
	pi.PtId = pb.GetPtId()
	pi.Ver = pb.GetVer()
	pi.RGID = pb.GetRGID()
}

func (di *DatabaseInfo) WalkRetentionPolicy(fn func(rp *RetentionPolicyInfo)) {
	for rpName := range di.RetentionPolicies {
		fn(di.RetentionPolicies[rpName])
	}
}

type DatabaseBriefInfo struct {
	Name           string
	EnableTagArray bool
}

func (di *DatabaseBriefInfo) Marshal() ([]byte, error) {
	pb := &proto2.DatabaseBriefInfo{}
	pb.Name = proto.String(di.Name)
	pb.EnableTagArray = proto.Bool(di.EnableTagArray)

	return proto.Marshal(pb)
}

func (di *DatabaseInfo) WalkContinuousQuery(fn func(cq *ContinuousQueryInfo)) {
	for cqName := range di.ContinuousQueries {
		fn(di.ContinuousQueries[cqName])
	}
}

func (di *DatabaseInfo) WalkRetentionPolicyOrderly(fn func(rp *RetentionPolicyInfo)) {
	rpNames := make([]string, 0, len(di.RetentionPolicies))
	for rpName := range di.RetentionPolicies {
		rpNames = append(rpNames, rpName)
	}
	sort.Strings(rpNames)
	for i := 0; i < len(rpNames); i++ {
		fn(di.RetentionPolicies[rpNames[i]])
	}
}
