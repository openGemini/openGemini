package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/meta/data.go

2022.01.23 Add Measurements to store measurement meta data
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

// RetentionPolicyInfo represents metadata about a retention policy.
type RetentionPolicyInfo struct {
	Name                 string
	ReplicaN             int
	Duration             time.Duration
	ShardGroupDuration   time.Duration
	HotDuration          time.Duration
	WarmDuration         time.Duration
	IndexGroupDuration   time.Duration
	IndexGroups          []IndexGroupInfo
	Measurements         map[string]*MeasurementInfo // {"cpu_0001": *MeasurementInfo}
	MstVersions          map[string]uint32           // { "cpu": 1}
	ShardGroups          []ShardGroupInfo
	Subscriptions        []SubscriptionInfo
	DownSamplePolicyInfo *DownSamplePolicyInfo
	MarkDeleted          bool
}

// NewRetentionPolicyInfo returns a new instance of RetentionPolicyInfo
// with default replication and duration.
func NewRetentionPolicyInfo(name string) *RetentionPolicyInfo {
	return &RetentionPolicyInfo{
		Name:        name,
		ReplicaN:    DefaultRetentionPolicyReplicaN,
		Duration:    DefaultRetentionPolicyDuration,
		MarkDeleted: false,
	}
}

// DefaultRetentionPolicyInfo returns a new instance of RetentionPolicyInfo
// with default name, replication, and duration.
func DefaultRetentionPolicyInfo() *RetentionPolicyInfo {
	return NewRetentionPolicyInfo(DefaultRetentionPolicyName)
}

func (rpi *RetentionPolicyInfo) updateWithOtherRetentionPolicy(newRpi *RetentionPolicyInfo) {
	rpi.Name = newRpi.Name
	rpi.ShardGroupDuration = newRpi.ShardGroupDuration
	rpi.HotDuration = newRpi.HotDuration
	rpi.WarmDuration = newRpi.WarmDuration
	rpi.IndexGroupDuration = newRpi.IndexGroupDuration
	rpi.Duration = newRpi.Duration
}

func (rpi *RetentionPolicyInfo) EqualsAnotherRp(other *RetentionPolicyInfo) bool {
	return rpi.ReplicaN == other.ReplicaN && rpi.Duration == other.Duration &&
		rpi.ShardGroupDuration == other.ShardGroupDuration &&
		rpi.IndexGroupDuration == other.IndexGroupDuration && rpi.HotDuration == other.HotDuration &&
		rpi.WarmDuration == other.WarmDuration
}

func (rpi *RetentionPolicyInfo) CheckSpecValid() error {
	// Normalise ShardDuration before comparing to any existing
	// retention policies. The client is supposed to do this, but
	// do it again to verify input.
	// create database with auto rp create should set index duration in data
	rpi.ShardGroupDuration = normalisedShardDuration(rpi.ShardGroupDuration, rpi.Duration)
	rpi.IndexGroupDuration = normalisedIndexDuration(rpi.IndexGroupDuration, rpi.ShardGroupDuration)

	// check other duration should be n*shard group duration
	if rpi.HotDuration%rpi.ShardGroupDuration != 0 || rpi.WarmDuration%rpi.ShardGroupDuration != 0 {
		return ErrIncompatibleShardGroupDurations
	}

	err := rpi.checkGeqThanMinDuration()
	if err != nil {
		return err
	}

	err = rpi.checkGeqThanShardGroupDuration()
	if err != nil {
		return err
	}

	err = rpi.checkLeqThanDuration()
	if err != nil {
		return err
	}

	return nil
}

func (rpi *RetentionPolicyInfo) checkGeqThanMinDuration() error {
	if rpi.Duration != 0 && rpi.Duration < MinRetentionPolicyDuration {
		return ErrRetentionPolicyDurationTooLow
	}

	if rpi.HotDuration != 0 && rpi.HotDuration < MinRetentionPolicyDuration {
		return ErrRetentionPolicyDurationTooLow
	}

	if rpi.WarmDuration != 0 && rpi.WarmDuration < MinRetentionPolicyDuration {
		return ErrRetentionPolicyDurationTooLow
	}
	return nil
}

func (rpi *RetentionPolicyInfo) checkGeqThanShardGroupDuration() error {
	if rpi.Duration != 0 && rpi.Duration < rpi.ShardGroupDuration {
		return ErrIncompatibleDurations
	}

	if rpi.HotDuration != 0 && rpi.HotDuration < rpi.ShardGroupDuration {
		return ErrIncompatibleHotDurations
	}

	if rpi.WarmDuration != 0 && rpi.WarmDuration < rpi.ShardGroupDuration {
		return ErrIncompatibleWarmDurations
	}
	return nil
}

func (rpi *RetentionPolicyInfo) checkLeqThanDuration() error {
	// every duration should litter than duration
	if rpi.Duration != 0 && rpi.HotDuration != 0 && rpi.HotDuration > rpi.Duration {
		return ErrIncompatibleHotDurations
	}

	if rpi.Duration != 0 && rpi.WarmDuration != 0 && rpi.WarmDuration > rpi.Duration {
		return ErrIncompatibleWarmDurations
	}

	return nil
}

// Apply applies a specification to the retention policy info.
func (rpi *RetentionPolicyInfo) Apply(spec *RetentionPolicySpec) *RetentionPolicyInfo {
	rp := &RetentionPolicyInfo{
		Name:               rpi.Name,
		ReplicaN:           rpi.ReplicaN,
		Duration:           rpi.Duration,
		ShardGroupDuration: rpi.ShardGroupDuration,
	}
	if spec.Name != "" {
		rp.Name = spec.Name
	}
	if spec.ReplicaN != nil {
		rp.ReplicaN = *spec.ReplicaN
	}
	if spec.Duration != nil {
		rp.Duration = *spec.Duration
	}
	if spec.HotDuration != nil {
		rp.HotDuration = *spec.HotDuration
	}
	if spec.WarmDuration != nil {
		rp.WarmDuration = *spec.WarmDuration
	}

	rp.ShardGroupDuration = normalisedShardDuration(spec.ShardGroupDuration, rp.Duration)
	rp.IndexGroupDuration = normalisedIndexDuration(spec.IndexGroupDuration, rp.ShardGroupDuration)
	return rp
}

func (rpi *RetentionPolicyInfo) shardingType() string {
	shardType := ""
	if len(rpi.Measurements) > 0 {
		for _, mst := range rpi.Measurements {
			if len(mst.ShardKeys) > 0 {
				shardType = mst.ShardKeys[0].Type
			}
		}
	}
	return shardType
}

func (rpi *RetentionPolicyInfo) TimeRangeInfo(shardID uint64) *ShardTimeRangeInfo {
	for i := len(rpi.ShardGroups) - 1; i >= 0; i-- {
		if rpi.ShardGroups[i].Shards[0].ID > shardID ||
			rpi.ShardGroups[i].Shards[len(rpi.ShardGroups[i].Shards)-1].ID < shardID {
			continue
		}
		timeRangeInfo := ShardTimeRangeInfo{}
		timeRangeInfo.TimeRange = TimeRangeInfo{StartTime: rpi.ShardGroups[i].StartTime, EndTime: rpi.ShardGroups[i].EndTime}
		shard := rpi.ShardGroups[i].Shard(shardID)
		if shard == nil {
			continue
		}

		timeRangeInfo.OwnerIndex = IndexDescriptor{IndexID: shard.IndexID}
		timeRangeInfo.OwnerIndex.IndexGroupID, timeRangeInfo.OwnerIndex.TimeRange = rpi.getIndexGroupTimeRange(shard.IndexID)
		timeRangeInfo.ShardDuration = &ShardDurationInfo{DurationInfo: DurationDescriptor{Duration: rpi.Duration,
			Tier: shard.Tier, TierDuration: rpi.TierDuration(shard.Tier)}}
		timeRangeInfo.ShardType = rpi.shardingType()
		return &timeRangeInfo
	}
	return nil
}

func (rpi *RetentionPolicyInfo) getIndexGroupTimeRange(indexID uint64) (indexGroupID uint64, timeRange TimeRangeInfo) {
	for i := len(rpi.IndexGroups) - 1; i >= 0; i-- {
		length := len(rpi.IndexGroups[i].Indexes)
		if indexID >= rpi.IndexGroups[i].Indexes[0].ID && indexID <= rpi.IndexGroups[i].Indexes[length-1].ID {
			indexGroupID = rpi.IndexGroups[i].ID
			timeRange = TimeRangeInfo{StartTime: rpi.IndexGroups[i].StartTime, EndTime: rpi.IndexGroups[i].EndTime}
			break
		}
	}
	return indexGroupID, timeRange
}

func (rpi *RetentionPolicyInfo) TierDuration(tier uint64) time.Duration {
	switch tier {
	case util.Hot:
		return rpi.HotDuration
	case util.Warm:
		return rpi.WarmDuration
	}
	return 0
}

// ShardGroupByTimestamp returns the shard group in the policy that contains the timestamp,
// or nil if no shard group matches.
func (rpi *RetentionPolicyInfo) ShardGroupByTimestamp(timestamp time.Time) *ShardGroupInfo {
	for i := len(rpi.ShardGroups) - 1; i >= 0; i-- {
		sgi := &rpi.ShardGroups[i]
		if sgi.Contains(timestamp) && !sgi.Deleted() && (!sgi.Truncated() || timestamp.Before(sgi.TruncatedAt)) {
			return &rpi.ShardGroups[i]
		}
	}

	return nil
}

// ExpiredShardGroups returns the Shard Groups which are considered expired, for the given time.
func (rpi *RetentionPolicyInfo) ExpiredShardGroups(t time.Time) []*ShardGroupInfo {
	var groups = make([]*ShardGroupInfo, 0)
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].Deleted() {
			continue
		}
		if rpi.Duration != 0 && rpi.ShardGroups[i].EndTime.Add(rpi.Duration).Before(t) {
			groups = append(groups, &rpi.ShardGroups[i])
		}
	}
	return groups
}

// DeletedShardGroups returns the Shard Groups which are marked as deleted.
func (rpi *RetentionPolicyInfo) DeletedShardGroups() []*ShardGroupInfo {
	var groups = make([]*ShardGroupInfo, 0)
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].Deleted() {
			groups = append(groups, &rpi.ShardGroups[i])
		}
	}
	return groups
}

func (rpi *RetentionPolicyInfo) walkShardGroups(fn func(sg *ShardGroupInfo)) {
	for i := range rpi.ShardGroups {
		fn(&rpi.ShardGroups[i])
	}
}

func (rpi *RetentionPolicyInfo) walkIndexGroups(fn func(ig *IndexGroupInfo)) {
	for i := range rpi.IndexGroups {
		fn(&rpi.IndexGroups[i])
	}
}

func (rpi *RetentionPolicyInfo) walkSubscriptions(fn func(subscription *SubscriptionInfo)) {
	for i := range rpi.Subscriptions {
		fn(&rpi.Subscriptions[i])
	}
}

// Marshal serializes to a protobuf representation.
func (rpi *RetentionPolicyInfo) Marshal() *proto2.RetentionPolicyInfo {
	pb := &proto2.RetentionPolicyInfo{
		Name:               proto.String(rpi.Name),
		ReplicaN:           proto.Uint32(uint32(rpi.ReplicaN)),
		Duration:           proto.Int64(int64(rpi.Duration)),
		ShardGroupDuration: proto.Int64(int64(rpi.ShardGroupDuration)),
		MarkDeleted:        proto.Bool(rpi.MarkDeleted),
		HotDuration:        proto.Int64(int64(rpi.HotDuration)),
		WarmDuration:       proto.Int64(int64(rpi.WarmDuration)),
		IndexGroupDuration: proto.Int64(int64(rpi.IndexGroupDuration)),
	}

	if len(rpi.Measurements) > 0 {
		pb.Measurements = make([]*proto2.MeasurementInfo, len(rpi.Measurements))
		i := 0
		for _, msti := range rpi.Measurements {
			pb.Measurements[i] = msti.marshal()
			i++
		}
	}

	if rpi.MstVersions != nil {
		pb.MstVersions = make(map[string]uint32, len(rpi.MstVersions))
		for n, v := range rpi.MstVersions {
			pb.MstVersions[n] = v
		}
	}

	pb.ShardGroups = make([]*proto2.ShardGroupInfo, len(rpi.ShardGroups))
	for i, sgi := range rpi.ShardGroups {
		pb.ShardGroups[i] = sgi.marshal()
	}

	pb.IndexGroups = make([]*proto2.IndexGroupInfo, len(rpi.IndexGroups))
	for i, igi := range rpi.IndexGroups {
		pb.IndexGroups[i] = igi.marshal()
	}

	if len(rpi.Subscriptions) > 0 {
		pb.Subscriptions = make([]*proto2.SubscriptionInfo, len(rpi.Subscriptions))
		for i, sub := range rpi.Subscriptions {
			pb.Subscriptions[i] = sub.marshal()
		}
	}

	if rpi.DownSamplePolicyInfo != nil {
		pb.DownSamplePolicyInfo = rpi.DownSamplePolicyInfo.Marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (rpi *RetentionPolicyInfo) unmarshal(pb *proto2.RetentionPolicyInfo) {
	rpi.Name = pb.GetName()
	rpi.ReplicaN = int(pb.GetReplicaN())
	rpi.Duration = time.Duration(pb.GetDuration())
	rpi.ShardGroupDuration = time.Duration(pb.GetShardGroupDuration())
	rpi.MarkDeleted = pb.GetMarkDeleted()
	rpi.HotDuration = time.Duration(pb.GetHotDuration())
	rpi.WarmDuration = time.Duration(pb.GetWarmDuration())
	rpi.IndexGroupDuration = time.Duration(pb.GetIndexGroupDuration())

	if len(pb.GetMeasurements()) > 0 {
		rpi.Measurements = make(map[string]*MeasurementInfo, len(pb.GetMeasurements()))
		for _, x := range pb.GetMeasurements() {
			msti := &MeasurementInfo{}
			msti.unmarshal(x)
			rpi.Measurements[msti.Name] = msti
		}
	}

	if len(pb.GetMstVersions()) > 0 {
		rpi.MstVersions = pb.GetMstVersions()
	}

	if len(pb.GetShardGroups()) > 0 {
		rpi.ShardGroups = make([]ShardGroupInfo, len(pb.GetShardGroups()))
		for i, x := range pb.GetShardGroups() {
			rpi.ShardGroups[i].unmarshal(x)
		}
	}

	if len(pb.GetIndexGroups()) > 0 {
		rpi.IndexGroups = make([]IndexGroupInfo, len(pb.GetIndexGroups()))
		for i, x := range pb.GetIndexGroups() {
			rpi.IndexGroups[i].unmarshal(x)
		}
	}

	if len(pb.GetSubscriptions()) > 0 {
		rpi.Subscriptions = make([]SubscriptionInfo, len(pb.GetSubscriptions()))
		for i, x := range pb.GetSubscriptions() {
			rpi.Subscriptions[i].unmarshal(x)
		}
	}

	if pb.GetDownSamplePolicyInfo() != nil {
		rpi.DownSamplePolicyInfo = &DownSamplePolicyInfo{}
		rpi.DownSamplePolicyInfo.Unmarshal(pb.GetDownSamplePolicyInfo())
	}
}

// Clone returns a deep copy of rpi.
func (rpi RetentionPolicyInfo) Clone() *RetentionPolicyInfo {
	other := rpi

	if rpi.ShardGroups != nil {
		other.ShardGroups = make([]ShardGroupInfo, len(rpi.ShardGroups))
		for i := range rpi.ShardGroups {
			other.ShardGroups[i] = rpi.ShardGroups[i].clone()
		}
	}

	if rpi.IndexGroups != nil {
		other.IndexGroups = make([]IndexGroupInfo, len(rpi.IndexGroups))
		for i := range rpi.IndexGroups {
			other.IndexGroups[i] = rpi.IndexGroups[i].clone()
		}
	}

	if rpi.Measurements != nil {
		other.Measurements = make(map[string]*MeasurementInfo, len(rpi.Measurements))
		for _, msti := range rpi.Measurements {
			other.Measurements[msti.Name] = msti.clone()
		}
	}

	return &other
}

// MarshalBinary encodes rpi to a binary format.
func (rpi *RetentionPolicyInfo) MarshalBinary() ([]byte, error) {
	return proto.Marshal(rpi.Marshal())
}

// UnmarshalBinary decodes rpi from a binary format.
func (rpi *RetentionPolicyInfo) UnmarshalBinary(data []byte) error {
	var pb proto2.RetentionPolicyInfo
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	rpi.unmarshal(&pb)
	return nil
}

func (rpi *RetentionPolicyInfo) MatchMeasurements(ms influxql.Measurements, ret map[string]*MeasurementInfo) {
	rpi.EachMeasurements(func(mi *MeasurementInfo) {
		if mi.MarkDeleted {
			return
		}

		key := rpi.Name + "." + mi.Name
		if len(ms) == 0 {
			ret[key] = mi
			return
		}

		for _, m := range ms {
			if m.RetentionPolicy != "" && m.RetentionPolicy != rpi.Name {
				continue
			}

			originName := mi.OriginName()
			if m.Regex != nil && m.Regex.Val.MatchString(originName) {
				ret[key] = mi
			}

			if m.Name == originName {
				ret[key] = mi
			}
		}
	})
}

func (rpi *RetentionPolicyInfo) EachMeasurements(fn func(m *MeasurementInfo)) {
	for _, msti := range rpi.Measurements {
		if msti.MarkDeleted {
			continue
		}
		fn(msti)
	}
}

// shardGroupDuration returns the default duration for a shard group based on a policy duration.
func shardGroupDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}

// normalisedShardDuration returns normalised shard duration based on a policy duration.
func normalisedShardDuration(sgd, d time.Duration) time.Duration {
	// If it is zero, it likely wasn't specified, so we default to the shard group duration
	if sgd == 0 {
		return shardGroupDuration(d)
	}
	// If it was specified, but it's less than the MinRetentionPolicyDuration, then normalize
	// to the MinRetentionPolicyDuration
	if sgd < MinRetentionPolicyDuration {
		return shardGroupDuration(MinRetentionPolicyDuration)
	}
	return sgd
}

func (rpi *RetentionPolicyInfo) GetMeasurement(name string) (*MeasurementInfo, error) {
	msti := rpi.Measurement(name)
	if msti == nil {
		return nil, ErrMeasurementNotFound
	}
	if msti.MarkDeleted {
		return nil, ErrMeasurementIsBeingDelete
	}
	return msti, nil
}

func (rpi *RetentionPolicyInfo) Measurement(name string) *MeasurementInfo {
	version := rpi.MstVersions[name]
	nameWithVer := influx.GetNameWithVersion(name, version)
	return rpi.Measurements[nameWithVer]
}

func (rpi *RetentionPolicyInfo) validMeasurementShardType(shardType, mstName string) error {
	var msti *MeasurementInfo
	for _, mst := range rpi.Measurements {
		if influx.GetOriginMstName(mst.Name) == mstName {
			continue
		}
		msti = mst
		break
	}
	if msti == nil || msti.ShardKeys == nil {
		return nil
	}

	if msti.ShardKeys[0].Type != shardType {
		return ErrShardingTypeNotEqual(rpi.Name, msti.ShardKeys[0].Type, shardType)
	}
	return nil
}

func (rpi *RetentionPolicyInfo) maxShardGroupID() uint64 {
	if len(rpi.ShardGroups) == 0 {
		return 0
	}
	maxId := rpi.ShardGroups[0].ID
	for i := range rpi.ShardGroups {
		if maxId < rpi.ShardGroups[i].ID {
			maxId = rpi.ShardGroups[i].ID
		}
	}
	return maxId
}

func (rpi *RetentionPolicyInfo) HasDownSamplePolicy() bool {
	return rpi.DownSamplePolicyInfo != nil && (rpi.DownSamplePolicyInfo.Calls != nil || rpi.DownSamplePolicyInfo.DownSamplePolicies != nil)
}

// RetentionPolicySpec represents the specification for a new retention policy.
type RetentionPolicySpec struct {
	Name               string
	ReplicaN           *int
	Duration           *time.Duration
	ShardGroupDuration time.Duration
	HotDuration        *time.Duration
	WarmDuration       *time.Duration
	IndexGroupDuration time.Duration
}

// NewRetentionPolicyInfo creates a new retention policy info from the specification.
func (s *RetentionPolicySpec) NewRetentionPolicyInfo() *RetentionPolicyInfo {
	return DefaultRetentionPolicyInfo().Apply(s)
}

// Matches checks if this retention policy specification matches
// an existing retention policy.
func (s *RetentionPolicySpec) Matches(rpi *RetentionPolicyInfo) bool {
	if rpi == nil {
		return false
	} else if s.Name != "" && s.Name != rpi.Name {
		return false
	} else if s.Duration != nil && *s.Duration != rpi.Duration {
		return false
	} else if s.ReplicaN != nil && *s.ReplicaN != rpi.ReplicaN {
		return false
	}

	// Normalise ShardDuration before comparing to any existing retention policies.
	// Normalize with the retention policy info's duration instead of the spec
	// since they should be the same and we're performing a comparison.
	sgDuration := normalisedShardDuration(s.ShardGroupDuration, rpi.Duration)
	return sgDuration == rpi.ShardGroupDuration
}

// marshal serializes to a protobuf representation.
func (s *RetentionPolicySpec) marshal() *proto2.RetentionPolicySpec {
	pb := &proto2.RetentionPolicySpec{}
	if s.Name != "" {
		pb.Name = proto.String(s.Name)
	}
	if s.Duration != nil {
		pb.Duration = proto.Int64(int64(*s.Duration))
	}
	if s.ShardGroupDuration > 0 {
		pb.ShardGroupDuration = proto.Int64(int64(s.ShardGroupDuration))
	}
	if s.ReplicaN != nil {
		pb.ReplicaN = proto.Uint32(uint32(*s.ReplicaN))
	}
	return pb
}

// unmarshal deserializes from a protobuf representation.
func (s *RetentionPolicySpec) unmarshal(pb *proto2.RetentionPolicySpec) {
	if pb.Name != nil {
		s.Name = pb.GetName()
	}
	if pb.Duration != nil {
		duration := time.Duration(pb.GetDuration())
		s.Duration = &duration
	}
	if pb.ShardGroupDuration != nil {
		s.ShardGroupDuration = time.Duration(pb.GetShardGroupDuration())
	}
	if pb.ReplicaN != nil {
		replicaN := int(pb.GetReplicaN())
		s.ReplicaN = &replicaN
	}
}

// MarshalBinary encodes RetentionPolicySpec to a binary format.
func (s *RetentionPolicySpec) MarshalBinary() ([]byte, error) {
	return proto.Marshal(s.marshal())
}

// UnmarshalBinary decodes RetentionPolicySpec from a binary format.
func (s *RetentionPolicySpec) UnmarshalBinary(data []byte) error {
	var pb proto2.RetentionPolicySpec
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	s.unmarshal(&pb)
	return nil
}

// RetentionPolicyUpdate represents retention policy fields to be updated.
type RetentionPolicyUpdate struct {
	Name               *string
	Duration           *time.Duration
	ReplicaN           *int
	ShardGroupDuration *time.Duration
	HotDuration        *time.Duration
	WarmDuration       *time.Duration
	IndexGroupDuration *time.Duration
}

// SetName sets the RetentionPolicyUpdate.Name.
func (rpu *RetentionPolicyUpdate) SetName(v string) { rpu.Name = &v }

// SetDuration sets the RetentionPolicyUpdate.Duration.
func (rpu *RetentionPolicyUpdate) SetDuration(v time.Duration) { rpu.Duration = &v }

// SetReplicaN sets the RetentionPolicyUpdate.ReplicaN.
func (rpu *RetentionPolicyUpdate) SetReplicaN(v int) { rpu.ReplicaN = &v }

// SetShardGroupDuration sets the RetentionPolicyUpdate.ShardGroupDuration.
func (rpu *RetentionPolicyUpdate) SetShardGroupDuration(v time.Duration) { rpu.ShardGroupDuration = &v }
