// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package meta

import (
	"errors"
	"sort"
	"time"

	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
)

type DurationDescriptor struct {
	Tier          uint64
	TierDuration  time.Duration
	Duration      time.Duration
	MergeDuration time.Duration
}

type ShardDurationInfo struct {
	Ident        ShardIdentifier
	DurationInfo DurationDescriptor
}

type IndexDurationInfo struct {
	Ident        IndexBuilderIdentifier
	DurationInfo DurationDescriptor
}

type ShardDurationResponse struct {
	DataIndex uint64
	Durations []ShardDurationInfo
}

func (r *ShardDurationResponse) marshal() *proto2.ShardDurationResponse {
	pb := &proto2.ShardDurationResponse{}
	pb.DataIndex = proto.Uint64(r.DataIndex)
	pb.Durations = make([]*proto2.ShardDurationInfo, len(r.Durations))
	for i := range r.Durations {
		pb.Durations[i] = r.Durations[i].marshal()
	}
	return pb
}

func (r *ShardDurationResponse) MarshalBinary() ([]byte, error) {
	pb := r.marshal()
	return proto.Marshal(pb)
}

func (r *ShardDurationResponse) unmarshal(pb *proto2.ShardDurationResponse) {
	r.DataIndex = pb.GetDataIndex()
	r.Durations = make([]ShardDurationInfo, len(pb.GetDurations()))
	for i := range pb.GetDurations() {
		r.Durations[i].unmarshal(pb.GetDurations()[i])
	}
}

func (r *ShardDurationResponse) UnmarshalBinary(buf []byte) error {
	pb := &proto2.ShardDurationResponse{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	r.unmarshal(pb)
	return nil
}

type IndexDurationResponse struct {
	DataIndex uint64
	Durations []IndexDurationInfo
}

func (r *IndexDurationResponse) marshal() *proto2.IndexDurationResponse {
	pb := &proto2.IndexDurationResponse{}
	pb.DataIndex = proto.Uint64(r.DataIndex)
	pb.Durations = make([]*proto2.IndexDurationInfo, len(r.Durations))
	for i := range r.Durations {
		pb.Durations[i] = r.Durations[i].marshal()
	}
	return pb
}

func (r *IndexDurationResponse) MarshalBinary() ([]byte, error) {
	pb := r.marshal()
	return proto.Marshal(pb)
}

func (r *IndexDurationResponse) unmarshal(pb *proto2.IndexDurationResponse) {
	r.DataIndex = pb.GetDataIndex()
	r.Durations = make([]IndexDurationInfo, len(pb.GetDurations()))
	for i := range pb.GetDurations() {
		r.Durations[i].unmarshal(pb.GetDurations()[i])
	}
}

func (r *IndexDurationResponse) UnmarshalBinary(buf []byte) error {
	pb := &proto2.IndexDurationResponse{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	r.unmarshal(pb)
	return nil
}

type ShardTimeRangeInfo struct {
	TimeRange     TimeRangeInfo
	OwnerIndex    IndexDescriptor
	ShardDuration *ShardDurationInfo
	ShardType     string
}

type ShardIdentifier struct {
	ShardID         uint64
	ShardGroupID    uint64
	Policy          string
	OwnerDb         string
	OwnerPt         uint32
	ShardType       string
	DownSampleLevel int
	DownSampleID    uint64
	ReadOnly        bool
	EngineType      uint32
	StartTime       time.Time
	EndTime         time.Time
}

type IndexBuilderIdentifier struct {
	IndexID      uint64
	IndexGroupID uint64
	Policy       string
	OwnerDb      string
	OwnerPt      uint32
	StartTime    time.Time
	EndTime      time.Time
}

type IndexIdentifier struct {
	Index   *IndexDescriptor
	OwnerDb string
	OwnerPt uint32
	Policy  string
}

type IndexDescriptor struct {
	IndexID      uint64
	IndexGroupID uint64
	TimeRange    TimeRangeInfo
}

type TimeRangeInfo struct {
	StartTime time.Time
	EndTime   time.Time
}

func (t *TimeRangeInfo) equals(t1 *TimeRangeInfo) bool {
	return t.StartTime.Equal(t1.StartTime) && t.EndTime.Equal(t1.EndTime)
}

func (t *ShardTimeRangeInfo) MarshalBinary() ([]byte, error) {
	pb := &proto2.ShardTimeRangeInfo{}
	pb.TimeRange = t.TimeRange.marshal()
	pb.OwnerIndex = t.OwnerIndex.marshal()
	pb.ShardDuration = t.ShardDuration.marshal()
	pb.ShardType = proto.String(t.ShardType)
	return proto.Marshal(pb)
}

func (t *ShardTimeRangeInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.ShardTimeRangeInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	t.TimeRange.unmarshal(pb.GetTimeRange())
	t.OwnerIndex.unmarshal(pb.GetOwnerIndex())
	t.ShardDuration = &ShardDurationInfo{}
	t.ShardDuration.unmarshal(pb.ShardDuration)
	t.ShardType = pb.GetShardType()
	return nil
}

func (t *TimeRangeInfo) marshal() *proto2.TimeRangeInfo {
	pb := &proto2.TimeRangeInfo{}
	pb.StartTime = proto.Int64(MarshalTime(t.StartTime))
	pb.EndTime = proto.Int64(MarshalTime(t.EndTime))
	return pb
}

func (t *TimeRangeInfo) unmarshal(timeRange *proto2.TimeRangeInfo) {
	t.StartTime = UnmarshalTime(timeRange.GetStartTime())
	t.EndTime = UnmarshalTime(timeRange.GetEndTime())
}

func (i *IndexDescriptor) marshal() *proto2.IndexDescriptor {
	pb := &proto2.IndexDescriptor{}
	pb.IndexID = proto.Uint64(i.IndexID)
	pb.IndexGroupID = proto.Uint64(i.IndexGroupID)
	pb.TimeRange = i.TimeRange.marshal()
	return pb
}

func (i *IndexDescriptor) unmarshal(index *proto2.IndexDescriptor) {
	i.IndexID = index.GetIndexID()
	i.IndexGroupID = index.GetIndexGroupID()
	i.TimeRange.unmarshal(index.GetTimeRange())
}

func (d *ShardDurationInfo) MarshalBinary() ([]byte, error) {
	return proto.Marshal(d.marshal())
}

func (d *ShardDurationInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.ShardDurationInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	d.unmarshal(pb)
	return nil
}

func (d *ShardDurationInfo) marshal() *proto2.ShardDurationInfo {
	pb := &proto2.ShardDurationInfo{}
	pb.Ident = d.Ident.marshal()
	pb.DurationInfo = d.DurationInfo.marshal()
	return pb
}

func (d *ShardDurationInfo) unmarshal(pb *proto2.ShardDurationInfo) {
	if pb.Ident != nil {
		d.Ident.unmarshal(pb.GetIdent())
	}
	d.DurationInfo.unmarshal(pb.GetDurationInfo())
}

func (d *IndexDurationInfo) MarshalBinary() ([]byte, error) {
	return proto.Marshal(d.marshal())
}

func (d *IndexDurationInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.IndexDurationInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	d.unmarshal(pb)
	return nil
}

func (d *IndexDurationInfo) marshal() *proto2.IndexDurationInfo {
	pb := &proto2.IndexDurationInfo{}
	pb.Ident = d.Ident.marshal()
	pb.DurationInfo = d.DurationInfo.marshal()
	return pb
}

func (d *IndexDurationInfo) unmarshal(pb *proto2.IndexDurationInfo) {
	if pb.Ident != nil {
		d.Ident.unmarshal(pb.GetIdent())
	}
	d.DurationInfo.unmarshal(pb.GetDurationInfo())
}

func (d *DurationDescriptor) marshal() *proto2.DurationDescriptor {
	pb := &proto2.DurationDescriptor{}
	pb.TierType = proto.Uint64(d.Tier)
	pb.TierDuration = proto.Int64(int64(d.TierDuration))
	pb.Duration = proto.Int64(int64(d.Duration))
	pb.MergeDuration = proto.Int64(int64(d.MergeDuration))
	return pb
}

func (d *DurationDescriptor) unmarshal(duration *proto2.DurationDescriptor) {
	d.Tier = duration.GetTierType()
	d.TierDuration = time.Duration(duration.GetTierDuration())
	d.Duration = time.Duration(duration.GetDuration())
	d.MergeDuration = time.Duration(duration.GetMergeDuration())
}

func (i *ShardIdentifier) IsRangeMode() bool {
	return i.ShardType == RANGE
}

func (i *ShardIdentifier) marshal() *proto2.ShardIdentifier {
	pb := &proto2.ShardIdentifier{}
	pb.ShardID = proto.Uint64(i.ShardID)
	pb.OwnerDb = proto.String(i.OwnerDb)
	pb.OwnerPt = proto.Uint32(i.OwnerPt)
	pb.ShardGroupID = proto.Uint64(i.ShardGroupID)
	pb.Policy = proto.String(i.Policy)
	pb.ShardType = proto.String(i.ShardType)
	pb.DownSampleLevel = proto.Int64(int64(i.DownSampleLevel))
	pb.DownSampleID = proto.Uint64(i.DownSampleID)
	pb.ReadOnly = proto.Bool(i.ReadOnly)
	pb.EngineType = proto.Uint32(i.EngineType)
	pb.StartTime = proto.Int64(MarshalTime(i.StartTime))
	pb.EndTime = proto.Int64(MarshalTime(i.EndTime))
	return pb
}

func (i *ShardIdentifier) Marshal() *proto2.ShardIdentifier {
	return i.marshal()
}

func (i *ShardIdentifier) Unmarshal(ident *proto2.ShardIdentifier) {
	i.unmarshal(ident)
}

func (i *ShardIdentifier) unmarshal(ident *proto2.ShardIdentifier) {
	i.OwnerDb = ident.GetOwnerDb()
	i.OwnerPt = ident.GetOwnerPt()
	i.ShardID = ident.GetShardID()
	i.ShardGroupID = ident.GetShardGroupID()
	i.Policy = ident.GetPolicy()
	i.ShardType = ident.GetShardType()
	i.DownSampleLevel = int(ident.GetDownSampleLevel())
	i.DownSampleID = ident.GetDownSampleID()
	i.ReadOnly = ident.GetReadOnly()
	i.EngineType = ident.GetEngineType()
	i.StartTime = UnmarshalTime(ident.GetStartTime())
	i.EndTime = UnmarshalTime(ident.GetEndTime())
}

func (i *IndexBuilderIdentifier) marshal() *proto2.IndexIdentifier {
	pb := &proto2.IndexIdentifier{}
	pb.IndexID = proto.Uint64(i.IndexID)
	pb.OwnerDb = proto.String(i.OwnerDb)
	pb.OwnerPt = proto.Uint32(i.OwnerPt)
	pb.IndexGroupID = proto.Uint64(i.IndexGroupID)
	pb.Policy = proto.String(i.Policy)
	pb.StartTime = proto.Int64(MarshalTime(i.StartTime))
	pb.EndTime = proto.Int64(MarshalTime(i.EndTime))
	return pb
}

func (i *IndexBuilderIdentifier) unmarshal(ident *proto2.IndexIdentifier) {
	i.OwnerDb = ident.GetOwnerDb()
	i.OwnerPt = ident.GetOwnerPt()
	i.IndexID = ident.GetIndexID()
	i.IndexGroupID = ident.GetIndexGroupID()
	i.Policy = ident.GetPolicy()
	i.StartTime = UnmarshalTime(ident.GetStartTime())
	i.EndTime = UnmarshalTime(ident.GetEndTime())
}

type NodeStartInfo struct {
	DataIndex          uint64
	NodeId             uint64
	PtIds              []uint32
	ShardDurationInfos map[uint64]*ShardDurationInfo
	DBBriefInfo        map[string]*DatabaseBriefInfo
	LTime              uint64
	ConnId             uint64
}

func (nsi NodeStartInfo) MarshalBinary() ([]byte, error) {
	pb := &proto2.NodeStartInfo{}
	pb.DataIndex = proto.Uint64(nsi.DataIndex)
	pb.NodeID = proto.Uint64(nsi.NodeId)
	pb.LTime = proto.Uint64(nsi.LTime)
	pb.ConnId = proto.Uint64(nsi.ConnId)

	if len(nsi.PtIds) != 0 {
		pb.PtIds = make([]uint32, len(nsi.PtIds))
		for i := range nsi.PtIds {
			pb.PtIds[i] = nsi.PtIds[i]
		}
	}
	if len(nsi.ShardDurationInfos) > 0 {
		pb.Durations = make([]*proto2.ShardDurationInfo, len(nsi.ShardDurationInfos))
		idx := 0
		for _, shardDuration := range nsi.ShardDurationInfos {
			pb.Durations[idx] = shardDuration.marshal()
			idx++
		}
	}

	if len(nsi.DBBriefInfo) > 0 {
		pb.DBBriefInfo = make([]*proto2.DatabaseBriefInfo, len(nsi.DBBriefInfo))
		idx := 0
		for dbName, dbInfo := range nsi.DBBriefInfo {
			pb.DBBriefInfo[idx] = &proto2.DatabaseBriefInfo{
				Name:           proto.String(dbName),
				EnableTagArray: proto.Bool(dbInfo.EnableTagArray),
			}
			idx++
		}
	}
	return proto.Marshal(pb)
}

func (nsi *NodeStartInfo) UnMarshalBinary(buf []byte) error {
	pb := &proto2.NodeStartInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	nsi.DataIndex = pb.GetDataIndex()
	nsi.NodeId = pb.GetNodeID()
	nsi.PtIds = pb.GetPtIds()
	nsi.LTime = pb.GetLTime()
	nsi.ConnId = pb.GetConnId()
	if len(pb.Durations) == 0 {
		return nil
	}
	nsi.ShardDurationInfos = make(map[uint64]*ShardDurationInfo, len(pb.Durations))
	for i := range pb.Durations {
		shardDuration := &ShardDurationInfo{}
		shardDuration.unmarshal(pb.Durations[i])
		nsi.ShardDurationInfos[shardDuration.Ident.ShardID] = shardDuration
	}
	nsi.DBBriefInfo = make(map[string]*DatabaseBriefInfo, len(pb.DBBriefInfo))
	for i := range pb.DBBriefInfo {
		dbBriefInfo := &DatabaseBriefInfo{
			Name:           pb.DBBriefInfo[i].GetName(),
			EnableTagArray: pb.DBBriefInfo[i].GetEnableTagArray(),
			Replicas:       int(pb.DBBriefInfo[i].GetReplicas()),
		}
		nsi.DBBriefInfo[pb.DBBriefInfo[i].GetName()] = dbBriefInfo
	}
	return nil
}

type CardinalityResponse struct {
	CardinalityInfos []MeasurementCardinalityInfo
	Err              error
}

func (r *CardinalityResponse) MarshalBinary() ([]byte, error) {
	pb := &proto2.CardinalityResponse{}
	pb.Infos = make([]*proto2.MeasurementCardinalityInfo, len(r.CardinalityInfos))
	for i := range r.CardinalityInfos {
		pb.Infos[i] = r.CardinalityInfos[i].marshal()
	}
	if r.Err != nil {
		pb.Err = proto.String(r.Err.Error())
	}
	return proto.Marshal(pb)
}

func (r *CardinalityResponse) UnmarshalBinary(buf []byte) error {
	pb := &proto2.CardinalityResponse{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}
	r.CardinalityInfos = make([]MeasurementCardinalityInfo, len(pb.Infos))
	for i := range pb.Infos {
		r.CardinalityInfos[i].unmarshal(pb.Infos[i])
	}
	if pb.Err != nil {
		r.Err = errors.New(pb.GetErr())
	}
	return nil
}

func (r *CardinalityResponse) Error() error {
	return r.Err
}

type MeasurementCardinalityInfo struct {
	Name             string
	CardinalityInfos []CardinalityInfo
}

func (m *MeasurementCardinalityInfo) marshal() *proto2.MeasurementCardinalityInfo {
	pb := &proto2.MeasurementCardinalityInfo{
		Name: proto.String(m.Name),
	}
	pb.Cardinality = make([]*proto2.CardinalityInfo, len(m.CardinalityInfos))
	for i := range m.CardinalityInfos {
		pb.Cardinality[i] = m.CardinalityInfos[i].marshal()
	}
	return pb
}

func (m *MeasurementCardinalityInfo) unmarshal(pb *proto2.MeasurementCardinalityInfo) {
	m.Name = pb.GetName()
	m.CardinalityInfos = make([]CardinalityInfo, len(pb.Cardinality))
	for i := range pb.Cardinality {
		m.CardinalityInfos[i].unmarshal(pb.Cardinality[i])
	}
}

type CardinalityInfos []CardinalityInfo

// Len implements sort.Interface.
func (cis *CardinalityInfos) Len() int { return len(*cis) }

// Swap implements sort.Interface.
func (cis *CardinalityInfos) Swap(i, j int) { (*cis)[i], (*cis)[j] = (*cis)[j], (*cis)[i] }

// Less implements sort.Interface.
func (cis *CardinalityInfos) Less(i, j int) bool {
	if (*cis)[i].TimeRange.StartTime.Equal((*cis)[j].TimeRange.StartTime) {
		return (*cis)[i].TimeRange.EndTime.Before((*cis)[j].TimeRange.EndTime)
	}
	return (*cis)[i].TimeRange.StartTime.Before((*cis)[j].TimeRange.StartTime)
}

func (cis *CardinalityInfos) SortAndMerge() {
	sort.Sort(cis)
	idx := 1
	for idx < len(*cis) {
		if (&(*cis)[idx].TimeRange).equals(&(*cis)[idx-1].TimeRange) {
			(*cis)[idx-1].Cardinality += (*cis)[idx].Cardinality
			if idx == len(*cis)-1 {
				*cis = (*cis)[:idx]
			} else {
				*cis = append((*cis)[:idx], (*cis)[idx+1:]...)
			}
		} else {
			idx++
		}
	}
}

type CardinalityInfo struct {
	TimeRange   TimeRangeInfo
	Cardinality uint64
}

func (c *CardinalityInfo) marshal() *proto2.CardinalityInfo {
	pb := &proto2.CardinalityInfo{
		TimeRange:   c.TimeRange.marshal(),
		Cardinality: proto.Uint64(c.Cardinality),
	}
	return pb
}

func (c *CardinalityInfo) unmarshal(pb *proto2.CardinalityInfo) {
	c.TimeRange.unmarshal(pb.GetTimeRange())
	c.Cardinality = pb.GetCardinality()
}

type NodeRow struct {
	Timestamp int64
	Status    string
	HostName  string
	NodeID    uint64
	NodeType  string
}

func (n *NodeRow) marshal() *proto2.NodeRow {
	pb := &proto2.NodeRow{
		Timestamp: proto.Int64(n.Timestamp),
		Status:    proto.String(n.Status),
		HostName:  proto.String(n.HostName),
		NodeID:    proto.Uint64(n.NodeID),
		NodeType:  proto.String(n.NodeType),
	}
	return pb
}

func (n *NodeRow) unmarshal(pb *proto2.NodeRow) {
	n.Timestamp = pb.GetTimestamp()
	n.Status = pb.GetStatus()
	n.HostName = pb.GetHostName()
	n.NodeID = pb.GetNodeID()
	n.NodeType = pb.GetNodeType()
}

type EventRow struct {
	OpId      uint64
	EventType string
	Db        string
	PtId      uint32
	SrcNodeId uint64
	DstNodeId uint64
	CurrState string
	PreState  string
}

func (e *EventRow) marshal() *proto2.EventRow {
	pb := &proto2.EventRow{
		OpId:      proto.Uint64(e.OpId),
		EventType: proto.String(e.EventType),
		Db:        proto.String(e.Db),
		PtId:      proto.Uint32(e.PtId),
		SrcNodeId: proto.Uint64(e.SrcNodeId),
		DstNodeId: proto.Uint64(e.DstNodeId),
		CurrState: proto.String(e.CurrState),
		PreState:  proto.String(e.PreState),
	}
	return pb
}

func (e *EventRow) unmarshal(pb *proto2.EventRow) {
	e.OpId = pb.GetOpId()
	e.EventType = pb.GetEventType()
	e.Db = pb.GetDb()
	e.PtId = pb.GetPtId()
	e.SrcNodeId = pb.GetSrcNodeId()
	e.DstNodeId = pb.GetDstNodeId()
	e.CurrState = pb.GetCurrState()
	e.PreState = pb.GetPreState()
}

type ShowClusterInfo struct {
	Nodes  []NodeRow
	Events []EventRow
}

func (s *ShowClusterInfo) MarshalBinary() ([]byte, error) {
	pb := &proto2.ShowClusterInfo{}
	pb.Nodes = make([]*proto2.NodeRow, len(s.Nodes))
	for i := range s.Nodes {
		pb.Nodes[i] = s.Nodes[i].marshal()
	}
	pb.Events = make([]*proto2.EventRow, len(s.Events))
	for i := range s.Events {
		pb.Events[i] = s.Events[i].marshal()
	}
	return proto.Marshal(pb)
}

func (s *ShowClusterInfo) UnmarshalBinary(buf []byte) error {
	pb := &proto2.ShowClusterInfo{}
	if err := proto.Unmarshal(buf, pb); err != nil {
		return err
	}

	s.Nodes = make([]NodeRow, len(pb.Nodes))
	for i := range pb.Nodes {
		s.Nodes[i].unmarshal(pb.Nodes[i])
	}
	s.Events = make([]EventRow, len(pb.Events))
	for i := range pb.Events {
		s.Events[i].unmarshal(pb.Events[i])
	}
	return nil
}
