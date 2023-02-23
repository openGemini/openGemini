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
	"time"

	"github.com/gogo/protobuf/proto"
	proto2 "github.com/openGemini/openGemini/open_src/influx/meta/proto"
)

func normalisedIndexDuration(igd, sgd time.Duration) time.Duration {
	if igd < sgd {
		return sgd
	}

	if igd%sgd == 0 {
		return igd
	}
	mul := igd / sgd
	return (mul + 1) * sgd
}

type IndexGroupInfo struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	Indexes   []IndexInfo
	DeletedAt time.Time
}

func (igi *IndexGroupInfo) canDelete() bool {
	for i := range igi.Indexes {
		if !igi.Indexes[i].MarkDelete {
			return false
		}
	}
	return true
}

func (igi *IndexGroupInfo) Contains(t time.Time) bool {
	return !t.Before(igi.StartTime) && t.Before(igi.EndTime)
}

func (igi *IndexGroupInfo) Overlaps(min, max time.Time) bool {
	return !igi.StartTime.After(max) && igi.EndTime.After(min)
}

func (igi *IndexGroupInfo) Deleted() bool {
	return !igi.DeletedAt.IsZero()
}

func (igi IndexGroupInfo) clone() IndexGroupInfo {
	other := igi

	if igi.Indexes != nil {
		other.Indexes = make([]IndexInfo, len(igi.Indexes))
		for i := range igi.Indexes {
			other.Indexes[i] = igi.Indexes[i].clone()
		}
	}

	return other
}

func (igi *IndexGroupInfo) marshal() *proto2.IndexGroupInfo {
	pb := &proto2.IndexGroupInfo{
		ID:        proto.Uint64(igi.ID),
		StartTime: proto.Int64(MarshalTime(igi.StartTime)),
		EndTime:   proto.Int64(MarshalTime(igi.EndTime)),
		DeletedAt: proto.Int64(MarshalTime(igi.DeletedAt)),
	}

	pb.Indexes = make([]*proto2.IndexInfo, len(igi.Indexes))
	for i := range igi.Indexes {
		pb.Indexes[i] = igi.Indexes[i].marshal()
	}

	return pb
}

func (igi *IndexGroupInfo) unmarshal(pb *proto2.IndexGroupInfo) {
	igi.ID = pb.GetID()
	if i := pb.GetStartTime(); i == 0 {
		igi.StartTime = time.Unix(0, 0).UTC()
	} else {
		igi.StartTime = UnmarshalTime(i)
	}
	if i := pb.GetEndTime(); i == 0 {
		igi.EndTime = time.Unix(0, 0).UTC()
	} else {
		igi.EndTime = UnmarshalTime(i)
	}
	igi.DeletedAt = UnmarshalTime(pb.GetDeletedAt())

	if len(pb.GetIndexes()) > 0 {
		igi.Indexes = make([]IndexInfo, len(pb.GetIndexes()))
		for i, x := range pb.GetIndexes() {
			igi.Indexes[i].unmarshal(x)
		}
	}
}

type IndexGroupInfos []IndexGroupInfo

func (igs IndexGroupInfos) Len() int { return len(igs) }

func (igs IndexGroupInfos) Swap(i, j int) { igs[i], igs[j] = igs[j], igs[i] }

func (igs IndexGroupInfos) Less(i, j int) bool {
	iEnd := igs[i].EndTime

	jEnd := igs[j].EndTime

	if iEnd.Equal(jEnd) {
		return igs[i].StartTime.Before(igs[j].StartTime)
	}

	return iEnd.Before(jEnd)
}

type IndexInfo struct {
	ID         uint64
	Owners     []uint32 // pt group for replications.
	MarkDelete bool
}

func (ii IndexInfo) clone() IndexInfo {
	other := ii

	other.Owners = make([]uint32, len(ii.Owners))
	for i := range ii.Owners {
		other.Owners[i] = ii.Owners[i]
	}

	return other
}

// marshal serializes to a protobuf representation.
func (ii IndexInfo) marshal() *proto2.IndexInfo {
	pb := &proto2.IndexInfo{
		ID:         proto.Uint64(ii.ID),
		MarkDelete: proto.Bool(ii.MarkDelete),
	}
	pb.OwnerIDs = make([]uint32, len(ii.Owners))
	for i := range ii.Owners {
		pb.OwnerIDs[i] = ii.Owners[i]
	}

	return pb
}

// UnmarshalBinary decodes the object from a binary format.
func (ii *IndexInfo) UnmarshalBinary(buf []byte) error {
	var pb proto2.IndexInfo
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	ii.unmarshal(&pb)
	return nil
}

// unmarshal deserializes from a protobuf representation.
func (ii *IndexInfo) unmarshal(pb *proto2.IndexInfo) {
	ii.ID = pb.GetID()
	ii.MarkDelete = pb.GetMarkDelete()

	ii.Owners = make([]uint32, len(pb.GetOwnerIDs()))
	for i, x := range pb.GetOwnerIDs() {
		ii.Owners[i] = x
	}
}
