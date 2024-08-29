// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package message

import (
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

type PeerInfo struct {
	PtId   uint32
	NodeId uint64

	// map key is the master shard ID
	// map value is the slave shard ID
	ShardMapper map[uint64]uint64
}

func NewPeerInfo(pt *meta.PtInfo) *PeerInfo {
	if pt == nil {
		return nil
	}

	return &PeerInfo{
		NodeId:      pt.Owner.NodeID,
		PtId:        pt.PtId,
		ShardMapper: make(map[uint64]uint64),
	}
}

func (p *PeerInfo) AddShardMapper(master, slave uint64) {
	if len(p.ShardMapper) == 0 {
		p.ShardMapper = make(map[uint64]uint64)
	}

	p.ShardMapper[master] = slave
}

func (p *PeerInfo) GetSlaveShardID(masterShardID uint64) uint64 {
	return p.ShardMapper[masterShardID]
}

//lint:ignore U1000 use for replication feature
type ReplicaInfo struct {
	ReplicaRole   meta.Role
	Master        *PeerInfo
	Peers         []*PeerInfo
	ReplicaStatus meta.RGStatus
	Term          uint64
}

func (r *ReplicaInfo) AddPeer(p *PeerInfo) {
	r.Peers = append(r.Peers, p)
}
