/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package message

import (
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/open_src/influx/meta"
)

type GetReplicaInfoRequest struct {
	Database string
	NodeID   uint64
	PtID     uint32
}

type GetReplicaInfoResponse struct {
	ReplicaInfo *ReplicaInfo
	Err         string
}

//lint:ignore U1000 use for replication feature
type PeerInfo struct {
	PtId   uint32
	NodeId uint64
}

func (p *PeerInfo) Update(pt *meta.PtInfo) {
	if pt == nil {
		return
	}

	p.NodeId = pt.Owner.NodeID
	p.PtId = pt.PtId
}

//lint:ignore U1000 use for replication feature
type ReplicaInfo struct {
	ReplicaRole   meta.Role
	Master        PeerInfo
	Peers         []PeerInfo
	ReplicaStatus meta.RGStatus
	Term          uint64
}

func (o *GetReplicaInfoRequest) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, o.Database)
	buf = codec.AppendUint64(buf, o.NodeID)
	buf = codec.AppendUint32(buf, o.PtID)

	return buf, err
}

func (o *GetReplicaInfoRequest) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.Database = dec.String()
	o.NodeID = dec.Uint64()
	o.PtID = dec.Uint32()

	return err
}

func (o *GetReplicaInfoRequest) Size() int {
	size := 0
	size += codec.SizeOfString(o.Database)
	size += codec.SizeOfUint64()
	size += codec.SizeOfUint32()

	return size
}

func (o *GetReplicaInfoRequest) Instance() transport.Codec {
	return &GetReplicaInfoRequest{}
}

func (o *GetReplicaInfoResponse) Marshal(buf []byte) ([]byte, error) {
	var err error

	func() {
		if o.ReplicaInfo == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(o.ReplicaInfo.Size()))
		buf, err = o.ReplicaInfo.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}
	buf = codec.AppendString(buf, o.Err)

	return buf, err
}

func (o *GetReplicaInfoResponse) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.ReplicaInfo = &ReplicaInfo{}
		err = o.ReplicaInfo.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}
	o.Err = dec.String()

	return err
}

func (o *GetReplicaInfoResponse) Size() int {
	size := 0

	size += codec.SizeOfUint32()
	if o.ReplicaInfo != nil {
		size += o.ReplicaInfo.Size()
	}
	size += codec.SizeOfString(o.Err)

	return size
}

func (o *GetReplicaInfoResponse) Instance() transport.Codec {
	return &GetReplicaInfoResponse{}
}

func (o *ReplicaInfo) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint8(buf, uint8(o.ReplicaRole))

	func() {
		buf = codec.AppendUint32(buf, uint32(o.Master.Size()))
		buf, err = o.Master.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	buf = codec.AppendUint32(buf, uint32(len(o.Peers)))
	for _, item := range o.Peers {
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	buf = codec.AppendUint8(buf, uint8(o.ReplicaStatus))
	buf = codec.AppendUint64(buf, o.Term)

	return buf, err
}

func (o *ReplicaInfo) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.ReplicaRole = meta.Role(dec.Uint8())

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		o.Master = PeerInfo{}
		err = o.Master.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	PeersLen := int(dec.Uint32())
	if PeersLen > 0 {
		o.Peers = make([]PeerInfo, PeersLen)
		for i := 0; i < PeersLen; i++ {
			subBuf := dec.BytesNoCopy()
			if len(subBuf) == 0 {
				continue
			}

			o.Peers[i] = PeerInfo{}
			if err := o.Peers[i].Unmarshal(subBuf); err != nil {
				return err
			}
		}
	}
	o.ReplicaStatus = meta.RGStatus(dec.Uint8())
	o.Term = dec.Uint64()

	return err
}

func (o *ReplicaInfo) Size() int {
	size := 0
	size += codec.SizeOfUint8()

	size += codec.SizeOfUint32()
	size += o.Master.Size()

	size += codec.MaxSliceSize
	for _, item := range o.Peers {
		size += codec.SizeOfUint32()
		size += item.Size()
	}
	size += codec.SizeOfUint8()
	size += codec.SizeOfUint64()

	return size
}

func (o *ReplicaInfo) Instance() transport.Codec {
	return &ReplicaInfo{}
}

func (p *PeerInfo) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendUint32(buf, p.PtId)
	buf = codec.AppendUint64(buf, p.NodeId)

	return buf, err
}

func (p *PeerInfo) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	p.PtId = dec.Uint32()
	p.NodeId = dec.Uint64()

	return err
}

func (p *PeerInfo) Size() int {
	size := 0
	size += codec.SizeOfUint32()
	size += codec.SizeOfUint64()

	return size
}

func (p *PeerInfo) Instance() transport.Codec {
	return &PeerInfo{}
}
