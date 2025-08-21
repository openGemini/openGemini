// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package consume

import (
	"net"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

type MetaDataV1 struct {
	mc metaclient.MetaClient
}

func NewMetaDataV1(mc metaclient.MetaClient) *MetaDataV1 {
	fetch := &MetaDataV1{
		mc: mc,
	}

	return fetch
}

func (h *MetaDataV1) Handle(header protocol.RequestHeader, body []byte, onMessage handle.OnMessage) error {
	req := &protocol.RequestMetadataV1{}
	err := protocol.Unmarshal(body, req)
	if err != nil {
		return err
	}

	nodes, err := h.mc.DataNodes()
	if err != nil {
		return err
	}

	resp := h.buildResp(header, nodes, req.Topics, config.GetStoreConfig().Consume.ConsumePort)

	return onMessage(resp)
}

func (h *MetaDataV1) buildResp(header protocol.RequestHeader, nodes []meta.DataNode, topics []string, port uint32) *protocol.MetadataResponseV1 {
	resp := &protocol.MetadataResponseV1{
		CorrelationID: header.CorrelationID,
		ControllerID:  0,
	}

	resp.Brokers = make([]protocol.BrokerMetadataV1, len(nodes))
	resp.Topics = make([]protocol.TopicMetadataV1, len(topics))

	for i := range nodes {
		broker := &resp.Brokers[i]
		host, _, err := net.SplitHostPort(nodes[i].Host)
		if err != nil {
			continue
		}
		broker.NodeID = uint32(nodes[i].ID)
		broker.Host = host
		broker.Port = port
	}

	for i := range topics {
		topic := &resp.Topics[i]
		topic.TopicName = topics[i]
		topic.TopicErrorCode = 0
		topic.Internal = false
		topic.Partitions = make([]protocol.PartitionMetadataV1, len(nodes))

		for j := range nodes {
			pt := &topic.Partitions[j]
			pt.PartitionID = uint32(j)
			pt.Leader = uint32(nodes[j].ID) // The ID of the broker where the current pt is located
			pt.Replicas = []uint32{}        // Replica is not supported for now
			pt.Isr = []uint32{}             // Isr is not supported for now
		}
	}

	return resp
}
