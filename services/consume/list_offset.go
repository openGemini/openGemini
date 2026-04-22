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
	"time"

	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

// ListOffsetV1 Currently, only the interaction process is implemented
type ListOffsetV1 struct {
}

func NewListOffsetV1() *ListOffsetV1 {
	return &ListOffsetV1{}
}

func (h *ListOffsetV1) Handle(header protocol.RequestHeader, body []byte, onMessage handle.OnMessage) error {
	req := &protocol.RequestPartitionOffsetV1{}
	err := protocol.Unmarshal(body, req)
	if err != nil {
		return err
	}

	resp := &protocol.ResponseTopicPartitionOffsetsV1{
		CorrelationID: header.CorrelationID,
		List:          make([]protocol.TopicPartitionOffsetsV1, len(req.Topics)),
	}

	for i := range req.Topics {
		ofs := &resp.List[i]
		ofs.TopicName = req.Topics[i]
		ofs.PartitionOffsets = []protocol.PartitionOffsetV1{
			{
				Partition: 0,
				ErrorCode: 0,
				Timestamp: uint64(time.Now().UnixNano()),
				Offset:    0,
			},
		}
	}

	return onMessage(resp)
}
