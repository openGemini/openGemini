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
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

// CommitOffsetV2 Currently, only the interaction process is implemented
type CommitOffsetV2 struct {
}

func NewCommitOffsetV2() *CommitOffsetV2 {
	return &CommitOffsetV2{}
}

func (h *CommitOffsetV2) Handle(header protocol.RequestHeader, body []byte, onMessage handle.OnMessage) error {
	req := &protocol.RequestOffsetCommitV2{}
	err := protocol.Unmarshal(body, req)
	if err != nil {
		return err
	}

	lenTopics := len(req.Topics)
	resp := &protocol.ResponseOffsetCommitV2{
		Responses: make([]protocol.ResponseOffsetCommitV2Response, lenTopics),
	}

	for i := range lenTopics {
		roc := &resp.Responses[i]
		roc.Topic = req.Topics[i].Topic
		roc.PartitionResponses = []protocol.ResponseOffsetCommitV2PartitionResponse{
			{
				Partition: 0,
				ErrorCode: 0,
			},
		}
	}

	return onMessage(resp)
}
