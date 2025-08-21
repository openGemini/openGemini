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

// HeartbeatV0 Currently, only the interaction process is implemented
type HeartbeatV0 struct {
}

func NewHeartbeatV0() *HeartbeatV0 {
	return &HeartbeatV0{}
}

func (h *HeartbeatV0) Handle(header protocol.RequestHeader, body []byte, onMessage handle.OnMessage) error {
	req := &protocol.RequestHeartbeatV0{}
	err := protocol.Unmarshal(body, req)
	if err != nil {
		return err
	}

	resp := &protocol.ResponseHeartbeatV0{
		ErrorCode: 0,
	}

	return onMessage(resp)
}
