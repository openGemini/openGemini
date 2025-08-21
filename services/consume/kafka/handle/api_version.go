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

package handle

import (
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

type ApiVersionHandler struct {
}

func NewApiVersionHandler() Handler {
	return &ApiVersionHandler{}
}

func (vh *ApiVersionHandler) Handle(header protocol.RequestHeader, _ []byte, onMessage OnMessage) error {
	versions := protocol.ResponseApiVersion{
		CorrelationID: header.CorrelationID,
		ErrorCode:     0,
		ThrottleTime:  -1,
		Versions: []protocol.ApiVersion{
			{ApiKey: Versions, MinVersion: 1, MaxVersion: 1},
			{ApiKey: ListOffsets, MinVersion: 1, MaxVersion: 1},
			{ApiKey: Metadata, MinVersion: 1, MaxVersion: 1},
			{ApiKey: Fetch, MinVersion: 2, MaxVersion: 2},
			{ApiKey: OffsetCommit, MinVersion: 2, MaxVersion: 2},
			{ApiKey: HeartBeat, MinVersion: 1, MaxVersion: 1},
		},
	}
	if header.ApiVersion > 0 {
		versions.ThrottleTime = 1
	}

	return onMessage(versions)
}
