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
	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
)

const (
	// defaultThrottle is the default throttle value used to control the frequency of operations.
	defaultThrottle = 1

	// MessageCount is the number of messages returned by each fetch request.
	MessageCount = 1
)

type FetchHandleV2 struct {
	offset    uint64
	processor ProcessorInterface
}

func NewFetchHandleV2(mc metaclient.MetaClient, eng engine.Engine) *FetchHandleV2 {
	fetch := &FetchHandleV2{
		offset:    0,
		processor: NewProcessor(mc, eng),
	}

	return fetch
}

func (h *FetchHandleV2) SetProcessor(processor ProcessorInterface) {
	h.processor = processor
}

func (h *FetchHandleV2) Handle(header protocol.RequestHeader, body []byte, onMessage handle.OnMessage) error {
	req := &protocol.RequestFetchV2{}
	err := protocol.Unmarshal(body, req)
	if err != nil {
		return err
	}

	if len(req.Topics) == 0 {
		return errno.NewError(errno.MissTopic)
	}
	if len(req.Partitions) == 0 {
		return errno.NewError(errno.MissPartitions)
	}

	return h.handle(header, req, onMessage)
}

func (h *FetchHandleV2) handle(header protocol.RequestHeader, req *protocol.RequestFetchV2, onMessage handle.OnMessage) error {
	topic := &Topic{}
	// Only supports consuming a single topic
	topic.Query = req.Topics[0]

	var err error
	if h.processor.IteratorSize() == 0 {
		if err = h.processor.Init(topic); err != nil {
			return err
		}
	}

	resp := &protocol.ResponseFetchV2{
		CorrelationID: header.CorrelationID,
		Throttle:      defaultThrottle,
		Topic:         req.Topics[0],
		Header: &protocol.FetchHeader{
			Partition: req.Partitions[0],
			ErrorCode: 0,
		},
		Messages: make(protocol.FetchMessages, 0, MessageCount),
	}

	if err = h.processor.Process(func(msg protocol.Marshaler) bool {
		resp.Messages = append(resp.Messages, protocol.FetchMessage{
			FirstOffset: h.offset,
			Message:     msg,
		})
		h.offset++
		return true
	}); err != nil {
		return err
	}

	resp.Header.HighwaterMarkOffset = h.offset + 1
	return onMessage(resp)
}
