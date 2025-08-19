// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/services/consume"
	kafkaServer "github.com/openGemini/openGemini/services/consume/kafka"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/openGemini/openGemini/services/consume/kafka/protocol"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func openListen() (s1, s2, s3 *kafkaServer.Server, err error) {
	debug.SetMemoryLimit(2 * 1e9)
	hf := handle.DefaultHandlerFactory()
	hf.RegisterV1(handle.Versions, handle.NewApiVersionHandler)
	hf.RegisterV1(handle.Metadata, func() handle.Handler {
		return &MetadataHandleV1{}
	})
	hf.RegisterV1(handle.ListOffsets, func() handle.Handler {
		return &ListOffsetHandleV1{}
	})
	hf.RegisterV2(handle.Fetch, func() handle.Handler {
		return &FetchHandleV2{}
	})

	s1 = &kafkaServer.Server{}
	s2 = &kafkaServer.Server{}
	s3 = &kafkaServer.Server{}

	servers := []*kafkaServer.Server{s1, s2, s3}
	addresses := []string{"127.0.0.1:9092", "127.0.0.2:9092", "127.0.0.3:9092"}

	var wg sync.WaitGroup
	errChan := make(chan error, len(servers))

	for i, server := range servers {
		wg.Add(1)
		go func(s *kafkaServer.Server, addr string) {
			defer wg.Done()
			if err := s.Open(addr, 1*1024*1024); err != nil {
				errChan <- err
			}
		}(server, addresses[i])
	}

	wg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		err = fmt.Errorf("failed to start servers: %v", errs)
		for _, server := range servers {
			if server != nil {
				_ = server.Close()
			}
		}
	}

	return s1, s2, s3, err
}

func TestClient(t *testing.T) {
	s1, s2, s3, err := openListen()
	require.NoError(t, err)
	defer func() {
		_ = s1.Close()
		_ = s2.Close()
		_ = s3.Close()
	}()
	time.Sleep(time.Second / 10)

	topic := "topic-1"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "127.0.0.1:9092", topic, partition)
	require.NoError(t, err)

	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	var rets []string
	for {
		msg, err := batch.ReadMessage()
		if err != nil {
			break
		}

		rets = append(rets, string(msg.Value))
	}

	require.Equal(t, 3, len(rets))
	require.NoError(t, batch.Close())
	require.NoError(t, conn.Close())
}

type MetadataHandleV1 struct {
}

func (h *MetadataHandleV1) Handle(header protocol.RequestHeader, _ []byte, onMsg handle.OnMessage) error {
	resp := &protocol.MetadataResponseV1{
		CorrelationID: header.CorrelationID,
		ControllerID:  0,
		Brokers: []protocol.BrokerMetadataV1{
			{
				NodeID: 0,
				Host:   "127.0.0.1",
				Port:   9092,
			},
			{
				NodeID: 1,
				Host:   "127.0.0.2",
				Port:   9092,
			},
		},
		Topics: []protocol.TopicMetadataV1{
			{
				TopicName: "topic-1",
				Partitions: []protocol.PartitionMetadataV1{
					{
						PartitionID: 0,
						Leader:      1,
						Replicas:    []uint32{0},
						Isr:         []uint32{1, 0},
					},
				},
			},
		},
	}

	return onMsg(resp)
}

type ListOffsetHandleV1 struct {
}

func (h *ListOffsetHandleV1) Handle(header protocol.RequestHeader, _ []byte, onMsg handle.OnMessage) error {
	ofs := protocol.TopicPartitionOffsetsV1{
		TopicName:        "topic-1",
		PartitionOffsets: make([]protocol.PartitionOffsetV1, 1),
	}

	ofs.PartitionOffsets[0] = protocol.PartitionOffsetV1{
		Partition: 0,
		ErrorCode: 0,
		Timestamp: uint64(time.Now().UnixNano()),
		Offset:    0,
	}

	resp := &protocol.ResponseTopicPartitionOffsetsV1{
		CorrelationID: header.CorrelationID,
		List:          []protocol.TopicPartitionOffsetsV1{ofs},
	}
	return onMsg(resp)
}

type FetchHandleV2 struct {
}

func (h *FetchHandleV2) Handle(header protocol.RequestHeader, body []byte, onMsg handle.OnMessage) error {
	dec := &codec.BinaryDecoder{}
	dec.Reset(body)

	req := &protocol.RequestFetchV2{}
	err := req.Unmarshal(dec)
	if err != nil {
		return err
	}

	resp := &protocol.ResponseFetchV2{
		CorrelationID: header.CorrelationID,
		Throttle:      1,
		Topic:         "",
		Header: &protocol.FetchHeader{
			Partition:           1,
			ErrorCode:           0,
			HighwaterMarkOffset: 1,
		},
		Messages: []protocol.FetchMessage{
			{
				FirstOffset:      0,
				CrcOrLeaderEpoch: 0,
				Magic:            0,
				Attributes:       0,
				Message: &MockMessage{
					"some data 001",
				},
			},
			{
				FirstOffset:      1,
				CrcOrLeaderEpoch: 0,
				Magic:            0,
				Attributes:       0,
				Message: &MockMessage{
					"some data 002",
				},
			},
			{
				FirstOffset:      2,
				CrcOrLeaderEpoch: 0,
				Magic:            0,
				Attributes:       0,
				Message: &MockMessage{
					"some data 003",
				},
			},
		},
	}
	return onMsg(resp)
}

type MockMessage struct {
	msg string
}

func (m *MockMessage) Marshal(dst []byte) []byte {
	return append(dst, m.msg...)
}

func TestRequestOffsetCommitV2(t *testing.T) {
	req := &protocol.RequestOffsetCommitV2{
		GroupID:       "testGroup1",
		GenerationID:  1,
		MemberID:      "testMember1",
		RetentionTime: 1,
		Topics: []protocol.RequestOffsetCommitV2Topic{
			{
				Topic: "testTopic",
				Partitions: []protocol.RequestOffsetCommitV2Partition{
					{
						Partition: 0,
						Offset:    0,
						Metadata:  "testMetadata",
					},
				},
			},
		},
	}
	var body []byte
	body = req.Marshal(body)

	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	reqUnMarshaled := &protocol.RequestOffsetCommitV2{}
	err := protocol.Unmarshal(bodyCopy, reqUnMarshaled)
	require.NoError(t, err)
	require.Equal(t, req, reqUnMarshaled)

	onMessage := func(resp protocol.Marshaler) error {
		r, ok := resp.(*protocol.ResponseOffsetCommitV2)
		if !ok {
			return errors.New("invalid response type")
		}

		marshaledResp := r.Marshal(nil)
		require.Equal(t, 25, len(marshaledResp))
		return nil
	}

	h := consume.NewCommitOffsetV2()
	err = h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.NoError(t, err)

	body = codec.AppendString(body, "testForUnmarshalErr")
	err = h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.Error(t, err)
}

func TestHeartbeatV0(t *testing.T) {
	req := &protocol.RequestHeartbeatV0{
		GroupID:      "testGroup1",
		GenerationID: 1,
		MemberID:     "testMember1",
	}

	var body []byte
	body = codec.AppendString(body, req.GroupID)
	body = codec.AppendInt32(body, req.GenerationID)
	body = codec.AppendString(body, req.MemberID)

	onMessage := func(resp protocol.Marshaler) error {
		r, ok := resp.(*protocol.ResponseHeartbeatV0)
		if !ok {
			return errors.New("invalid response type")
		}
		require.Equal(t, int16(0), r.ErrorCode)

		marshaledResp := r.Marshal(nil)
		require.Equal(t, 2, len(marshaledResp))

		return nil
	}

	h := consume.NewHeartbeatV0()
	err := h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.NoError(t, err)

	body = codec.AppendString(body, "testForUnmarshalErr")
	err = h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.Error(t, err)
}

func TestListOffsetV1(t *testing.T) {
	req := &protocol.RequestPartitionOffsetV1{
		ReplicaID: 0,
		Topics:    []string{"testTopic"},
		Partition: []uint32{0},
		Timestamp: 0,
	}

	var body []byte
	body = codec.AppendInt32(body, req.ReplicaID)
	body = codec.AppendUint32(body, uint32(len(req.Topics)))
	body = codec.AppendString(body, req.Topics[0])
	body = codec.AppendUint32(body, uint32(len(req.Partition)))
	body = codec.AppendUint32(body, req.Partition[0])
	body = codec.AppendUint64(body, req.Timestamp)

	onMessage := func(resp protocol.Marshaler) error {
		r, ok := resp.(*protocol.ResponseTopicPartitionOffsetsV1)
		if !ok {
			return errors.New("invalid response type")
		}

		marshaledResp := r.Marshal(nil)
		require.Equal(t, 45, len(marshaledResp))

		return nil
	}

	h := consume.NewListOffsetV1()
	err := h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.NoError(t, err)

	body = codec.AppendString(body, "testForUnmarshalErr")
	err = h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.Error(t, err)
}

type MockMetaClient struct {
	metaclient.MetaClient
	isDatanodeAcquired bool
}

func (c MockMetaClient) DataNodes() ([]meta.DataNode, error) {
	if c.isDatanodeAcquired {
		return []meta.DataNode{
			{NodeInfo: meta.NodeInfo{
				ID:      1,
				Host:    "",
				RPCAddr: "",
				TCPHost: "",
				Status:  0,
			}},
			{NodeInfo: meta.NodeInfo{
				ID:      2,
				Host:    "",
				RPCAddr: "",
				TCPHost: "",
				Status:  0,
			}},
		}, nil
	}
	return nil, errors.New("no dataNodes")
}

type MockEngine struct {
	engine.Engine
}

func TestMetaDataV1(t *testing.T) {
	req := &protocol.RequestMetadataV1{
		Topics: []string{"testTopic"},
	}

	var body []byte
	body = codec.AppendUint32(body, uint32(len(req.Topics)))
	body = codec.AppendString(body, req.Topics[0])

	onMessage := func(resp protocol.Marshaler) error {
		r, ok := resp.(*protocol.MetadataResponseV1)
		if !ok {
			return errors.New("invalid response type")
		}

		marshaledResp := r.Marshal(nil)
		require.Equal(t, 94, len(marshaledResp))

		return nil
	}

	mc := &MockMetaClient{isDatanodeAcquired: true}
	h := consume.NewMetaDataV1(mc)
	err := h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.NoError(t, err)

	mc = &MockMetaClient{isDatanodeAcquired: false}
	h = consume.NewMetaDataV1(mc)
	err = h.Handle(protocol.RequestHeader{}, body, onMessage)
	require.Error(t, err)
}

func NewMockRequestFetchV2() *protocol.RequestFetchV2 {
	return &protocol.RequestFetchV2{
		ReplicaID:   0,
		MaxWaitTime: 0,
		MinBytes:    0,
		Topics:      []string{"testTopics"},
		Partitions:  []uint32{0},
		Offset:      0,
		MaxBytes:    0,
	}
}

type MockProcessor struct {
	consume.ProcessorInterface
	InitErr    error
	ProcessErr error
}

func (m *MockProcessor) Init(topic *consume.Topic) error {
	if m.InitErr != nil {
		return m.InitErr
	}
	return nil
}

func (m *MockProcessor) Process(onMsg func(msg protocol.Marshaler) bool) error {
	if m.ProcessErr != nil {
		return m.ProcessErr
	}
	return nil
}

func (m *MockProcessor) IteratorSize() int {
	return 0
}

func TestFetchHandleV2_Handle(t *testing.T) {
	mockHeader := protocol.RequestHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationID: 0,
		ClientID:      "testClientID",
	}

	fetchReq := NewMockRequestFetchV2()
	var fetchBody []byte
	fetchBody = fetchReq.Marshal(fetchBody)

	mc := &MockMetaClient{}
	eng := &MockEngine{}

	tests := []struct {
		name      string
		body      []byte
		processor *MockProcessor
		wantErr   bool
	}{
		{
			name:      "case1: body unmarshal err",
			body:      []byte("invalid body"),
			processor: nil,
			wantErr:   true,
		},
		{
			name: "case2: no topic in request",
			body: func() []byte {
				req := &protocol.RequestFetchV2{
					Topics: []string{"testTopics"},
				}
				return req.Marshal(nil)
			}(),
			processor: nil,
			wantErr:   true,
		},
		{
			name: "case3: no partition in request",
			body: func() []byte {
				req := &protocol.RequestFetchV2{
					Partitions: []uint32{0},
				}
				return req.Marshal(nil)
			}(),
			processor: nil,
			wantErr:   true,
		},
		{
			name: "case4: processor init err",
			body: fetchBody,
			processor: &MockProcessor{
				InitErr: errno.NewError(errno.InternalError),
			},
			wantErr: true,
		},
		{
			name: "case5: processor process err",
			body: fetchBody,
			processor: &MockProcessor{
				ProcessErr: errno.NewError(errno.InternalError),
			},
			wantErr: true,
		},
		{
			name:      "case6: normal handle",
			body:      fetchBody,
			processor: &MockProcessor{},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetchHandle := consume.NewFetchHandleV2(mc, eng)
			if tt.processor != nil {
				fetchHandle.SetProcessor(tt.processor)
			}
			err := fetchHandle.Handle(mockHeader, tt.body, func(protocol.Marshaler) error { return nil })
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
