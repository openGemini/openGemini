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

package pubsub_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openGemini/openGemini/services/pubsub"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

type PubSubMessage struct {
	data string
}

func NewPubSubMessage(data any) *PubSubMessage {
	return &PubSubMessage{data: cast.ToString(data)}
}

func (m *PubSubMessage) Ref() {

}

func (m *PubSubMessage) UnRef() {

}

func (m *PubSubMessage) UniqueId() uint64 {
	return uint64(time.Now().UnixNano())
}

// TestBasicPublishSubscribe 测试基本的发布订阅功能
func TestBasicPublishSubscribe(t *testing.T) {
	topicName := "test-basic"
	msg := &PubSubMessage{"hello world"}

	lst := pubsub.Subscribe(topicName)

	pubsub.Publish(topicName, msg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lst.Listen(ctx, func(got pubsub.Message) {
		m, ok := got.(*PubSubMessage)
		require.True(t, ok)
		require.Equal(t, msg.data, m.data)
	})
}

// TestMultipleSubscribers
func TestMultipleSubscribers(t *testing.T) {
	topicName := "test-multiple-subs"
	msg := NewPubSubMessage("broadcast message")
	const N = 5

	listeners := make([]*pubsub.Listener, 0, N)

	for i := 0; i < N; i++ {
		lst := pubsub.Subscribe(topicName)
		listeners = append(listeners, lst)
	}

	// Publish message
	pubsub.Publish(topicName, msg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	gotMsgCount := 0
	// Verify all subscribers receive the message
	for _, lst := range listeners {
		lst.Listen(ctx, func(got pubsub.Message) {
			m, ok := got.(*PubSubMessage)
			require.True(t, ok)
			require.Equal(t, msg.data, m.data)
			gotMsgCount++
		})
	}
	require.Equal(t, N, gotMsgCount)
}

// TestSubscribeAfterPublish
func TestSubscribeAfterPublish(t *testing.T) {
	topicName := "test-sub-after-pub"
	msg := NewPubSubMessage("you should not receive this")

	// Publish message first
	pubsub.Publish(topicName, msg)

	// Subscribe afterwards
	lst := pubsub.Subscribe(topicName)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Ensure the previous message is not received
	received := false
	lst.Listen(ctx, func(got pubsub.Message) {
		received = true
	})
	require.False(t, received)
}

// TestConcurrencyPublish
func TestConcurrencyPublish(t *testing.T) {
	topicName := "test-concurrent-publish"
	msgCount := 1000

	lst := pubsub.Subscribe(topicName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(msgCount)

	// Concurrently publish messages
	for i := 0; i < msgCount; i++ {
		go func(idx int) {
			defer wg.Done()
			pubsub.Publish(topicName, NewPubSubMessage(idx))
		}(i)
	}

	// Wait for all publish calls to complete
	wg.Wait()

	gotCount := 0
	for {
		lst = lst.Listen(ctx, func(msg pubsub.Message) {
			gotCount++
		})
		if lst == nil || gotCount >= msgCount {
			break
		}
	}
	require.Equal(t, gotCount, msgCount)
}

// TestConcurrencySubscribe
func TestConcurrencySubscribe(t *testing.T) {
	topicName := "test-concurrent-subscribe"
	msg := NewPubSubMessage("test message")

	const N = 100
	listeners := make([]*pubsub.Listener, 0, N)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := 0; i < N; i++ {
		lst := pubsub.Subscribe(topicName)
		listeners = append(listeners, lst)
	}

	// Publish message
	pubsub.Publish(topicName, msg)

	wg := sync.WaitGroup{}
	wg.Add(len(listeners))

	msgCount := int64(0)
	for i := range listeners {
		go func(lst *pubsub.Listener) {
			defer wg.Done()
			lst.Listen(ctx, func(got pubsub.Message) {
				atomic.AddInt64(&msgCount, 1)
			})
		}(listeners[i])
	}
	wg.Wait()

	require.Equal(t, N, int(msgCount))
}
