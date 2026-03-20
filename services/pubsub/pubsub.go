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

package pubsub

import (
	"sync"
)

type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topic
}

func newBroker() *Broker {
	return &Broker{
		topics: make(map[string]*topic),
	}
}

func (b *Broker) publish(topicName string, msg Message) {
	t, ok := b.getTopic(topicName)
	if !ok {
		return
	}

	t.bc.Broadcast(msg)
}

func (b *Broker) getTopic(topicName string) (*topic, bool) {
	b.mu.RLock()
	t, ok := b.topics[topicName]
	b.mu.RUnlock()

	return t, ok
}

func (b *Broker) createTopicIfNotExists(topicName string) *topic {
	// RLock topic, atomically inc ref; avoid write lock.
	t, ok := b.getTopic(topicName)

	if ok {
		return t
	}

	// Topic missing; take write lock and create (double-check).
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok = b.topics[topicName]
	if !ok {
		t = newTopic()
		b.topics[topicName] = t
	}

	return t
}

func (b *Broker) listen(topicName string) *Listener {
	t := b.createTopicIfNotExists(topicName)
	return t.bc.GetListener()
}

type topic struct {
	bc *Broadcaster
}

func newTopic() *topic {
	t := &topic{
		bc: NewBroadcaster(),
	}
	return t
}
