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
	"context"
	"errors"
	"sync"
)

var DefaultCachedPubSub = NewRegistry()

type CachedPubSub struct {
	mu    sync.RWMutex
	state map[string]map[uint64]Message
}

type Message interface {
	Ref()
	UnRef()
	UniqueId() uint64
}

func NewRegistry() *CachedPubSub {
	return &CachedPubSub{
		state: make(map[string]map[uint64]Message),
	}
}

func (r *CachedPubSub) Publish(topicName string, msg Message) {
	r.mu.Lock()
	if r.state[topicName] == nil {
		r.state[topicName] = make(map[uint64]Message)
	}
	r.state[topicName][msg.UniqueId()] = msg
	r.mu.Unlock()

	Publish(topicName, msg)
}

type Handler func(msg Message)

func (r *CachedPubSub) Subscribe(topicName string, ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	r.mu.RLock()
	currents := r.state[topicName]
	r.mu.RUnlock()

	for _, currentMsg := range currents {
		handler(currentMsg)
	}

	lst := Subscribe(topicName)
	for {
		lst = lst.Listen(ctx, func(msg Message) {
			handler(msg)
		})
		if lst == nil {
			break
		}
	}
	return nil
}
