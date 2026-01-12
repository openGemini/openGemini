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
	"sync"
	"time"
)

const (
	MsgHoldDuration = time.Second * 10
)

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		lst: NewListener(),
	}
}

type Broadcaster struct {
	mu  sync.RWMutex
	lst *Listener
}

func (b *Broadcaster) Broadcast(msg Message) {
	msg.Ref()
	// Subscribers need to ensure that broadcast messages are processed promptly and not blocked.
	// 10 seconds is enough for all subscribers to receive the message and increment the reference count as needed.
	time.AfterFunc(MsgHoldDuration, func() {
		msg.UnRef()
	})

	b.mu.Lock()
	defer b.mu.Unlock()

	lst := b.lst
	b.lst = lst.Publish(msg)
}

// GetListener Message subscribers obtain message listener from the broadcaster to receive broadcasts.
// The listener is similar to a radio.
func (b *Broadcaster) GetListener() *Listener {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.lst
}

type Listener struct {
	next *Listener

	signal chan struct{}
	msg    Message
}

func NewListener() *Listener {
	return &Listener{
		signal: make(chan struct{}),
	}
}

func (l *Listener) Publish(msg Message) *Listener {
	l.msg = msg
	l.next = NewListener()
	close(l.signal)
	return l.next
}

func (l *Listener) Listen(ctx context.Context, handle func(msg Message)) *Listener {
	select {
	case <-ctx.Done():
		return nil
	case <-l.signal:
		handle(l.msg)
	}

	return l.next
}
