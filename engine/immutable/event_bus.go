// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/scheduler"
)

type EventType int

const (
	EventTypeMergeSelf EventType = iota
	EventTypeStreamCompact
	EventTypeEnd
)

type Event interface {
	Instance() Event
	Init(mst string, level uint16)
	Enable() bool
	OnWriteRecord(rec *record.Record)
	OnWriteChunkMeta(cm *ChunkMeta)
	OnNewFile(f TSSPFile)
	OnReplaceFile(shardDir string, lockFile string) error
	OnInterrupt()
	OnFinish(ctx EventContext)
}

type EventContext struct {
	mergeSet  IndexMergeSet
	scheduler *scheduler.TaskScheduler
}

func NewEventContext(idx IndexMergeSet, scheduler *scheduler.TaskScheduler) *EventContext {
	return &EventContext{idx, scheduler}
}

type Events struct {
	events []Event
}

func (es *Events) Register(e Event) {
	es.events = append(es.events, e)
}

func (es *Events) Instance() *Events {
	ins := &Events{}
	for _, e := range es.events {
		ins.Register(e.Instance())
	}
	return ins
}

func (es *Events) TriggerWriteRecord(rec *record.Record) {
	es.walkEvents(func(e Event) {
		e.OnWriteRecord(rec)
	})
}

func (es *Events) TriggerWriteChunkMeta(cm *ChunkMeta) {
	es.walkEvents(func(e Event) {
		e.OnWriteChunkMeta(cm)
	})
}

func (es *Events) TriggerNewFile(f TSSPFile) {
	es.walkEvents(func(e Event) {
		e.OnNewFile(f)
	})
}

func (es *Events) TriggerReplaceFile(shardDir, lock string) error {
	var err error
	es.walkEvents(func(e Event) {
		err = e.OnReplaceFile(shardDir, lock)
	})
	return err
}

func (es *Events) Finish(success bool, ctx EventContext) {
	es.walkEvents(func(e Event) {
		if success {
			e.OnFinish(ctx)
			return
		}
		e.OnInterrupt()
	})
	es.events = nil
}

func (es *Events) walkEvents(callback func(e Event)) {
	for _, e := range es.events {
		if e.Enable() {
			callback(e)
		}
	}
}

type EventBus struct {
	events [EventTypeEnd]Events
}

var defaultEventBus = &EventBus{}

func DefaultEventBus() *EventBus {
	return defaultEventBus
}

func (b *EventBus) Register(typ EventType, e Event) {
	b.events[typ].Register(e)
}

func (b *EventBus) NewEvents(typ EventType, mst string, level uint16) *Events {
	es := b.events[typ].Instance()
	for _, e := range es.events {
		e.Init(mst, level)
	}
	return es
}
