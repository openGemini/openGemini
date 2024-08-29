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
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/scheduler"
	"go.uber.org/zap"
)

type CompactTask struct {
	scheduler.BaseTask

	plan  *CompactGroup
	table *MmsTables
	full  bool
}

func NewCompactTask(table *MmsTables, plan *CompactGroup, full bool) *CompactTask {
	task := &CompactTask{
		full:  full,
		plan:  plan,
		table: table,
	}
	task.Init(plan.name)
	return task
}

func (t *CompactTask) BeforeExecute() bool {
	if !t.table.acquire(t.plan.group) {
		return false
	}
	t.OnFinish(func() {
		t.table.CompactDone(t.plan.group)
		t.table.blockCompactStop(t.plan.name)
	})
	return true
}

func (t *CompactTask) Finish() {
	t.BaseTask.Finish()
	t.plan = nil
	t.table = nil
}

func (t *CompactTask) Execute() {
	group := t.plan
	m := t.table

	if group.Len() == 1 {
		err := m.RenameFileToLevel(group)
		if err != nil {
			log.Error("compact error", zap.Error(err))
		}
		return
	}

	t.IncrFull(1)
	orderWg, inorderWg := m.ImmTable.refMmsTable(m, group.name, false)
	defer func() {
		if config.GetStoreConfig().Compact.CompactRecovery {
			CompactRecovery(m.path, group)
		}
		m.ImmTable.unrefMmsTable(orderWg, inorderWg)
		t.IncrFull(-1)
	}()

	if !m.CompactionEnabled() {
		return
	}

	fi, err := m.ImmTable.NewFileIterators(m, group)
	if err != nil {
		log.Error(err.Error())
		compactStat.AddErrors(1)
		return
	}

	err = m.ImmTable.compactToLevel(m, fi, t.full, NonStreamingCompaction(fi))
	if err != nil {
		compactStat.AddErrors(1)
		log.Error("compact error", zap.Error(err))
	}
}

func (t *CompactTask) IncrFull(n int64) {
	if t.full {
		atomic.AddInt64(&fullCompactingCount, n)
	}
}

type CompactGroupBuilder struct {
	limit        int
	parquetLevel uint16
	lowLevelMode bool
	level        uint16

	group  *CompactGroup
	groups []*CompactGroup
}

func (b *CompactGroupBuilder) Init(name string, closing *int64, size int) {
	b.group = &CompactGroup{
		dropping: closing,
		name:     name,
		group:    make([]string, 0, size),
	}
}

func (b *CompactGroupBuilder) AddFile(f TSSPFile) bool {
	if b.lowLevelMode {
		return b.addLowLevelMode(f)
	}

	return b.add(f)
}

func (b *CompactGroupBuilder) add(f TSSPFile) bool {
	lv, _ := f.LevelAndSequence()
	if b.parquetLevel > 0 && lv < b.parquetLevel {
		b.group.reset()
		return false
	}

	b.group.UpdateLevel(lv + 1)
	b.group.Add(f.Path())
	return true
}

func (b *CompactGroupBuilder) addLowLevelMode(f TSSPFile) bool {
	b.group.toLevel = b.level
	lv, _ := f.LevelAndSequence()
	if lv < b.level {
		b.group.Add(f.Path())
		return true
	}

	if b.group.Len() > 0 {
		b.SwitchGroup()
		b.group = &CompactGroup{
			name:     b.group.name,
			dropping: b.group.dropping,
		}
	}

	return true
}

func (b *CompactGroupBuilder) SwitchGroup() {
	if b.group == nil || b.group.Len() == 0 {
		return
	}
	b.groups = append(b.groups, b.group)
}

func (b *CompactGroupBuilder) Limited() bool {
	return len(b.groups) < b.limit
}

func (b *CompactGroupBuilder) Release() {
	b.groups = nil
}
