/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immutable

import (
	"container/heap"
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
)

type MergeSelfHook interface {
	OnWriteRecord(record *record.Record)
	OnNewFile(f TSSPFile)
}

type MergeSelf struct {
	once   sync.Once
	signal chan struct{}
	mts    *MmsTables
	lg     *logger.Logger

	hook MergeSelfHook
}

func NewMergeSelf(mts *MmsTables, lg *logger.Logger) *MergeSelf {
	return &MergeSelf{
		signal: make(chan struct{}),
		mts:    mts,
		lg:     lg,
	}
}

func (m *MergeSelf) SetHook(hook MergeSelfHook) {
	m.hook = hook
}

func (m *MergeSelf) Merge(mst string, files []TSSPFile) (TSSPFile, error) {
	builder := m.createMsBuilder(mst, files[0].FileName())
	sh := record.NewColumnSortHelper()
	defer sh.Release()

	itrs := m.createIterators(files)

	for {
		sid, rec, err := itrs.Next()
		if err != nil {
			builder.Reset()
			return nil, err
		}

		if rec == nil || sid == 0 {
			break
		}

		if m.hook != nil {
			m.hook.OnWriteRecord(rec)
		}

		record.CheckRecord(rec)
		rec = sh.Sort(rec)
		itrs.merged = rec
		builder, err = builder.WriteRecord(sid, rec, nil)
		if err != nil {
			builder.Reset()
			return nil, err
		}
	}

	itrs.Close()

	merged, err := builder.NewTSSPFile(true)
	if m.hook != nil {
		m.hook.OnNewFile(merged)
	}
	return merged, err
}

func (m *MergeSelf) createIterators(files []TSSPFile) *ChunkIterators {
	var dropping int64 = 0
	itrs := &ChunkIterators{
		dropping:      &dropping,
		closed:        m.signal,
		stopCompMerge: m.signal,
		itrs:          make([]*ChunkIterator, 0, len(files)),
		merged:        &record.Record{},
	}
	itrs.WithLog(m.lg)

	for _, f := range files {
		fi := NewFileIterator(f, m.lg)
		itr := NewChunkIterator(fi)
		itr.WithLog(m.lg)
		ok := itr.Next()
		if !ok || itr.err != nil {
			itr.Close()
			continue
		}
		itrs.itrs = append(itrs.itrs, itr)
	}

	heap.Init(itrs)
	return itrs
}

func (m *MergeSelf) createMsBuilder(mst string, fileName TSSPFileName) *MsBuilder {
	fileName.merge++
	fileName.lock = m.mts.lock
	builder := NewMsBuilder(m.mts.path, mst, m.mts.lock, m.mts.Conf,
		0, fileName, 0, nil, 0, config.TSSTORE, nil, m.mts.shardId)
	return builder
}

func (m *MergeSelf) Stop() {
	m.once.Do(func() {
		close(m.signal)
	})
}
