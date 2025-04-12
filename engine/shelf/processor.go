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

package shelf

import (
	"fmt"
	"io"
	"log"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"go.uber.org/zap"
)

var walSeq = uint64(time.Now().UnixNano())
var runner *Runner
var conf = &config.GetStoreConfig().ShelfMode
var stat = statistics.NewShelf()
var tsspConvertLimited limiter.Fixed

func AllocTSSPConvertSource() (bool, func()) {
	if tsspConvertLimited.TryTake() {
		return true, tsspConvertLimited.Release
	}

	return false, nil
}

func AllocWalSeq() uint64 {
	return atomic.AddUint64(&walSeq, 1)
}

func Open() {
	conf = &config.GetStoreConfig().ShelfMode
	if !conf.Enabled {
		return
	}
	log.Println("Open shelf runner. Concurrent:", conf.Concurrent, conf.TSSPConvertConcurrent)
	runner = newRunner(conf.Concurrent)
	initWalCtxPool()
	mutable.SetShelfMemRecordReader(ReadRecord)
	tsspConvertLimited = limiter.NewFixed(conf.TSSPConvertConcurrent)
}

func NewRunner() *Runner {
	return runner
}

type Index interface {
	GetSeriesIdBySeriesKeyFromCache([]byte) (uint64, error)
	CreateIndexIfNotExistsBySeries([]byte, []byte, influx.PointTags) (uint64, error)
}

type Runner struct {
	processors []*Processor
	runner     *util.ConcurrentRunner[record.MemGroup]
}

func (r *Runner) Size() int {
	return r.runner.Size()
}

func (r *Runner) Schedule(shardID uint64, rg *record.MemGroup) {
	rg.SetShardID(shardID)
	r.runner.Schedule(rg.Hash(), rg)
}

func (r *Runner) Close() {
	if r == nil {
		return
	}
	r.runner.Stop()
	r.runner.Close()
}

func (r *Runner) RegisterShard(id uint64, info *ShardInfo) {
	if r == nil {
		return
	}
	for _, p := range r.processors {
		p.RegisterShard(id, info)
	}
}

func (r *Runner) UnregisterShard(id uint64) {
	if r == nil {
		return
	}
	for _, p := range r.processors {
		p.UnRegisterShard(id)
	}
}

func (r *Runner) GetWALs(shardID uint64, mst string, tr *util.TimeRange) ([]*Wal, func()) {
	var wals []*Wal
	for idx := range r.processors {
		shard := r.processors[idx].GetShards(shardID)
		if shard != nil {
			wals = shard.GetWalReaders(wals, mst, tr)
		}
	}

	return wals, func() {
		for _, wal := range wals {
			wal.Unref()
		}
	}
}

type Processor struct {
	id     int
	signal *util.Signal
	queue  *util.Queue[record.MemGroup]

	mu sync.RWMutex
	// map key is ShardID
	shards map[uint64]*Shard
}

func newRunner(size int) *Runner {
	r := &Runner{processors: make([]*Processor, size)}

	queues := make([]*util.Queue[record.MemGroup], size)
	for i := 0; i < size; i++ {
		queues[i] = util.NewQueue[record.MemGroup](max(1, size/2))
	}
	processors := make([]util.Processor, size)
	for i := 0; i < size; i++ {
		p := NewProcessor(i, queues[i])
		r.processors[i] = p
		processors[i] = p
	}

	r.runner = util.NewConcurrentRunner[record.MemGroup](queues, processors)
	return r
}

func NewProcessor(id int, queue *util.Queue[record.MemGroup]) *Processor {
	p := &Processor{
		id:     id,
		signal: util.NewSignal(),
		shards: make(map[uint64]*Shard),
		queue:  queue,
	}
	return p
}

func (p *Processor) RegisterShard(shardID uint64, info *ShardInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.shards[shardID]
	if ok {
		return
	}

	shard := NewShard(p.id, info, DefaultShardFreeDuration)
	shard.Run()
	p.shards[shardID] = shard
}

func (p *Processor) UnRegisterShard(id uint64) {
	p.mu.Lock()
	shard, ok := p.shards[id]
	delete(p.shards, id)
	p.mu.Unlock()
	if !ok || shard == nil {
		return
	}

	shard.Stop()
}

func (p *Processor) GetShards(sharID uint64) *Shard {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.shards[sharID]
}

func (p *Processor) Run() {
	for {
		rg, ok := p.queue.Pop()
		if !ok {
			break
		}

		err := p.process(rg)
		rg.Done(err)
		if err != nil {
			stat.WriteFailedCount.Incr()
			logger.GetLogger().Error("failed to write rows", zap.Error(err))
		}
	}
}

func (p *Processor) Stop() {
	p.signal.CloseOnce(func() {
		for _, shard := range p.shards {
			shard.Stop()
		}
	})
}

func (p *Processor) Close() {

}

func (p *Processor) process(rg *record.MemGroup) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("recover: %s", e)
		}
	}()

	stat.ScheduleDurSum.Add(rg.MicroSince())
	defer statistics.MicroTimeUse(stat.WriteCount, stat.WriteDurSum)()

	recs := rg.Records()
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Name < recs[j].Name
	})

	p.mu.RLock()
	shard, ok := p.shards[rg.ShardID()]
	p.mu.RUnlock()

	if !ok || shard == nil {
		return fmt.Errorf("shard %d not exist", rg.ShardID())
	}

	j := 0
	for i := 1; i < len(recs); i++ {
		if recs[i].Name == recs[i-1].Name {
			continue
		}

		err = shard.Write(recs[j:i])
		if err != nil {
			return err
		}
		j = i
	}

	return shard.Write(recs[j:])
}

func ReadRecord(shardID uint64, mst string, sid uint64, tr *util.TimeRange,
	schema record.Schemas, ascending bool) *record.Record {

	swap := &record.Record{}
	rec := &record.Record{}

	ctx, release := NewWalCtx()
	wals, unref := NewRunner().GetWALs(shardID, mst, tr)
	defer func() {
		unref()
		release()
	}()

	for _, wal := range wals {
		swap.ResetDeep()
		err := wal.ReadRecord(ctx, sid, swap)
		if err == io.EOF {
			continue
		}
		if err != nil {
			logger.GetLogger().Error("failed to read record from wal", zap.Error(err))
			return nil
		}

		if swap.RowNums() == 0 {
			continue
		}

		if rec.RowNums() == 0 {
			rec, swap = swap, rec
			continue
		}

		rec.Merge(swap)
	}

	if !slices.IsSorted(rec.Times()) {
		hlp := record.NewColumnSortHelper()
		defer hlp.Release()
		rec = hlp.Sort(rec)
	}

	if rec == nil || rec.RowNums() == 0 {
		return nil
	}

	return rec.Copy(ascending, tr, schema)
}
