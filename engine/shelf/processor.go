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
	"cmp"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/openGemini/openGemini/engine/immutable"
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
var conf = config.GetShelfMode()
var stat *statistics.Shelf
var tsspConvertLimited limiter.Fixed

func AllocWalSeq() uint64 {
	return atomic.AddUint64(&walSeq, 1)
}

func Open() {
	conf = config.GetShelfMode()
	if !conf.Enabled {
		return
	}
	stat = statistics.NewShelf()
	log.Println("Open shelf runner. Concurrent:", conf.Concurrent, conf.TSSPConvertConcurrent)
	runner = newRunner(conf.Concurrent)
	initWalCtxPool()
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
	icm        *IndexCreatorManager
	processors []*Processor
	runner     *util.ConcurrentRunner[Blob]
}

func (r *Runner) IndexCreatorManager() *IndexCreatorManager {
	return r.icm
}

func (r *Runner) Size() int {
	return r.runner.Size()
}

func (r *Runner) ScheduleGroup(shardID uint64, group *BlobGroup) error {
	group.ResetTime()
	for i := range group.blobs {
		blob := &group.blobs[i]
		if blob.IsEmpty() {
			continue
		}
		group.wg.Add(1)
		blob.done = group.wg.Done
		blob.ResetTime()
		runner.Schedule(shardID, blob)
	}

	group.Wait()
	return group.Error()
}

func (r *Runner) Schedule(shardID uint64, blob *Blob) {
	blob.SetShardID(shardID)
	r.runner.Schedule(blob.Hash(), blob)
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

func (r *Runner) ForceFlush(shardID uint64) {
	if r == nil {
		return
	}
	for _, p := range r.processors {
		p.ForceFlush(shardID)
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
	queue  *util.Queue[Blob]

	mu sync.RWMutex
	// map key is ShardID
	shards map[uint64]*Shard
}

func newRunner(size int) *Runner {
	r := &Runner{processors: make([]*Processor, size)}

	queues := make([]*util.Queue[Blob], size)
	for i := 0; i < size; i++ {
		queues[i] = util.NewQueue[Blob](max(1, size/2))
	}
	processors := make([]util.Processor, size)
	for i := 0; i < size; i++ {
		p := NewProcessor(i, queues[i])
		r.processors[i] = p
		processors[i] = p
	}

	r.runner = util.NewConcurrentRunner[Blob](queues, processors)
	r.icm = NewIndexCreatorManager()
	return r
}

func NewProcessor(id int, queue *util.Queue[Blob]) *Processor {
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

func (p *Processor) ForceFlush(shardID uint64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	shard, ok := p.shards[shardID]
	if ok && shard != nil {
		shard.ForceFlush()
	}
}

func (p *Processor) GetShards(shardID uint64) *Shard {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.shards[shardID]
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

func (p *Processor) process(blob *Blob) (err error) {
	defer func() {
		if e := recover(); e != nil {
			logger.GetLogger().Error("panic", zap.Any("recover", e))
			err = fmt.Errorf("recover: %s", e)
		}
	}()

	stat.ScheduleDurSum.Add(blob.MicroSince())
	defer statistics.MicroTimeUse(stat.WriteCount, stat.WriteDurSum)()

	p.mu.RLock()
	shard, ok := p.shards[blob.ShardID()]
	p.mu.RUnlock()

	if !ok || shard == nil {
		return fmt.Errorf("shard %d not exist", blob.ShardID())
	}

	var wal *Wal
	defer func() {
		wal.EndWrite()
	}()

	itr := blob.Iterator()
	for {
		seriesKey, data, err := itr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		wal = shard.UpdateWal(blob.TimeRange())
		err = shard.Write(wal, seriesKey, data)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadRecord(shardID uint64, mst string, sid uint64, tr *util.TimeRange,
	schema record.Schemas, ascending bool) *record.Record {

	reader := NewWalReader(shardID, mst, tr)
	reader.Ref()
	defer func() {
		reader.UnRef()
	}()
	return reader.Values(mst, sid, *tr, schema, ascending)
}

type WalReader struct {
	wals     []*Wal
	excludes []bool
	release  func()
}

func (r *WalReader) Exclude(files ...immutable.TSSPFile) {
	for i, wal := range r.wals {
		if !r.excludes[i] && wal.TargetContain(files...) {
			r.excludes[i] = true
		}
	}
}

func (r *WalReader) Ref() {

}

func (r *WalReader) UnRef() {
	if r.release != nil {
		r.release()
		r.release = nil
		r.wals = nil
	}
}

func NewWalReader(shardID uint64, mst string, tr *util.TimeRange) *WalReader {
	wals, release := NewRunner().GetWALs(shardID, mst, tr)
	return &WalReader{
		wals:     wals,
		excludes: make([]bool, len(wals)),
		release:  release,
	}
}

func (r *WalReader) Values(_ string, sid uint64, tr util.TimeRange, schema record.Schemas, ascending bool) *record.Record {
	if len(r.wals) == 0 {
		return nil
	}

	ctx, release := NewWalCtx()
	defer release()

	swap := &record.Record{}
	swap.ResetWithSchema(schema)
	sort.Sort(swap)
	record.PadTimeColIfNeeded(swap)

	rec := &record.Record{
		Schema:  swap.Schema,
		ColVals: make([]record.ColVal, swap.Len()),
	}

	for i, wal := range r.wals {
		if r.excludes[i] {
			continue
		}

		swap.InitColVal(0, swap.Len())
		err := wal.ReadRecord(ctx, sid, swap, true)
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

	if rec == nil || rec.RowNums() == 0 {
		return nil
	}

	if !IsUniqueSorted(rec.Times()) {
		hlp := record.NewColumnSortHelper()
		defer hlp.Release()
		rec = hlp.Sort(rec)
	}

	return rec.Copy(ascending, &tr, schema)
}

func IsUniqueSorted[S ~[]E, E cmp.Ordered](x S) bool {
	for i := len(x) - 1; i > 0; i-- {
		if x[i] <= x[i-1] {
			return false
		}
	}
	return true
}
