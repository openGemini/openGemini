/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
	"math"
	"sort"
	"sync"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

var oooMergeContextPool = sync.Pool{New: func() interface{} { return &OutOfOrderMergeContext{} }}

func NewOutOfOrderMergeContext() *OutOfOrderMergeContext {
	ctx := oooMergeContextPool.Get().(*OutOfOrderMergeContext)
	return ctx
}

func (ctx *OutOfOrderMergeContext) Release() {
	ctx.reset()
	oooMergeContextPool.Put(ctx)
}

type OrderInfo struct {
	orderSeqs  []TSSPFileName
	orderNames []string
}

func (inf *OrderInfo) Len() int { return len(inf.orderSeqs) }

func (inf *OrderInfo) Less(i, j int) bool {
	if inf.orderSeqs[i].seq != inf.orderSeqs[j].seq {
		return inf.orderSeqs[i].seq < inf.orderSeqs[j].seq
	} else {
		return inf.orderSeqs[i].extent < inf.orderSeqs[j].extent
	}
}

func (inf *OrderInfo) Swap(i, j int) {
	inf.orderNames[i], inf.orderNames[j] = inf.orderNames[j], inf.orderNames[i]
	inf.orderSeqs[i], inf.orderSeqs[j] = inf.orderSeqs[j], inf.orderSeqs[i]
}

type OutOfOrderMergeContext struct {
	mstName string
	shId    uint64

	tr    record.TimeRange
	size  int64
	Seqs  []uint64
	Names []string

	OrderInfo
}

func (ctx *OutOfOrderMergeContext) reset() {
	ctx.mstName = ""
	ctx.shId = 0
	ctx.tr.Min = 0
	ctx.tr.Max = 0
	ctx.size = 0
	ctx.Seqs = ctx.Seqs[:0]
	ctx.Names = ctx.Names[:0]
	ctx.orderSeqs = ctx.orderSeqs[:0]
	ctx.orderNames = ctx.orderNames[:0]
}

func (ctx *OutOfOrderMergeContext) Add(f TSSPFile) bool {
	min, max, err := f.MinMaxTime()
	if err != nil {
		log.Error("failed to get min, max time")
		return false
	}

	if ctx.tr.Min > min {
		ctx.tr.Min = min
	}
	if ctx.tr.Max < max {
		ctx.tr.Max = max
	}

	_, seq := f.LevelAndSequence()
	ctx.size += f.FileSize()
	ctx.Seqs = append(ctx.Seqs, seq)
	ctx.Names = append(ctx.Names, f.Path())
	return !(len(ctx.Seqs) > MaxNumOfFileToMerge || ctx.size > MaxSizeOfFileToMerge)
}

type InMerge struct {
	mu     sync.Mutex
	tables map[string]struct{}
}

func (m *InMerge) Add(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[name]; ok {
		return false
	}

	m.tables[name] = struct{}{}
	return true
}

func (m *InMerge) Del(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tables, name)
}

func NewInMerge() *InMerge {
	return &InMerge{tables: make(map[string]struct{}, defaultCap)}
}

type waitMergedRecord struct {
	appendRec *record.AppendRecord
	rec       *record.Record

	min, max int64
	times    []int64

	// data before offset has been merged
	offset int
}

func (r *waitMergedRecord) setRecord(rec *record.Record) {
	record.ReleaseAppendRecord(r.appendRec)
	r.rec = rec
	r.appendRec = record.NewAppendRecord(rec)
	r.times = rec.Times()
	if len(r.times) == 0 {
		return
	}

	r.min = r.times[0]
	r.max = r.times[len(r.times)-1]
	r.offset = 0
}

func newWaitMergedRecord(rec *record.Record) *waitMergedRecord {
	ret := &waitMergedRecord{}

	if rec != nil {
		ret.setRecord(rec)
	}

	return ret
}

type mergeHelper struct {
	logger *logger.Logger
	tier   uint64
	name   string
	path   string

	// data to be merged, map key is Series Id
	waits     map[uint64]*waitMergedRecord
	deleted   []*waitMergedRecord
	seriesIds []uint64

	// reuse Record
	record       *record.Record
	seriesRecord *record.Record

	cancelFunc func() bool
	schemas    int

	stat *statistics.MergeStatItem

	newFiles []TSSPFile
	Conf     *Config
}

func NewMergeHelper(logger *logger.Logger, tier uint64, name string, path string, cancelFunc func() bool) *mergeHelper {
	hlp := &mergeHelper{
		logger:       logger,
		tier:         tier,
		name:         name,
		path:         path,
		waits:        make(map[uint64]*waitMergedRecord, defaultCap),
		record:       recordPool.get(),
		seriesRecord: recordPool.get(),
		cancelFunc:   cancelFunc,
		stat:         &statistics.MergeStatItem{},
	}

	return hlp
}

func (c *mergeHelper) MergeTo(dstFiles *TSSPFiles) ([]TSSPFile, error) {
	c.seriesIds = append(c.seriesIds, math.MaxUint64)
	sort.Slice(c.seriesIds, func(i, j int) bool {
		return c.seriesIds[i] < c.seriesIds[j]
	})

	c.newFiles = make([]TSSPFile, 0, dstFiles.Len())
	for i, dst := range dstFiles.files {
		if c.cancelFunc() {
			return nil, errno.NewError(errno.MergeCanceled)
		}

		if err := c.merge(dst, i == dstFiles.Len()-1); err != nil {
			return c.newFiles, err
		}
	}

	for _, w := range c.waits {
		recordPool.put(w.rec)
	}
	for _, w := range c.deleted {
		recordPool.put(w.rec)
	}
	recordPool.put(c.record)
	recordPool.put(c.seriesRecord)

	return c.newFiles, nil
}

func (c *mergeHelper) newFileName(fileName TSSPFileName) TSSPFileName {
	exist := false
	for _, f := range c.newFiles {
		newName := f.(*tsspFile).name
		if newName.Equal(&fileName) {
			exist = true
			break
		}
	}

	if exist {
		last := c.newFiles[len(c.newFiles)-1]
		lastNewName := last.(*tsspFile).name
		fileName.SetExtend(lastNewName.extent + 1)
	}

	return fileName
}

func (c *mergeHelper) merge(f TSSPFile, last bool) (err error) {
	if len(c.waits) == 0 {
		return nil
	}

	fSize := f.FileSize()
	if !last && !c.contains(f) {
		return nil
	}
	c.stat.StatOrderFile(fSize)
	pos := len(c.newFiles)
	defer func() {
		for _, nf := range c.newFiles[pos:] {
			c.stat.StatMergedFile(nf.FileSize())
		}
	}()

	fi := NewFileIterator(f, CLog)
	itr := NewChunkIterator(fi)
	itr.WithLog(c.logger)

	level, seq := f.LevelAndSequence()
	fileName := NewTSSPFileName(seq, level, f.FileNameMerge()+1, f.FileNameExtend(), true)
	fileName = c.newFileName(fileName)
	builder := AllocMsBuilder(c.path, c.name, c.Conf, itr.chunkN, fileName, c.tier, nil, int(fSize))
	builder.WithLog(c.logger)
	defer func(msb **MsBuilder) {
		if err != nil {
			for _, nf := range c.newFiles[pos:] {
				_ = nf.Close()
				_ = nf.Remove()
			}
			c.newFiles = c.newFiles[:pos]
			nm := (*msb).fd.Name()
			_ = (*msb).fd.Close()
			lock := fileops.FileLockOption("")
			_ = fileops.Remove(nm, lock)
		}

		PutMsBuilder(*msb)
	}(&builder)

	dst := newWaitMergedRecord(nil)
	err = c.ReadSeriesRecord(itr, func(sid uint64, rec *record.Record) error {
		if rec.RowNums() == 0 {
			return nil
		}

		if last {
			err = c.writeToLast(sid, &builder)
			if err != nil {
				return err
			}
		}
		wait, ok := c.waits[sid]

		dst.setRecord(rec)
		if !ok || wait == nil || (!last && wait.min > dst.max) {
			builder, err = builder.WriteRecord(sid, rec, func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
				fn.extent++
				fn = c.newFileName(fn)
				return fn.seq, fn.level, fn.merge, fn.extent
			})

			return err
		}

		merged, err := c.mergeRecordByTime(dst, wait, last)
		if err != nil {
			return err
		}

		if merged != nil && merged.RowNums() > 0 {
			dst.rec = merged
		}

		if wait.min == math.MaxInt64 {
			c.deleted = append(c.deleted, wait)
			delete(c.waits, sid)
		}

		builder, err = builder.WriteRecord(sid, dst.rec, func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			fn.extent++
			fn = c.newFileName(fn)
			return fn.seq, fn.level, fn.merge, fn.extent
		})

		return err
	})

	if err != nil {
		return
	}

	if last {
		err = c.writeToLast(math.MaxUint64, &builder)
		if err != nil {
			return
		}
	}

	var nf TSSPFile
	nf, err = builder.NewTSSPFile(true)
	if err != nil {
		c.logger.Error("new tmp file fail", zap.Error(err))
		return
	}
	c.newFiles = append(c.newFiles, builder.Files...)
	if nf != nil {
		c.newFiles = append(c.newFiles, nf)
	}

	return
}

// write remaining data to the last file
func (c *mergeHelper) writeToLast(maxSid uint64, msb **MsBuilder) (err error) {
	builder := *msb
	for i, sid := range c.seriesIds {
		if c.cancelFunc() {
			return errno.NewError(errno.MergeCanceled)
		}

		if sid >= maxSid {
			c.seriesIds = c.seriesIds[i:]
			return nil
		}

		wait, ok := c.waits[sid]
		if !ok || wait == nil || wait.min == math.MinInt64 {
			continue
		}
		c.deleted = append(c.deleted, wait)
		delete(c.waits, sid)

		c.logger.Debug("write remaining data", zap.Uint64("SeriesId", sid),
			zap.Int("offset", wait.offset), zap.Int("size", len(wait.times)))

		var rec *record.Record
		if wait.offset == 0 {
			rec = wait.rec
		} else {
			rec = c.resetRecord(&record.Record{}, wait.rec.Schema)
			rec.SliceFromRecord(wait.rec, wait.offset, len(wait.times))
		}

		builder, err = builder.WriteRecord(sid, rec, func(fn TSSPFileName) (seq uint64, lv uint16, merge uint16, ext uint16) {
			fn.extent++
			fn = c.newFileName(fn)
			return fn.seq, fn.level, fn.merge, fn.extent
		})
		*msb = builder
		if err != nil {
			return
		}
	}

	return
}

// merge the data whose time in src is less than or equal to the maximum time in dst and all dst a new record
func (c *mergeHelper) mergeRecordByTime(dst *waitMergedRecord, src *waitMergedRecord, last bool) (*record.Record, error) {
	if last {
		src.min = math.MaxInt64
		return c.mergeRecord(dst, src, src.offset, len(src.times))
	}

	tl := len(src.times)
	start, end := src.offset, src.offset

	for i := src.offset; i < tl; i++ {
		if src.times[i] > dst.max {
			break
		}
		end++
		if end >= tl {
			src.min = math.MaxInt64
			break
		}
		src.min = src.times[end]
	}

	if start == end {
		return nil, nil
	}

	return c.mergeRecord(dst, src, start, end)
}

func (c *mergeHelper) appendColVal(dst *record.AppendRecord, src *waitMergedRecord, limit int) {
	if limit == 0 {
		return
	}

	err := dst.AppendSequence(src.appendRec, limit)
	if err != nil {
		c.logger.Error("failed to append col val", zap.Error(err))
		return
	}
}

// combine all of dst and start-to-end of src into a new record
func (c *mergeHelper) mergeRecord(dst *waitMergedRecord, src *waitMergedRecord, start, end int) (*record.Record, error) {
	i, j := 0, start

	src.rec.PadRecord(dst.rec)
	dst.rec.PadRecord(src.rec)
	src.appendRec.UpdateCols()
	dst.appendRec.UpdateCols()

	if err := src.appendRec.Check(dst.appendRec); err != nil {
		return nil, err
	}

	record.CheckRecord(src.rec)
	ret := c.resetRecord(c.record, dst.rec.Schema)
	appendRec := record.NewAppendRecord(ret)

	var state *waitMergedRecord
	lo := len(dst.times)

	var changeState = func(new *waitMergedRecord, end int) {
		if state == new || (state != nil && end <= state.offset) {
			return
		}

		if state != nil {
			c.appendColVal(appendRec, state, end-state.offset)
			state.offset = end
		}
		state = new
	}

	for {
		if i == lo {
			c.appendColVal(appendRec, dst, lo-dst.offset)
			c.appendColVal(appendRec, src, end-src.offset)
			break
		}

		if j == end {
			c.appendColVal(appendRec, src, end-src.offset)
			c.appendColVal(appendRec, dst, lo-dst.offset)
			break
		}

		// replace the old value with the new value at the same time.
		if src.times[j] == dst.times[i] {
			if state == src {
				changeState(nil, j)
			} else if state == dst {
				changeState(nil, i)
			}
			c.appendColVal(appendRec, src, 1)
			appendRec.AppendNotNil(dst.appendRec)
			src.offset++
			dst.offset++
			i++
			j++
			continue
		}

		if src.times[j] <= dst.times[i] {
			changeState(src, i)
			j++
		} else {
			changeState(dst, j)
			i++
		}
	}

	src.offset = end
	return ret, nil
}

func (c *mergeHelper) resetRecord(rec *record.Record, schema []record.Field) *record.Record {
	rec.Reset()
	rec.Schema = append(rec.Schema[:0], schema...)
	rec.ReserveColVal(len(schema))
	return rec
}

func (c *mergeHelper) contains(f TSSPFile) bool {
	var tr record.TimeRange
	var err error
	for sid, wr := range c.waits {
		tr.Min, tr.Max, err = f.MinMaxTime()
		if err != nil {
			return false
		}

		if wr.min > tr.Max {
			continue
		}

		if ok, _ := f.Contains(sid); ok {
			return true
		}
	}

	return false
}

func (c *mergeHelper) ReadSeriesRecord(itr *ChunkIterator, callback func(sid uint64, rec *record.Record) error) error {
	for {
		if c.cancelFunc() {
			return errno.NewError(errno.MergeCanceled)
		}

		if !itr.Next() {
			return nil
		}
		if itr.id == 0 {
			return errno.NewError(errno.SeriesIdIsZero, itr.r.Path())
		}

		c.seriesRecord = c.resetRecord(c.seriesRecord, itr.merge.Schema)
		c.seriesRecord.Merge(itr.merge)
		if err := callback(itr.id, c.seriesRecord); err != nil {
			return err
		}
	}
}

func (c *mergeHelper) ReadWaitMergedRecords(files *TSSPFiles) bool {
	for _, f := range files.Files() {
		c.stat.StatOutOfOrderFile(f.FileSize())

		fi := NewFileIterator(f, CLog)
		itr := NewChunkIterator(fi)
		itr.WithLog(c.logger)

		err := c.readWaitMergedRecords(itr)
		if err != nil {
			c.logger.Error("failed to readWaitMergedRecords", zap.Error(err),
				zap.String("file path", f.Path()))
			return false
		}
	}
	return true
}

func (c *mergeHelper) readWaitMergedRecords(itr *ChunkIterator) error {
	err := c.ReadSeriesRecord(itr, func(sid uint64, rec *record.Record) error {
		if rec.RowNums() == 0 {
			return nil
		}

		if len(rec.Times()) == 0 {
			c.logger.Error("RowNums() not 0 but the length of Times() is 0")
		}

		if c.schemas < rec.Len() {
			c.schemas = rec.Len()
		}

		wait, ok := c.waits[sid]
		if !ok {
			c.seriesIds = append(c.seriesIds, sid)
			c.waits[sid] = newWaitMergedRecord(rec)

			// the record is saved to c.waits
			// therefore, do not reuse this Record
			c.RenewSeriesRecord()
			return nil
		}

		merged, err := c.mergeRecord(wait, newWaitMergedRecord(rec), 0, rec.RowNums())
		if err != nil {
			return err
		}

		c.record = wait.rec
		c.record.ResetDeep()
		wait.setRecord(merged)

		if len(wait.times) == 0 {
			c.logger.Error("length of wait.times is 0")
		}

		return nil
	})
	return err
}

func (c *mergeHelper) RenewSeriesRecord() {
	c.seriesRecord = recordPool.get()
}

var recordPool = &mergeRecordPool{}

type mergeRecordPool struct {
	pool sync.Pool
}

func (p *mergeRecordPool) get() *record.Record {
	stat.AddRecordPoolGetTotal(1)
	rec, ok := p.pool.Get().(*record.Record)
	if !ok || rec == nil {
		return &record.Record{}
	}
	stat.AddRecordPoolHitTotal(1)
	return rec
}

func (p *mergeRecordPool) put(rec *record.Record) {
	if rec == nil {
		return
	}
	rec.ResetDeep()
	p.pool.Put(rec)
}
