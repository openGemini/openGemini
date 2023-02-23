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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"go.uber.org/zap"
)

type idInfo struct {
	lastFlushTime int64
	rows          int64
}

type MmsIdTime struct {
	mu     sync.RWMutex
	idTime map[uint64]*idInfo
}

func (m *MmsIdTime) get(id uint64) (int64, int64) {
	m.mu.RLock()
	info, ok := m.idTime[id]
	m.mu.RUnlock()
	if !ok {
		return math.MinInt64, 0
	}

	return info.lastFlushTime, info.rows
}

func (m *MmsIdTime) addRowCounts(id uint64, rowCounts int64) {
	m.mu.RLock()
	info, ok := m.idTime[id]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		info = m.createIdInfo(id)
		m.mu.Unlock()
	}

	atomic.AddInt64(&info.rows, rowCounts)
}

func (m *MmsIdTime) batchUpdate(p *IdTimePairs) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range p.Ids {
		info := m.createIdInfo(p.Ids[i])
		info.lastFlushTime = p.Tms[i]
	}
}

func (m *MmsIdTime) batchUpdateCheckTime(p *IdTimePairs) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range p.Ids {
		info := m.createIdInfo(p.Ids[i])

		info.rows += p.Rows[i]
		if len(p.Tms) > i && p.Tms[i] > info.lastFlushTime {
			info.lastFlushTime = p.Tms[i]
		}
	}
}

func (m *MmsIdTime) createIdInfo(id uint64) *idInfo {
	info, ok := m.idTime[id]
	if !ok {
		info = &idInfo{lastFlushTime: math.MinInt64, rows: 0}
		m.idTime[id] = info
	}
	return info
}

func NewMmsIdTime() *MmsIdTime {
	return &MmsIdTime{
		idTime: make(map[uint64]*idInfo, 32),
	}
}

type Sequencer struct {
	mu        sync.RWMutex
	mmsIdTime map[string]*MmsIdTime // {"cpu_0001": *MmsIdTime}
	seqMu     sync.RWMutex          // only one goroutine can reload sequencer and others wait
	isLoading bool                  // is loading for mmsIdTime, set isLoading false when loading mmsIdTime finish
	isFree    bool                  // if free successfully, set isFree true else false
	ref       int32                 // used for mark use of sequencer, if sequencer is in used, can not free
}

func NewSequencer() *Sequencer {
	return &Sequencer{
		mmsIdTime: make(map[string]*MmsIdTime, 16),
	}
}

func (s *Sequencer) free() bool {
	if s.isFree {
		return false
	}
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	if atomic.LoadInt32(&s.ref) != 0 {
		return false
	}
	s.mmsIdTime = make(map[string]*MmsIdTime, 16) // keep this map to avoid lose memtable id time in flush
	s.isFree = true
	return true
}

// pass nil MmsTables if do not need reload id time
func (s *Sequencer) addRef(m *MmsTables) error {
	s.seqMu.RLock()
	if s.isFree && m != nil {
		s.seqMu.RUnlock()
		err := s.reloadIdTime(m)
		if err != nil {
			return err
		}
		s.seqMu.RLock()
	}
	atomic.AddInt32(&s.ref, 1)
	s.seqMu.RUnlock()
	return nil
}

func (s *Sequencer) reloadIdTime(m *MmsTables) error {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	if !s.isFree {
		return nil
	}
	count, err := m.loadIdTimesInLock()
	if m.addFunc != nil && !m.isAdded {
		// add row count to shard
		m.addFunc(count)
		m.isAdded = true
	}
	s.isFree = false
	if err != nil {
		s.mmsIdTime = make(map[string]*MmsIdTime, len(s.mmsIdTime))
		s.isFree = true
	}
	return err
}

func (s *Sequencer) unRef() {
	atomic.AddInt32(&s.ref, -1)
}

func (s *Sequencer) getMmsIdTime(name string) *MmsIdTime {
	s.mu.RLock()
	mmsIdTime, ok := s.mmsIdTime[name]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		mmsIdTime, ok = s.mmsIdTime[name]
		if !ok {
			mmsIdTime = NewMmsIdTime()
			s.mmsIdTime[name] = mmsIdTime
		}
		s.mu.Unlock()
	}
	return mmsIdTime
}

func (s *Sequencer) BatchUpdate(p *IdTimePairs) {
	mmsIdTime := s.getMmsIdTime(p.Name)
	mmsIdTime.batchUpdate(p)
}

func (s *Sequencer) BatchUpdateCheckTime(p *IdTimePairs) {
	defer PutIDTimePairs(p)
	start := time.Now()
	mmsIdTime := s.getMmsIdTime(p.Name)
	mmsIdTime.batchUpdateCheckTime(p)
	s.isLoading = false
	log.Info("batch update check time success", zap.String("time used", time.Since(start).String()), zap.Int("series ids", len(p.Ids)))
}

func (s *Sequencer) IsLoading() bool {
	s.seqMu.RLock()
	defer s.seqMu.RUnlock()
	return s.isLoading
}

func (s *Sequencer) Get(mn string, id uint64) (lastFlushTime, rowCnt int64) {
	s.mu.RLock()
	mmsIdTime, exist := s.mmsIdTime[mn]
	s.mu.RUnlock()
	if !exist {
		return math.MinInt64, 0
	}
	return mmsIdTime.get(id)
}

func (s *Sequencer) AddRowCounts(mn string, id uint64, rowCounts int64) {
	mmsIdTime := s.getMmsIdTime(mn)
	mmsIdTime.addRowCounts(id, rowCounts)
}

// IdTimePairs If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
var idTimesPool = sync.Pool{}

type IdTimePairs struct {
	Name string
	Ids  []uint64
	Tms  []int64
	Rows []int64
}

func GetIDTimePairs(name string) *IdTimePairs {
	v := idTimesPool.Get()
	if v == nil {
		return &IdTimePairs{
			Ids:  make([]uint64, 0, 64),
			Tms:  make([]int64, 0, 64),
			Rows: make([]int64, 0, 64),
			Name: name,
		}
	}
	p, ok := v.(*IdTimePairs)
	if !ok {
		panic("GetIDTimePairs idTimesPool Get value isn't *IdTimePairs type")
	}
	p.Reset(name)
	return p
}

func PutIDTimePairs(pair *IdTimePairs) {
	idTimesPool.Put(pair)
}

func (p *IdTimePairs) Add(id uint64, tm int64) {
	p.Ids = append(p.Ids, id)
	p.Tms = append(p.Tms, tm)
}

func (p *IdTimePairs) AddRowCounts(rowCounts int64) {
	p.Rows = append(p.Rows, rowCounts)
}

func (p *IdTimePairs) Len() int {
	return len(p.Ids)
}

func (p *IdTimePairs) Reset(name string) {
	p.Name = name
	p.Ids = p.Ids[:0]
	p.Tms = p.Tms[:0]
	p.Rows = p.Rows[:0]
}

func (p *IdTimePairs) Marshal(isOrder bool, dst []byte, ctx *CoderContext) []byte {
	var err error
	maxBlock := uint32(DefaultMaxRowsPerSegment) * 2
	rows := uint32(len(p.Tms))
	blocks := rows / maxBlock
	if rows%maxBlock > 0 {
		blocks++
	}

	dst = numberenc.MarshalUint32Append(dst, rows)
	dst = numberenc.MarshalUint32Append(dst, blocks)
	startIdx, count := uint32(0), maxBlock
	for i := uint32(0); i < blocks; i++ {
		if i == blocks-1 {
			count = rows - startIdx
		}
		dst = numberenc.MarshalUint32Append(dst, count)

		// encode series ids
		var buf [4]byte
		pos := len(dst)
		dst = append(dst, buf[:]...)
		dst, err = EncodeUnsignedBlock(record.Uint64Slice2byte(p.Ids[startIdx:startIdx+count]), dst, ctx)
		if err != nil {
			panic(err)
		}
		size := len(dst) - pos - 4
		sb := numberenc.MarshalUint32Append(buf[:0], uint32(size))
		copy(dst[pos:pos+4], sb[:4])

		// encode row counts
		pos = len(dst)
		dst = append(dst, buf[:]...)
		dst, err = EncodeIntegerBlock(record.Int64Slice2byte(p.Rows[startIdx:startIdx+count]), dst, ctx)
		if err != nil {
			panic(err)
		}
		size = len(dst) - pos - 4
		sb = numberenc.MarshalUint32Append(buf[:0], uint32(size))
		copy(dst[pos:pos+4], sb[:4])

		if isOrder {
			// encode flush times
			pos = len(dst)
			dst = append(dst, buf[:]...)
			dst, err = EncodeIntegerBlock(record.Int64Slice2byte(p.Tms[startIdx:startIdx+count]), dst, ctx)
			if err != nil {
				panic(err)
			}
			size = len(dst) - pos - 4
			sb = numberenc.MarshalUint32Append(buf[:0], uint32(size))
			copy(dst[pos:pos+4], sb[:4])
		}

		startIdx += maxBlock
	}

	return dst
}

func (p *IdTimePairs) Unmarshal(isOrder bool, src []byte) ([]byte, error) {
	var err error
	if len(src) < 8 {
		err = fmt.Errorf("too small data for id time, %d", len(src))
		log.Error(err.Error())
		return nil, err
	}

	rows := int(numberenc.UnmarshalUint32(src))
	src = src[4:]
	if cap(p.Ids) < rows {
		delta := rows - cap(p.Ids)
		p.Ids = p.Ids[:cap(p.Ids)]
		p.Ids = append(p.Ids, make([]uint64, delta)...)

		p.Tms = p.Tms[:cap(p.Tms)]
		p.Tms = append(p.Tms, make([]int64, delta)...)

		p.Rows = p.Rows[:cap(p.Rows)]
		p.Rows = append(p.Rows, make([]int64, delta)...)
	}
	p.Ids = p.Ids[:rows]
	p.Tms = p.Tms[:rows]
	p.Rows = p.Rows[:rows]

	blocks := numberenc.UnmarshalUint32(src)
	src = src[4:]

	decoder := NewCoderContext()
	defer decoder.Release()

	decoder.timeCoder = GetTimeCoder()
	startIdx := 0
	for i := uint32(0); i < blocks; i++ {
		if len(src) < 8 {
			return nil, fmt.Errorf("block(%d) smaller data (%v) for number and id length", i, len(src))
		}
		// decode count
		n := int(numberenc.UnmarshalUint32(src))
		if startIdx+n > rows {
			return nil, fmt.Errorf("block(%d) not enouph free id time slice to unmarshal %d, %d, %d", i, rows, n, startIdx)
		}
		src = src[4:]

		// decode series ids
		idLen := int(numberenc.UnmarshalUint32(src))
		if len(src) < idLen {
			return nil, fmt.Errorf("block(%d) smaller (%d) data (%v) for id length", i, idLen, len(src))
		}
		src = src[4:]
		idBytes := record.Uint64Slice2byte(p.Ids[startIdx : startIdx+n])
		idBytes = idBytes[:0]
		_, err = DecodeUnsignedBlock(src[:idLen], &idBytes, decoder)
		if err != nil {
			return nil, err
		}
		src = src[idLen:]

		// decode row counts
		rowCountsLen := int(numberenc.UnmarshalUint32(src))
		if len(src) < rowCountsLen {
			return nil, fmt.Errorf("block(%d) smaller (%d) data (%v) for row counts length", i, rowCountsLen, len(src))
		}
		src = src[4:]
		rowCountsBytes := record.Int64Slice2byte(p.Rows[startIdx : startIdx+n])
		rowCountsBytes = rowCountsBytes[:0]
		_, err = DecodeIntegerBlock(src[:rowCountsLen], &rowCountsBytes, decoder)
		if err != nil {
			return nil, err
		}
		src = src[rowCountsLen:]

		if isOrder {
			// decode last flush times
			timeLen := int(numberenc.UnmarshalUint32(src))
			if len(src) < timeLen {
				return nil, fmt.Errorf("block(%d) smaller (%d) data (%v) for time length", i, timeLen, len(src))
			}
			src = src[4:]
			timeBytes := record.Int64Slice2byte(p.Tms[startIdx : startIdx+n])
			timeBytes = timeBytes[:0]
			_, err = DecodeIntegerBlock(src[:timeLen], &timeBytes, decoder)
			if err != nil {
				return nil, err
			}
			src = src[timeLen:]
		}
		startIdx += n
	}
	return src, nil
}
