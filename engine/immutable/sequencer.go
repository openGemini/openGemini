// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
)

const minIdTimeBlockSize = 8

type idInfo struct {
	lastFlushTime int64
	rows          int64
}

type MmsIdTime struct {
	mu     sync.RWMutex
	idTime map[uint32]map[uint32]*idInfo //map[seriesId's high4Bytes]map[seriesId's low4Bytes]*idInfo, series id split into high 4 Bytes and low 4 Bytes to reduce memory
	sc     *SeriesCounter
}

func (m *MmsIdTime) Get(id uint64) (int64, int64) {
	return m.get(id)
}

func (m *MmsIdTime) getIdInfo(id uint64) (*idInfo, bool) {
	high32 := uint32(id >> 16)
	low32 := uint32(id)
	infos, ok := m.idTime[high32]
	if !ok {
		return nil, false
	}
	info, ok := infos[low32]
	if !ok {
		return nil, false
	}
	return info, true
}

func (m *MmsIdTime) Size() int {
	return len(m.idTime)
}

func (m *MmsIdTime) CreateIdInfo(id uint64) (info *idInfo) {
	high32 := uint32(id >> 16)
	low32 := uint32(id)
	infos, ok := m.idTime[high32]
	if !ok {
		infos = make(map[uint32]*idInfo, 32)
		m.idTime[high32] = infos
	}
	info, ok = infos[low32]
	if ok {
		return info
	}
	info = &idInfo{lastFlushTime: math.MinInt64, rows: 0}
	infos[low32] = info
	m.sc.Incr()
	return info
}

func (m *MmsIdTime) getIdCount() int64 {
	var count int64
	m.mu.RLock()
	for _, subM := range m.idTime {
		count += int64(len(subM))
	}
	m.mu.RUnlock()
	return count
}

func (m *MmsIdTime) get(id uint64) (int64, int64) {
	m.mu.RLock()
	info, ok := m.getIdInfo(id)
	m.mu.RUnlock()
	if !ok {
		return math.MinInt64, 0
	}

	return info.lastFlushTime, info.rows
}

func (m *MmsIdTime) addRowCounts(id uint64, rowCounts int64) {
	m.mu.RLock()
	info, ok := m.getIdInfo(id)
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		info = m.CreateIdInfo(id)
		m.mu.Unlock()
	}

	atomic.AddInt64(&info.rows, rowCounts)
}

func (m *MmsIdTime) batchUpdateCheckTime(p *IdTimePairs, incrRows bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range p.Ids {
		info := m.CreateIdInfo(p.Ids[i])

		if incrRows {
			info.rows += p.Rows[i]
		}
		if len(p.Tms) > i && p.Tms[i] > info.lastFlushTime {
			info.lastFlushTime = p.Tms[i]
		}
	}
}

func NewMmsIdTime(sc *SeriesCounter) *MmsIdTime {
	return &MmsIdTime{
		sc:     sc,
		idTime: make(map[uint32]map[uint32]*idInfo, 32),
	}
}

type Sequencer struct {
	mu        sync.RWMutex
	mmsIdTime map[string]*MmsIdTime // {"cpu_0001": *MmsIdTime}
	seqMu     sync.RWMutex          // only one goroutine can reload sequencer and others wait
	isLoading bool                  // is loading for mmsIdTime, set isLoading false when loading mmsIdTime finish
	isFree    bool                  // if free successfully, set isFree true else false
	ref       int32                 // used for mark use of sequencer, if sequencer is in used, can not free
	sc        SeriesCounter
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
	s.sc.Reset()
	s.mmsIdTime = make(map[string]*MmsIdTime, 16) // keep this map to avoid lose memtable id time in flush
	s.isFree = true
	return true
}

func (s *Sequencer) addRef() {
	s.seqMu.RLock()
	atomic.AddInt32(&s.ref, 1)
	s.seqMu.RUnlock()
}

func (s *Sequencer) UnRef() {
	atomic.AddInt32(&s.ref, -1)
}

func (s *Sequencer) ResetMmsIdTime() {
	s.seqMu.Lock()
	s.mmsIdTime = make(map[string]*MmsIdTime, len(s.mmsIdTime))
	s.seqMu.Unlock()
}

func (s *Sequencer) SetStat(free, loading bool) {
	s.seqMu.Lock()
	s.isFree = free
	s.isLoading = loading
	s.seqMu.Unlock()
}

func (s *Sequencer) SetToInLoading() bool {
	s.seqMu.Lock()
	ok := s.isFree && !s.isLoading
	if ok {
		s.isLoading = true
	}
	s.seqMu.Unlock()

	return ok
}

func (s *Sequencer) getMmsIdTime(name string) *MmsIdTime {
	s.mu.RLock()
	mmsIdTime, ok := s.mmsIdTime[name]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		mmsIdTime, ok = s.mmsIdTime[name]
		if !ok {
			mmsIdTime = NewMmsIdTime(&s.sc)
			s.mmsIdTime[name] = mmsIdTime
		}
		s.mu.Unlock()
	}
	return mmsIdTime
}

func (s *Sequencer) BatchUpdateCheckTime(p *IdTimePairs, incrRows bool) {
	if config.GetStoreConfig().UnorderedOnly {
		return
	}

	mmsIdTime := s.getMmsIdTime(p.Name)
	mmsIdTime.batchUpdateCheckTime(p, incrRows)
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

func (s *Sequencer) SeriesTotal() uint64 {
	return s.sc.Get()
}

func (s *Sequencer) DelMmsIdTime(name string) {
	s.mu.Lock()
	mmsIdTime, ok := s.mmsIdTime[name]
	delete(s.mmsIdTime, name)
	s.mu.Unlock()

	if !ok {
		return
	}

	s.sc.DecrN(uint64(mmsIdTime.getIdCount()))
}

func (s *Sequencer) GetMmsIdTime(name string) *MmsIdTime {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isFree || s.isLoading {
		return nil
	}

	return s.mmsIdTime[name]
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

func (p *IdTimePairs) Marshal(encTimes bool, dst []byte, ctx *encoding.CoderContext) []byte {
	var err error
	maxBlock := uint32(util.DefaultMaxRowsPerSegment4TsStore) * 2
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
		dst, err = encoding.EncodeUnsignedBlock(util.Uint64Slice2byte(p.Ids[startIdx:startIdx+count]), dst, ctx)
		if err != nil {
			panic(err)
		}
		size := len(dst) - pos - 4
		sb := numberenc.MarshalUint32Append(buf[:0], uint32(size))
		copy(dst[pos:pos+4], sb[:4])

		// encode row counts
		pos = len(dst)
		dst = append(dst, buf[:]...)
		dst, err = encoding.EncodeIntegerBlock(util.Int64Slice2byte(p.Rows[startIdx:startIdx+count]), dst, ctx)
		if err != nil {
			panic(err)
		}
		size = len(dst) - pos - 4
		sb = numberenc.MarshalUint32Append(buf[:0], uint32(size))
		copy(dst[pos:pos+4], sb[:4])

		if encTimes {
			// encode flush times
			pos = len(dst)
			dst = append(dst, buf[:]...)
			dst, err = encoding.EncodeIntegerBlock(util.Int64Slice2byte(p.Tms[startIdx:startIdx+count]), dst, ctx)
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

func (p *IdTimePairs) reserve(size int) {
	if cap(p.Ids) < size {
		p.Ids = make([]uint64, size)
		p.Tms = make([]int64, size)
		p.Rows = make([]int64, size)
	}
	p.Ids = p.Ids[:size]
	p.Tms = p.Tms[:size]
	p.Rows = p.Rows[:size]
}

func (p *IdTimePairs) UnmarshalHeader(src []byte) (uint32, error) {
	if int64(len(src)) < idTimeHeaderSize {
		return 0, fmt.Errorf("too small data for id time header, %d", len(src))
	}
	rows := int(numberenc.UnmarshalUint32(src))
	p.reserve(rows)
	blocks := numberenc.UnmarshalUint32(src[4:])
	return blocks, nil
}

func (p *IdTimePairs) UnmarshalBlocks(decTimes bool, src []byte, startIdx int, decoder *encoding.CoderContext) (int, int, int, error) {
	var err error
	var hasIncompleteBlock bool
	var startLen int
	size := 0
	blockNum := 0

	if len(src) < minIdTimeBlockSize {
		err = fmt.Errorf("too small data for id time, %d", len(src))
		log.Error(err.Error())
		return size, blockNum, startIdx, err
	}

	for len(src) > minIdTimeBlockSize {
		startLen = len(src)
		// decode count
		n := int(numberenc.UnmarshalUint32(src))
		src = src[4:]

		// decode series ids
		src, hasIncompleteBlock, err = p.decodeUnsignedBlock(src, p.Ids[startIdx:startIdx+n], decoder)
		if err != nil || hasIncompleteBlock {
			return size, blockNum, startIdx, err
		}

		// decode row counts
		src, hasIncompleteBlock, err = p.decodeIntegerBlock(src, p.Rows[startIdx:startIdx+n], decoder)
		if err != nil || hasIncompleteBlock {
			return size, blockNum, startIdx, err
		}

		if decTimes {
			// decode last flush times
			src, hasIncompleteBlock, err = p.decodeIntegerBlock(src, p.Tms[startIdx:startIdx+n], decoder)
			if err != nil || hasIncompleteBlock {
				return size, blockNum, startIdx, err
			}
		}
		size = size + (startLen - len(src))
		blockNum++
		startIdx += n
	}
	return size, blockNum, startIdx, nil
}

func (p *IdTimePairs) decodeIntegerBlock(src []byte, dst []int64, ctx *encoding.CoderContext) ([]byte, bool, error) {
	if len(src) < 4 {
		return nil, true, nil
	}
	size := int(numberenc.UnmarshalUint32(src))
	src = src[4:]
	if len(src) < size {
		return nil, true, nil
	}
	buf := util.Int64Slice2byte(dst)
	buf = buf[:0]
	_, err := encoding.DecodeIntegerBlock(src[:size], &buf, ctx)

	return src[size:], false, err
}

func (p *IdTimePairs) decodeUnsignedBlock(src []byte, dst []uint64, ctx *encoding.CoderContext) ([]byte, bool, error) {
	if len(src) < 4 {
		return nil, true, nil
	}
	size := int(numberenc.UnmarshalUint32(src))
	src = src[4:]
	if len(src) < size {
		return nil, true, nil
	}
	buf := util.Uint64Slice2byte(dst)
	buf = buf[:0]
	_, err := encoding.DecodeUnsignedBlock(src[:size], &buf, ctx)

	return src[size:], false, err
}

type SeriesCounter struct {
	total uint64
}

func (sc *SeriesCounter) Get() uint64 {
	return sc.total
}

func (sc *SeriesCounter) Incr() {
	atomic.AddUint64(&sc.total, 1)
}

func (sc *SeriesCounter) Reset() {
	sc.total = 0
}

func (sc *SeriesCounter) DecrN(n uint64) {
	if n == 0 {
		return
	}
	atomic.AddUint64(&sc.total, ^(n - 1))
}
