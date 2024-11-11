// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const (
	ChunkMetaCompressNone   = 0
	ChunkMetaCompressSnappy = 1
	ChunkMetaCompressLZ4    = 2
	ChunkMetaCompressEnd    = 3
)

var chunkMetaCompressMode = ChunkMetaCompressNone
var zeroPreAgg = make([]byte, 48)

func SetChunkMetaCompressMode(mode int) {
	if mode < ChunkMetaCompressNone || mode >= ChunkMetaCompressEnd {
		return
	}
	chunkMetaCompressMode = mode
}

func GetChunkMetaCompressMode() uint8 {
	return uint8(chunkMetaCompressMode)
}

// Segment offset/size/minT/maxT
type Segment struct {
	offset int64
	size   uint32
}

func (s Segment) bytes() int {
	return 8 + 4
}

func (s *Segment) reset() {
	s.offset = 0
	s.size = 0
}

func (s *Segment) setOffset(off int64) {
	s.offset = off
}

func (s *Segment) setSize(size uint32) {
	s.size = size
}

func (s *Segment) offsetSize() (int64, uint32) {
	return s.offset, s.size
}

func (s *Segment) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, s.offset)
	dst = numberenc.MarshalUint32Append(dst, s.size)
	return dst
}

func (s *Segment) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8+4 {
		err := fmt.Errorf("too small data for segment %v < %v", len(src), 12)
		log.Error(err.Error())
		return src, err
	}
	s.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	s.size = numberenc.UnmarshalUint32(src)
	return src[4:], nil
}

type SegmentRange [2]int64 // min/max

func (sr SegmentRange) bytes() int {
	return len(sr)
}

func (sr *SegmentRange) setMinTime(min int64) {
	sr[0] = min
}

func (sr *SegmentRange) setMaxTime(max int64) {
	sr[1] = max
}

func (sr *SegmentRange) minTime() int64 {
	return sr[0]
}

func (sr *SegmentRange) maxTime() int64 {
	return sr[1]
}

func (sr *SegmentRange) contains(tm int64) bool {
	return tm >= sr[0] && tm <= sr[1]
}

func (sr *SegmentRange) marshal(dst []byte) []byte {
	dst = numberenc.MarshalInt64Append(dst, sr[0])
	dst = numberenc.MarshalInt64Append(dst, sr[1])
	return dst
}

func (sr *SegmentRange) unmarshal(src []byte) ([]byte, error) {
	if len(src) < sr.bytes() {
		return nil, fmt.Errorf("too smaller data for segment ragne %v", len(src))
	}

	sr[0], src = numberenc.UnmarshalInt64(src), src[8:]
	sr[1], src = numberenc.UnmarshalInt64(src), src[8:]
	return src, nil
}

type ColumnMeta struct {
	name    string
	ty      byte
	preAgg  []byte
	entries []Segment
}

func (m *ColumnMeta) GetPreAgg() []byte {
	return m.preAgg
}

func (m *ColumnMeta) GetSegment(i int) (int64, uint32) {
	return m.entries[i].offset, m.entries[i].size
}

func (m *ColumnMeta) Equal(name string, ty int) bool {
	return m.name == name && int(m.ty) == ty
}

func (m *ColumnMeta) IsTime() bool {
	return m.name == record.TimeField
}

func (m *ColumnMeta) Name() string {
	return m.name
}

func (m *ColumnMeta) Type() uint8 {
	return m.ty
}

func (m *ColumnMeta) Clone() ColumnMeta {
	return ColumnMeta{
		name:    m.Name(),
		ty:      m.ty,
		entries: append([]Segment{}, m.entries...),
	}
}

func (m *ColumnMeta) reset() {
	if m == nil {
		return
	}
	m.name = ""
	m.ty = 0
	m.preAgg = m.preAgg[:0]
	for i := range m.entries {
		m.entries[i].reset()
	}
	m.entries = m.entries[:0]
}

func (m *ColumnMeta) RowCount(ref *record.Field, decs *ReadContext) (int64, error) {
	if decs.preAggBuilders == nil {
		decs.preAggBuilders = newPreAggBuilders()
	}

	cb := decs.preAggBuilders.aggBuilder(ref)
	_, err := cb.unmarshal(m.preAgg)
	if err != nil {
		log.Error("unmarshal pre-agg data fail", zap.Error(err))
		return 0, err
	}

	return cb.count(), err
}

func (m *ChunkMeta) GetSid() (sid uint64) {
	return m.sid
}

func (m *ChunkMeta) TimeMeta() *ColumnMeta {
	return &m.colMeta[len(m.colMeta)-1]
}

func (m ColumnMeta) bytes(segs int) int {
	return 2 + len(m.name) + // name length + name
		1 + // type
		2 + // count
		2 + len(m.preAgg) + //preAgg length + preAggData
		SegmentLen*segs //segment meta
}

func (m *ColumnMeta) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint16Append(dst, uint16(len(m.name)))
	dst = append(dst, m.name...)
	dst = append(dst, m.ty)

	if config.GetCommon().PreAggEnabled || m.name == record.TimeField {
		dst = numberenc.MarshalUint16Append(dst, uint16(len(m.preAgg)))
		dst = append(dst, m.preAgg...)
	} else {
		dst = numberenc.MarshalUint16Append(dst, 0)
	}

	for i := range m.entries {
		seg := m.entries[i]
		dst = seg.marshal(dst)
	}

	return dst
}

func (m *ColumnMeta) unmarshal(src []byte, segs int) ([]byte, error) {
	name, src, err := m.unmarshalName(src)
	if err != nil {
		return src, err
	}
	m.name = stringinterner.InternSafe(name)

	m.ty = src[0]
	src, err = m.unmarshalPreagg(src[1:])
	if err != nil {
		return src, err
	}

	return m.unmarshalEntries(src, segs)
}

func (m *ColumnMeta) unmarshalEntries(src []byte, segs int) ([]byte, error) {
	segSize := segs * SegmentLen
	if len(src) < segSize {
		err := fmt.Errorf("too smaller data for segment meta,  %v < %v(%v)", len(src), segSize, segs)
		log.Error(err.Error())
		return nil, err
	}
	if cap(m.entries) < segs {
		delta := segs - cap(m.entries)
		m.entries = m.entries[:cap(m.entries)]
		m.entries = append(m.entries, make([]Segment, delta)...)
	}
	m.entries = m.entries[:segs]
	for i := range m.entries {
		segData := src[:SegmentLen]
		_, err := m.entries[i].unmarshal(segData)
		if err != nil {
			return nil, err
		}
		src = src[SegmentLen:]
	}
	return src, nil
}

func (m *ColumnMeta) unmarshalPreagg(src []byte) ([]byte, error) {
	n, src := int(numberenc.UnmarshalUint16(src)), src[2:]
	if n == 0 {
		m.preAgg = append(m.preAgg[:0], zeroPreAgg...)
		return src, nil
	}

	if len(src) < n {
		err := fmt.Errorf("too smaller data for preagg,  %v < %v", len(src), n)
		log.Error(err.Error())
		return src, err
	}

	m.preAgg = append(m.preAgg[:0], src[:n]...)
	return src[n:], nil
}

func (m *ColumnMeta) unmarshalName(src []byte) (string, []byte, error) {
	var err error
	if len(src) < 2 {
		err = fmt.Errorf("too smaller data for column name length,  %v < 2 ", len(src))
		log.Error(err.Error())
		return "", nil, err
	}

	l := int(numberenc.UnmarshalUint16(src))
	src = src[2:]
	if len(src) < l+5 {
		err = fmt.Errorf("too smaller data for column name,  %v < %v", len(src), l+5)
		log.Error(err.Error())
		return "", nil, err
	}

	return util.Bytes2str(src[:l]), src[l:], nil
}

// Decode the column specified by the third parameter.
func (m *ColumnMeta) unmarshalSpecific(src []byte, segs int, column string) ([]byte, bool, error) {
	var name string
	var err error
	orig := src

	for {
		name, src, err = m.unmarshalName(src)
		if err != nil {
			return src, false, err
		}

		if name == column {
			break
		}

		if (name > column && column != record.TimeField) || name == record.TimeField {
			return orig, false, nil
		}

		preAggSize := int(numberenc.UnmarshalUint16(src[1:]))
		ofs := 1 + // type
			2 + preAggSize + // preAgg length + preAggData
			SegmentLen*segs // segment meta
		if len(src) < ofs {
			err = fmt.Errorf("too smaller data for column,  %d < %d ", len(src), ofs)
			log.Error(err.Error())
			return orig, false, err
		}
		src = src[ofs:]
		orig = src
	}

	m.name = stringinterner.InternSafe(name)
	m.ty = src[0]
	src, err = m.unmarshalPreagg(src[1:])
	if err != nil {
		return nil, false, err
	}

	src, err = m.unmarshalEntries(src, segs)

	return src, true, err
}

func (m *ColumnMeta) growEntry() {
	if len(m.entries) < cap(m.entries) {
		m.entries = m.entries[:len(m.entries)+1]
	} else {
		m.entries = append(m.entries, Segment{})
	}
}

type ChunkMeta struct {
	sid         uint64
	offset      int64
	size        uint32
	columnCount uint32
	segCount    uint32
	timeRange   []SegmentRange
	colMeta     []ColumnMeta
}

// NewChunkMeta only use for test,used by engine/record_plan_test.go
func NewChunkMeta(sid uint64, minT, maxT int64, count int) *ChunkMeta {
	b := newPreAggBuilders()
	b.intBuilder.addCount(10)
	var dst []byte
	cm := &ChunkMeta{
		sid:       sid,
		timeRange: []SegmentRange{SegmentRange{minT, maxT}},
		colMeta:   []ColumnMeta{ColumnMeta{preAgg: b.intBuilder.marshal(dst)}},
	}
	return cm
}

func (m *ChunkMeta) segmentCount() int {
	return int(m.segCount)
}

func (m *ChunkMeta) SegmentCount() int {
	return int(m.segCount)
}

func (m *ChunkMeta) timeMeta() *ColumnMeta {
	return &m.colMeta[len(m.colMeta)-1]
}

func (m *ChunkMeta) minTime() int64 {
	return m.timeRange[0].minTime()
}

func (m *ChunkMeta) maxTime() int64 {
	return m.timeRange[len(m.timeRange)-1].maxTime()
}

func (m *ChunkMeta) GetTimeRangeBy(index int) SegmentRange {
	return m.timeRange[index]
}

func (m *ChunkMeta) MinMaxTime() (min int64, max int64) {
	return m.minTime(), m.maxTime()
}

func (m *ChunkMeta) resize(columns int, segs int) {
	if cap(m.colMeta) < columns {
		delta := columns - cap(m.colMeta)
		m.colMeta = m.colMeta[:cap(m.colMeta)]
		m.colMeta = append(m.colMeta, make([]ColumnMeta, delta)...)
	}
	m.colMeta = m.colMeta[:columns]

	for i := range m.colMeta {
		cm := &m.colMeta[i]
		if cap(cm.entries) < segs {
			delta := segs - cap(cm.entries)
			cm.entries = cm.entries[:cap(cm.entries)]
			cm.entries = append(cm.entries, make([]Segment, delta)...)
		}
		cm.entries = cm.entries[:segs]
		cm.preAgg = cm.preAgg[:0]
	}

	if cap(m.timeRange) < segs {
		m.timeRange = m.timeRange[:cap(m.timeRange)]
		delta := segs - len(m.timeRange)
		m.timeRange = append(m.timeRange, make([]SegmentRange, delta)...)
	}
	m.timeRange = m.timeRange[:segs]
}

func (m *ChunkMeta) GetColMeta() []ColumnMeta {
	return m.colMeta
}

func (m *ChunkMeta) DelEmptyColMeta() {
	if len(m.colMeta) == 0 {
		return
	}

	emptyCount := 0
	for i := range m.colMeta {
		if len(m.colMeta[i].entries) == 0 {
			emptyCount++
		}
	}
	if emptyCount == 0 {
		return
	}

	newColMeta := make([]ColumnMeta, len(m.colMeta)-emptyCount)
	j := 0
	for i := range m.colMeta {
		if len(m.colMeta[i].entries) == 0 {
			continue
		}
		newColMeta[j] = m.colMeta[i]
		j++
	}
	m.colMeta = newColMeta
}

func (m *ChunkMeta) AllocColMeta(ref *record.Field) *ColumnMeta {
	size := len(m.colMeta)
	if cap(m.colMeta) == size {
		m.colMeta = append(m.colMeta, ColumnMeta{})
	}

	m.colMeta = m.colMeta[:size+1]
	colMeta := &m.colMeta[size]

	colMeta.reset()
	colMeta.name = ref.Name
	colMeta.ty = byte(ref.Type)
	return colMeta
}

func (m *ChunkMeta) Clone() *ChunkMeta {
	newMeta := &ChunkMeta{
		sid:         m.sid,
		offset:      m.offset,
		size:        m.size,
		columnCount: m.columnCount,
		segCount:    m.segCount,
		colMeta:     make([]ColumnMeta, 0, len(m.colMeta)),
	}

	for i := 0; i < len(m.colMeta); i++ {
		newMeta.colMeta = append(newMeta.colMeta, m.colMeta[i].Clone())
	}

	return newMeta
}

func (m *ChunkMeta) reset() {
	m.sid = 0
	m.offset = 0
	m.size = 0
	m.segCount = 0
	m.columnCount = 0

	for i := range m.colMeta {
		m.colMeta[i].reset()
	}
	m.colMeta = m.colMeta[:0]
}

func (m *ChunkMeta) minBytes() int {
	// 8-byte attribute: m.sid, m.offset
	// 8-byte attribute: m.size, m.columnCount, m.segCount
	return 8*2 + 4*3 +
		MinMaxTimeLen + // least 1 segment
		ColumnMetaLenMin*2 // least two columns
}

func (m *ChunkMeta) Size() int {
	n := 8*3 + 2
	for i := range m.colMeta {
		n += m.colMeta[i].bytes(int(m.segCount))
	}

	return n
}

func (m *ChunkMeta) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, m.sid)
	dst = numberenc.MarshalInt64Append(dst, m.offset)
	dst = numberenc.MarshalUint32Append(dst, m.size)
	dst = numberenc.MarshalUint32Append(dst, m.columnCount)
	dst = numberenc.MarshalUint32Append(dst, m.segCount)
	for i := range m.timeRange {
		tr := &m.timeRange[i]
		dst = tr.marshal(dst)
	}
	for i := range m.colMeta {
		dst = m.colMeta[i].marshal(dst)
	}

	return dst
}

func (m *ChunkMeta) validation() {
	if m.sid == 0 {
		panic("series is is 0")
	}

	if int(m.segCount) != len(m.timeRange) {
		panic("length of m.timeRange is not equal to m.segCount")
	}

	if int(m.columnCount) != len(m.colMeta) {
		panic("length of m.colMeta is not equal to m.columnCount")
	}

	for i := range m.colMeta {
		item := &m.colMeta[i]

		if len(item.entries) != int(m.segCount) {
			panic(fmt.Sprintf("length of m.colMeta[%d].entries(%d) is not equal to m.segCount(%d)",
				i, len(item.entries), m.segCount))
		}
	}
}

func (m *ChunkMeta) unmarshal(src []byte) ([]byte, error) {
	var err error
	src, err = m.unmarshalBaseAttr(src, 0)
	if err != nil {
		return nil, err
	}

	for i := range m.colMeta {
		src, err = m.colMeta[i].unmarshal(src, int(m.segCount))
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
	}

	return src, nil
}

func (m *ChunkMeta) UnmarshalWithColumns(src []byte, columns []string) ([]byte, error) {
	// All columns are time
	// For example: SELECT count(time) FROM xxx
	if len(columns) == 0 || columns[0] == record.TimeField {
		return m.unmarshal(src)
	}

	var err error
	src, err = m.unmarshalBaseAttr(src, len(columns))
	if err != nil {
		return nil, err
	}

	ok := false
	j := 0
	for i := range columns {
		if i > 0 && columns[i] == columns[i-1] {
			continue
		}

		src, ok, err = m.colMeta[j].unmarshalSpecific(src, int(m.segCount), columns[i])
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		if ok {
			j++
		}
	}
	m.colMeta = m.colMeta[:j]
	m.columnCount = uint32(j)
	return src, nil
}

func (m *ChunkMeta) unmarshalBaseAttr(src []byte, columnCount int) ([]byte, error) {
	var err error
	if len(src) < ChunkMetaMinLen {
		err = fmt.Errorf("to smaller data for unmarshal %v < 24", len(src))
		log.Error(err.Error())
		return nil, err
	}

	m.sid, src = numberenc.UnmarshalUint64(src), src[8:]
	m.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	m.size, src = numberenc.UnmarshalUint32(src), src[4:]
	m.columnCount, src = numberenc.UnmarshalUint32(src), src[4:]
	m.segCount, src = numberenc.UnmarshalUint32(src), src[4:]

	if columnCount == 0 {
		columnCount = int(m.columnCount)
	}

	m.resize(columnCount, int(m.segCount))
	for i := range m.timeRange {
		tr := &m.timeRange[i]
		src, err = tr.unmarshal(src)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
	}

	return src, nil
}

func (m *ChunkMeta) columnIndex(ref *record.Field) int {
	for i := range m.colMeta {
		if m.colMeta[i].Equal(ref.Name, ref.Type) {
			return i
		}
	}
	return -1
}

func (m *ChunkMeta) columnIndexFastPath(ref *record.Field, itrFieldIndex int) int {
	for i := itrFieldIndex; i < len(m.colMeta); i++ {
		if m.colMeta[i].Equal(ref.Name, ref.Type) {
			return i
		}
	}
	return -1
}

func (m *ChunkMeta) allRowsInRange(tr util.TimeRange) bool {
	min, max := m.MinMaxTime()
	return tr.Min <= min && tr.Max >= max
}

func (m *ChunkMeta) Rows(ab PreAggBuilder) int {
	cm := &m.colMeta[len(m.colMeta)-1]
	ab.reset()
	_, err := ab.unmarshal(cm.preAgg)
	if err != nil {
		panic(err)
	}
	return int(ab.count())
}

func (m *ChunkMeta) growTimeRangeEntry() {
	if len(m.timeRange) < cap(m.timeRange) {
		m.timeRange = m.timeRange[:len(m.timeRange)+1]
	} else {
		m.timeRange = append(m.timeRange, SegmentRange{})
	}
}

func (m *ChunkMeta) Len() int {
	return len(m.colMeta)
}

func (m *ChunkMeta) Less(i, j int) bool {
	return m.colMeta[i].name < m.colMeta[j].name
}

func (m *ChunkMeta) Swap(i, j int) {
	m.colMeta[i], m.colMeta[j] = m.colMeta[j], m.colMeta[i]
}

// MetaIndex If you change the order of the elements in the structure,
// remember to modify marshal() and unmarshal() as well.
type MetaIndex struct {
	id      uint64
	minTime int64
	maxTime int64
	offset  int64  // chunkmeta block offset
	count   uint32 // number of ChunkMeta
	size    uint32 // chunkmeta block size
}

func (m *MetaIndex) GetID() uint64 {
	return m.id
}

func (m *MetaIndex) GetOffset() int64 {
	return m.offset
}

func (m *MetaIndex) GetSize() uint32 {
	return m.size
}

func (m *MetaIndex) marshal(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, m.id)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = numberenc.MarshalInt64Append(dst, m.offset)
	dst = numberenc.MarshalUint32Append(dst, m.count)
	dst = numberenc.MarshalUint32Append(dst, m.size)

	return dst
}

func (m *MetaIndex) marshalDetached(dst []byte) []byte {
	dst = numberenc.MarshalUint64Append(dst, m.id)
	dst = numberenc.MarshalInt64Append(dst, m.minTime)
	dst = numberenc.MarshalInt64Append(dst, m.maxTime)
	dst = numberenc.MarshalInt64Append(dst, m.offset)
	dst = numberenc.MarshalUint32Append(dst, m.size)
	return dst
}

func (m *MetaIndex) unmarshal(src []byte) ([]byte, error) {
	if len(src) < MetaIndexLen {
		return nil, fmt.Errorf("too small data (%v) for MetaIndex", len(src))
	}

	m.id, src = numberenc.UnmarshalUint64(src), src[8:]
	m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	m.count, src = numberenc.UnmarshalUint32(src), src[4:]
	m.size, src = numberenc.UnmarshalUint32(src), src[4:]

	return src, nil
}

func (m *MetaIndex) unmarshalDetached(src []byte) ([]byte, error) {
	if len(src) < DetachedMetaIndexLen {
		return nil, fmt.Errorf("too small data (%v) for MetaIndex", len(src))
	}

	m.id, src = numberenc.UnmarshalUint64(src), src[8:]
	m.minTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.maxTime, src = numberenc.UnmarshalInt64(src), src[8:]
	m.offset, src = numberenc.UnmarshalInt64(src), src[8:]
	m.size, src = numberenc.UnmarshalUint32(src), src[4:]
	return src, nil
}

func (m *MetaIndex) reset() {
	m.id = 0
	m.minTime = math.MaxInt64
	m.maxTime = math.MinInt64
	m.offset = 0
	m.size = 0
	m.count = 0
}

func (m *MetaIndex) IsExist(tr util.TimeRange) bool {
	return !(m.minTime > tr.Max || m.maxTime < tr.Min)
}
