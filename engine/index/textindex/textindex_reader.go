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

package textindex

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding/lz4"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

const (
	ArrayContainerMaxSize = 4096
	BitSetContainerSize   = (1 << 16) / 64
)

var _ = sparseindex.RegistrySKFileReaderCreator(uint32(index.Text), &TextIndexReaderCreator{})

type TextIndexReaderCreator struct {
}

func (index *TextIndexReaderCreator) CreateSKFileReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (sparseindex.SKFileReader, error) {
	return NewTextIndexReader(rpnExpr, schema, option, isCache)
}

// read the part headers
func GetPartHeaders(path, file, field string, obsOpts *obs.ObsOptions) ([]*PartHeader, error) {
	// create reader
	fd, err := fileops.OpenObsFile(path, colstore.AppendSecondaryIndexSuffix(file, field, index.Text, colstore.TextIndexPart), obsOpts, true)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	defer dr.Close()
	fileSize, err := dr.Size()
	if err != nil {
		return nil, err
	}

	phSize := GetPartHeaderSize()
	if fileSize%int64(phSize) != 0 {
		return nil, fmt.Errorf("the size of PartHeader is not right: [%d,%d]", fileSize, phSize)
	}
	// read
	partBuf := make([]byte, 0, fileSize)
	partBuf, err = dr.ReadAt(0, uint32(fileSize), &partBuf, fileops.IO_PRIORITY_HIGH)
	if err != nil {
		return nil, err
	}
	phs := make([]*PartHeader, 0, fileSize/int64(phSize))
	for i := 0; i < cap(phs); i++ {
		ph := NewPartHeader()
		partBuf, err = ph.Unmarshal(partBuf)
		if err != nil {
			return nil, err
		}
		phs = append(phs, ph)
	}
	return phs, nil
}

// read the BlockHeaders
type BlockHeadReader struct {
	path    string
	file    string
	obsOpts *obs.ObsOptions
}

func NewBlockReader(path, file string, obsOpts *obs.ObsOptions) *BlockHeadReader {
	return &BlockHeadReader{
		path:    path,
		file:    file,
		obsOpts: obsOpts,
	}
}

func (r *BlockHeadReader) ReadBlockHeaders(partHeader *PartHeader) ([]BlockHeader, error) {
	// create reader
	fd, err := fileops.OpenObsFile(r.path, r.file, r.obsOpts, true)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	defer dr.Close()
	blockBuf := make([]byte, 0, partHeader.BlockHeaderSize)
	blockBuf, err = dr.ReadAt(int64(partHeader.BlockHeaderOffset), partHeader.BlockHeaderSize, &blockBuf, fileops.IO_PRIORITY_HIGH)
	if err != nil {
		return nil, err
	}
	// parse BlockHeaders
	bhs := make([]BlockHeader, partHeader.BlockHeaderCnt)
	for i := 0; i < len(bhs); i++ {
		blockBuf, err = bhs[i].Unmarshal(blockBuf)
		if err != nil {
			return nil, err
		}
	}
	return bhs, nil
}

func popcntAndSlice(s, m []uint64) int {
	cnt := 0
	for i := range s {
		cnt += bits.OnesCount64(s[i] & m[i])
	}
	return cnt
}

const (
	BitsetContainerType uint8 = 1
	ArrayContainerType  uint8 = 2
	RunContainerType    uint8 = 3
)

type ContainerHead struct {
	ContainerType uint8
	ContainerKey  uint16
	Cardinality   uint32
}

func (h *ContainerHead) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < 5 {
		return nil, fmt.Errorf("cannot unmarshal ContainerHead from %d bytes; need at least 5 bytes", len(src))
	}
	h.ContainerType = src[0]
	h.ContainerKey = encoding.UnmarshalUint16(src[1:])
	h.Cardinality = uint32(encoding.UnmarshalUint16(src[3:])) + 1
	return src[5:], nil
}

type BitsetContainer struct {
	ContainerHead
	Bitset []uint64 // len is 1024
}

func NewBitsetContainer(head *ContainerHead) *BitsetContainer {
	return &BitsetContainer{
		ContainerHead: *head,
		Bitset:        make([]uint64, BitSetContainerSize),
	}
}

func (c *BitsetContainer) Key() uint16 {
	return c.ContainerKey
}

func (c *BitsetContainer) SetBitsetRange(start int, end int) {
	if start >= end {
		return
	}
	firstword := start >> 6
	endword := (end - 1) >> 6
	if firstword == endword {
		// 63=0b111111, used to take a value of 6 bits lower
		c.Bitset[firstword] |= (^uint64(0) << uint(start&63)) & (^uint64(0) >> (uint(-end) & 63))
		return
	}
	c.Bitset[firstword] |= ^uint64(0) << uint(start&63)
	for i := firstword + 1; i < endword; i++ {
		c.Bitset[i] = ^uint64(0)
	}
	c.Bitset[endword] |= ^uint64(0) >> (uint(-end) & 63)
}

func (c *BitsetContainer) bitValue(i uint16) uint64 {
	x := uint(i)
	w := c.Bitset[x>>6]
	return (w >> (x & 63)) & 1
}

func (c *BitsetContainer) andBitset(b *BitsetContainer) Container {
	if c.ContainerKey != b.ContainerKey {
		return nil
	}
	newCard := popcntAndSlice(c.Bitset, b.Bitset)
	if newCard > ArrayContainerMaxSize {
		res := NewBitsetContainer(&b.ContainerHead)
		for i := 0; i < BitSetContainerSize; i++ {
			res.Bitset[i] = b.Bitset[i] & c.Bitset[i]
		}
		res.Cardinality = uint32(newCard)
		return res
	}
	res := NewArrayContainerSize(&b.ContainerHead, newCard)
	res.fillByBitsetAnd(b.Bitset, c.Bitset)
	return res
}

func (c *BitsetContainer) andArray(b *ArrayContainer) Container {
	if c.ContainerKey != b.ContainerKey {
		return nil
	}
	res := NewArrayContainerSize(&b.ContainerHead, len(b.Rows))
	for i := 0; i < len(b.Rows); i++ {
		v := b.Rows[i]
		if c.bitValue(v) == 0 {
			continue
		}
		res.Rows = append(res.Rows, v)
	}
	res.Cardinality = uint32(len(res.Rows))
	return res
}

func (c *BitsetContainer) andRun(b *RunContainer) Container {
	bm := b.toBitsetContainer()
	return c.andBitset(bm)
}

func (c *BitsetContainer) And(a Container) Container {
	switch x := a.(type) {
	case *BitsetContainer:
		return c.andBitset(x)
	case *ArrayContainer:
		return c.andArray(x)
	case *RunContainer:
		return c.andRun(x)
	default:
		panic("unsupported container type")
	}
}

func (c *BitsetContainer) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < BitSetContainerSize*8 {
		return nil, fmt.Errorf("cannot unmarshal BitsetContainer from %d bytes; need at least %d bytes", len(src), BitSetContainerSize*8)
	}
	c.Bitset = c.Bitset[:0]
	for i := 0; i < BitSetContainerSize; i++ {
		c.Bitset = append(c.Bitset, encoding.UnmarshalUint64(src))
		src = src[8:]
	}
	return src, nil
}

func (c *BitsetContainer) Serialize() []uint32 {
	rows := make([]uint32, 0, c.Cardinality)
	base := uint32(c.ContainerKey) << 16
	for k := 0; k < len(c.Bitset); k++ {
		bitset := c.Bitset[k]
		for bitset != 0 {
			t := bitset & -bitset
			rows = append(rows, base+uint32(bits.OnesCount64(t-1)))
			bitset ^= t
		}
		base += 64
	}
	return rows
}

type ArrayContainer struct {
	ContainerHead
	Rows []uint16
}

func NewArrayContainer(head *ContainerHead) *ArrayContainer {
	return &ArrayContainer{
		ContainerHead: *head,
	}
}

func NewArrayContainerSize(head *ContainerHead, size int) *ArrayContainer {
	return &ArrayContainer{
		ContainerHead: *head,
		Rows:          make([]uint16, 0, size),
	}
}

func (c *ArrayContainer) Key() uint16 {
	return c.ContainerKey
}

func (c *ArrayContainer) fillByBitsetAnd(bitmap1, bitmap2 []uint64) {
	if len(bitmap1) != len(bitmap2) {
		panic("bitset lengths don't match")
	}
	c.Rows = c.Rows[:0]
	for k := range bitmap1 {
		bitset := bitmap1[k] & bitmap2[k]
		for bitset != 0 {
			t := bitset & -bitset
			row := uint16((k*64 + bits.OnesCount64(t-1)))
			c.Rows = append(c.Rows, row)
			bitset ^= t
		}
	}
	c.Cardinality = uint32(len(c.Rows))
}

func (c *ArrayContainer) andArray(b *ArrayContainer) Container {
	if c.ContainerKey != b.ContainerKey {
		return nil
	}
	res := NewArrayContainerSize(&b.ContainerHead, util.Min(len(b.Rows), len(c.Rows)))
	var i, j int
	for (i < len(b.Rows)) && (j < len(c.Rows)) {
		if b.Rows[i] < c.Rows[j] {
			i++
		} else if b.Rows[i] > c.Rows[j] {
			j++
		} else { // eg. b.Rows[i] == c.Rows[j]
			res.Rows = append(res.Rows, b.Rows[i])
			i++
			j++
		}
	}
	res.Cardinality = uint32(len(res.Rows))
	if len(res.Rows) == 0 {
		return nil
	}
	return res
}

func (c *ArrayContainer) andRun(b *RunContainer) Container {
	if c.ContainerKey != b.ContainerKey {
		return nil
	}
	res := NewArrayContainerSize(&c.ContainerHead, len(c.Rows))
	i := uint16(0)
	j := int(0)
	for (i < b.Nruns) && (j < len(c.Rows)) {
		var k uint16
		for (k <= b.Runs[i].Len) && (j < len(c.Rows)) {
			val := b.Runs[i].Value + k
			if val < c.Rows[j] {
				k++
			} else if val > c.Rows[j] {
				j++
			} else { // eg. val == c.Rows[j]
				res.Rows = append(res.Rows, val)
				k++
				j++
			}
		}
		if k > b.Runs[i].Len {
			i++
		}
	}
	res.Cardinality = uint32(len(res.Rows))
	return res
}

func (c *ArrayContainer) And(a Container) Container {
	switch x := a.(type) {
	case *BitsetContainer:
		return x.andArray(c)
	case *ArrayContainer:
		return c.andArray(x)
	case *RunContainer:
		return c.andRun(x)
	default:
		panic("unsupported container type")
	}
}

func (c *ArrayContainer) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < int(c.Cardinality)*2 {
		return nil, fmt.Errorf("cannot unmarshal ArrayContainer from %d bytes; need at least %d bytes", len(src), c.Cardinality*2)
	}
	c.Rows = make([]uint16, 0, c.Cardinality)
	for i := 0; i < int(c.Cardinality); i++ {
		c.Rows = append(c.Rows, encoding.UnmarshalUint16(src))
		src = src[2:]
	}
	return src, nil
}

func (c *ArrayContainer) Serialize() []uint32 {
	rows := make([]uint32, len(c.Rows))
	base := uint32(c.ContainerKey) << 16
	for i := 0; i < len(c.Rows); i++ {
		rows[i] = base + uint32(c.Rows[i])
	}
	return rows
}

type RunPair struct {
	Value uint16
	Len   uint16
}

func haveOverlap(a, b RunPair) bool {
	if a.Value+a.Len < b.Value {
		return false
	}
	return b.Value+b.Len > a.Value
}

// direct = 1 : step b
// direct = -1 : step a
// direct = 0 : step a and b
func TravelDirection(a, b RunPair) int {
	if a.Value+a.Len > b.Value+b.Len {
		return 1
	} else if a.Value+a.Len < b.Value+b.Len {
		return -1
	} else {
		return 0
	}
}

func IntersectRunPair(a, b RunPair) (RunPair, int, bool) {
	var res RunPair
	direct := TravelDirection(a, b)
	if !haveOverlap(a, b) {
		return res, direct, false
	}
	if a.Value > b.Value {
		res.Value = a.Value
	} else {
		res.Value = b.Value
	}
	if a.Value+a.Len < b.Value+b.Len {
		res.Len = a.Value + a.Len - res.Value
	} else {
		res.Len = b.Value + b.Len - res.Value
	}
	return res, direct, true
}

func (p *RunPair) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("cannot unmarshal RunPair from %d bytes; need at least 4 bytes", len(src))
	}
	p.Value = encoding.UnmarshalUint16(src)
	src = src[2:]
	p.Len = encoding.UnmarshalUint16(src)
	src = src[2:]
	return src, nil
}

type RunContainer struct {
	ContainerHead
	Nruns uint16
	Runs  []RunPair
}

func NewRunContainer(head *ContainerHead) *RunContainer {
	return &RunContainer{
		ContainerHead: *head,
	}
}

func NewRunContainerSize(head *ContainerHead, size int) *RunContainer {
	return &RunContainer{
		ContainerHead: *head,
		Runs:          make([]RunPair, 0, size),
	}
}

func (c *RunContainer) Key() uint16 {
	return c.ContainerKey
}

func (c *RunContainer) toBitsetContainer() *BitsetContainer {
	res := NewBitsetContainer(&c.ContainerHead)
	res.Cardinality = c.Cardinality
	res.Bitset = make([]uint64, BitSetContainerSize)
	for i := 0; i < len(c.Runs); i++ {
		res.SetBitsetRange(int(c.Runs[i].Value), int(c.Runs[i].Value+c.Runs[i].Len)+1)
	}
	return res
}

func (c *RunContainer) andRun(b *RunContainer) Container {
	if b.ContainerKey != c.ContainerKey {
		return nil
	}
	res := NewRunContainerSize(&b.ContainerHead, util.Min(len(b.Runs), len(c.Runs)))
	var i, j uint16
	for (i < c.Nruns) && (j < b.Nruns) {
		resPair, direct, overlap := IntersectRunPair(c.Runs[i], b.Runs[j])
		if overlap {
			res.Runs = append(res.Runs, resPair)
		}
		if direct > 0 {
			j++
		} else if direct < 0 {
			i++
		} else {
			i++
			j++
		}
	}
	res.Nruns = uint16(len(res.Runs))
	return res
}

func (c *RunContainer) And(a Container) Container {
	switch x := a.(type) {
	case *BitsetContainer:
		return x.andRun(c)
	case *ArrayContainer:
		return x.andRun(c)
	case *RunContainer:
		return c.andRun(x)
	default:
		panic("unsupported container type")
	}
}

func (c *RunContainer) Unmarshal(src []byte) ([]byte, error) {
	c.Nruns = encoding.UnmarshalUint16(src)
	src = src[2:]
	if len(src) < int(c.Nruns)*4 {
		return nil, fmt.Errorf("cannot unmarshal RunContainer from %d bytes; need at least %d bytes", len(src), int(c.Nruns)*4)
	}
	var err error
	var pair RunPair
	c.Runs = make([]RunPair, 0, c.Nruns)
	for i := 0; i < int(c.Nruns); i++ {
		src, err = pair.Unmarshal(src)
		if err != nil {
			return nil, err
		}
		c.Runs = append(c.Runs, pair)
	}
	return src, nil
}

func (c *RunContainer) Serialize() []uint32 {
	rows := make([]uint32, 0, c.Cardinality)
	base := uint32(c.ContainerKey) << 16
	for i := 0; i < len(c.Runs); i++ {
		for j := 0; j <= int(c.Runs[i].Len); j++ {
			rows = append(rows, base+uint32(c.Runs[i].Value)+uint32(j))
		}
	}
	return rows
}

type Container interface {
	Key() uint16
	And(a Container) Container
	Unmarshal(src []byte) ([]byte, error)
	Serialize() []uint32
}

type GroupContainers []Container

func (cntrs GroupContainers) TransToPostingContainer(header *PartHeader) *PostingContainer {
	segRangeLen := 1
	for i := 1; i < len(header.SegmentRange); i++ {
		if header.SegmentRange[i] <= header.SegmentRange[i-1] {
			break
		} else {
			segRangeLen = i + 1
		}
	}
	postingCntr := NewPostingContainer(segRangeLen)
	k := 0
	for i := 0; i < len(cntrs); i++ {
		j := 0
		tmpRows := cntrs[i].Serialize()
		for (j < len(tmpRows)) && (k < segRangeLen) {
			if (k < segRangeLen-1) && tmpRows[j] >= header.SegmentRange[k+1] {
				k++
				continue
			}
			postingCntr.SegCntrs[k].RowIds = append(postingCntr.SegCntrs[k].RowIds, tmpRows[j]-header.SegmentRange[k])
			j++
		}
	}
	return postingCntr
}

func IntersectContainers(cntrs0, cntrs1 []Container) []Container {
	res := make([]Container, 0)
	var i, j int
	for (i < len(cntrs0)) && (j < len(cntrs1)) {
		if cntrs0[i].Key() > cntrs1[j].Key() {
			j++
			continue
		} else if cntrs0[i].Key() < cntrs1[j].Key() {
			i++
			continue
		}
		curCntr := cntrs0[i].And(cntrs1[j])
		if curCntr != nil {
			res = append(res, curCntr)
		}
		i++
		j++
	}
	return res
}

func UnmarshalContainer(src []byte) ([]Container, error) {
	// Unmarshal containerCount
	containerCount := encoding.UnmarshalUint16(src)
	src = src[6:] // ignore cardinality count

	var err error
	var head ContainerHead
	constainers := make([]Container, 0, containerCount)
	for i := 0; i < int(containerCount); i++ {
		src, err = head.Unmarshal(src)
		if err != nil {
			return nil, err
		}
		var container Container
		if head.ContainerType == RunContainerType {
			container = NewRunContainer(&head)
			src, err = container.Unmarshal(src)
			if err != nil {
				return nil, err
			}
		} else if head.ContainerType == ArrayContainerType {
			container = NewArrayContainer(&head)
			src, err = container.Unmarshal(src)
			if err != nil {
				return nil, err
			}
		} else if head.ContainerType == BitsetContainerType {
			container = NewBitsetContainer(&head)
			src, err = container.Unmarshal(src)
			if err != nil {
				return nil, err
			}
		} else {
			panic(fmt.Errorf("wrong container type:%d", head.ContainerType))
		}
		constainers = append(constainers, container)
	}
	return constainers, nil
}

type BlockReadData struct {
	// packKeyData + packPostData
	packData     []byte
	keysPackSize uint32 // boundary point between key and off
	// keys info
	unpackKeyData  []byte
	keysUnpackSize uint32
	keysSize       uint32   // used to split keys and keys-offsets
	keysOffs       []uint32 // parse from unpackKeyData[keysSize:]
	// post info
	unpackPostData []byte
	postUnpackSize uint32
	postSize       uint32   // used to split posts and posts-offsets
	postsOffs      []uint32 // parse from unpackPostData[postSize:]
}

func NewBlockReadData() *BlockReadData {
	return &BlockReadData{}
}

func (bd *BlockReadData) Init(bh *BlockHeader) {
	bd.keysPackSize = bh.KeysPackSize
	bd.keysUnpackSize = bh.KeysUnpackSize
	bd.keysSize = bh.KeysSize
	bd.postUnpackSize = bh.PostUnpackSize
	bd.postSize = bh.PostSize
}

func (bd *BlockReadData) ResizePackData(size int) {
	if ss := size - cap(bd.packData); ss > 0 {
		bd.packData = append(bd.packData, make([]byte, ss)...)
	}
	bd.packData = bd.packData[:0]
}

func (bd *BlockReadData) ResizeUnpackKeyData(size int) {
	if ss := size - cap(bd.unpackKeyData); ss > 0 {
		bd.unpackKeyData = append(bd.unpackKeyData, make([]byte, ss)...)
	}
	bd.unpackKeyData = bd.unpackKeyData[:cap(bd.unpackKeyData)]
}

func (bd *BlockReadData) DecompressKeyData() error {
	bd.ResizeUnpackKeyData(int(bd.keysUnpackSize))
	unpackLen, err := lz4.DecompressSafe(bd.packData[:bd.keysPackSize], bd.unpackKeyData)
	if err != nil {
		return err
	}
	bd.unpackKeyData = bd.unpackKeyData[:unpackLen]
	bd.keysOffs = UnarshalUint32Slice(bd.unpackKeyData[bd.keysSize:])
	return nil
}

func (bd *BlockReadData) ResizeUnackPostData(size int) {
	if ss := size - cap(bd.unpackPostData); ss > 0 {
		bd.unpackPostData = append(bd.unpackPostData, make([]byte, ss)...)
	}
	bd.unpackPostData = bd.unpackPostData[:cap(bd.unpackPostData)]
}

func (bd *BlockReadData) DecompressPostData() error {
	bd.ResizeUnackPostData(int(bd.postUnpackSize))
	unpackLen, err := lz4.DecompressSafe(bd.packData[bd.keysPackSize:], bd.unpackPostData)
	if err != nil {
		return err
	}
	bd.unpackPostData = bd.unpackPostData[:unpackLen]
	bd.postsOffs = UnarshalUint32Slice(bd.unpackPostData[bd.postSize:])
	return nil
}

func (bd *BlockReadData) Query(queryStr string) ([]Container, error) {
	if len(bd.unpackKeyData) == 0 {
		if err := bd.DecompressKeyData(); err != nil {
			return nil, err
		}
	}

	// find the first item which greater than or equal to queryStr
	idx := sort.Search(len(bd.keysOffs), func(i int) bool {
		start, end := DecodeOffs(bd.keysOffs, i)
		return queryStr <= string(bd.unpackKeyData[start:end])
	})

	idxs := make([]int, 0)
	for i := idx; i < len(bd.keysOffs); i++ {
		start, end := DecodeOffs(bd.keysOffs, i)
		if queryStr != string(bd.unpackKeyData[start:end]) {
			break
		}
		idxs = append(idxs, i)
	}

	if len(idxs) == 0 {
		return nil, nil
	}

	if len(bd.unpackPostData) == 0 {
		if err := bd.DecompressPostData(); err != nil {
			return nil, err
		}
	}

	containers := make([]Container, 0)
	for i := 0; i < len(idxs); i++ {
		start, end := DecodeOffs(bd.postsOffs, idxs[i])
		tmpContainers, err := UnmarshalContainer(bd.unpackPostData[start:end])
		if err != nil {
			return nil, err
		}
		containers = append(containers, tmpContainers...)
	}

	return containers, nil
}

type BlockDataReader struct {
	path    string
	file    string
	obsOpts *obs.ObsOptions
	bdh     fileops.BasicFileReader
}

func NewBlockDataReader(path, file string, obsOpts *obs.ObsOptions) *BlockDataReader {
	return &BlockDataReader{
		path:    path,
		file:    file,
		obsOpts: obsOpts,
	}
}

func (r *BlockDataReader) Open() error {
	fd, err := fileops.OpenObsFile(r.path, r.file, r.obsOpts, true)
	if err != nil {
		return err
	}
	r.bdh = fileops.NewFileReader(fd, nil)
	return nil
}

func (r *BlockDataReader) Close() error {
	return r.bdh.Close()
}

func (r *BlockDataReader) GetBlockReadData(bh *BlockHeader) (*BlockReadData, error) {
	packSize := bh.KeysPackSize + bh.PostPackSize
	blockReadData := NewBlockReadData()
	blockReadData.Init(bh)
	blockReadData.ResizePackData(int(packSize))

	var err error
	blockReadData.packData, err = r.bdh.ReadAt(int64(bh.KeysOffset), packSize, &(blockReadData.packData), fileops.IO_PRIORITY_HIGH)
	if err != nil {
		return nil, err
	}
	return blockReadData, nil
}

type SegRowContainer struct {
	RowIds []uint32
}

type PostingContainer struct {
	SegCntrs []SegRowContainer // idx: segmentId-startSegmentIdOfPart  -> SegRowContainer
}

func NewPostingContainer(size int) *PostingContainer {
	return &PostingContainer{
		SegCntrs: make([]SegRowContainer, size),
	}
}

type PartContainer struct {
	bhs        []BlockHeader
	blockCache map[uint64]*BlockReadData    // map-key is BlockHeader.KeysOffset
	containers map[string]*PostingContainer // token -> PostingList
}

func NewPartContainers() *PartContainer {
	return &PartContainer{
		blockCache: make(map[uint64]*BlockReadData),
		containers: make(map[string]*PostingContainer, 0),
	}
}

func (p *PartContainer) FilterByQuery(queryStr string) []BlockHeader {
	// find the first item which greater than or equal to queryStr
	idx := sort.Search(len(p.bhs), func(i int) bool {
		return queryStr <= string(p.bhs[i].LastItem)
	})

	start := idx
	end := idx
	for end < len(p.bhs) {
		if queryStr < string(p.bhs[end].FirstItem) ||
			queryStr > string(p.bhs[end].LastItem) {
			break
		}
		end++
	}
	return p.bhs[start:end]
}

type TextIndexFilterReader struct {
	path      string
	file      string
	field     string
	obsOpts   *obs.ObsOptions
	opt       *influxql.IndexOption
	tokenizer *tokenizer.StandardTokenizer

	partHeaders     []*PartHeader
	postingCache    map[int]*PartContainer // partId -> PartContainer
	blockHeadReader *BlockHeadReader
	blockDataReader *BlockDataReader
}

func NewTextIndexFilterReader(path, file, field string, obsOpts *obs.ObsOptions, opt *influxql.IndexOption) (*TextIndexFilterReader, error) {
	if opt == nil {
		return nil, fmt.Errorf("invalid IndexOption")
	}
	// part headers
	phs, err := GetPartHeaders(path, file, field, obsOpts)
	if err != nil {
		return nil, err
	}
	// block header
	blockHeadReader := NewBlockReader(path, colstore.AppendSecondaryIndexSuffix(file, field, index.Text, colstore.TextIndexHead), obsOpts)
	// data reader
	blockDataReader := NewBlockDataReader(path, colstore.AppendSecondaryIndexSuffix(file, field, index.Text, colstore.TextIndexData), obsOpts)
	if err := blockDataReader.Open(); err != nil {
		return nil, err
	}
	tokenizer := tokenizer.NewStandardTokenizer(opt.Tokens)
	r := &TextIndexFilterReader{
		path:            path,
		file:            file,
		field:           field,
		obsOpts:         obsOpts,
		opt:             opt,
		tokenizer:       tokenizer,
		partHeaders:     phs,
		postingCache:    make(map[int]*PartContainer),
		blockHeadReader: blockHeadReader,
		blockDataReader: blockDataReader,
	}
	return r, nil
}

func (r *TextIndexFilterReader) Close() error {
	if r.blockDataReader != nil {
		if err := r.blockDataReader.Close(); err != nil {
			logger.GetLogger().Info("TextIndexFilterReader closed failed", zap.Error(err))
		}
		r.blockDataReader = nil
	}
	return nil
}

func (r *TextIndexFilterReader) GetPartContainer(partId int) (*PartContainer, error) {
	if partId >= len(r.partHeaders) {
		return nil, nil
	}
	bhs, err := r.blockHeadReader.ReadBlockHeaders(r.partHeaders[partId])
	if err != nil {
		return nil, err
	}
	partContainer := NewPartContainers()
	partContainer.bhs = bhs
	r.postingCache[partId] = partContainer
	return partContainer, nil
}

func (r *TextIndexFilterReader) FilterByPostinglist(partContainer *PartContainer, queryStr string) ([]Container, error) {
	var err error
	// filter by BlockHheader
	bhs := partContainer.FilterByQuery(queryStr)
	if len(bhs) == 0 {
		return nil, nil
	}
	// filter by postinglist
	containers := make([]Container, 0)
	for i := 0; i < len(bhs); i++ {
		blockData, ok := partContainer.blockCache[bhs[i].KeysOffset]
		if !ok {
			blockData, err = r.blockDataReader.GetBlockReadData(&bhs[i])
			if err != nil {
				return nil, err
			}
			partContainer.blockCache[bhs[i].KeysOffset] = blockData
		}
		tmpContainers, err := blockData.Query(queryStr)
		if err != nil {
			return nil, err
		}
		containers = append(containers, tmpContainers...)
	}
	return containers, nil
}

func (r *TextIndexFilterReader) IsExist(segId uint32, queryStr string) (bool, error) {
	// get the partContainer
	var err error
	partId := int(segId) / segmentCntInPart
	segOff := int(segId) % segmentCntInPart
	partContainer, ok := r.postingCache[partId]
	if !ok {
		partContainer, err = r.GetPartContainer(partId)
		if partContainer == nil || err != nil {
			return false, err
		}
	}
	// filter by cache
	postingContainer, ok := partContainer.containers[queryStr]
	if ok {
		return segOff < len(postingContainer.SegCntrs) && len(postingContainer.SegCntrs[segOff].RowIds) != 0, nil
	}
	// tokinzer and read posting list from disk
	queryStrs := r.tokenizer.Split(queryStr)
	if len(queryStrs) == 0 {
		return false, nil
	}
	var res GroupContainers
	for i := 0; i < len(queryStrs); i++ {
		// filter by partHeaders
		if !r.partHeaders[partId].Contain(queryStrs[i]) {
			return false, nil
		}
		// filter by postinglist
		containers, err := r.FilterByPostinglist(partContainer, queryStrs[i])
		if err != nil || len(containers) == 0 {
			return false, err
		}
		if len(res) == 0 {
			res = containers
		} else {
			res = IntersectContainers(res, containers)
		}
		if len(res) == 0 {
			return false, nil
		}
	}
	postingContainer = res.TransToPostingContainer(r.partHeaders[partId])
	partContainer.containers[queryStr] = postingContainer
	return len(postingContainer.SegCntrs[segOff].RowIds) != 0, nil
}

type TextIndexFilterReaders struct {
	path    string
	file    string
	ir      *influxql.IndexRelation
	obsOpts *obs.ObsOptions
	schemas record.Schemas
	readers []*TextIndexFilterReader
	span    *tracing.Span
}

func NewTextIndexFilterReaders(path, file string, schemas record.Schemas, ir *influxql.IndexRelation) *TextIndexFilterReaders {
	return &TextIndexFilterReaders{
		path:    path,
		file:    file,
		ir:      ir,
		schemas: schemas,
		readers: make([]*TextIndexFilterReader, len(schemas)),
	}
}

func (r *TextIndexFilterReaders) Close() error {
	var resErr error
	for i := 0; i < len(r.readers); i++ {
		if r.readers[i] != nil {
			err := r.readers[i].Close()
			if err != nil {
				resErr = err
			}
		}
	}
	return resErr
}

func (r *TextIndexFilterReaders) StartSpan(span *tracing.Span) {
	r.span = span
}

func (r *TextIndexFilterReaders) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	if elem == nil {
		return false, fmt.Errorf("the input SKRPNElement is nil")
	}
	idx := r.schemas.FieldIndex(elem.Key)
	if idx < 0 {
		return false, fmt.Errorf("can not find the index for the filed:%s", elem.Key)
	}

	var err error
	reader := r.readers[idx]
	if reader == nil {
		opt := r.ir.FindIndexOption(uint32(index.Text), elem.Key)
		reader, err = NewTextIndexFilterReader(r.path, r.file, elem.Key, r.obsOpts, opt)
		if err != nil {
			return false, err
		}
		r.readers[idx] = reader
	}

	queryStr, ok := elem.Value.(string)
	if !ok {
		return false, fmt.Errorf("compare value of text index should be string")
	}

	return reader.IsExist(uint32(blockId), queryStr)
}

type TsspFile interface {
	Name() string
	Path() string
}

type TextIndexReader struct {
	isCache bool
	schema  record.Schemas // all schemas related to TextIndex
	option  hybridqp.Options
	sk      sparseindex.SKCondition
	readers *TextIndexFilterReaders
	span    *tracing.Span
}

func NewTextIndexReader(rpnExpr *rpn.RPNExpr, schema record.Schemas, option hybridqp.Options, isCache bool) (*TextIndexReader, error) {
	sk, err := sparseindex.NewSKCondition(rpnExpr, schema)
	if err != nil {
		return nil, err
	}
	ti := &TextIndexReader{
		schema:  schema,
		option:  option,
		isCache: isCache,
		sk:      sk}
	return ti, nil
}

// MayBeInFragment determines whether a fragment in a file meets the query condition.
func (r *TextIndexReader) MayBeInFragment(fragId uint32) (bool, error) {
	return r.sk.IsExist(int64(fragId), r.readers)
}

func (r *TextIndexReader) StartSpan(span *tracing.Span) {
	r.span = span
}

// ReInit is used to that a SKFileReader is reused among multiple files.
func (r *TextIndexReader) ReInit(file interface{}) error {
	var ir *influxql.IndexRelation
	measurements := r.option.GetMeasurements()
	if len(measurements) != 0 {
		ir = measurements[0].IndexRelation
	}
	if r.readers != nil {
		if err := r.readers.Close(); err != nil {
			return err
		}
	}
	if f1, ok := file.(TsspFile); ok {
		index := strings.LastIndex(f1.Path(), "/")
		path := f1.Path()[:index]                           // "/data/data/db0/0...columnstore/mst_0000"
		fileName := strings.Split(f1.Path()[index+1:], ".") // "00000001-0000-00000000.tssp" -> ["00000001-0000-00000000", "tssp"]
		readers := NewTextIndexFilterReaders(path, fileName[0], r.schema, ir)
		r.readers = readers
		return nil
	}
	return fmt.Errorf("unsupport file type for textIndex:%+v", file)
}

// Close is used to close the SKFileReader
func (r *TextIndexReader) Close() error {
	var err error
	if r.readers != nil {
		err = r.readers.Close()
		r.readers = nil
	}
	return err
}
