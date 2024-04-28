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

package textindex

import (
	"path"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/encoding/lz4"
)

const (
	tmpFileSuffix          = ".init"
	tmpBufSize       int   = 1024
	dataBufSize      int   = 1 * 1024 * 1024
	fixItemLen       uint8 = 8
	segmentCntInPart int   = 16
)

var padZero []byte = make([]byte, fixItemLen)

type BlockData struct {
	Keys       []byte
	PackedKeys []byte
	Data       []byte
	PackedData []byte
}

func NewBlockData() *BlockData {
	return &BlockData{
		Keys:       make([]byte, 0, dataBufSize),
		PackedKeys: make([]byte, 0, dataBufSize),
		Data:       make([]byte, 0, dataBufSize),
		PackedData: make([]byte, 0, dataBufSize),
	}
}

func (data *BlockData) Reset() {
	data.Keys = data.Keys[:0]
	data.PackedKeys = data.PackedKeys[:0]
	data.Data = data.Data[:0]
	data.PackedData = data.PackedData[:0]
}

func (d *BlockData) GrowPackedBuf(keyDstLen, dataDstLen int) {
	if cap(d.PackedKeys) >= keyDstLen {
		d.PackedKeys = d.PackedKeys[:keyDstLen]
	} else {
		d.PackedKeys = d.PackedKeys[:cap(d.PackedKeys)]
		d.PackedKeys = append(d.PackedKeys, make([]byte, keyDstLen-cap(d.PackedKeys))...)
	}

	if cap(d.PackedData) >= dataDstLen {
		d.PackedData = d.PackedData[:dataDstLen]
	} else {
		d.PackedData = d.PackedData[:cap(d.PackedData)]
		d.PackedData = append(d.PackedData, make([]byte, dataDstLen-cap(d.PackedData))...)
	}
}

var bdPool sync.Pool

func GetBlockData() *BlockData {
	v := bdPool.Get()
	if v == nil {
		return NewBlockData()
	}
	return v.(*BlockData)
}

func PutBlockData(bd *BlockData) {
	bd.Reset()
	bdPool.Put(bd)
}

type BlockHeader struct {
	FirstItem   []byte
	LastItem    []byte
	MarshalType uint8
	ItemsCount  uint32
	// keys info
	KeysOffset     uint64
	KeysUnpackSize uint32 // Uncompressed length(keys+keysOffs)
	KeysPackSize   uint32 // Compressed length(keys+keysOffs)
	KeysSize       uint32 // Uncompressed length(only keys)
	// posting list info
	PostOffset     uint64
	PostUnpackSize uint32 // Uncompressed length(posts+postsOffs)
	PostPackSize   uint32 // Compressed length(posts+postsOffs)
	PostSize       uint32 // Uncompressed length(only posts)
}

func NewBlockHeader() *BlockHeader {
	return &BlockHeader{}
}

func (bh *BlockHeader) Reset() {
	bh.FirstItem = bh.FirstItem[:0]
	bh.LastItem = bh.LastItem[:0]
	bh.MarshalType = 0
	bh.ItemsCount = 0
	bh.KeysOffset = 0
	bh.KeysUnpackSize = 0
	bh.KeysPackSize = 0
	bh.KeysSize = 0
	bh.PostOffset = 0
	bh.PostUnpackSize = 0
	bh.PostPackSize = 0
	bh.PostSize = 0
}

func (bh *BlockHeader) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, bh.FirstItem)
	dst = encoding.MarshalBytes(dst, bh.LastItem)
	dst = append(dst, byte(bh.MarshalType))
	dst = encoding.MarshalUint32(dst, bh.ItemsCount)
	dst = encoding.MarshalUint64(dst, bh.KeysOffset)
	dst = encoding.MarshalUint32(dst, bh.KeysUnpackSize)
	dst = encoding.MarshalUint32(dst, bh.KeysPackSize)
	dst = encoding.MarshalUint32(dst, bh.KeysSize)
	dst = encoding.MarshalUint64(dst, bh.PostOffset)
	dst = encoding.MarshalUint32(dst, bh.PostUnpackSize)
	dst = encoding.MarshalUint32(dst, bh.PostPackSize)
	dst = encoding.MarshalUint32(dst, bh.PostSize)
	return dst
}

var bhPool sync.Pool

func GetBlockReader() *BlockHeader {
	v := bhPool.Get()
	if v == nil {
		return NewBlockHeader()
	}
	return v.(*BlockHeader)
}

func PutBlockReader(bh *BlockHeader) {
	bh.Reset()
	bhPool.Put(bh)
}

// Metandex is stored together with BlockHeaders for attach scenes. For detach scenes, it is stored as a separate file
type PartHeader struct {
	FirstItemLen      uint8
	LastItemLen       uint8
	FirstItem         []byte // cap is 8
	LastItem          []byte // cap is 8
	Flag              uint16 // Flag[0:1] -> msharsalType, Flag[2:15] -> reserved
	BlockHeaderCnt    uint32 // BlockHeadersCount
	BlockHeaderOffset uint64
	BlockHeaderSize   uint32 // BlockHeaders + SegmentRanges
	SegmentRangeCnt   uint32
	SegmentRange      []uint32 // cap is segmentCntInPart
}

func NewPartHeader() *PartHeader {
	return &PartHeader{
		FirstItem:    make([]byte, 0, fixItemLen),
		LastItem:     make([]byte, 0, fixItemLen),
		SegmentRange: make([]uint32, 0, segmentCntInPart),
	}
}

func (ph *PartHeader) Reset() {
	ph.FirstItemLen = 0
	ph.LastItemLen = 0
	ph.FirstItem = ph.FirstItem[:0]
	ph.LastItem = ph.LastItem[:0]
	ph.Flag = 0
	ph.BlockHeaderCnt = 0
	ph.BlockHeaderOffset = 0
	ph.BlockHeaderSize = 0
	ph.SegmentRangeCnt = 0
	ph.SegmentRange = ph.SegmentRange[:0]
}

func (ph *PartHeader) UpdateFirstItem(firstItem []byte) {
	ph.FirstItem = ph.FirstItem[:0]
	if len(firstItem) > int(fixItemLen) {
		ph.FirstItem = append(ph.FirstItem, firstItem[:fixItemLen]...)
	} else {
		ph.FirstItem = append(ph.FirstItem, firstItem...)
	}
}

func (ph *PartHeader) UpdateLastItem(lastItem []byte) {
	ph.LastItem = ph.LastItem[:0]
	if len(lastItem) > int(fixItemLen) {
		ph.LastItem = append(ph.LastItem, lastItem[:fixItemLen]...)
	} else {
		ph.LastItem = append(ph.LastItem, lastItem...)
	}
}

func (ph *PartHeader) UpdateSegmentRange(rowsPerSegment []int, segId int) {
	i := 0
	ph.SegmentRangeCnt = 0
	ph.SegmentRange = ph.SegmentRange[:0]
	for i < segmentCntInPart {
		if segId < len(rowsPerSegment)-1 {
			ph.SegmentRangeCnt++
			ph.SegmentRange = append(ph.SegmentRange, uint32(rowsPerSegment[segId]))
		} else if segId == len(rowsPerSegment)-1 {
			ph.SegmentRangeCnt++
			ph.SegmentRange = append(ph.SegmentRange, uint32(rowsPerSegment[segId]+1))
		} else {
			ph.SegmentRange = append(ph.SegmentRange, 0)
		}
		i++
		segId++
	}
}

func (ph *PartHeader) Marshal(dst []byte) []byte {
	// ItemLen
	ph.FirstItemLen = uint8(len(ph.FirstItem))
	ph.LastItemLen = uint8(len(ph.LastItem))
	dst = append(dst, ph.FirstItemLen)
	dst = append(dst, ph.LastItemLen)
	// FirstItem
	if ph.FirstItemLen >= fixItemLen {
		dst = append(dst, ph.FirstItem[:8]...)
	} else {
		dst = append(dst, ph.FirstItem...)
		dst = append(dst, padZero[:8-ph.FirstItemLen]...)
	}
	// LastItem
	if ph.LastItemLen >= fixItemLen {
		dst = append(dst, ph.LastItem[:8]...)
	} else {
		dst = append(dst, ph.LastItem...)
		dst = append(dst, padZero[:8-ph.LastItemLen]...)
	}
	// block
	dst = encoding.MarshalUint16(dst, ph.Flag)
	dst = encoding.MarshalUint32(dst, ph.BlockHeaderCnt)
	dst = encoding.MarshalUint64(dst, ph.BlockHeaderOffset)
	dst = encoding.MarshalUint32(dst, ph.BlockHeaderSize)
	// segment range
	dst = encoding.MarshalUint32(dst, ph.SegmentRangeCnt)
	for i := 0; i < len(ph.SegmentRange); i++ {
		dst = encoding.MarshalUint32(dst, ph.SegmentRange[i])
	}
	return dst
}

var phPool sync.Pool

func GetPartHeader() *PartHeader {
	v := phPool.Get()
	if v == nil {
		return NewPartHeader()
	}
	return v.(*PartHeader)
}

func PutPartHeader(ph *PartHeader) {
	ph.Reset()
	phPool.Put(ph)
}

type TextIndexWriter struct {
	builder  *FullTextIndexBuilder
	dir      string
	msName   string
	filePath string
	lockPath string
	tokens   string
}

func NewTextIndexWriter(dir, msName, filePath, lockPath string, tokens string) *TextIndexWriter {
	builder := NewFullTextIndexBuilder(tokens, false)
	indexWriter := &TextIndexWriter{
		builder:  builder,
		dir:      dir,
		msName:   msName,
		filePath: filePath,
		lockPath: lockPath,
		tokens:   tokens,
	}
	return indexWriter
}

func GetRowIdRange(rowsPerSegment []int, segId int) (int, int) {
	startRow := 0
	if segId > 0 {
		startRow = rowsPerSegment[segId-1]
	}

	endRow := 0
	segmentCnt := len(rowsPerSegment)
	if segId+segmentCntInPart < segmentCnt {
		endRow = rowsPerSegment[segId+segmentCntInPart-1]
	} else {
		endRow = rowsPerSegment[segmentCnt-1] + 1
	}
	return startRow, endRow
}

func (w *TextIndexWriter) Open() error {
	return nil
}

func (w *TextIndexWriter) Close() error {
	FreeFullTextIndexBuilder(w.builder)
	return nil
}

func (w *TextIndexWriter) GetTextIndexFilePath(field string, fileType int) string {
	return path.Join(w.dir, w.msName, colstore.AppendSecondaryIndexSuffix(w.filePath, field, index.Text, fileType)+tmpFileSuffix)
}

func (w *TextIndexWriter) GetIndexFileWriters(field string) (*colstore.IndexWriter, *colstore.IndexWriter, *colstore.IndexWriter, error) {
	blockDataPath := w.GetTextIndexFilePath(field, colstore.TextIndexData)
	blockHeadPath := w.GetTextIndexFilePath(field, colstore.TextIndexHead)
	blockPartPath := w.GetTextIndexFilePath(field, colstore.TextIndexPart)
	dataWriter, err := colstore.NewIndexWriter(&w.lockPath, blockDataPath)
	if err != nil {
		return nil, nil, nil, err
	}
	headWriter, err := colstore.NewIndexWriter(&w.lockPath, blockHeadPath)
	if err != nil {
		dataWriter.Reset()
		return nil, nil, nil, err
	}
	partWriter, err := colstore.NewIndexWriter(&w.lockPath, blockPartPath)
	if err != nil {
		dataWriter.Reset()
		headWriter.Reset()
		return nil, nil, nil, err
	}
	return dataWriter, headWriter, partWriter, nil
}

func (w *TextIndexWriter) CloseIndexWriters(dataWriter, headWriter, partWriter *colstore.IndexWriter) {
	dataWriter.Reset()
	headWriter.Reset()
	partWriter.Reset()
}

func (w *TextIndexWriter) CreateAttachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int) error {
	data := GetBlockData()
	bh := GetBlockReader()
	ph := GetPartHeader()

	headBuf := make([]byte, 0, tmpBufSize)
	partBuf := make([]byte, 0, tmpBufSize)
	for _, idx := range schemaIdx {
		field := writeRec.Schema[idx].Name
		dataWriter, headWriter, partWriter, err := w.GetIndexFileWriters(field)
		if err != nil {
			return err
		}
		var dataOffset uint64
		var headOffset uint64
		segmentCnt := len(rowsPerSegment)
		i := 0
		for i < segmentCnt {
			// Splitting words and constructing an inverted table
			startRow, endRow := GetRowIdRange(rowsPerSegment, i)
			i += segmentCntInPart
			memElement, err := w.builder.AddDocument(writeRec.ColVals[idx].Val, writeRec.ColVals[idx].Offset, startRow, endRow)
			if err != nil {
				return err
			}
			// Get the block data
			ph.BlockHeaderCnt = 0
			ph.BlockHeaderOffset = headOffset
			ph.UpdateSegmentRange(rowsPerSegment, i)
			next := true
			for next {
				next = w.builder.Next(memElement, data, bh)
				// Compress LZ4 and Data write
				keyDstLen := lz4.CompressBlockBound(int(bh.KeysUnpackSize))
				postDstLen := lz4.CompressBlockBound(int(bh.PostUnpackSize))
				data.GrowPackedBuf(keyDstLen, postDstLen)
				keysPackSize, err := lz4.CompressBlock(data.Keys, data.PackedKeys)
				if err != nil {
					w.CloseIndexWriters(dataWriter, headWriter, partWriter)
					return err
				}
				postPackSize, err := lz4.CompressBlock(data.Data, data.PackedData)
				if err != nil {
					w.CloseIndexWriters(dataWriter, headWriter, partWriter)
					return err
				}
				if err := dataWriter.WriteData(data.PackedKeys[:keysPackSize]); err != nil {
					w.CloseIndexWriters(dataWriter, headWriter, partWriter)
					return err
				}
				if err := dataWriter.WriteData(data.PackedData[:postPackSize]); err != nil {
					w.CloseIndexWriters(dataWriter, headWriter, partWriter)
					return err
				}
				// update blockHeader and marshal
				bh.KeysOffset = dataOffset
				bh.KeysPackSize = uint32(keysPackSize)
				bh.PostOffset = dataOffset + uint64(bh.KeysPackSize)
				bh.PostPackSize = uint32(postPackSize)
				headBuf = bh.Marshal(headBuf)
				// update PartHeader
				if ph.BlockHeaderCnt == 0 {
					ph.UpdateFirstItem(bh.FirstItem)
				}
				if !next {
					ph.UpdateLastItem(bh.LastItem)
				}
				ph.BlockHeaderCnt++
				// reset
				dataOffset += uint64(bh.KeysPackSize + bh.PostPackSize)
				data.Reset()
				bh.Reset()
			}
			// partHeader marshal
			ph.BlockHeaderSize = uint32(len(headBuf)) - uint32(headOffset)
			headOffset = uint64(len(headBuf))
			partBuf = ph.Marshal(partBuf)
			// reset
			ph.Reset()
			PutInvertMemElement(memElement)
		}
		if err := headWriter.WriteData(headBuf); err != nil {
			w.CloseIndexWriters(dataWriter, headWriter, partWriter)
			return err
		}
		if err := partWriter.WriteData(partBuf); err != nil {
			w.CloseIndexWriters(dataWriter, headWriter, partWriter)
			return err
		}
		w.CloseIndexWriters(dataWriter, headWriter, partWriter)
		headBuf = headBuf[:0]
		partBuf = partBuf[:0]
	}

	PutBlockData(data)
	PutBlockReader(bh)
	PutPartHeader(ph)
	return nil
}

func (w *TextIndexWriter) CreateDetachIndex(writeRec *record.Record, schemaIdx, rowsPerSegment []int, dataBuf [][]byte) ([][]byte, []string) {
	return nil, nil
}
