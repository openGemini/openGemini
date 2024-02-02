/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
//lint:file-ignore U1000 Ignore all unused code, it's generated
package bloomfilter

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"time"

	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	PIECE_NUM int64 = 128
	limitNum  int   = 8
)

const (
	VerticalFilterReaderSizeSpan = "vertical_filter_size_span"
	VerticalFilterReaderNumSpan  = "vertical_filter_num_span"
	VerticalFilterReaderDuration = "vertical_filter_duration"
)

type FilterReader struct {
	isFilter             bool
	version              uint32
	filterLogCount       int64
	verticalFilterCount  int64
	splitMap             map[string][]byte
	missSplitIndex       map[string]uint8
	lineFilterReader     *LineFilterReader
	verticalFilterReader *VerticalFilterReader
	expr                 influxql.Expr
	hashes               map[string][]uint64
	span                 *tracing.Span
	option               *obs.ObsOptions
}

func NewFilterReader(option *obs.ObsOptions, expr influxql.Expr, splitMap map[string][]byte, isCache bool, isStat bool, version uint32, linePath, verticalPath, linFilterName, verticalFilter string) (*FilterReader, error) {
	filterReader := &FilterReader{option: option, splitMap: splitMap}
	if expr == nil {
		filterReader.isFilter = false
		return filterReader, nil
	}
	filterReader.isFilter = true
	var err error
	filterReader.lineFilterReader, err = NewLineFilterReader(linePath, nil, expr, version, splitMap, linFilterName)
	if err != nil {
		return nil, err
	}
	filterReader.verticalFilterReader, err = NewVerticalFilterReader(verticalPath, option, expr, version, splitMap, verticalFilter)
	if err != nil {
		return nil, err
	}
	filterReader.verticalFilterCount = filterReader.verticalFilterReader.verticalFilterCount
	filterReader.filterLogCount = filterReader.lineFilterReader.filterLogCount
	filterReader.lineFilterReader.verticalFilterCount = filterReader.verticalFilterReader.verticalFilterCount
	return filterReader, nil
}

func (s *FilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	if !s.isFilter {
		return true, nil
	}
	if blockId >= s.verticalFilterCount+s.filterLogCount {
		return false, nil
	}
	if blockId < s.verticalFilterCount {
		return s.verticalFilterReader.isExist(blockId)
	}
	return s.lineFilterReader.isExist(blockId)
}

func (s *FilterReader) getAllHashes(expr influxql.Expr) {
	switch n := expr.(type) {
	case *influxql.ParenExpr:
		s.getAllHashes(n.Expr)
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.AND:
			s.getAllHashes(n.LHS)
			s.getAllHashes(n.RHS)
		case influxql.OR:
			s.getAllHashes(n.LHS)
			s.getAllHashes(n.RHS)
		case influxql.MATCHPHRASE:
			val := n.RHS.(*influxql.StringLiteral).Val
			hashValues := make([]uint64, 0)
			leftV := n.LHS.(*influxql.VarRef).Val
			var currTokenizer tokenizer.Tokenizer
			if split, ok := s.splitMap[leftV]; ok {
				currTokenizer = tokenizer.NewSimpleGramTokenizer(split, s.version, s.missSplitIndex[leftV])
			} else {
				return
			}
			currTokenizer.InitInput([]byte(val))
			for currTokenizer.Next() {
				if currTokenizer.CurrentHash() == 0 {
					continue
				}
				hashValues = append(hashValues, currTokenizer.CurrentHash())
			}
			s.hashes[val] = hashValues
		}
	default:
		return
	}
}

func (s *FilterReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	if s.verticalFilterReader != nil {
		s.verticalFilterReader.StartSpan(span)
	}
}

func (s *FilterReader) Close() {
	if s.lineFilterReader != nil {
		s.lineFilterReader.close()
	}
	if s.verticalFilterReader != nil {
		s.verticalFilterReader.close()
	}
}

type VerticalFilterReader struct {
	FilterReader
	r                  fileops.BasicFileReader
	bloomFilter        bloomfilter.Bloomfilter
	currentBlockId     int64
	groupIndex         int64
	verticalPieceCount int64
	groupNewCache      map[int64]map[int64][]uint64
	groupNewPreCache   map[int64]map[int64][]uint64
}

func NewVerticalFilterReader(path string, obsOpts *obs.ObsOptions, expr influxql.Expr, version uint32, splitMap map[string][]byte, fileName string) (*VerticalFilterReader, error) {
	fd, err := obs.OpenObsFile(path, fileName, obsOpts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	v := &VerticalFilterReader{
		r:           dr,
		groupIndex:  -1,
		bloomFilter: bloomfilter.DefaultOneHitBloomFilter(version),
	}
	v.version = version
	verticalFilterLen, err := dr.Size()
	if err != nil {
		return nil, err
	}
	v.verticalFilterCount = verticalFilterLen / logstore.GetConstant(version).VerticalGroupDiskSize * logstore.GetConstant(version).FilterCntPerVerticalGorup
	v.verticalPieceCount = v.verticalFilterCount / logstore.GetConstant(version).FilterCntPerVerticalGorup
	v.hashes = make(map[string][]uint64, 0)
	v.splitMap = splitMap
	v.expr = expr
	v.getAllHashes(expr)
	return v, nil
}

func (s *VerticalFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	return s.isExist(blockId)
}

func (s *VerticalFilterReader) isExist(blockId int64) (bool, error) {
	var t time.Time
	if s.span != nil {
		t = time.Now()
	}
	s.currentBlockId = blockId
	offsetsSet := map[int64]bool{}
	groupIndex := blockId / logstore.GetConstant(s.version).FilterCntPerVerticalGorup
	s.groupIndex = groupIndex
	verticalPieceDiskSize := logstore.GetConstant(s.version).VerticalPieceDiskSize
	if _, ok := s.groupNewCache[groupIndex]; !ok {
		if _, ok2 := s.groupNewPreCache[groupIndex]; !ok2 {
			s.groupNewPreCache = s.groupNewCache
			s.groupNewCache = make(map[int64]map[int64][]uint64)
			pieceNum := PIECE_NUM
			if groupIndex+pieceNum > s.verticalPieceCount {
				pieceNum = s.verticalPieceCount - groupIndex
			}
			for i := int64(0); i < pieceNum; i++ {
				for _, hashes := range s.hashes {
					for _, hash := range hashes {
						pieceOffset, offsetInLong := s.getPieceOffset(hash, groupIndex+i)
						offsetsSet[pieceOffset] = true
						if offsetInLong != 0 {
							offsetsSet[pieceOffset+verticalPieceDiskSize] = true
						}
					}
				}
			}
			offsetsLen := len(offsetsSet)
			offsets := make([]int64, 0)
			for key := range offsetsSet {
				offsets = append(offsets, key)
			}
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			lens := make([]int64, offsetsLen)
			for i := range lens {
				lens[i] = verticalPieceDiskSize
			}
			results := make(map[int64][]byte)
			c := make(chan *request.StreamReader, 1)
			s.r.StreamReadBatch(offsets, lens, c, limitNum)
			for r := range c {
				if r.Err != nil {
					return false, r.Err
				}
				results[r.Offset] = r.Content
			}
			if s.span != nil {
				for k := range results {
					s.span.Count(VerticalFilterReaderSizeSpan, int64(len(results[k])))
				}
				s.span.Count(VerticalFilterReaderNumSpan, int64(len(offsets)))
			}
			var err error
			for i, result := range results {
				subGroupIndex := i / logstore.GetConstant(s.version).VerticalGroupDiskSize
				if _, subOk := s.groupNewCache[subGroupIndex]; !subOk {
					s.groupNewCache[subGroupIndex] = make(map[int64][]uint64)
				}
				s.groupNewCache[subGroupIndex][i], err = s.getPieceLongs(result)
				if err != nil {
					return false, err
				}
			}
		}
	}

	isHit := s.hitExpr(s.expr)
	if s.span != nil {
		s.span.Count(VerticalFilterReaderDuration, int64(time.Since(t)))
	}
	return isHit, nil
}

func (s *VerticalFilterReader) hitExpr(expr influxql.Expr) bool {
	switch n := expr.(type) {
	case *influxql.ParenExpr:
		return s.hitExpr(n.Expr)
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.AND:
			return s.hitExpr(n.LHS) && s.hitExpr(n.RHS)
		case influxql.OR:
			return s.hitExpr(n.LHS) || s.hitExpr(n.RHS)
		case influxql.MATCHPHRASE:
			val := n.RHS.(*influxql.StringLiteral).Val
			hashValues := s.hashes[val]
			isExist := false
			for _, hash := range hashValues {
				isExist = true
				pieceOffset, offsetInLong := s.getPieceOffset(hash, s.groupIndex)
				hashSlot := s.loadHash(pieceOffset, offsetInLong, s.currentBlockId)
				if !s.bloomFilter.LoadHit(hash, hashSlot) {
					return false
				}
			}
			return isExist
		}
	default:
		return true
	}
	return true
}

func (s *VerticalFilterReader) loadHash(pieceOffset int64, offsetInLong int64, blockId int64) uint64 {
	var piece []uint64
	if _, ok := s.groupNewCache[s.groupIndex]; ok {
		piece = s.groupNewCache[s.groupIndex][pieceOffset]
	} else {
		piece = s.groupNewPreCache[s.groupIndex][pieceOffset]
	}
	offsetInPiece := blockId % logstore.GetConstant(s.version).FilterCntPerVerticalGorup
	hash := piece[offsetInPiece]
	verticalPieceDiskSize := logstore.GetConstant(s.version).VerticalPieceDiskSize
	if offsetInLong != 0 {
		if _, ok := s.groupNewCache[s.groupIndex]; ok {
			piece = s.groupNewCache[s.groupIndex][pieceOffset+verticalPieceDiskSize]
		} else {
			piece = s.groupNewPreCache[s.groupIndex][pieceOffset+verticalPieceDiskSize]
		}
		secondHash := piece[offsetInPiece]
		hash = (hash >> (offsetInLong * 8)) | (secondHash << (64 - offsetInLong*8))
	}
	return hash
}

func (s *VerticalFilterReader) getPieceOffset(hash uint64, groupIndex int64) (int64, int64) {
	bytesOffset := s.bloomFilter.GetBytesOffset(hash)
	offsetInLong := bytesOffset % 8
	pieceIndex := bytesOffset >> 3
	pieceOffset := logstore.GetConstant(s.version).VerticalGroupDiskSize*groupIndex +
		pieceIndex*logstore.GetConstant(s.version).VerticalPieceDiskSize
	return pieceOffset, offsetInLong
}

func (s *VerticalFilterReader) getPieceLongs(bytes []byte) ([]uint64, error) {
	verticalPieceMemSize := logstore.GetConstant(s.version).VerticalPieceMemSize
	verticalPieceDiskSize := logstore.GetConstant(s.version).VerticalPieceDiskSize
	pieceLongs := make([]uint64, verticalPieceDiskSize>>3)
	for i := 0; i*8 < int(verticalPieceMemSize); i += 1 {
		pieceLongs[i] = binary.LittleEndian.Uint64(bytes[i*8 : (i+1)*8])
	}
	loadValue := binary.LittleEndian.Uint32(bytes[verticalPieceMemSize:verticalPieceDiskSize])
	checkValue := crc32.Checksum(bytes[:verticalPieceMemSize], crc32.MakeTable(crc32.Castagnoli))
	if loadValue != checkValue {
		return nil, fmt.Errorf("load vertical filter checksum(%d) mismatch computed valued(%d)", loadValue, checkValue)
	}
	return pieceLongs, nil
}

func (s *VerticalFilterReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	s.span.CreateCounter(VerticalFilterReaderSizeSpan, "")
	s.span.CreateCounter(VerticalFilterReaderNumSpan, "")
	s.span.CreateCounter(VerticalFilterReaderDuration, "ns")
}

func (s *VerticalFilterReader) close() {

}

type LineFilterReader struct {
	FilterReader
	r              fileops.BasicFileReader
	currentBlockId int64
	bloomCache     map[int64]bloomfilter.Bloomfilter
	isCached       bool
}

func NewLineFilterReader(path string, obsOpts *obs.ObsOptions, expr influxql.Expr, version uint32, splitMap map[string][]byte, fileName string) (*LineFilterReader, error) {
	fd, err := obs.OpenObsFile(path, fileName, obsOpts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	l := &LineFilterReader{
		r:          dr,
		bloomCache: make(map[int64]bloomfilter.Bloomfilter),
		isCached:   false,
	}
	l.version = version
	filterLogLen, err := dr.Size()
	if err != nil {
		return nil, err
	}
	l.filterLogCount = filterLogLen / logstore.GetConstant(version).FilterDataDiskSize
	l.hashes = make(map[string][]uint64, 0)
	l.splitMap = splitMap
	l.expr = expr
	l.getAllHashes(expr)
	return l, nil
}

func (s *LineFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	return s.isExist(blockId)
}

func (s *LineFilterReader) isExist(blockId int64) (bool, error) {
	s.currentBlockId = blockId
	filterDataDiskSize := logstore.GetConstant(s.version).FilterDataDiskSize
	if !s.isCached {
		length := filterDataDiskSize * s.filterLogCount
		bloomBuf := make([]byte, length)
		var err error
		bloomBuf, err = s.r.ReadAt(0, uint32(length), &bloomBuf, fileops.IO_PRIORITY_HIGH)
		if err != nil {
			return false, err
		}
		for i := 0; i < int(s.filterLogCount); i++ {
			filterPart := bloomBuf[i*int(filterDataDiskSize) : (i+1)*int(filterDataDiskSize)-4]
			checkSumPart := bloomBuf[(i+1)*int(filterDataDiskSize)-4 : (i+1)*int(filterDataDiskSize)]
			loadCheckValue := binary.LittleEndian.Uint32(checkSumPart)
			checkValue := crc32.Checksum(filterPart, crc32.MakeTable(crc32.Castagnoli))
			if checkValue != loadCheckValue {
				return false, fmt.Errorf("load filter log checksum(%d) mismatch computed valued(%d)", loadCheckValue, checkValue)
			}
			bloomFilter := bloomfilter.NewOneHitBloomFilter(filterPart, s.version)
			s.bloomCache[int64(i)*filterDataDiskSize+s.verticalFilterCount*filterDataDiskSize] = bloomFilter
		}
		s.isCached = true
	}
	return s.hitExpr(s.expr), nil
}

func (s *LineFilterReader) hitExpr(expr influxql.Expr) bool {
	switch n := expr.(type) {
	case *influxql.ParenExpr:
		return s.hitExpr(n.Expr)
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.AND:
			return s.hitExpr(n.LHS) && s.hitExpr(n.RHS)
		case influxql.OR:
			return s.hitExpr(n.LHS) || s.hitExpr(n.RHS)
		case influxql.MATCHPHRASE:
			val := n.RHS.(*influxql.StringLiteral).Val
			hashValues := make([]uint64, 0)
			leftV := n.LHS.(*influxql.VarRef).Val
			var currTokenizer tokenizer.Tokenizer
			if split, ok := s.splitMap[leftV]; ok {
				currTokenizer = tokenizer.NewSimpleGramTokenizer(split, s.version, s.missSplitIndex[leftV])
			} else {
				return true
			}
			currTokenizer.InitInput([]byte(val))
			for currTokenizer.Next() {
				if currTokenizer.CurrentHash() == 0 {
					continue
				}
				hashValues = append(hashValues, currTokenizer.CurrentHash())
			}

			blockOffset := s.currentBlockId * logstore.GetConstant(s.version).FilterDataDiskSize
			bloomFilter := s.bloomCache[blockOffset]
			isExist := false
			for _, hash := range hashValues {
				isExist = true
				if !bloomFilter.Hit(hash) {
					return false
				}
			}
			return isExist
		}
	default:
		return true
	}
	return true
}

func (s *LineFilterReader) close() {
	if s.r != nil {
		_ = s.r.Close()
	}
}
