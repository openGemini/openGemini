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
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

type SKRPNElement struct {
	Key   string
	Value string
}

func NewSKRPNElement(key, value string) *SKRPNElement {
	return &SKRPNElement{
		Key:   key,
		Value: value,
	}
}

type MultiFieldFilterReader struct {
	isFilter             bool
	version              uint32
	filterLogCount       int64
	verticalFilterCount  int64
	splitMap             map[string][]byte
	missSplitIndex       map[string]uint8
	lineFilterReader     *MultiFiledLineFilterReader
	verticalFilterReader *MultilFieldVerticalFilterReader
	expr                 []*SKRPNElement
	hashes               map[string][]uint64
	span                 *tracing.Span
	option               *obs.ObsOptions
}

func NewMultiFieldFilterReader(option *obs.ObsOptions, expr []*SKRPNElement, splitMap map[string][]byte, isCache bool, isStat bool, version uint32, linePath, verticalPath, linFilterName, verticalFilter string) (*MultiFieldFilterReader, error) {
	filterReader := &MultiFieldFilterReader{option: option, splitMap: splitMap}
	if len(expr) == 0 {
		filterReader.isFilter = false
		return filterReader, nil
	}
	filterReader.isFilter = true
	var err error
	filterReader.lineFilterReader, err = NewMultiFiledLineFilterReader(linePath, nil, expr, version, splitMap, linFilterName)
	if err != nil {
		return nil, err
	}
	filterReader.verticalFilterReader, err = NewMultiFieldVerticalFilterReader(verticalPath, option, expr, version, splitMap, verticalFilter)
	if err != nil {
		return nil, err
	}
	filterReader.verticalFilterCount = filterReader.verticalFilterReader.verticalFilterCount
	filterReader.filterLogCount = filterReader.lineFilterReader.filterLogCount
	filterReader.lineFilterReader.verticalFilterCount = filterReader.verticalFilterReader.verticalFilterCount
	return filterReader, nil
}

func (s *MultiFieldFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	if !s.isFilter {
		return true, nil
	}
	if blockId >= s.verticalFilterCount+s.filterLogCount {
		return false, nil
	}
	if blockId < s.verticalFilterCount {
		return s.verticalFilterReader.isExist(blockId, elem)
	}
	return s.lineFilterReader.isExist(blockId, elem)
}

func (s *MultiFieldFilterReader) getAllHashes(expr []*SKRPNElement) {
	for _, v := range expr {
		leftV := v.Key
		val := v.Value
		hashValues := make([]uint64, 0)
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
}

func (s *MultiFieldFilterReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	if s.verticalFilterReader != nil {
		s.verticalFilterReader.StartSpan(span)
	}
}

func (s *MultiFieldFilterReader) Close() {
	if s.lineFilterReader != nil {
		s.lineFilterReader.close()
	}
	if s.verticalFilterReader != nil {
		s.verticalFilterReader.close()
	}
}

type MultilFieldVerticalFilterReader struct {
	MultiFieldFilterReader
	r                  logstore.BloomFilterReader
	bloomFilter        bloomfilter.Bloomfilter
	currentBlockId     int64
	groupIndex         int64
	verticalPieceCount int64
	groupNewCache      map[int64]map[int64][]uint64
	groupNewPreCache   map[int64]map[int64][]uint64
}

func NewMultiFieldVerticalFilterReader(path string, obsOpts *obs.ObsOptions, expr []*SKRPNElement, version uint32, splitMap map[string][]byte, fileName string) (*MultilFieldVerticalFilterReader, error) {
	dr, err := logstore.NewBloomfilterReader(obsOpts, path, fileName, version)
	if err != nil {
		return nil, err
	}
	v := &MultilFieldVerticalFilterReader{
		r:           dr,
		groupIndex:  -1,
		bloomFilter: bloomfilter.DefaultOneHitBloomFilter(version, logstore.GetConstant(version).FilterDataMemSize),
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

	logstore.SendLogRequestWithHash(&logstore.LogPath{Path: path, FileName: fileName, Version: version, ObsOpt: obsOpts}, v.hashes)
	return v, nil
}

func (s *MultilFieldVerticalFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	return s.isExist(blockId, elem)
}

func (s *MultilFieldVerticalFilterReader) isExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
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
			pieceIndex := groupIndex / PIECE_NUM
			startPieceIndex := pieceIndex * PIECE_NUM
			endPieceIndex := startPieceIndex + PIECE_NUM
			if startPieceIndex+PIECE_NUM > s.verticalPieceCount {
				endPieceIndex = s.verticalPieceCount
			}
			for startPieceIndex < endPieceIndex {
				for _, hashes := range s.hashes {
					for _, hash := range hashes {
						pieceOffset, offsetInLong := s.getPieceOffset(hash, startPieceIndex)
						offsetsSet[pieceOffset] = true
						if offsetInLong != 0 {
							offsetsSet[pieceOffset+verticalPieceDiskSize] = true
						}
					}
				}
				startPieceIndex++
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
			err := s.r.ReadBatch(offsets, lens, limitNum, true, results)
			if err != nil {
				return false, err
			}
			if s.span != nil {
				for k := range results {
					s.span.Count(VerticalFilterReaderSizeSpan, int64(len(results[k])))
				}
				s.span.Count(VerticalFilterReaderNumSpan, int64(len(offsets)))
			}
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

	isHit := s.hitExpr(elem.Value.(string))
	if s.span != nil {
		s.span.Count(VerticalFilterReaderDuration, int64(time.Since(t)))
	}
	return isHit, nil
}

func (s *MultilFieldVerticalFilterReader) hitExpr(val string) bool {
	hashValues := s.hashes[val]
	if len(hashValues) == 0 {
		return true
	}
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

func (s *MultilFieldVerticalFilterReader) loadHash(pieceOffset int64, offsetInLong int64, blockId int64) uint64 {
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

func (s *MultilFieldVerticalFilterReader) getPieceOffset(hash uint64, groupIndex int64) (int64, int64) {
	bytesOffset := s.bloomFilter.GetBytesOffset(hash)
	offsetInLong := bytesOffset % 8
	pieceIndex := bytesOffset >> 3
	pieceOffset := logstore.GetConstant(s.version).VerticalGroupDiskSize*groupIndex +
		pieceIndex*logstore.GetConstant(s.version).VerticalPieceDiskSize
	return pieceOffset, offsetInLong
}

func (s *MultilFieldVerticalFilterReader) getPieceLongs(bytes []byte) ([]uint64, error) {
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

func (s *MultilFieldVerticalFilterReader) StartSpan(span *tracing.Span) {
	if span == nil {
		return
	}
	s.span = span
	if s.r != nil {
		s.r.StartSpan(span)
	}
	s.span.CreateCounter(VerticalFilterReaderSizeSpan, "")
	s.span.CreateCounter(VerticalFilterReaderNumSpan, "")
	s.span.CreateCounter(VerticalFilterReaderDuration, "ns")
}

func (s *MultilFieldVerticalFilterReader) close() {
}

type MultiFiledLineFilterReader struct {
	MultiFieldFilterReader
	r              fileops.BasicFileReader
	currentBlockId int64
	bloomCache     map[int64]bloomfilter.Bloomfilter
	isCached       bool
}

func NewMultiFiledLineFilterReader(path string, obsOpts *obs.ObsOptions, expr []*SKRPNElement, version uint32, splitMap map[string][]byte, fileName string) (*MultiFiledLineFilterReader, error) {
	fd, err := fileops.OpenObsFile(path, fileName, obsOpts, true)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	l := &MultiFiledLineFilterReader{
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

func (s *MultiFiledLineFilterReader) IsExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
	return s.isExist(blockId, elem)
}

func (s *MultiFiledLineFilterReader) isExist(blockId int64, elem *rpn.SKRPNElement) (bool, error) {
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
				logger.GetLogger().Warn("load filter log checksum mismatch computed valued", zap.Uint32("loadCheckValue", loadCheckValue),
					zap.Uint32("computedCheckValue", checkValue))
				util.MemorySet(filterPart, 0xff)
			}
			bloomFilter := bloomfilter.NewOneHitBloomFilter(filterPart, s.version)
			s.bloomCache[int64(i)*filterDataDiskSize+s.verticalFilterCount*filterDataDiskSize] = bloomFilter
		}
		s.isCached = true
	}
	return s.hitExpr(elem.Value.(string)), nil
}

func (s *MultiFiledLineFilterReader) hitExpr(val string) bool {
	hashValues := s.hashes[val]
	if len(hashValues) == 0 {
		return true
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

func (s *MultiFiledLineFilterReader) close() {
	if s.r != nil {
		_ = s.r.Close()
	}
}
