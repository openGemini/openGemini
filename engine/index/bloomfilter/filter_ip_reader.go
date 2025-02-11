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

package bloomfilter

import (
	"encoding/binary"
	"hash/crc32"
	"net"

	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/rpn"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

type LineFilterIpReader struct {
	FilterReader
	r              fileops.BasicFileReader
	currentBlockId int64
	bloomCache     map[int64]bloomfilter.Bloomfilter
	isCached       bool
}

func NewLineFilterIpReader(path string, obsOpts *obs.ObsOptions, expr influxql.Expr, version uint32, splitMap map[string][]byte, fileName string) (rpn.SKBaseReader, error) {
	fd, err := fileops.OpenObsFile(path, fileName, obsOpts, true)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	l := &LineFilterIpReader{
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

func (s *LineFilterIpReader) IsExist(blockId int64, _ *rpn.SKRPNElement) (bool, error) {
	return s.isExist(blockId)
}

func (s *LineFilterIpReader) isExist(blockId int64) (bool, error) {
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
			start := i * int(filterDataDiskSize)
			end := (i + 1) * int(filterDataDiskSize)
			filterPart := bloomBuf[start : end-4]
			checkSumPart := bloomBuf[end-4 : end]
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
	return s.hitExpr(s.expr), nil
}

func (s *LineFilterIpReader) hitExpr(expr influxql.Expr) bool {
	switch n := expr.(type) {
	case *influxql.ParenExpr:
		return s.hitExpr(n.Expr)
	case *influxql.BinaryExpr:
		switch n.Op {
		case influxql.EQ:
			return s.hitIp(n)
		case influxql.AND:
			return s.hitExpr(n.LHS) && s.hitExpr(n.RHS)
		case influxql.OR:
			return s.hitExpr(n.LHS) || s.hitExpr(n.RHS)
		case influxql.IPINRANGE:
			return s.hitIpSubnet(n)
		}
	default:
		return true
	}
	return true
}

func (s *LineFilterIpReader) Close() {
	if s.r != nil {
		_ = s.r.Close()
	}
}

func (s *LineFilterIpReader) hitIp(n *influxql.BinaryExpr) bool {
	val := n.RHS.(*influxql.StringLiteral).Val
	hashValue := uint64(0)
	currTokenizer := tokenizer.NewIpTokenizer()
	currTokenizer.InitInput([]byte(val))
	for currTokenizer.Next() {
		hashValue = currTokenizer.CurrentHash()
		break
	}

	if hashValue == 0 {
		return true
	}

	blockOffset := s.currentBlockId * logstore.GetConstant(s.version).FilterDataDiskSize
	bloomFilter := s.bloomCache[blockOffset]

	return bloomFilter.Hit(hashValue)
}

func (s *LineFilterIpReader) hitIpSubnet(n *influxql.BinaryExpr) bool {
	isExist := false
	// convert ipSegment to the nearest hash
	subnetVal := n.RHS.(*influxql.StringLiteral).Val
	_, ipNet, err := net.ParseCIDR(subnetVal)
	if err != nil {
		return isExist
	}
	hashValues := make([]uint64, 0)
	currTokenizer := tokenizer.NewIpTokenizer()
	// using tokenizer get hashValue
	currTokenizer.HashWithMaskIndex(&ipNet.IP, currTokenizer.GetMatchedMaskIndex(ipNet.Mask))
	if currTokenizer.CurrentHash() != 0 {
		hashValues = append(hashValues, currTokenizer.CurrentHash())
		s.hashes[subnetVal] = hashValues
	}

	blockOffset := s.currentBlockId * logstore.GetConstant(s.version).FilterDataDiskSize
	bloomFilter := s.bloomCache[blockOffset]

	for _, hash := range hashValues {
		isExist = true
		if !bloomFilter.Hit(hash) {
			return false
		}
	}
	return isExist
}
