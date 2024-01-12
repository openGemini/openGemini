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
package logstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
)

func WriteSfsData(tmpDir string, contents []string) error {
	maxTimeStamp := uint64(len(contents))
	minTimeStamp := uint64(0)
	maxSeq := uint64(len(contents))
	minSeq := uint64(0)
	//write filter
	version := uint32(4)
	verticalFilterName := tmpDir + "/" + logstore.OBSVLMFileName
	filterLogName := tmpDir + "/" + logstore.FilterLogName

	verticalFilterFd, err := fileops.OpenFile(verticalFilterName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, logstore.GetConstant(version).FilterDataDiskSize)
	bloomFilter := bloomfilter.NewOneHitBloomFilter(bytes, version)
	for _, v := range contents {
		bloomFilter.Add(Hash([]byte(v)))
	}

	verticalPieceMemSize := logstore.GetConstant(version).VerticalPieceMemSize
	piece := make([]byte, logstore.GetConstant(version).VerticalPieceDiskSize)
	for i := 0; i < int(logstore.GetConstant(version).VerticalPieceCntPerFilter); i++ {
		src := binary.LittleEndian.Uint64(bytes[i*8 : i*8+8])
		for j := 0; j < int(logstore.GetConstant(version).FilterCntPerVerticalGorup); j++ {
			binary.LittleEndian.PutUint64(piece[j*8:j*8+8], src)
		}
		crc := crc32.Checksum(piece[0:verticalPieceMemSize], crc32.MakeTable(crc32.Castagnoli))
		binary.LittleEndian.PutUint32(piece[verticalPieceMemSize:verticalPieceMemSize+4], crc)
		verticalFilterFd.Write(piece)
	}
	verticalFilterFd.Sync()

	filterDataMemSize := logstore.GetConstant(version).FilterDataMemSize
	crc := crc32.Checksum(bytes[0:filterDataMemSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[filterDataMemSize:filterDataMemSize+4], crc)
	for i := 0; i < 1000; i++ {
		filterLogFd.Write(bytes)
	}
	filterLogFd.Sync()

	// write content
	fn := tmpDir + "/" + CONTENT_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	recordsBytes := make([]byte, RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(recordsBytes, 0)
	checkSum := crc32.Checksum(recordsBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(recordsBytes[8:12], checkSum)

	tagStr := "test_tag"
	length := TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (FLAG_TAGS << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(recordsBytes[12:16], uint32(flag))
	copy(recordsBytes[16:24], tagStr)

	offset := 24
	for k, v := range contents {
		content := v
		length := CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (FLAG_CONTENT << FLAG_HEAD_SHIFT) | (length & FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(recordsBytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(recordsBytes[offset+4:offset+8], uint32(12))
		binary.LittleEndian.PutUint64(recordsBytes[offset+8:offset+16], uint64(k))
		binary.LittleEndian.PutUint64(recordsBytes[offset+16:offset+24], uint64(k))
		copy(recordsBytes[offset+24:offset+24+len(content)], content)
		offset += length
	}
	compressed := make([]byte, MAX_BLOCK_LOG_STORE_N_BYTES)
	binary.LittleEndian.PutUint32(compressed[:4], uint32(offset))
	compressSize, err := lz4.CompressBlock(recordsBytes[0:offset], compressed[4:], make([]int, 64<<10))
	if err != nil {
		return fmt.Errorf("lz4 compress error")
	}
	checkSum = crc32.Checksum(compressed[:4+compressSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(compressed[4+compressSize:4+compressSize+4], checkSum)

	for i := 0; i < 10; i++ {
		fd.Write(compressed[:4+compressSize+4])
	}
	fd.Sync()

	offsets := make([]int64, 10)
	lengths := make([]int64, 10)
	for i := 0; i < 10; i++ {
		offsets[i] = int64(i * (4 + compressSize + 4))
		lengths[i] = int64(4 + compressSize + 4)
	}

	// write meta
	fn = tmpDir + "/" + META_NAME

	fd, err = fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}
	var contentLength uint32
	for _, v := range contents {
		contentLength += uint32(len(v))
	}
	bytes = make([]byte, META_STORE_N_BYTES)
	binary.LittleEndian.PutUint64(bytes[0:8], maxTimeStamp)
	binary.LittleEndian.PutUint64(bytes[8:16], minTimeStamp)
	binary.LittleEndian.PutUint64(bytes[16:24], maxSeq)
	binary.LittleEndian.PutUint64(bytes[24:32], minSeq)
	binary.LittleEndian.PutUint64(bytes[32:40], 0)
	binary.LittleEndian.PutUint32(bytes[40:44], uint32(compressSize+8))
	binary.LittleEndian.PutUint32(bytes[44:48], uint32(len(contents)))
	crc = crc32.Checksum(bytes[0:48], crc32CastagnoliTable)
	binary.LittleEndian.PutUint32(bytes[48:52], crc)

	fd.Write(bytes)
	fd.Sync()
	return nil
}

func TestSegmentReader(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	version := uint32(4)
	s, _ := NewSegmentReader(nil, tmpDir, version, util.TimeRange{Min: int64(0), Max: int64(100)}, &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}, nil, false)
	a, b, c := s.Next()
	assert.Equal(t, c, nil)
	assert.Equal(t, int(b), 0)
	for k, v := range a {
		assert.Equal(t, string(v[1]), contents[k])
	}
	a, b, c = s.Next()
	s.SetTr(util.TimeRange{Min: 1, Max: 5})
	num := s.GetRowCount()
	assert.Equal(t, int64(2), num)
	s.StartSpan(nil)
	s, _ = NewSegmentReader(nil, tmpDir+"err", version, util.TimeRange{Min: int64(0), Max: int64(100)}, &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}, nil, false)
	if s != nil {
		t.Error("get wrong reader")
	}
}

func TestSegmentReaderBySpan(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	version := uint32(4)
	s, _ := NewSegmentReader(nil, tmpDir, version, util.TimeRange{Min: int64(0), Max: int64(100)}, &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}, nil, false)
	_, span := tracing.NewTrace("root")
	s.StartSpan(span)
	a, b, c := s.Next()
	assert.Equal(t, c, nil)
	assert.Equal(t, int(b), 0)
	for k, v := range a {
		assert.Equal(t, string(v[1]), contents[k])
	}
}

func TestSegmentReaderByCache(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)

	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	version := uint32(4)
	for i := 0; i < 2; i++ {
		s, _ := NewSegmentReader(nil, tmpDir, version, util.TimeRange{Min: int64(0), Max: int64(100)}, &query.ProcessorOptions{
			Condition: &influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
				RHS: &influxql.StringLiteral{Val: "hello"},
			},
		}, nil, false)
		a, b, c := s.Next()
		assert.Equal(t, c, nil)
		assert.Equal(t, int(b), 0)
		for k, v := range a {
			assert.Equal(t, string(v[1]), contents[k])
		}
		s.UpdateTr(2)
		s.Close()
	}
}

func TestSegmentReaderByAscending(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)

	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents)
	if err != nil {
		t.Fatal("write data failed")
	}
	version := uint32(4)
	for i := 0; i < 2; i++ {
		s, _ := NewSegmentReader(nil, tmpDir, version, util.TimeRange{Min: int64(0), Max: int64(100)}, &query.ProcessorOptions{
			Condition: &influxql.BinaryExpr{
				Op:  influxql.MATCHPHRASE,
				LHS: &influxql.VarRef{Val: "content", Type: influxql.String},
				RHS: &influxql.StringLiteral{Val: "hello"},
			},
		}, nil, true)
		a, b, c := s.Next()
		assert.Equal(t, c, nil)
		assert.Equal(t, int(b), 0)
		for k, v := range a {
			assert.Equal(t, string(v[1]), contents[k])
		}
		s.UpdateTr(2)
		s.Close()
	}
}

func Hash(bytes []byte) uint64 {
	var hash uint64 = 0
	for _, b := range bytes {
		hash ^= bits.RotateLeft64(hash, 11) ^ (uint64(b) * logstore.Prime_64)
	}
	return hash
}
