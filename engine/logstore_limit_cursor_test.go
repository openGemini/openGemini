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
package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
	"os"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable/logstore"
	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	logstore2 "github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/pierrec/lz4/v4"
)

func TestLimitCursorReadByCond(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	opt := query.ProcessorOptions{
		LogQueryCurrId: "^^1",
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{},
		Ascending:  true,
		ChunkSize:  100,
		StartTime:  math.MinInt64,
		EndTime:    math.MaxInt64,
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "test", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)
	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	contents := []string{"hello", "hellos"}
	err = WriteSfsData(tmpDir, contents, false)
	if err != nil {
		t.Fatal("write data failed")
	}
	s, _ := NewLogStoreLimitCursor(nil, tmpDir, 4, ctx, nil, schema)
	data, _, err := s.Next()
	if err != nil {
		t.Error(err)
	}
	if data != nil {
		t.Error("get error data")
	}
}

func TestLimitCursorReadBySpan(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"hello", "hellos"}
	err := WriteSfsData(tmpDir, contents, false)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := query.ProcessorOptions{
		LogQueryCurrId: "^^1",
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{},
		Ascending:  true,
		ChunkSize:  100,
		StartTime:  math.MinInt64,
		EndTime:    math.MaxInt64,
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "test", Type: influxql.String},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)
	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	s, _ := NewLogStoreLimitCursor(nil, tmpDir, 4, ctx, nil, schema)
	_, span := tracing.NewTrace("root")
	s.StartSpan(span)
	data, _, err := s.Next()
	if err != nil {
		t.Error(err)
	}
	if data != nil {
		t.Error("get error data")
	}
}

func TestLimitCursorReadTimeIsSame(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}
	reverse := []bool{false, true}
	for _, v := range reverse {
		opt := query.ProcessorOptions{
			LogQueryCurrId: "^^1",
			Interval: hybridqp.Interval{
				Duration: 10 * time.Nanosecond,
			},
			Dimensions: []string{},
			Ascending:  v,
			ChunkSize:  100,
			StartTime:  math.MinInt64,
			EndTime:    math.MaxInt64,
			Limit:      2,
		}
		fields := make(influxql.Fields, 0, 2)
		fields = append(fields,
			&influxql.Field{
				Expr: &influxql.VarRef{
					Val:  "tag",
					Type: influxql.String,
				},
				Alias: "",
			},
			&influxql.Field{
				Expr: &influxql.VarRef{
					Val:  "content",
					Type: influxql.String,
				},
				Alias: "",
			},
		)
		schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)
		e := &Engine{}
		ctx, err := e.InitLogStoreCtx(schema)
		s, _ := NewLogStoreLimitCursor(nil, tmpDir, 4, ctx, nil, schema)
		data, _, err := s.Next()
		if err != nil {
			t.Error(err)
		}
		if data.RowNums() != 2 {
			t.Error("data is not right")
		}
	}
}

func TestLimitCursorReadWhenReverse(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}

	reverse := []bool{false, true}
	scrollid := []string{"0||0|2^^", "0||0|4^^", "0||0|0^^", "0|2|3|4^^", "0||-1|7^^", "0||1|6^^"}
	for _, r := range reverse {
		for _, id := range scrollid {
			opt := query.ProcessorOptions{
				Interval: hybridqp.Interval{
					Duration: 10 * time.Nanosecond,
				},
				Dimensions:     []string{},
				Ascending:      r,
				ChunkSize:      100,
				StartTime:      math.MinInt64,
				EndTime:        math.MaxInt64,
				Limit:          2,
				LogQueryCurrId: id,
			}
			fields := make(influxql.Fields, 0, 2)
			fields = append(fields,
				&influxql.Field{
					Expr: &influxql.VarRef{
						Val:  "tag",
						Type: influxql.String,
					},
					Alias: "",
				},
				&influxql.Field{
					Expr: &influxql.VarRef{
						Val:  "content",
						Type: influxql.String,
					},
					Alias: "",
				},
			)
			schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)
			e := &Engine{}
			ctx, err := e.InitLogStoreCtx(schema)
			s, _ := NewLogStoreLimitCursor(nil, tmpDir, 4, ctx, nil, schema)
			data, _, err := s.Next()
			if err != nil {
				t.Error(err)
			}
			if data.RowNums() > 2 {
				t.Error("data is not right")
			}
		}
	}
}

func WriteSfsData(tmpDir string, contents []string, isSameTime bool) error {
	maxTimeStamp := uint64(len(contents))
	minTimeStamp := uint64(0)
	maxSeq := uint64(len(contents))
	minSeq := uint64(0)
	//write filter
	verticalFilterName := tmpDir + "/" + logstore2.OBSVLMFileName
	filterLogName := tmpDir + "/" + logstore2.FilterLogName

	verticalFilterFd, err := fileops.OpenFile(verticalFilterName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, logstore2.GetConstant(4).FilterDataDiskSize)
	bloomFilter := bloomfilter.NewOneHitBloomFilter(bytes, 4)
	for _, v := range contents {
		bloomFilter.Add(Hash([]byte(v)))
	}

	verticalPieceMemSize := logstore2.GetConstant(4).VerticalPieceMemSize
	piece := make([]byte, logstore2.GetConstant(4).VerticalPieceDiskSize)
	for i := 0; i < int(logstore2.GetConstant(4).VerticalPieceCntPerFilter); i++ {
		src := binary.LittleEndian.Uint64(bytes[i*8 : i*8+8])
		for j := 0; j < int(logstore2.GetConstant(4).FilterCntPerVerticalGorup); j++ {
			binary.LittleEndian.PutUint64(piece[j*8:j*8+8], src)
		}
		crc := crc32.Checksum(piece[0:verticalPieceMemSize], crc32.MakeTable(crc32.Castagnoli))
		binary.LittleEndian.PutUint32(piece[verticalPieceMemSize:verticalPieceMemSize+4], crc)
		verticalFilterFd.Write(piece)
	}
	verticalFilterFd.Sync()

	filterDataMemSize := logstore2.GetConstant(4).FilterDataMemSize
	crc := crc32.Checksum(bytes[0:filterDataMemSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[filterDataMemSize:filterDataMemSize+4], crc)
	for i := 0; i < 1000; i++ {
		filterLogFd.Write(bytes)
	}
	filterLogFd.Sync()

	// write content
	fn := tmpDir + "/" + logstore.CONTENT_NAME

	fd, err := fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}

	recordsBytes := make([]byte, logstore.RECORD_DATA_MAX_N_BYTES)
	binary.LittleEndian.PutUint64(recordsBytes, 0)
	checkSum := crc32.Checksum(recordsBytes[:8], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(recordsBytes[8:12], checkSum)

	tagStr := "test_tag"
	length := logstore.TAGS_RECORD_PREFIX_N_BYTES + len(tagStr)
	flag := (logstore.FLAG_TAGS << logstore.FLAG_HEAD_SHIFT) | (length & logstore.FLAG_LENGTH_MASK)
	binary.LittleEndian.PutUint32(recordsBytes[12:16], uint32(flag))
	copy(recordsBytes[16:24], tagStr)

	offset := 24
	for k, v := range contents {
		content := v
		length := logstore.CONTENT_RECORD_PREFIX_N_BYTES + len(content)
		flag := (logstore.FLAG_CONTENT << logstore.FLAG_HEAD_SHIFT) | (length & logstore.FLAG_LENGTH_MASK)
		binary.LittleEndian.PutUint32(recordsBytes[offset:offset+4], uint32(flag))
		binary.LittleEndian.PutUint32(recordsBytes[offset+4:offset+8], uint32(12))
		if isSameTime {
			binary.LittleEndian.PutUint64(recordsBytes[offset+8:offset+16], uint64(0))

		} else {
			binary.LittleEndian.PutUint64(recordsBytes[offset+8:offset+16], uint64(k))
		}
		binary.LittleEndian.PutUint64(recordsBytes[offset+16:offset+24], uint64(k))
		copy(recordsBytes[offset+24:offset+24+len(content)], content)
		offset += length
	}
	compressed := make([]byte, logstore.MAX_BLOCK_LOG_STORE_N_BYTES)
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
	fn = tmpDir + "/" + logstore.META_NAME

	fd, err = fileops.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}
	var contentLength uint32
	for _, v := range contents {
		contentLength += uint32(len(v))
	}
	bytes = make([]byte, logstore.META_STORE_N_BYTES)
	binary.LittleEndian.PutUint64(bytes[0:8], maxTimeStamp)
	binary.LittleEndian.PutUint64(bytes[8:16], minTimeStamp)
	binary.LittleEndian.PutUint64(bytes[16:24], maxSeq)
	binary.LittleEndian.PutUint64(bytes[24:32], minSeq)
	binary.LittleEndian.PutUint64(bytes[32:40], 0)
	binary.LittleEndian.PutUint32(bytes[40:44], uint32(compressSize+8))
	binary.LittleEndian.PutUint32(bytes[44:48], uint32(len(contents)))
	crc = crc32.Checksum(bytes[0:48], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[48:52], crc)

	fd.Write(bytes)
	fd.Sync()
	return nil
}

func Hash(bytes []byte) uint64 {
	var hash uint64 = 0
	for _, b := range bytes {
		hash ^= bits.RotateLeft64(hash, 11) ^ (uint64(b) * logstore2.Prime_64)
	}
	return hash
}
