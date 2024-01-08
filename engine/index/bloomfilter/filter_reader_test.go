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
package bloomfilter

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestGetAllHashes(t *testing.T) {
	left := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content"},
			RHS: &influxql.StringLiteral{Val: "http"},
		},
	}
	right := &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "http1"},
	}
	root := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left,
		RHS: right,
	}
	tagSplit := make([]byte, 256)
	for _, c := range " \t," {
		tagSplit[c] = 1
	}
	contentSplit := make([]byte, 256)
	for _, c := range " \n\t`-=~!@#$%^&*()_+[]{}\\|;':\",.<>/?" {
		contentSplit[c] = 1
	}
	split := make(map[string][]byte)
	split["tag"] = tagSplit
	split["content"] = contentSplit
	filterReader := &FilterReader{
		hashes:   make(map[string][]uint64),
		splitMap: split,
	}
	filterReader.missSplitIndex = make(map[string]uint8)
	for s, v := range split {
		isTagsIndex := false
		for k := range v {
			if v[k] == 0 {
				isTagsIndex = true
				filterReader.missSplitIndex[s] = uint8(k)
				break
			}
		}
		if !isTagsIndex {
			panic("tags split table is full")
		}
	}
	filterReader.getAllHashes(root)
	assert.Equal(t, Hash([]byte("http")), filterReader.hashes["http"][0])
	assert.Equal(t, Hash([]byte("http1")), filterReader.hashes["http1"][0])
	left1 := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "tag"},
			RHS: &influxql.StringLiteral{Val: "http"},
		},
	}
	root1 := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left1,
		RHS: right,
	}
	filterReader.getAllHashes(root1)
	filterReader.getAllHashes(root1)
}

func TestReadVerticalFilter(t *testing.T) {
	version := uint32(0)
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	verticalFilterName := tmpDir + "/" + logstore.OBSVLMFileName
	filterLogName := tmpDir + "/" + logstore.FilterLogName

	verticalFilterFd, err := fileops.OpenFile(verticalFilterName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, logstore.GetConstant(version).FilterDataDiskSize)
	bloomFilter := bloomfilter.NewOneHitBloomFilter(bytes, version)
	bloomFilter.Add(Hash([]byte("hello")))
	bloomFilter.Add(Hash([]byte("world")))

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

	left := &influxql.ParenExpr{Expr: &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "hello"},
	},
	}
	right := &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "world"},
	}
	trueExpr := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left,
		RHS: right,
	}
	tagSpilt := make([]byte, 256)
	for _, c := range " \t," {
		tagSpilt[c] = 1
	}
	contentSpilt := make([]byte, 256)
	for _, c := range " \n\t`-=~!@#$%^&*()_+[]{}\\|;':\",.<>/?" {
		contentSpilt[c] = 1
	}
	spilt := make(map[string][]byte)
	spilt["tag"] = tagSpilt
	spilt["content"] = contentSpilt

	filterReader, _ := NewVerticalFilterReader(tmpDir, nil, trueExpr, version, spilt, logstore.OBSVLMFileName)
	for i := 0; i < 1024; i++ {
		isExist, _ := filterReader.isExist(int64(i))
		if !isExist {
			t.Errorf("filter get wrong")
		}
	}

	left = &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content"},
			RHS: &influxql.StringLiteral{Val: "hello1"},
		},
	}
	right = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "world1"},
	}
	falseExpr := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left,
		RHS: right,
	}
	filterReader, _ = NewVerticalFilterReader(tmpDir, nil, falseExpr, version, spilt, logstore.OBSVLMFileName)

	for i := 0; i < 1024; i++ {
		isExist, _ := filterReader.isExist(int64(i))
		assert.False(t, isExist)
	}
	filterReader.close()
}

func TestReadVerticalFilterErr(t *testing.T) {
	tagSpilt := make([]byte, 256)
	for _, c := range " \t," {
		tagSpilt[c] = 1
	}
	contentSpilt := make([]byte, 256)
	for _, c := range " \n\t`-=~!@#$%^&*()_+[]{}\\|;':\",.<>/?" {
		contentSpilt[c] = 1
	}
	spilt := make(map[string][]byte)
	spilt["tag"] = tagSpilt
	spilt["content"] = contentSpilt
	_, err := NewVerticalFilterReader("", nil, nil, 4, spilt, "")

	assert.False(t, false, IsInterfaceNil(err))
}

func IsInterfaceNil(value interface{}) bool {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		return val.IsNil()
	}

	if value == nil {
		return true
	}

	return false
}

func TestReadFilter(t *testing.T) {
	var version uint32
	version = 4
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	verticalFilterName := tmpDir + "/" + logstore.OBSVLMFileName
	filterLogName := tmpDir + "/" + logstore.FilterLogName

	verticalFilterFd, err := fileops.OpenFile(verticalFilterName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, logstore.GetConstant(version).FilterDataDiskSize)
	bloomFilter := bloomfilter.NewOneHitBloomFilter(bytes, version)
	bloomFilter.Add(Hash([]byte("hello")))
	bloomFilter.Add(Hash([]byte("world")))

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

	left := &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content"},
			RHS: &influxql.StringLiteral{Val: "hello"},
		},
	}
	right := &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "world"},
	}
	trueExpr := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left,
		RHS: right,
	}
	splitMap := make(map[string][]byte)
	splitMap["content"] = tokenizer.CONTENT_SPLIT_TABLE

	filterReader, _ := NewFilterReader(nil, trueExpr, splitMap, false, true, version, tmpDir, tmpDir, logstore.FilterLogName, logstore.OBSVLMFileName)
	for i := 0; i < 128; i++ {
		isExist, _ := filterReader.IsExist(int64(i))
		assert.True(t, isExist)
	}
	for i := 128; i < 128+1; i++ {
		isExist, _ := filterReader.IsExist(int64(i))
		assert.True(t, isExist)
	}

	left = &influxql.ParenExpr{
		Expr: &influxql.BinaryExpr{
			Op:  influxql.MATCHPHRASE,
			LHS: &influxql.VarRef{Val: "content"},
			RHS: &influxql.StringLiteral{Val: "hello1"},
		},
	}
	right = &influxql.BinaryExpr{
		Op:  influxql.MATCHPHRASE,
		LHS: &influxql.VarRef{Val: "content"},
		RHS: &influxql.StringLiteral{Val: "world1"},
	}
	falseExpr := &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: left,
		RHS: right,
	}

	filterReader, _ = NewFilterReader(nil, falseExpr, splitMap, false, true, 4, tmpDir, tmpDir, logstore.FilterLogName, logstore.OBSVLMFileName)
	for i := 0; i < 129; i++ {
		isExist, _ := filterReader.IsExist(int64(i))
		assert.False(t, isExist)
	}
	filterReader.Close()

	filterReader, _ = NewFilterReader(nil, nil, splitMap, false, true, 4, tmpDir, tmpDir, logstore.FilterLogName, logstore.OBSVLMFileName)
	assert.False(t, filterReader.isFilter)
	_, err = NewFilterReader(nil, falseExpr, splitMap, false, true, 4, tmpDir+"test", tmpDir+"test", logstore.FilterLogName, "test")
	if err == nil {
		t.Error("get wrong reader")
	}
}
