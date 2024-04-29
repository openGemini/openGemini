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
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestWriteDetachedData local bf is not exist
func TestCheckDetachedFiles(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(1024)
	immutable.SetDetachedFlushEnabled(true)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:        sortKey,
			PrimaryKey:     primaryKey,
			CompactionType: config.BLOCK,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	sh.SetClient(&MockMetaClient{
		mstInfo: []*meta.MeasurementInfo{mstsInfo[defaultMeasurementName]},
	})
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	mutable.SetWriteChunk(msInfo, rec)
	// wait mem table flush
	sh.commitSnapshot(sh.activeTbl)

	require.Equal(t, 4*100, int(sh.rowCount))
	detachedInfo := NewDetachedMetaInfo()
	err = detachedInfo.checkAndTruncateDetachedFiles(sh.filesPath, defaultMeasurementName, bfColumn)
	if err != nil {
		t.Fatal(err)
	}

	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

// TestWriteDetachedDataV2 local bf exist, and flush multi times
func TestTestCheckDetachedFilesV2(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	immutable.SetDetachedFlushEnabled(true)
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	// step2: write data
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    sortKey,
			PrimaryKey: primaryKey,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	sh.SetMstInfo(mstsInfo[defaultMeasurementName])
	sh.SetClient(&MockMetaClient{
		mstInfo: []*meta.MeasurementInfo{mstsInfo[defaultMeasurementName]},
	})
	err = sh.WriteRows(rows, nil)
	msInfo, err := sh.activeTbl.GetMsInfo(defaultMeasurementName)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 1)
	flushTimes := 3
	rec := msInfo.GetRowChunks().GetWriteChunks()[0].WriteRec.GetRecord()
	for i := 0; i < flushTimes; i++ {
		mutable.SetWriteChunk(msInfo, rec)
		sh.commitSnapshot(sh.activeTbl)
		detachedInfo := NewDetachedMetaInfo()
		err = detachedInfo.checkAndTruncateDetachedFiles(sh.filesPath, defaultMeasurementName, bfColumn)
		if err != nil {
			t.Fatal(err)
		}
	}
	require.Equal(t, 4*100, int(sh.rowCount))
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckMetaIndexFile(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	err := detachedInfo.checkAndTruncateDetachedFiles("unknownPath", "cpu", nil)
	if err == nil {
		t.Fatal("should return open metaIndex file path error")
	}

	//test fileInfo.Size() < immutable.MetaIndexHeaderSize+immutable.MetaIndexItemSize
	err = detachedInfo.checkAndTruncateMetaIndexFile(testDir)
	if err != nil {
		t.Fatal(err)
	}

	//test fileInfo.Size() > immutable.MetaIndexHeaderSize+immutable.MetaIndexItemSize
	data := make([]byte, immutable.MetaIndexHeaderSize)
	data = numberenc.MarshalUint32Append(data, 0) //crc32
	buf := make([]byte, immutable.MetaIndexItemSize+1)
	data = append(data, buf...)
	crc := crc32.ChecksumIEEE(data[immutable.MetaIndexHeaderSize+CRCLen : immutable.MetaIndexHeaderSize+immutable.MetaIndexItemSize+CRCLen])
	numberenc.MarshalUint32Copy(data[immutable.MetaIndexHeaderSize:immutable.MetaIndexHeaderSize+CRCLen], crc)
	fd, err := fileops.OpenObsFile(testDir, immutable.MetaIndexFile, nil, false)
	if err != nil {
		log.Error("open detached metaIndex file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()
	_, err = fd.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncateMetaIndexFile(testDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestCheckChunkMetaFile(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	err := detachedInfo.checkAndTruncateChunkMeta("unknownPath")
	if err == nil {
		t.Fatal("should return open chunkMeta file path error")
	}

	//test cmFileSize = 0
	err = detachedInfo.checkAndTruncateChunkMeta(testDir)
	if err != nil {
		t.Fatal(err)
	}

	//test there is some dirty data in chunkMeta, so truncate file
	detachedInfo.lastMetaIdxOff = 0
	detachedInfo.lastMetaIdxSize = uint32(CRCLen + util.Uint64SizeBytes*2 + util.Uint32SizeBytes)
	buf := make([]byte, detachedInfo.lastMetaIdxSize+10)
	fd, err := fileops.OpenObsFile(testDir, immutable.ChunkMetaFile, nil, false)
	if err != nil {
		log.Error("open detached metaIndex file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncateChunkMeta(testDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestCheckDataFile(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	err := detachedInfo.checkAndTruncateDataFile("unknownPath")
	if err == nil {
		t.Fatal("should return open data file path error")
	}

	buf := make([]byte, 10)
	fd, err := fileops.OpenObsFile(testDir, immutable.DataFile, nil, false)
	if err != nil {
		log.Error("open detached data file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncateDataFile(testDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestCheckPkMetaIndexFile(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	err := detachedInfo.checkAndTruncatePkMetaIdxFile("unknownPath")
	if err == nil {
		t.Fatal("should return open pk meta file path error")
	}

	//test fileInfo.Size() < immutable.PKMetaInfoLength+int64(util.Uint32SizeBytes)
	err = detachedInfo.checkAndTruncatePkMetaIdxFile(testDir)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, immutable.PKMetaPrefixSize-util.Uint32SizeBytes)
	fd, err := fileops.OpenObsFile(testDir, immutable.PrimaryMetaFile, nil, false)
	if err != nil {
		log.Error("open detached data file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncatePkMetaIdxFile(testDir)
	if err != nil {
		t.Fatal(err)
	}

	buf = make([]byte, immutable.PKMetaPrefixSize+util.Uint32SizeBytes*2)
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	detachedInfo.lastMetaIdxBlockId = 1
	err = detachedInfo.checkAndTruncatePkMetaIdxFile(testDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestCheckPkIndexFile(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	err := detachedInfo.checkAndTruncatePkIdxFile("unknownPath")
	if err == nil {
		t.Fatal("should return open pk index file path error")
	}

	//test fileInfo.Size() > dataFileSize
	buf := make([]byte, 10)
	fd, err := fileops.OpenObsFile(testDir, immutable.PrimaryKeyFile, nil, false)
	if err != nil {
		log.Error("open detached pk index file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = detachedInfo.checkAndTruncatePkIdxFile(testDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestCheckBloomFilterFiles(t *testing.T) {
	testDir := t.TempDir()
	detachedInfo := NewDetachedMetaInfo()
	bfCols := []string{"tags"}
	err := detachedInfo.checkAndTruncateBfFiles("unknownPath", "", bfCols)
	if err == nil {
		t.Fatal("should return open bloom filter file path error")
	}
	constant := logstore.GetConstant(logstore.CurrentLogTokenizerVersion)
	buf := make([]byte, constant.FilterDataDiskSize*2)
	lock := fileops.FileLockOption("")
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	filePath := sparseindex.GetBloomFilterFilePath(testDir, "", bfCols[0])

	fd, err := fileops.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("open detached bloom filter file fail", zap.String("name", testDir), zap.Error(err))
		t.Fatal(err)
	}
	defer func() {
		_ = fd.Close()
	}()

	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncateBfFiles(testDir, "", bfCols)
	if err != nil {
		t.Fatal(err)
	}

	detachedInfo.lastPkMetaEndBlockId = 3
	_, err = fd.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	err = detachedInfo.checkAndTruncateBfFiles(testDir, "", bfCols)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.RemoveAll(testDir)
}

func TestLoadProcess(t *testing.T) {
	testDir := t.TempDir()
	_ = os.RemoveAll(testDir)
	// step1: create shard
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}

	detachedInfo := NewDetachedMetaInfo()
	primaryKey := []string{"time"}
	sortKey := []string{"time"}
	list := make([]*influxql.IndexList, 1)
	bfColumn := []string{"field1_string"}
	iList := influxql.IndexList{IList: bfColumn}
	list[0] = &iList
	mstsInfo := make(map[string]*meta.MeasurementInfo)
	mstsInfo[defaultMeasurementName] = &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    sortKey,
			PrimaryKey: primaryKey,
		},
		IndexRelation: influxql.IndexRelation{IndexNames: []string{"bloomfilter"},
			Oids:      []uint32{uint32(index.BloomFilter)},
			IndexList: list},
	}

	_ = os.Mkdir(filepath.Join(sh.filesPath, defaultMeasurementName), 0750)
	err = checkAndTruncateDetachedFiles(detachedInfo, mstsInfo[defaultMeasurementName], sh)
	if err != nil {
		t.Fatal(err)
	}
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}
