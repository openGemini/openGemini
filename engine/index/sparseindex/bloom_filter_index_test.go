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

package sparseindex_test

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"
	"os"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func TestBloomFilterIndexReader(t *testing.T) {
	tmpDir := t.TempDir()
	filterLogName := tmpDir + "/" + "00000001-0001-00000001.content.bf"

	filterLogFd, err := fileops.OpenFile(filterLogName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Errorf("open file failed: %s", err)
	}

	bytes := make([]byte, logstore.GetConstant(0).FilterDataDiskSize)
	bloomFilter := bloomfilter.NewOneHitBloomFilter(bytes, 0)
	bloomFilter.Add(Hash([]byte("hello")))
	bloomFilter.Add(Hash([]byte("world")))

	filterDataMemSize := logstore.GetConstant(0).FilterDataMemSize
	crc := crc32.Checksum(bytes[0:filterDataMemSize], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(bytes[filterDataMemSize:filterDataMemSize+4], crc)
	for i := 0; i < 1000; i++ {
		filterLogFd.Write(bytes)
	}
	filterLogFd.Sync()

	schema := record.Schemas{{Name: "content", Type: influx.Field_Type_String}}
	option := &query.ProcessorOptions{}
	reader, err := sparseindex.NewBloomFilterIndexReader(schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	dataFile := &MockTssp{path: tmpDir + "/00000001-0001-00000001.tssp"}

	assert.Equal(t, reader.ReInit(dataFile), nil)
	ok, err := reader.MayBeInFragment(0)
	assert.Equal(t, ok, true)
}

type MockTssp struct {
	path string
}

func (f *MockTssp) Path() string {
	return f.path
}

func (f *MockTssp) Name() string {
	return ""
}

func Hash(bytes []byte) uint64 {
	var hash uint64 = 0
	for _, b := range bytes {
		hash ^= bits.RotateLeft64(hash, 11) ^ (uint64(b) * logstore.Prime_64)
	}
	return hash
}
