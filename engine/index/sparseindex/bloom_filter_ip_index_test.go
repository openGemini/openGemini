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

package sparseindex

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestBloomFilterIpIndexWriter(t *testing.T) {
	rec := record.NewRecord(record.Schemas{{Name: "srcIp", Type: influx.Field_Type_String}}, false)
	rec.ColVals[0].AppendStrings("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4")

	bloomFilterWriter := NewBloomFilterIpWriter("", "", "", "", tokenizer.CONTENT_SPLITTER)
	bloomFilterWriter.Open()
	defer bloomFilterWriter.Close()

	err := bloomFilterWriter.CreateAttachIndex(rec, []int{0}, []int{4})
	if err != nil {
		t.Fatal(err)
	}
	bloomFilterWriter.CreateDetachIndex(rec, []int{0}, []int{4}, nil)

	bytes := bloomFilterWriter.GenBloomFilterData(&rec.ColVals[0], []int{2, 2}, influx.Field_Type_String)

	segBfSize := int(logstore.GetConstant(2).FilterDataDiskSize)
	assert.Equal(t, 2*segBfSize, len(bytes))

	crc := crc32.Checksum(bytes[0:segBfSize-4], crc32.MakeTable(crc32.Castagnoli))
	assert.Equal(t, crc, binary.LittleEndian.Uint32(bytes[segBfSize-4:segBfSize]))

	bf := bloomfilter.NewOneHitBloomFilter(bytes[0:segBfSize-4], 3)
	ipTokenizer := tokenizer.NewIpTokenizer()
	defer tokenizer.FreeSimpleGramTokenizer(ipTokenizer)
	ipTokenizer.InitInput([]byte("1.1.1.1"))
	ipTokenizer.Next()
	assert.True(t, bf.Hit(ipTokenizer.CurrentHash()))
	ipTokenizer.InitInput([]byte("5.5.5.5"))
	ipTokenizer.Next()
	assert.False(t, bf.Hit(ipTokenizer.CurrentHash()))

	bf = bloomfilter.NewOneHitBloomFilter(bytes[segBfSize:2*segBfSize-4], 3)
	ipTokenizer.InitInput([]byte("1.1.1.1"))
	ipTokenizer.Next()
	assert.False(t, bf.Hit(ipTokenizer.CurrentHash()))
	ipTokenizer.InitInput([]byte("3.3.3.3"))

	count := 0
	for ipTokenizer.Next() {
		assert.True(t, bf.Hit(ipTokenizer.CurrentHash()))
		count++
	}
	assert.Equal(t, 4, count)
}
