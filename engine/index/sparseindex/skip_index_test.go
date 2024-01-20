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
	"testing"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/bloomfilter"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/tokenizer"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestSKIndexReader(t *testing.T) {
	dataFile := "00000001-0001-00000001.tssp"
	reader := sparseindex.NewSKIndexReader(2, 2, 0)
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	mstInfo := &influxql.Measurement{IndexRelation: &influxql.IndexRelation{
		Oids:       []uint32{uint32(tsi.MinMax)},
		IndexNames: []string{colstore.MinMaxIndex},
		IndexList:  []*influxql.IndexList{{IList: []string{"value"}}},
	}}
	readers, err := reader.CreateSKFileReaders(option, mstInfo, true)
	assert.Equal(t, err, nil)

	var frs = fragment.FragmentRanges{{Start: 0, End: 2}}
	for i := range readers {
		readers[i].(*sparseindex.MinMaxIndexReader).ReadFunc = MinMaxIndexDataRead
		if err = readers[i].ReInit(dataFile); err != nil {
			t.Fatal(err)
		}
		frs, err = reader.Scan(readers[i], frs)
		assert.Equal(t, err, nil)
	}
	assert.Equal(t, len(frs), 1)
	assert.Equal(t, reader.Close(), nil)
}

func TestSKBFIndexReader(t *testing.T) {
	reader := sparseindex.NewSKIndexReader(2, 2, 0)
	option := &query.ProcessorOptions{Condition: &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: &influxql.VarRef{Val: "value", Type: influxql.Integer},
		RHS: &influxql.IntegerLiteral{Val: 2},
	}}
	mstInfo := &influxql.Measurement{IndexRelation: &influxql.IndexRelation{
		Oids:       []uint32{uint32(tsi.BloomFilterFullText)},
		IndexNames: []string{colstore.BloomFilterFullTextIndex},
		IndexList:  []*influxql.IndexList{{IList: []string{"value"}}},
	}}
	r, err := reader.CreateSKFileReaders(option, mstInfo, true)
	assert.Equal(t, err, nil)
	err = r[0].Close()
	assert.Equal(t, err, nil)
}

func TestCreateSkipIndex(t *testing.T) {
	var colVal record.ColVal
	colVal.AppendStrings("hello", "world", "hello", "test")
	index := &sparseindex.BloomFilterImpl{}
	bytes, err := index.CreateSkipIndex(&colVal, []int{2, 2}, influx.Field_Type_String)

	segBfSize := int(logstore.GetConstant(2).FilterDataDiskSize)
	assert.Nil(t, err)
	assert.Equal(t, 2*segBfSize, len(bytes))

	crc := crc32.Checksum(bytes[0:segBfSize-4], crc32.MakeTable(crc32.Castagnoli))
	assert.Equal(t, crc, binary.LittleEndian.Uint32(bytes[segBfSize-4:segBfSize]))

	bf := bloomfilter.NewOneHitBloomFilter(bytes[0:segBfSize-4], 3)
	assert.True(t, bf.Hit(tokenizer.Hash([]byte("hello"))))
	assert.True(t, bf.Hit(tokenizer.Hash([]byte("world"))))
	assert.False(t, bf.Hit(tokenizer.Hash([]byte("test"))))

	bf = bloomfilter.NewOneHitBloomFilter(bytes[segBfSize:2*segBfSize-4], 3)
	assert.True(t, bf.Hit(tokenizer.Hash([]byte("hello"))))
	assert.True(t, bf.Hit(tokenizer.Hash([]byte("test"))))
	assert.False(t, bf.Hit(tokenizer.Hash([]byte("world"))))
}
