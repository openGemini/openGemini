/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package immutable

import (
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func TestAppendDataBlock(t *testing.T) {
	bs := 2048
	mb := &MemBlock{blockSize: int64(bs)}
	mb.ReserveDataBlock(2)

	srcData := make([]byte, bs)

	mb.AppendDataBlock(srcData)
	chunk := mb.data[len(mb.data)-1]
	if len(chunk) != bs && len(mb.data) > 0 {
		t.Fatalf("chunk len:%v != %v or len(mb.data):%v > 0", len(chunk), bs, len(mb.data))
	}

	chunk[0] = 1
	mb.AppendDataBlock(srcData[:1024])
	if len(mb.data) != 2 {
		t.Fatalf("len(mb.data) != 1")
	}

	chunk = mb.data[len(mb.data)-1]
	if len(chunk) != len(srcData[:1024]) {
		t.Fatalf("len(chunk) != 1024")
	}

	mb.AppendDataBlock(srcData)
	chunk = mb.data[len(mb.data)-1]
	if len(chunk) != 1024 && len(mb.data) != 3 {
		t.Fatalf("len(chunk) != 1024 && len(mb.data) != 2")
	}
}

func TestSearchChunkMetaBlock(t *testing.T) {
	type input struct {
		metaIdx int
		sid     uint64
		find    uint64
	}
	mb := &MemBlock{}
	getChunkMetaBlocks(mb, 10)
	/*
		0: 1==>10		1: 11==>20
		2: 21==>30		3: 31==>40
		4: 41==>50		5: 51==>60
		6: 61==>70		7: 71==>80
		8: 81==>90		9: 91==>100
	*/
	inputs := []input{
		{0, 1, 1},

		{-1, 1, 1},
		{-1, 10, 1},
		{-1, 2, 1},
		{-1, 55, 51},
		{-1, 95, 91},
		{-1, 101, 0},
	}

	for _, in := range inputs {
		blk := mb.ReadChunkMetaBlock(in.metaIdx, in.sid, 10)
		if in.find > 0 {
			if len(blk) == 0 {
				t.Fatalf("chunk meta not find, metainex:%v, sid:%v", in.metaIdx, in.sid)
			}

			id := numberenc.UnmarshalUint64(blk)
			if id != in.find {
				t.Fatalf("id(%v) != in.sid(%v)", id, in.sid)
			}
		} else {
			if len(blk) > 0 {
				t.Fatalf("sid %v should not find in metablocks", in.sid)
			}
		}
	}
}

func getChunkMetaBlocks(mb *MemBlock, n int) {
	mb.ReserveMetaBlock(n)

	arr := []int64{10, 90, 100, 10, 1110000, 9990000}
	agg := record.Int64Slice2byte(arr[:])
	sid := uint64(1)

	cm := ChunkMeta{}
	cm.resize(2, 1)
	cm.sid = sid
	cm.columnCount = 2
	cm.segCount = 1
	cm.timeRange[0][0] = 1110000
	cm.timeRange[0][1] = 9990000
	cm.colMeta[0].name = "field1"
	cm.colMeta[0].ty = influx.Field_Type_Int
	cm.colMeta[0].preAgg = append(cm.colMeta[0].preAgg[:0], agg...)
	cm.colMeta[0].entries[0].offset = 10
	cm.colMeta[0].entries[0].size = 10

	cm.colMeta[1].name = "time"
	cm.colMeta[1].ty = influx.Field_Type_Int
	cm.colMeta[1].preAgg = numberenc.MarshalUint32Append(cm.colMeta[0].preAgg[:0], 10)
	cm.colMeta[1].entries[0].offset = 20
	cm.colMeta[1].entries[0].size = 10

	mOfs := make([]uint32, 0, 10)
	for i := 0; i < n; i++ {
		mOfs = mOfs[:0]
		mBlock := make([]byte, 0, 1024)
		for j := 0; j < 10; j++ {
			cm.sid = sid
			mOfs = append(mOfs, uint32(len(mBlock)))
			mBlock = cm.marshal(mBlock)
			sid++
		}
		ofs := record.Uint32Slice2ByteBigEndian(mOfs)
		mBlock = append(mBlock, ofs...)
		mb.chunkMetas = append(mb.chunkMetas, mBlock)
	}
}

func TestLoadDataBlock(t *testing.T) {
	fn := "/tmp/test_load.tssp"
	defer fileops.RemoveAll(fn)

	fd, _ := fileops.Create(fn)
	defer fd.Close()
	_, _ = fd.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	_ = fd.Truncate(4*1024*1024 + 1024)
	fi, _ := fd.Stat()
	lockPath := ""
	dr := NewDiskFileReader(fd, &lockPath)
	defer dr.Close()

	tr := &Trailer{dataOffset: 16, dataSize: fi.Size() - 16}
	mb := &MemBlock{}

	if err := mb.loadDataBlock(dr, tr); err != nil {
		t.Fatal(err)
	}

	dSize := 0
	for _, blk := range mb.data {
		dSize += len(blk)
	}
	if int64(dSize) != fi.Size() {
		t.Fatalf("load data block fail, load size:%v, exp:%v", dSize, fi.Size())
	}
}

func TestReadDataBlock(t *testing.T) {
	mb := &MemBlock{}
	mb.ReserveDataBlock(3)

	for i := 0; i < 3; i++ {
		buf := getDataBlockBuffer(10)
		buf = buf[:cap(buf)]
		mb.data = append(mb.data, buf)
	}
	mb.data[2] = mb.data[2][:512]

	blkSize := len(mb.data[0])
	mb.blockSize = int64(blkSize)

	var rBuf []byte

	off := mb.blockSize*3 + 1
	rb, err := mb.ReadDataBlock(off, 10, &rBuf)
	if err == nil || len(rb) > 0 {
		t.Fatalf("offset:%v is bigger than file size", off)
	}

	off = 10
	size := mb.blockSize - off - 50
	rb, err = mb.ReadDataBlock(off, uint32(size), &rBuf)
	if err != nil || len(rb) != int(size) {
		t.Fatalf("read data block fail, err:%v, read size:%v, exp size:%v", err, len(rb), size)
	}

	size = mb.blockSize - off + 50
	rb, err = mb.ReadDataBlock(off, uint32(size), &rBuf)
	if err != nil || len(rb) != int(size) {
		t.Fatalf("read data block fail, err:%v, read size:%v, exp size:%v", err, len(rb), size)
	}

	off = mb.blockSize*2 + 12
	size = 500
	rb, err = mb.ReadDataBlock(off, uint32(size), &rBuf)
	if err != nil || len(rb) != int(size) {
		t.Fatalf("read data block fail, err:%v, read size:%v, exp size:%v", err, len(rb), size)
	}
}
