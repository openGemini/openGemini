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

package executor_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/bufferpool"
	"github.com/openGemini/openGemini/lib/codec/gen"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func TestChunkCodec(t *testing.T) {
	chunk := makeChunk().(*executor.ChunkImpl)
	var err error
	pc := make([]byte, 0, chunk.Size())

	pc, err = chunk.Marshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(pc) != chunk.Size() {
		t.Fatalf("error size, exp: %d; got: %d", len(pc), chunk.Size())
	}

	fmt.Println("Protobuf marshal: chunk buffer length", len(pc))

	var columns []*executor.ColumnImpl
	for _, c := range chunk.Columns() {
		columns = append(columns, c.(*executor.ColumnImpl))
	}

	newChunk := &executor.ChunkImpl{}
	err = newChunk.Unmarshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if len(columns) != len(newChunk.Columns()) {
		t.Fatalf("marshal columns failed. diff length")
	}

	for i, c := range newChunk.Columns() {
		newColumn := c.(*executor.ColumnImpl)
		if !reflect.DeepEqual(columns[i], newColumn) {
			t.Fatalf("marshal columns failed. diff column: %d", i)
		}
	}

	if !compareTagsSlice(chunk.Tags(), newChunk.Tags()) {
		t.Fatalf("marshal tags failed.")
	}

	if !reflect.DeepEqual(chunk.TagIndex(), newChunk.TagIndex()) {
		t.Fatalf("marshal tagIndex failed.")
	}

	if !reflect.DeepEqual(chunk.Time(), newChunk.Time()) {
		t.Fatalf("marshal time failed.")
	}

	if !reflect.DeepEqual(chunk.IntervalIndex(), newChunk.IntervalIndex()) {
		t.Fatalf("marshal intervalIndex failed.")
	}
}

// Runs only in the development phase
func TestGenTrunk(t *testing.T) {
	if os.Getenv("GO_DEV") != "true" {
		return
	}

	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "value1", Type: influxql.Float},
		influxql.VarRef{Val: "value2", Type: influxql.Float},
	)
	cb := executor.NewChunkBuilder(rowDataType)
	obj := cb.NewChunk("mst")
	obj.SetColumn(executor.NewColumnImpl(influxql.Float), 0)

	g := gen.NewCodecGen("executor")
	g.Exclude("hybridqp.RowDataType", "record.Record")
	g.Gen(obj)

	_, f, _, _ := runtime.Caller(0)
	g.SaveTo(filepath.Dir(f) + "/chunk_codec.gen.go")
}

func compareTagsSlice(a, b []executor.ChunkTags) bool {
	l := len(a)
	if len(b) != l {
		return false
	}

	for i := 0; i < l; i++ {
		if !compareTags(a[i], b[i]) {
			return false
		}
	}
	return true
}

func compareTags(a, b executor.ChunkTags) bool {
	if bytes.Compare(a.Subset(nil), b.Subset(nil)) != 0 {
		return false
	}

	return true
}

func makeChunk() executor.Chunk {
	rdt := buildRowDataType()
	builder := executor.NewChunkBuilder(rdt)
	chunk := builder.NewChunk("test")

	for i := 0; i < 10; i++ {
		tags := ParseChunkTags(fmt.Sprintf("id=A01,tid=%02d,name=sandy,school=xxx", i))
		chunk.AddTagAndIndex(*tags, i)
	}

	chunk.AddIntervalIndex(1)
	chunk.AddIntervalIndex(2)

	c0 := chunk.Column(0)
	c1 := chunk.Column(1)
	c2 := chunk.Column(2)
	c3 := executor.NewColumnImpl(influxql.Boolean)

	for i := 0; i < 4096; i++ {
		chunk.AppendTime(time.Now().UnixNano())
		c0.AppendIntegerValues(int64(i))
		c1.AppendStringValues(fmt.Sprintf("tid_%04d", i))
		c2.AppendFloatValues(float64(i) * 78.67)
		c3.AppendBooleanValues(i%2 == 0)

		c0.AppendNilsV2(true)
		c1.AppendNilsV2(true)
		c2.AppendNilsV2(true)
		c3.AppendNilsV2(true)
	}

	for i := 0; i < 50; i++ {
		chunk.AppendTime(time.Now().UnixNano())
		c0.AppendIntegerValues(int64(i))
		c0.AppendNilsV2(true)

		if i%2 == 0 {
			c1.AppendStringValues(fmt.Sprintf("tid_%04d", i))
			c1.AppendNilsV2(true)
		} else {
			c1.AppendNil()
		}

		if i%3 == 0 {
			c2.AppendFloatValues(float64(i) * 78.67)
			c3.AppendBooleanValues(i%2 == 0)
			c2.AppendNilsV2(true)
			c3.AppendNilsV2(true)
		} else {
			c2.AppendNil()
			c3.AppendNil()
		}
	}

	chunk.AddColumn(c3)

	return chunk
}

func BenchmarkMarshal(b *testing.B) {
	chunk := makeChunk()

	b.ResetTimer()
	pool := bufferpool.NewByteBufferPool(0)
	var err error

	for i := 0; i < b.N; i++ {
		//buf := make([]byte, 0, chunk.(*executor.ChunkImpl).Size())
		buf := pool.Get()
		bufferpool.Resize(buf, chunk.(*executor.ChunkImpl).Size())
		buf, err = chunk.(*executor.ChunkImpl).Marshal(buf[:0])
		if err != nil {
			b.Fatalf("%v", err)
		}
		pool.Put(buf)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	chunk := makeChunk()
	var pc []byte
	pc, _ = chunk.(*executor.ChunkImpl).Marshal(pc)
	bs := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		bs[i] = make([]byte, len(pc))
		copy(bs[i], pc)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		newChunk := &executor.ChunkImpl{}
		newChunk.Unmarshal(bs[i])
	}
}

func BenchmarkMarshalUnmarshal(b *testing.B) {
	chunk := makeChunk()

	b.ResetTimer()
	pool := bufferpool.NewByteBufferPool(0)

	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		bufferpool.Resize(buf, chunk.(*executor.ChunkImpl).Size())

		buf, _ = chunk.(*executor.ChunkImpl).Marshal(buf[:0])
		newChunk := &executor.ChunkImpl{}
		newChunk.Unmarshal(buf)
		pool.Put(buf)
	}
}
