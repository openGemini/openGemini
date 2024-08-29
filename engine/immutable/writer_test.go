// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func cleanDir(dir string) {
	_ = fileops.RemoveAll(dir)
}

func getChunkMeta() *ChunkMeta {
	shemas := record.Schemas{
		record.Field{Name: "f1_Int", Type: influx.Field_Type_Int},
		record.Field{Name: "f2_Float", Type: influx.Field_Type_Float},
		record.Field{Name: "f3_String", Type: influx.Field_Type_String},
		record.Field{Name: "f4_Boolean", Type: influx.Field_Type_Boolean},
		record.Field{Name: "time", Type: influx.Field_Type_Int},
	}
	sort.Sort(shemas[:len(shemas)-1])
	intArr := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	floatArr := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	strArr := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
	boolArr := []bool{true, false, true, false, true, false, true, false, true, false, true, false, true, false, true}

	var offset int64 = 16
	ab := newPreAggBuilders()
	var col record.ColVal
	var cm ChunkMeta
	cm.resize(shemas.Len(), 2)
	for i, ref := range shemas {
		m := &cm.colMeta[i]
		m.name = ref.Name
		m.ty = byte(ref.Type)
		col.Init()
		switch ref.Type {
		case influx.Field_Type_Int:
			col.AppendIntegers(intArr...)
			if ref.Name == "time" {
				ab.timeBuilder.addValues(&col, intArr)
				m.preAgg = ab.timeBuilder.marshal(nil)
			} else {
				ab.intBuilder.addValues(&col, intArr)
				m.preAgg = ab.intBuilder.marshal(nil)
			}
		case influx.Field_Type_Float:
			col.AppendFloats(floatArr...)
			ab.floatBuilder.addValues(&col, intArr)
			m.preAgg = ab.floatBuilder.marshal(nil)
		case influx.Field_Type_String:
			col.AppendStrings(strArr...)
			ab.stringBuilder.addValues(&col, intArr)
			m.preAgg = ab.stringBuilder.marshal(nil)
		case influx.Field_Type_Boolean:
			col.AppendBooleans(boolArr...)
			ab.boolBuilder.addValues(&col, intArr)
			m.preAgg = ab.boolBuilder.marshal(nil)
		}

		tm := &cm.timeRange[0]
		tm.setMinTime(1)
		tm.setMaxTime(9)
		tm = &cm.timeRange[1]
		tm.setMinTime(10)
		tm.setMaxTime(15)
		m.entries[0].setOffset(offset)
		m.entries[0].setSize(32)
		offset += 32
		m.entries[1].setOffset(offset)
		m.entries[1].setSize(32)
		offset += 32
	}
	cm.sid = 1
	cm.offset = 16
	cm.size = uint32(offset - 16)
	cm.columnCount = uint32(len(cm.colMeta))
	cm.segCount = uint32(len(cm.timeRange))
	return &cm
}

func TestIndexWriterWithoutCacheMeta(t *testing.T) {
	cleanDir(testDir)
	defer cleanDir(testDir)
	_ = fileops.MkdirAll(testDir, 0750)
	fn := filepath.Join(testDir, "index.data")
	lockPath := ""
	InitWriterPool(8)
	wr := NewPKIndexWriter(fn, false, false, &lockPath)
	cm := getChunkMeta()

	meta := cm.marshal(nil)
	var written int
	n, err := wr.Write(meta)
	if err != nil {
		t.Fatal(err)
	}
	written += n

	if !wr.allInBuffer() {
		t.Fatal("meta data should be all in buffer")
	}

	bb := &bytes.Buffer{}
	n, err = wr.CopyTo(bb)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(meta) {
		t.Fatal("copy meta fail")
	}

	for wr.allInBuffer() {
		n, err = wr.Write(meta)
		if err != nil {
			t.Fatal(err)
		}
		written += n
	}

	n, err = wr.Write(meta)
	if err != nil {
		t.Fatal(err)
	}
	written += n

	if _, err = fileops.Stat(fn); err != nil {
		t.Fatal(err)
	}

	bb.Reset()
	n, err = wr.CopyTo(bb)
	if err != nil {
		t.Fatal(err)
	}
	if n != written {
		t.Fatalf("exp copy size:%v, get:%v", wr.Size(), n)
	}

	if written != wr.Size() {
		t.Fatalf("written size fail, exp:%v, get:%v", written, wr.Size())
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = fileops.Stat(fn); err == nil {
		t.Fatalf("file(%v) not delete", fn)
	}
}

func TestIndexWriterWithCacheMeta(t *testing.T) {
	bufSize := 256 * 1024
	cleanDir(testDir)
	defer cleanDir(testDir)
	_ = fileops.MkdirAll(testDir, 0750)
	fn := filepath.Join(testDir, "index.data")
	lockPath := ""
	InitWriterPool(8)
	wr := NewPKIndexWriter(fn, true, true, &lockPath).(*indexWriter)
	cm := getChunkMeta()

	meta := cm.marshal(nil)
	var written int

	for len(wr.metas) < 10 {
		for len(wr.buf) < bufSize {
			n, err := wr.Write(meta)
			if err != nil {
				t.Fatal(err)
			}
			written += n
		}
		wr.SwitchMetaBuffer()
	}

	n, _ := wr.Write(meta)
	written += n

	if _, err := fileops.Stat(fn); err == nil {
		t.Fatal("should not create file")
	}

	blks := wr.MetaDataBlocks(nil)
	if len(blks) < 10 {
		t.Fatal("invalid meta blocks")
	}
	wr.metas = append(wr.metas[:0], blks...)

	bb := &bytes.Buffer{}
	n, err := wr.CopyTo(bb)
	if err != nil {
		t.Fatal(err)
	}
	if n != written {
		t.Fatalf("exp copy size:%v, get:%v", wr.wn, n)
	}

	if written != wr.Size() {
		t.Fatalf("written size fail, exp:%v, get:%v", written, wr.Size())
	}

	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChunkMeta_validation(t *testing.T) {
	var validation = func(cm *ChunkMeta) (err string) {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Sprintf("%v", e)
			}
		}()

		cm.validation()
		return
	}

	cm := &ChunkMeta{}
	require.Equal(t, "series is is 0", validation(cm))

	cm.sid = 100
	cm.segCount = 1
	require.Equal(t, "length of m.timeRange is not equal to m.segCount", validation(cm))

	cm.timeRange = append(cm.timeRange, SegmentRange{})
	cm.columnCount = 1
	require.Equal(t, "length of m.colMeta is not equal to m.columnCount", validation(cm))

	cm.colMeta = append(cm.colMeta, ColumnMeta{
		name:    "foo",
		ty:      1,
		preAgg:  nil,
		entries: nil,
	})
	require.Equal(t, "length of m.colMeta[0].entries(0) is not equal to m.segCount(1)", validation(cm))

	cm.colMeta[0].entries = []Segment{{0, 10}}
	require.Equal(t, "", validation(cm))
}

func TestDelEmptyColMeta(t *testing.T) {
	cm := &ChunkMeta{}
	cm.DelEmptyColMeta()
	require.Equal(t, 0, len(cm.colMeta))

	cm.colMeta = append(cm.colMeta, ColumnMeta{}, ColumnMeta{}, ColumnMeta{})
	cm.DelEmptyColMeta()
	require.Equal(t, 0, len(cm.colMeta))

	cm.colMeta = append(cm.colMeta, ColumnMeta{}, ColumnMeta{}, ColumnMeta{entries: []Segment{{}, {}}})
	cm.DelEmptyColMeta()
	require.Equal(t, 1, len(cm.colMeta))
	require.Equal(t, 2, len(cm.colMeta[0].entries))

	cm.colMeta = append(cm.colMeta, ColumnMeta{
		entries: []Segment{{}, {}, {}},
	}, ColumnMeta{}, ColumnMeta{})

	cm.DelEmptyColMeta()
	require.Equal(t, 2, len(cm.colMeta))
	require.Equal(t, 2, len(cm.colMeta[0].entries))
	require.Equal(t, 3, len(cm.colMeta[1].entries))

	cm.colMeta = append(cm.colMeta, ColumnMeta{}, ColumnMeta{
		entries: []Segment{{}, {}, {}, {}},
	})
	cm.DelEmptyColMeta()
	require.Equal(t, 3, len(cm.colMeta))
	require.Equal(t, 2, len(cm.colMeta[0].entries))
	require.Equal(t, 3, len(cm.colMeta[1].entries))
	require.Equal(t, 4, len(cm.colMeta[2].entries))
}

func TestIndexWriterWithCompress(t *testing.T) {
	dir := t.TempDir()
	fn := filepath.Join(dir, "index.data")
	lockPath := ""
	InitWriterPool(8)
	cm := getChunkMeta()
	meta := cm.marshal(nil)

	var run = func() {
		_ = os.Remove(fn)
		wr := NewPKIndexWriter(fn, false, true, &lockPath)
		size := 0
		for i := 0; i < 15000; i++ {
			n, err := wr.Write(meta)
			require.NoError(t, err)
			size += n
		}

		buf := bytes.NewBuffer(nil)
		copySize, err := wr.CopyTo(buf)
		require.NoError(t, err)
		require.Equal(t, copySize, size)
		require.NoError(t, wr.Close())
		require.Equal(t, meta, buf.Bytes()[:len(meta)])
	}

	for _, v := range []int{0, 1, 2} {
		SetIndexCompressMode(v)
		run()
	}
}

func TestIndexWriterWithCompress_reset_big(t *testing.T) {
	dir := t.TempDir()
	fn := filepath.Join(dir, "index.data")
	lockPath := ""
	InitWriterPool(8)
	cm := getChunkMeta()
	meta := cm.marshal(nil)

	for i := 0; i < 13; i++ {
		meta = append(meta, meta...)
	}

	wr := NewPKIndexWriter(fn, false, true, &lockPath).(*indexWriter)
	var run = func() {
		_ = os.Remove(fn)
		size, err := wr.Write(meta)
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)
		copySize, err := wr.CopyTo(buf)
		require.NoError(t, err)
		require.Equal(t, copySize, size)
		require.Equal(t, meta, buf.Bytes()[:len(meta)])
		wr.reset()
	}

	for _, v := range []int{110, 1, 2} {
		SetIndexCompressMode(v)
		run()
	}
}

func TestFileSwapper_error(t *testing.T) {
	file := t.TempDir() + "/index.data"

	var run = func(writeBuf bool) {
		fs, err := NewFileSwapper(file, "", true, SwapperCompressZSTD)
		require.NoError(t, err)

		buf := bytes.NewBuffer(nil)

		if writeBuf {
			_, err = fs.Write([]byte{1, 1, 1, 1, 1})
			require.NoError(t, err)
		}

		fs.MustClose()
		fs.MustClose()
		_, err = fs.CopyTo(buf, make([]byte, 1024))
		require.NotNil(t, err)
	}

	run(true)
	run(false)
}

func TestGetObsFileWriterInfo(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "TestDiskWriter")
	fd := &mockFile{
		NameFn: func() string {
			return name
		},
		CloseFn: func() error {
			return nil
		},
		WriteFn: func(p []byte) (n int, err error) {
			return 0, fmt.Errorf("write fail")
		},
	}
	w := &obsFileWriter{
		dataWriter: &obsWriter{fd: fd},
		metaWriter: &obsWriter{},
	}

	_ = w.GetFileWriter()
	_ = w.DataSize()
	_ = w.ChunkMetaSize()
	_ = w.ChunkMetaBlockSize()
	_ = w.AppendChunkMetaToData()
	_, _ = w.SwitchMetaBuffer()
	_ = w.MetaDataBlocks(nil)

	_ = w.Name()
}

func TestGetObsIndexWriterInfo(t *testing.T) {
	w := &obsIndexWriter{
		metaIndexWriter:      &obsWriter{},
		primaryKeyWriter:     &obsWriter{},
		PrimaryKeyMetaWriter: &obsWriter{},
		bloomfilterWriters:   make([]*obsWriter, 1),
	}
	w.bloomfilterWriters[0] = &obsWriter{}

	_ = w.GetMetaIndexSize()
	_ = w.GetPrimaryKeySize()
	_ = w.GetPrimaryKeyMetaSize()
	_ = w.GetBloomFilterSize(0)
}

func TestGetObsWriterInfo(t *testing.T) {
	w := &obsWriter{}

	_ = w.GetFileWriter()
	_ = w.Size()
}
