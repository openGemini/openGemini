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

package immutable_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func setCompressModeSelf() {
	immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressSelf)
}

func setCompressModeNone() {
	immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressNone)
}

func iterateChunkMeta(file immutable.TSSPFile, hook func(cm *immutable.ChunkMeta)) {
	fi := immutable.NewFileIterator(file, immutable.CLog)
	itr := immutable.NewChunkIterator(fi)

	for {
		if !itr.NextChunkMeta() {
			break
		}
		cm := itr.GetCurtChunkMeta()

		hook(cm)

		itr.IncrChunkUsed()
	}
}

func marshalChunkMetaSelf(ctx *immutable.ChunkMetaCodecCtx, cm *immutable.ChunkMeta) ([]byte, error) {
	defer immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressNone)
	immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressSelf)

	return immutable.MarshalChunkMeta(ctx, cm, nil)
}

func marshalChunkMetaNormal(cm *immutable.ChunkMeta) ([]byte, error) {
	return immutable.MarshalChunkMeta(nil, cm, nil)
}

func createOneTsspFile() (immutable.TSSPFile, func(), error) {
	var begin int64 = 1e12
	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	rg := newRecordGenerator(begin, defaultInterval, true)
	schema := getDefaultSchemas()

	for i := 0; i < 4; i++ {
		mh.addRecord(uint64(100+i), rg.generate(schema, (i+1)*1000))
	}
	err := mh.saveToOrder()
	if err != nil {
		mh.store.Close()
		return nil, nil, err
	}

	file := mh.store.Order["mst"].Files()[0]
	return file, func() {
		mh.store.Close()
	}, nil
}

func TestChunkMetaCodec(t *testing.T) {
	defer beforeTest(t, 0)()

	file, release, err := createOneTsspFile()
	require.NoError(t, err)
	defer release()

	ctx := immutable.GetChunkMetaCodecCtx()
	trailer := &immutable.Trailer{}
	trailer.ChunkMetaHeader = &immutable.ChunkMetaHeader{}
	ctx.SetTrailer(trailer)
	defer ctx.Release()

	require.Equal(t, uint8(1), file.FileStat().TimeStoreFlag)
	p := &immutable.IdTimePairs{}
	require.NoError(t, file.LoadIdTimes(p))

	iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
		exp, err := marshalChunkMetaNormal(cm)
		require.NoError(t, err)

		buf, err := marshalChunkMetaSelf(ctx, cm)
		require.NoError(t, err)

		trailer.ChunkMetaHeader = ctx.GetHeader()
		other := &immutable.ChunkMeta{}
		_, err = immutable.UnmarshalChunkMeta(ctx, other, buf)
		other.Validation()
		require.NoError(t, err)

		got, err := marshalChunkMetaNormal(cm)
		require.Equal(t, exp, got)
	})

	iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
		buf, err := marshalChunkMetaSelf(ctx, cm)
		require.NoError(t, err)

		trailer.ChunkMetaHeader = ctx.GetHeader()
		names := []string{"float", "not_exists", "time"}
		other := &immutable.ChunkMeta{}
		_, err = immutable.UnmarshalChunkMetaWithColumns(ctx, other, names, buf)
		other.Validation()
		require.NoError(t, err)

		require.Equal(t, 2, len(other.GetColMeta()))

		names = []string{"float", "time"}
		for i, col := range other.GetColMeta() {
			require.Equal(t, names[i], col.Name())
		}
	})
}

func TestChunkMetaHeaderCodec(t *testing.T) {
	values := []string{"foo", "user", "time"}
	header := &immutable.ChunkMetaHeader{}
	for _, s := range values {
		header.AppendValue(s)
	}

	buf := header.Marshal(nil)
	other := &immutable.ChunkMetaHeader{}
	other.Unmarshal(buf)
	require.Equal(t, 3, other.Len())

	var gotValues []string
	for i := 0; i < other.Len(); i++ {
		gotValues = append(gotValues, other.GetValue(i))
	}
	require.Equal(t, values, gotValues)
	require.Equal(t, "", other.GetValue(len(values)))
}

func TestUnmarshalChunkMetaSelf_error(t *testing.T) {
	ctx := immutable.GetChunkMetaCodecCtx()
	trailer := &immutable.Trailer{}
	trailer.ChunkMetaHeader = &immutable.ChunkMetaHeader{}
	ctx.SetTrailer(trailer)
	defer ctx.Release()

	var buf []byte
	var err error

	var unmarshal = func(contains string) {
		other := &immutable.ChunkMeta{}
		_, err = immutable.UnmarshalChunkMeta(ctx, other, buf)
		require.ErrorContains(t, err, contains)
	}

	buf = binary.BigEndian.AppendUint64(buf, 0)
	unmarshal("invalid data offset")

	buf = binary.AppendUvarint(buf, 0)
	unmarshal("invalid data size")

	buf = binary.AppendUvarint(buf, 0)
	unmarshal("invalid column count")

	buf = binary.AppendUvarint(buf, 0)
	unmarshal("invalid segment count")

	buf = binary.AppendUvarint(buf, 0)
	unmarshal("invalid time range")
}

func TestChunkMetaCompressSelf(t *testing.T) {
	defer beforeTest(t, 0)()
	immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressSelf)
	defer func() {
		immutable.SetChunkMetaCompressMode(immutable.ChunkMetaCompressNone)
	}()

	file, release, err := createOneTsspFile()
	require.NoError(t, err)
	defer release()

	ctx := immutable.GetChunkMetaCodecCtx()
	defer ctx.Release()

	rowNumbers := map[uint64]int{100: 1000, 101: 2000, 102: 3000, 103: 4000}

	itrTSSPFile(file, func(sid uint64, rec *record.Record) {
		require.Equal(t, rowNumbers[sid], rec.RowNums())
	})

	lock := ""
	f2, err := immutable.OpenTSSPFile(file.Path(), &lock, true)
	defer func() {
		f2.Close()
	}()
	require.NoError(t, err)
	itrTSSPFile(f2, func(sid uint64, rec *record.Record) {
		require.Equal(t, rowNumbers[sid], rec.RowNums())
	})
}

func BenchmarkChunkMetaCodec(b *testing.B) {
	//b.Skip()
	var begin int64 = 1e12
	defer beforeBenchmark(b, 0)()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)
	schema := record.Schemas{}

	columnNum := 300
	for i := 0; i < columnNum/4; i++ {
		schema = append(schema, record.Schemas{
			record.Field{Type: influx.Field_Type_Int, Name: fmt.Sprintf("int_%d", i)},
			record.Field{Type: influx.Field_Type_Float, Name: fmt.Sprintf("float_%d", i)},
			record.Field{Type: influx.Field_Type_Boolean, Name: fmt.Sprintf("bool_%d", i)},
			record.Field{Type: influx.Field_Type_String, Name: fmt.Sprintf("string_%d", i)},
		}...)
	}

	baseSid := uint64(1000)
	for _, i := range []int{1, 2, 4, 8, 16} {
		for _, j := range []int{1, 3, 15, 63, 255} {
			sid := baseSid
			mh.addRecord(sid, rg.generate(schema[:j:j], i*1000))
			baseSid++
		}
	}
	require.NoError(b, mh.saveToOrder())

	file := mh.store.Order["mst"].Files()[0]

	var err error
	var metas [][]byte
	iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
		buf, err := immutable.MarshalChunkMeta(nil, cm, nil)
		require.NoError(b, err)
		metas = append(metas, buf)
	})

	chunkMetas := make([]immutable.ChunkMeta, len(metas))
	for i := range chunkMetas {
		_, _ = chunkMetas[i].UnmarshalWithColumns(metas[i], nil)
	}

	defer setCompressModeNone()

	var swap = make([]byte, 64*1000)
	ctx := immutable.GetChunkMetaCodecCtx()

	var marshal = func(cm *immutable.ChunkMeta) {
		normalName := fmt.Sprintf("marshal_normal_%d_%d", cm.Len(), cm.SegmentCount())
		compressName := fmt.Sprintf("marshal_compress_%d_%d", cm.Len(), cm.SegmentCount())

		b.ResetTimer()
		setCompressModeNone()
		b.Run(normalName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				swap, _ = immutable.MarshalChunkMeta(ctx, cm, swap[:0])
			}
		})

		setCompressModeSelf()
		b.Run(compressName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				swap, _ = immutable.MarshalChunkMeta(ctx, cm, swap[:0])
			}
		})
	}
	tr := file.FileStat()
	tr.SetChunkMetaHeader(ctx.GetHeader())
	ctx.SetTrailer(tr)

	var unmarshal = func(cm *immutable.ChunkMeta) {
		normalName := fmt.Sprintf("unmarshal_normal_%d_%d", cm.Len(), cm.SegmentCount())
		compressName := fmt.Sprintf("unmarshal_compress_%d_%d", cm.Len(), cm.SegmentCount())

		b.ResetTimer()
		setCompressModeNone()
		b.Run(normalName, func(b *testing.B) {
			swap, _ = immutable.MarshalChunkMeta(ctx, cm, swap[:0])
			other := &immutable.ChunkMeta{}
			for i := 0; i < b.N; i++ {
				_, err = other.UnmarshalWithColumns(swap, nil)
				assertError(err)
			}
		})

		setCompressModeSelf()
		b.Run(compressName, func(b *testing.B) {
			swap, _ = immutable.MarshalChunkMeta(ctx, cm, swap[:0])
			other := &immutable.ChunkMeta{}
			for i := 0; i < b.N; i++ {
				_, err = immutable.UnmarshalChunkMeta(ctx, other, swap)
				assertError(err)
			}
		})
	}

	for i := range chunkMetas {
		marshal(&chunkMetas[i])
	}
	for i := range chunkMetas {
		unmarshal(&chunkMetas[i])
	}
}

func TestChunkMetaCodec_compare(t *testing.T) {
	t.Skip()
	config.GetCommon().PreAggEnabled = false

	var run = func(columnNum int, segCount int) {
		var begin int64 = 1e12
		defer recoverConfig(0)()

		mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
		defer mh.store.Close()
		rg := newRecordGenerator(begin, defaultInterval, true)
		schema := record.Schemas{}

		for i := 0; i < columnNum/4; i++ {
			schema = append(schema, record.Schemas{
				record.Field{Type: influx.Field_Type_Int, Name: fmt.Sprintf("int_%d", i)},
				record.Field{Type: influx.Field_Type_Float, Name: fmt.Sprintf("float_%d", i)},
				record.Field{Type: influx.Field_Type_Boolean, Name: fmt.Sprintf("bool_%d", i)},
				record.Field{Type: influx.Field_Type_String, Name: fmt.Sprintf("string_%d", i)},
			}...)
		}

		for i := 0; i < 16; i++ {
			mh.addRecord(uint64(100+i), rg.generate(schema, segCount*1000))
		}
		require.NoError(t, mh.saveToOrder())

		file := mh.store.Order["mst"].Files()[0]

		var metas [][]byte
		iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
			buf, err := immutable.MarshalChunkMeta(nil, cm, nil)
			require.NoError(t, err)
			metas = append(metas, buf)
		})
		compare(t, file.Path())
	}

	for _, i := range []int{8, 16, 32} {
		run(i, 1)
	}
}

func compare(t *testing.T, file string) {
	lockPath := ""
	f, err := immutable.OpenTSSPFile(file, &lockPath, false)
	require.NoError(t, err)

	stat := f.FileStat()
	fmt.Println(stat.IndexSize())

	swap := make([]byte, 1024*64)
	ctx := immutable.GetChunkMetaCodecCtx()
	total := 0
	origi := 0
	seriesCount := 0

	var metaBlocksOrig [][]byte
	var metaBlocksSelf [][]byte

	defer setCompressModeNone()

	once := sync.Once{}
	iterateChunkMeta(f, func(cm *immutable.ChunkMeta) {
		once.Do(func() {
			fmt.Println("column count", len(cm.GetColMeta()), "; segment count", cm.SegmentCount())
		})

		setCompressModeNone()
		buf, err := immutable.MarshalChunkMeta(nil, cm, nil)
		require.NoError(t, err)

		if len(metaBlocksOrig) < 16 {
			metaBlocksOrig = append(metaBlocksOrig, buf)
			swap = nil
		}

		origi += len(buf)

		setCompressModeSelf()
		buf, err = immutable.MarshalChunkMeta(ctx, cm, swap[:0])
		require.NoError(t, err)
		config.GetStoreConfig().ChunkMetaCompressMode = 0

		if len(metaBlocksSelf) < 16 {
			metaBlocksSelf = append(metaBlocksSelf, buf)
			swap = nil
		} else {
			return
		}

		total += len(buf)
		seriesCount++
	})

	compareMarshalChunkMeta(t, metaBlocksOrig)
}

func compareMarshalChunkMeta(t *testing.T, metaBlocks [][]byte) {
	metas := make([]immutable.ChunkMeta, len(metaBlocks))
	other := make([]immutable.ChunkMeta, len(metaBlocks))

	for i := range metas {
		_, err := metas[i].UnmarshalWithColumns(metaBlocks[i], nil)
		require.NoError(t, err)
	}

	n := 200000

	data := make([]byte, 64*1024)
	snappyCmp := make([]byte, 16*64*1024)
	snappyTmp := make([]byte, 16*64*1024)

	var err error

	setCompressModeNone()
	TimeCost("normal-encode", n, func() {
		data = data[:0]
		for i := range metas {
			data, _ = immutable.MarshalChunkMeta(nil, &metas[i], data[:0])
		}
	})
	TimeCost("normal-decode", n, func() {
		buf := data
		for i := range other {
			_, err = other[i].UnmarshalWithColumns(buf, nil)
			assertError(err)
		}
	})

	TimeCost("snappy-encode", n, func() {
		data = data[:0]
		snappyTmp = snappyTmp[:0]
		for i := range metas {
			data, _ = immutable.MarshalChunkMeta(nil, &metas[i], data[:0])
			snappyTmp = append(snappyTmp, data...)
		}
		snappyCmp = snappy.Encode(snappyCmp[:0], snappyTmp)
	})

	TimeCost("snappy-decode", n, func() {
		buf := data
		for i := range other {
			_, err = other[i].UnmarshalWithColumns(buf, nil)
			assertError(err)
		}

		for _ = range other {
			snappyTmp, err = snappy.Decode(snappyTmp[:0], snappyCmp)
			assertError(err)
		}
	})

	setCompressModeSelf()
	ctx := immutable.GetChunkMetaCodecCtx()
	TimeCost("self-encode", n, func() {
		data = data[:0]
		for i := range metas {
			data, err = immutable.MarshalChunkMeta(ctx, &metas[i], data[:0])
			assertError(err)
		}
	})

	tr := &immutable.Trailer{}
	tr.SetChunkMetaHeader(ctx.GetHeader())
	ctx.SetTrailer(tr)

	TimeCost("self-decode", n, func() {
		buf := data
		for i := range other {
			_, err = immutable.UnmarshalChunkMeta(ctx, &other[i], buf)
			assertError(err)
		}
	})
}

func assertError(err error) {
	if err != nil {
		panic(err)
	}
}

func TimeCost(name string, n int, fn func()) {
	begin := time.Now()
	for i := 0; i < n; i++ {
		fn()
	}
	fmt.Printf("%s\t%d\n", name, time.Since(begin).Nanoseconds()/int64(n))
}

func TestCodecTrailer(t *testing.T) {
	var run = func(buf []byte, timeStoreFlag, compressFlag uint8) {
		ed := &immutable.ExtraData{}
		_, err := ed.UnmarshalExtraData(buf)
		require.NoError(t, err)
		require.Equal(t, timeStoreFlag, ed.TimeStoreFlag)
		require.Equal(t, compressFlag, ed.ChunkMetaCompressFlag)
	}

	var unmarshalError = func(b []byte) error {
		ed := &immutable.ExtraData{}
		_, err := ed.UnmarshalExtraData(b)
		return err
	}

	run([]byte{0, 2, 1, 2}, 1, 2)
	run([]byte{0, 10, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 1, 2)

	require.NotEmpty(t, unmarshalError([]byte{0, 8, 1, 2, 0, 0}))
	require.NotEmpty(t, unmarshalError([]byte{0, 8, 1, 2, 0, 0, 1, 1, 0, 0, 0, 10}))
	require.NotEmpty(t, unmarshalError([]byte{0, 8, 1, 2, 0, 0, 0, 0, 0, 0, 0, 10}))
	require.NotEmpty(t, unmarshalError([]byte{0, 8, 1, 2, 0, 0, 0, 0, 0, 0, 0, 10}))
	require.NotEmpty(t, unmarshalError([]byte{1}))
	require.NotEmpty(t, unmarshalError([]byte{}))
}

func TestLargeExtraData(t *testing.T) {
	tr := &immutable.Trailer{}
	header := &immutable.ChunkMetaHeader{}
	tr.SetChunkMetaHeader(header)

	n := 10000
	for i := 0; i < n; i++ {
		header.AppendValue(fmt.Sprintf("column_%08d", i))
	}
	buf := tr.Marshal(nil)
	other := &immutable.Trailer{}
	_, err := other.Unmarshal(buf)
	require.NoError(t, err)

	require.Equal(t, n, other.ChunkMetaHeader.Len())
	for i := 0; i < n; i++ {
		require.Equal(t, header.GetValue(i), other.ChunkMetaHeader.GetValue(i))
	}
}

func TestMergeAndCompact_ChunkMeta_CodecSelf(t *testing.T) {
	immutable.SetMergeFlag4TsStore(util.StreamingCompact)
	setCompressModeSelf()
	defer func() {
		setCompressModeNone()
		immutable.SetMergeFlag4TsStore(util.AutoCompact)
	}()

	var begin int64 = 1e12
	defer beforeTest(t, 256)()
	schemas := getDefaultSchemas()

	mh := NewMergeTestHelper(immutable.NewTsStoreConfig())
	defer mh.store.Close()
	rg := newRecordGenerator(begin, defaultInterval, true)

	for i := 0; i < 10; i++ {
		rg.setBegin(begin)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 10))
		require.NoError(t, mh.saveToOrder())
	}

	for i := 0; i < 10; i++ {
		rg.setBegin(begin).incrBegin(5)
		mh.addRecord(uint64(100+i), rg.generate(schemas, 10))
		require.NoError(t, mh.saveToUnordered())
	}

	require.NoError(t, mh.mergeAndCompact(false))
	require.NoError(t, mh.mergeAndCompact(true))
	require.NoError(t, compareRecords(mh.readExpectRecord(), mh.readMergedRecord()))
	require.Equal(t, 3, len(mh.store.Order["mst"].Files()))
}
