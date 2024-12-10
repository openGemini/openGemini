// Copyright 2024 openGemini Authors
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

package compress

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/pierrec/lz4/v4"
)

var (
	gzipWriterPool   = NewGzipWriterPool()
	zstdWriterPool   = NewZstdWriterPool()
	snappyWriterPool = NewSnappyWriterPool()
	lz4WriterPool    = NewLz4WriterPool()

	gzipReaderPool sync.Pool
)

type FixedCachePool struct {
	cache chan interface{}
	sp    sync.Pool
}

func NewFixedCachePool(size int, newFunc func() interface{}) *FixedCachePool {
	return &FixedCachePool{
		cache: make(chan interface{}, size),
		sp: sync.Pool{
			New: newFunc,
		},
	}
}

func (p *FixedCachePool) Get() interface{} {
	select {
	case item := <-p.cache:
		return item
	default:
		item := p.sp.Get()
		if item == nil {
			item = p.sp.New()
		}
		return item
	}
}

func (p *FixedCachePool) Put(item interface{}) {
	select {
	case p.cache <- item:
	default:
		p.sp.Put(item)
	}
}

// ******************** region gzip write pool ***********************
type GzipWriterPool struct {
	pool *FixedCachePool
}

func NewGzipWriterPool() *GzipWriterPool {
	p := &GzipWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			return gzip.NewWriter(nil)
		}),
	}
	return p
}

func (p *GzipWriterPool) Get() *gzip.Writer {
	return p.pool.Get().(*gzip.Writer)
}

func (p *GzipWriterPool) Put(gz *gzip.Writer) {
	p.pool.Put(gz)
}

func GetGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get()
	gz.Reset(w)
	return gz
}

func PutGzipWriter(gz *gzip.Writer) {
	gz.Close()
	gzipWriterPool.Put(gz)
}

// ******************** endregion gzip write pool ***********************

// ******************** region gzip read pool ***************************

func GetGzipReader(r io.Reader) (*gzip.Reader, error) {
	v := gzipReaderPool.Get()
	if v == nil {
		return gzip.NewReader(r)
	}
	zr := v.(*gzip.Reader)
	if err := zr.Reset(r); err != nil {
		return nil, err
	}
	return zr, nil
}

// PutGzipReader returns back gzip reader obtained via GetGzipReader.
func PutGzipReader(zr *gzip.Reader) {
	_ = zr.Close()
	gzipReaderPool.Put(zr)
}

// ******************** endregion gzip read pool ***************************

// ******************** region zstd write pool ***********************
type ZstdWriterPool struct {
	pool *FixedCachePool
}

func NewZstdWriterPool() *ZstdWriterPool {
	p := &ZstdWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			encoder, _ := zstd.NewWriter(nil)
			return encoder
		}),
	}
	return p
}

func (p *ZstdWriterPool) Get() *zstd.Encoder {
	return p.pool.Get().(*zstd.Encoder)
}

func (p *ZstdWriterPool) Put(zstdEncoder *zstd.Encoder) {
	p.pool.Put(zstdEncoder)
}

func GetZstdWriter(w io.Writer) *zstd.Encoder {
	zstdEncoder := zstdWriterPool.Get()
	zstdEncoder.Reset(w)
	return zstdEncoder
}

func PutZstdWriter(zstdEncoder *zstd.Encoder) {
	zstdEncoder.Close()
	zstdWriterPool.Put(zstdEncoder)
}

// ******************** endregion zstd write pool ***********************

// ******************** region snappy write pool ***************************
type SnappyWriterPool struct {
	pool *FixedCachePool
}

func NewSnappyWriterPool() *SnappyWriterPool {
	p := &SnappyWriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			return snappy.NewBufferedWriter(nil)
		}),
	}
	return p
}

func (p *SnappyWriterPool) Get() *snappy.Writer {
	return p.pool.Get().(*snappy.Writer)
}

func (p *SnappyWriterPool) Put(snappyWriter *snappy.Writer) {
	p.pool.Put(snappyWriter)
}

func GetSnappyWriter(w io.Writer) *snappy.Writer {
	snappyWriter := snappyWriterPool.Get()
	snappyWriter.Reset(w)
	return snappyWriter
}

func PutSnappyWriter(snappyWriter *snappy.Writer) {
	snappyWriter.Close()
	snappyWriterPool.Put(snappyWriter)
}

// ******************** endregion snappy write pool ***************************

// ******************** region lz4 write pool ***************************
type Lz4WriterPool struct {
	pool *FixedCachePool
}

func NewLz4WriterPool() *Lz4WriterPool {
	p := &Lz4WriterPool{
		pool: NewFixedCachePool(cpu.GetCpuNum()*2, func() interface{} {
			return lz4.NewWriter(nil)
		}),
	}
	return p
}

func (p *Lz4WriterPool) Get() *lz4.Writer {
	return p.pool.Get().(*lz4.Writer)
}

func (p *Lz4WriterPool) Put(lz4Writer *lz4.Writer) {
	p.pool.Put(lz4Writer)
}

func GetLz4Writer(w io.Writer) *lz4.Writer {
	lz4Writer := lz4WriterPool.Get()
	lz4Writer.Reset(w)
	return lz4Writer
}

func PutLz4Writer(lz4Writer *lz4.Writer) {
	lz4Writer.Close()
	lz4WriterPool.Put(lz4Writer)
}

// ******************** endregion lz4 write pool ***************************
