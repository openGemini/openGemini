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
//nolint
package pool

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

const (
	DataBlockMaxRowNum = 100000
)

type DataBlock struct {
	ReqBuf          []byte
	Rows            []influx.Row
	TagPool         []influx.Tag
	FieldPool       []influx.Field
	IndexKeyPool    []byte
	IndexOptionPool []influx.IndexOption
	LastResetTime   uint64
}

func NewDataBlock() *DataBlock {
	d := &DataBlock{}
	d.ReqBuf = make([]byte, 0, 1)
	d.Rows = make([]influx.Row, 0, 1)
	d.TagPool = make([]influx.Tag, 0, 1)
	d.FieldPool = make([]influx.Field, 0, 1)
	d.IndexKeyPool = make([]byte, 0, 1)
	d.IndexOptionPool = make([]influx.IndexOption, 0, 1)
	return d
}

func (d *DataBlock) Reset() {
	if (len(d.ReqBuf)*4 > cap(d.ReqBuf) || len(d.Rows)*4 > cap(d.Rows)) && fasttime.UnixTimestamp()-d.LastResetTime > 10 {
		d.ReqBuf = nil
		d.Rows = nil
		d.LastResetTime = fasttime.UnixTimestamp()
	}
	d.ReqBuf = d.ReqBuf[:0]
	d.Rows = d.Rows[:0]
	d.TagPool = d.TagPool[:0]
	d.FieldPool = d.FieldPool[:0]
	d.IndexKeyPool = d.IndexKeyPool[:0]
	d.IndexOptionPool = d.IndexOptionPool[:0]
}

func (d *DataBlock) Exchange(reqBuf *[]byte, rows *[]influx.Row, tagPool *[]influx.Tag, fieldPool *[]influx.Field,
	indexKeyPool *[]byte, indexOptionPool *[]influx.IndexOption, lastResetTime *uint64) {
	d.ReqBuf, *reqBuf = *reqBuf, d.ReqBuf
	d.Rows, *rows = *rows, d.Rows
	d.TagPool, *tagPool = *tagPool, d.TagPool
	d.FieldPool, *fieldPool = *fieldPool, d.FieldPool
	d.IndexKeyPool, *indexKeyPool = *indexKeyPool, d.IndexKeyPool
	d.IndexOptionPool, *indexOptionPool = *indexOptionPool, d.IndexOptionPool
	d.LastResetTime, *lastResetTime = *lastResetTime, d.LastResetTime
}

// DataBlock in pool can be gc, so use sync.Pool
var dataBlockPool sync.Pool

func StreamDataBlockGet() *DataBlock {
	v := dataBlockPool.Get()
	if v != nil {
		data, ok := v.(*DataBlock)
		if !ok {
			return NewDataBlock()
		}
		return data
	}
	data := NewDataBlock()
	return data
}

func StreamDataBlockPut(data *DataBlock) {
	if len(data.Rows) > DataBlockMaxRowNum {
		return
	}
	data.Reset()
	dataBlockPool.Put(data)
}
