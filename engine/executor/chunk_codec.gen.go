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

package executor

import (
	"github.com/openGemini/openGemini/engine/executor/spdy/transport"
	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func (b *Bitmap) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, b.bits)
	buf = codec.AppendUint16Slice(buf, b.array)
	buf = codec.AppendInt(buf, b.length)
	buf = codec.AppendInt(buf, b.nilCount)

	return buf, err
}

func (b *Bitmap) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	b.bits = dec.Bytes()
	b.array = dec.Uint16Slice()
	b.length = dec.Int()
	b.nilCount = dec.Int()

	return err
}

func (b *Bitmap) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(b.bits)
	size += codec.SizeOfUint16Slice(b.array)
	size += codec.SizeOfInt()
	size += codec.SizeOfInt()

	return size
}

func (b *Bitmap) Instance() transport.Codec {
	return &Bitmap{}
}

func (c *ChunkImpl) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendString(buf, c.name)

	buf = codec.AppendUint32(buf, uint32(len(c.tags)))
	for _, item := range c.tags {
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	buf = codec.AppendIntSlice(buf, c.tagIndex)
	buf = codec.AppendInt64Slice(buf, c.time)
	buf = codec.AppendIntSlice(buf, c.intervalIndex)

	buf = codec.AppendUint32(buf, uint32(len(c.columns)))
	for _, item := range c.columns {
		if item == nil {
			buf = codec.AppendUint32(buf, 0)
			continue
		}
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	return buf, err
}

func (c *ChunkImpl) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	c.name = dec.String()

	tagsLen := int(dec.Uint32())
	c.tags = make([]ChunkTags, tagsLen)
	for i := 0; i < tagsLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}

		c.tags[i] = ChunkTags{}
		if err := c.tags[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}
	c.tagIndex = dec.IntSlice()
	c.time = dec.Int64Slice()
	c.intervalIndex = dec.IntSlice()

	columnsLen := int(dec.Uint32())
	c.columns = make([]Column, columnsLen)
	for i := 0; i < columnsLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}

		c.columns[i] = &ColumnImpl{}
		if err := c.columns[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}

	return err
}

func (c *ChunkImpl) Size() int {
	size := 0
	size += codec.SizeOfString(c.name)

	size += codec.MaxSliceSize
	for _, item := range c.tags {
		size += codec.SizeOfUint32()
		size += item.Size()
	}
	size += codec.SizeOfIntSlice(c.tagIndex)
	size += codec.SizeOfInt64Slice(c.time)
	size += codec.SizeOfIntSlice(c.intervalIndex)

	size += codec.MaxSliceSize
	for _, item := range c.columns {
		size += codec.SizeOfUint32()
		if item == nil {
			continue
		}
		size += item.Size()
	}

	return size
}

func (c *ChunkImpl) Instance() transport.Codec {
	return &ChunkImpl{}
}

func (ct *ChunkTags) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendBytes(buf, ct.subset)

	return buf, err
}

func (ct *ChunkTags) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	ct.subset = dec.Bytes()

	return err
}

func (ct *ChunkTags) Size() int {
	size := 0
	size += codec.SizeOfByteSlice(ct.subset)

	return size
}

func (ct *ChunkTags) Instance() transport.Codec {
	return &ChunkTags{}
}

func (c *ColumnImpl) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendInt(buf, int(c.dataType))
	buf = codec.AppendFloat64Slice(buf, c.floatValues)
	buf = codec.AppendInt64Slice(buf, c.integerValues)
	buf = codec.AppendBytes(buf, c.stringBytes)
	buf = codec.AppendUint32Slice(buf, c.offset)
	buf = codec.AppendBoolSlice(buf, c.booleanValues)
	buf = codec.AppendInt64Slice(buf, c.times)

	buf = codec.AppendUint32(buf, uint32(len(c.floatTuples)))
	for _, item := range c.floatTuples {
		buf = codec.AppendUint32(buf, uint32(item.Size()))
		buf, err = item.Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	func() {
		if c.nilsV2 == nil {
			buf = codec.AppendUint32(buf, 0)
			return
		}
		buf = codec.AppendUint32(buf, uint32(c.nilsV2.Size()))
		buf, err = c.nilsV2.Marshal(buf)
	}()
	if err != nil {
		return nil, err
	}

	return buf, err
}

func (c *ColumnImpl) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	c.dataType = influxql.DataType(dec.Int())
	c.floatValues = dec.Float64Slice()
	c.integerValues = dec.Int64Slice()
	c.stringBytes = dec.Bytes()
	c.offset = dec.Uint32Slice()
	c.booleanValues = dec.BoolSlice()
	c.times = dec.Int64Slice()

	floatTuplesLen := int(dec.Uint32())
	if floatTuplesLen > 0 {
		c.floatTuples = make([]floatTuple, floatTuplesLen)
		for i := 0; i < floatTuplesLen; i++ {
			subBuf := dec.BytesNoCopy()
			if len(subBuf) == 0 {
				continue
			}

			c.floatTuples[i] = floatTuple{}
			if err := c.floatTuples[i].Unmarshal(subBuf); err != nil {
				return err
			}
		}
	}

	func() {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			return
		}
		c.nilsV2 = &Bitmap{}
		err = c.nilsV2.Unmarshal(subBuf)
	}()
	if err != nil {
		return err
	}

	return err
}

func (c *ColumnImpl) Size() int {
	size := 0
	size += codec.SizeOfInt()
	size += codec.SizeOfFloat64Slice(c.floatValues)
	size += codec.SizeOfInt64Slice(c.integerValues)
	size += codec.SizeOfByteSlice(c.stringBytes)
	size += codec.SizeOfUint32Slice(c.offset)
	size += codec.SizeOfBoolSlice(c.booleanValues)
	size += codec.SizeOfInt64Slice(c.times)

	size += codec.MaxSliceSize
	for _, item := range c.floatTuples {
		size += codec.SizeOfUint32()
		size += item.Size()
	}

	size += codec.SizeOfUint32()
	if c.nilsV2 != nil {
		size += c.nilsV2.Size()
	}

	return size
}

func (c *ColumnImpl) Instance() transport.Codec {
	return &ColumnImpl{}
}

func (o *floatTuple) Marshal(buf []byte) ([]byte, error) {
	var err error
	buf = codec.AppendFloat64Slice(buf, o.values)

	return buf, err
}

func (o *floatTuple) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)
	o.values = dec.Float64Slice()

	return err
}

func (o *floatTuple) Size() int {
	size := 0
	size += codec.SizeOfFloat64Slice(o.values)

	return size
}

func (o *floatTuple) Instance() transport.Codec {
	return &floatTuple{}
}
