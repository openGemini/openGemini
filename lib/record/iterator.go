// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package record

import (
	"encoding/binary"

	"github.com/openGemini/openGemini/lib/codec"
)

const (
	DefaultTagsNumPerSeriesKey = 16
)

type Iterator interface {
	Next() (*ConsumeRecord, error)
	Release()
	SidCnt() int
}

type ConsumeRecord struct {
	Tags []*Tag
	PK   map[string]interface{}
	Rec  *Record
}

func (c *ConsumeRecord) Marshal(buf []byte) []byte {
	buf = codec.AppendUint32(buf, uint32(len(c.Tags)))
	for _, t := range c.Tags {
		buf = codec.AppendUint32(buf, uint32(t.Size()))
		buf = t.Marshal(buf)
	}
	buf = c.Rec.Marshal(buf)
	return buf
}

func (c *ConsumeRecord) Unmarshal(buf []byte) {
	if len(buf) == 0 {
		return
	}
	dec := codec.NewBinaryDecoder(buf)
	tagLen := int(dec.Uint32())
	c.Tags = make([]*Tag, tagLen)
	tags := make([]Tag, tagLen)
	for i := 0; i < tagLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}
		c.Tags[i] = &tags[i]
		c.Tags[i].Unmarshal(subBuf)
	}
	c.Rec.Unmarshal(dec.Remain())
}

func (c *ConsumeRecord) Size() int {
	size := 0
	size += codec.SizeOfUint32()
	for i := 0; i < len(c.Tags); i++ {
		size += codec.SizeOfUint32()
		size += c.Tags[i].Size()
	}
	size += c.Rec.CodecSize()
	return size
}

// Tag is used to return the tag column information in the data consumption response.
type Tag struct {
	Key     string
	Value   string
	IsArray bool
}

func (tag *Tag) Marshal(buf []byte) []byte {
	buf = codec.AppendString(buf, tag.Key)
	buf = codec.AppendString(buf, tag.Value)
	buf = codec.AppendBool(buf, tag.IsArray)
	return buf
}

func (tag *Tag) Unmarshal(buf []byte) {
	if len(buf) == 0 {
		return
	}
	dec := codec.NewBinaryDecoder(buf)
	tag.Key = dec.String()
	tag.Value = dec.String()
	tag.IsArray = dec.Bool()
}

func (tag *Tag) Size() int {
	size := 0
	size += codec.SizeOfString(tag.Key)
	size += codec.SizeOfString(tag.Value)
	size += codec.SizeOfBool()
	return size
}

type RecIterator interface {
	Next() (*Record, error)
	Release()
}

type AddableIterator interface {
	AddIterator(itr RecIterator)
}

type SmartQueryResult struct {
	data           []*ConsumeRecord
	isSingleSeries bool
}

func NewSmartQueryResult(recs []*ConsumeRecord, isSingleSeries bool) *SmartQueryResult {
	return &SmartQueryResult{data: recs, isSingleSeries: isSingleSeries}
}

func (s *SmartQueryResult) IsEmpty() bool {
	return s == nil || len(s.data) == 0
}

func (s *SmartQueryResult) Marshal(dst []byte) []byte {
	n := len(dst)

	dst = codec.AppendBool(dst, s.isSingleSeries)
	dst = codec.AppendUint16(dst, uint16(len(s.data)))
	for _, item := range s.data {
		dst = codec.AppendUint32(dst, uint32(item.Size()))
		dst = item.Marshal(dst)
	}

	// used for data verification
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(dst)-n))
	return dst
}

func (s *SmartQueryResult) Unmarshal(buf []byte) {
	dec := codec.NewBinaryDecoder(buf)
	s.isSingleSeries = dec.Bool()

	recNum := int(dec.Uint16())
	s.data = make([]*ConsumeRecord, recNum)
	for i := range s.data {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}
		rec := &ConsumeRecord{
			Rec: &Record{},
		}
		rec.Unmarshal(subBuf)
		s.data[i] = rec
	}
}

func (s *SmartQueryResult) SetData(recs []*ConsumeRecord) {
	s.data = recs
}

func (s *SmartQueryResult) GetData() []*ConsumeRecord {
	return s.data
}

func (s *SmartQueryResult) IsSingleSeries() bool {
	return s.isSingleSeries
}
