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
	"github.com/openGemini/openGemini/lib/codec"
)

type Iterator interface {
	Next() (uint64, *ConsumeRecord, error)
	Release()
}

type ConsumeRecord struct {
	Tags []*Tag
	Rec  *Record
}

func (c *ConsumeRecord) Marshal(buf []byte) []byte {
	buf = codec.AppendUint32(buf, uint32(len(c.Tags)))
	for _, t := range c.Tags {
		buf = t.Marshal(buf)
	}

	buf = c.Rec.Marshal(buf)
	return buf
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
