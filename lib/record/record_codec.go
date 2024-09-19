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

package record

import (
	"github.com/openGemini/openGemini/lib/codec"
)

func (rec *Record) Marshal(buf []byte) ([]byte, error) {
	var err error
	// Schema
	buf = codec.AppendUint32(buf, uint32(len(rec.Schema)))
	for i := 0; i < len(rec.Schema); i++ {
		buf = codec.AppendUint32(buf, uint32(rec.Schema[i].Size()))
		buf, err = rec.Schema[i].Marshal(buf)
		if err != nil {
			return nil, err
		}
	}

	// ColVal
	buf = codec.AppendUint32(buf, uint32(len(rec.ColVals)))
	for i := 0; i < len(rec.ColVals); i++ {
		buf = codec.AppendUint32(buf, uint32(rec.ColVals[i].Size()))
		buf, err = rec.ColVals[i].Marshal(buf)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func (rec *Record) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	var err error
	dec := codec.NewBinaryDecoder(buf)

	// Schema
	fieldLen := int(dec.Uint32())
	rec.Schema = make([]Field, fieldLen)
	for i := 0; i < fieldLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}
		rec.Schema[i] = Field{}
		if err = rec.Schema[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}

	// ColVal
	colLen := int(dec.Uint32())
	rec.ColVals = make([]ColVal, colLen)
	for i := 0; i < colLen; i++ {
		subBuf := dec.BytesNoCopy()
		if len(subBuf) == 0 {
			continue
		}
		rec.ColVals[i] = ColVal{}
		if err = rec.ColVals[i].Unmarshal(subBuf); err != nil {
			return err
		}
	}
	return nil
}

func (rec *Record) CodecSize() int {
	size := 0

	// Schema
	size += codec.SizeOfUint32()
	for i := 0; i < len(rec.Schema); i++ {
		size += codec.SizeOfUint32()
		size += rec.Schema[i].Size()
	}

	// ColVal
	size += codec.SizeOfUint32()
	for i := 0; i < len(rec.ColVals); i++ {
		size += codec.SizeOfUint32()
		size += rec.ColVals[i].Size()
	}
	return size
}
