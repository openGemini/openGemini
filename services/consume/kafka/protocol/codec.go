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

package protocol

import (
	"encoding/binary"
	"errors"

	"github.com/openGemini/openGemini/lib/codec"
	"github.com/openGemini/openGemini/lib/util"
)

type Marshaler interface {
	Marshal([]byte) []byte
}

type Unmarshaler interface {
	Unmarshal(*codec.BinaryDecoder) error
}

func MarshalSlice[T Marshaler](dst []byte, s []T) []byte {
	dst = codec.AppendUint32(dst, uint32(len(s)))
	for i := range s {
		dst = s[i].Marshal(dst)
	}
	return dst
}

func MarshalSize(dst []byte, msg Marshaler) []byte {
	ofs := len(dst)

	dst = codec.AppendUint32(dst, 0) // message length placeholder
	dst = msg.Marshal(dst)

	size := len(dst) - ofs - util.Uint32SizeBytes
	binary.BigEndian.PutUint32(dst[ofs:], uint32(size)) // overwrite the message length

	return dst
}

func Unmarshal(buf []byte, dst Unmarshaler) error {
	dec := &codec.BinaryDecoder{}
	dec.Reset(buf)

	if err := dst.Unmarshal(dec); err != nil {
		return err
	}

	if dec.RemainSize() > 0 {
		return errors.New("unmarshal: trailing data after unmarshal")
	}

	return nil
}
