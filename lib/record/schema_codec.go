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

import "github.com/openGemini/openGemini/lib/codec"

func (f *Field) Marshal(buf []byte) ([]byte, error) {
	buf = codec.AppendString(buf, f.Name)
	buf = codec.AppendInt(buf, f.Type)
	return buf, nil
}

func (f *Field) Unmarshal(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}

	dec := codec.NewBinaryDecoder(buf)
	f.Name = dec.String()
	f.Type = dec.Int()
	return nil
}

func (f *Field) Size() int {
	size := 0
	size += codec.SizeOfString(f.Name)
	size += codec.SizeOfInt()
	return size
}
