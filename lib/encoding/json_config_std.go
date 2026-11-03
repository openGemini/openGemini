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

package encoding

import (
	"bytes"
	"encoding/json"
	"io"
)

type stdJSON struct{}

var StdJSONConfig = stdJSON{}

func (stdJSON) Marshal(val any) ([]byte, error) {
	return StdJSONConfig.MarshalIndent(val, "", "")
}

func (stdJSON) MarshalIndent(val any, prefix, indent string) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent(prefix, indent)
	err := enc.Encode(val)
	out := buf.Bytes()
	if len(out) > 0 && out[len(out)-1] == '\n' {
		out = out[:len(out)-1]
	}
	return out, err
}

func (stdJSON) MarshalToString(val any) (string, error) {
	out, err := StdJSONConfig.Marshal(val)
	return string(out), err
}

func (stdJSON) Unmarshal(buf []byte, val any) error {
	return json.Unmarshal(buf, val)
}

func (stdJSON) UnmarshalFromString(buf string, val any) error {
	return json.Unmarshal([]byte(buf), val)
}

func (stdJSON) NewEncoder(writer io.Writer) *json.Encoder {
	enc := json.NewEncoder(writer)
	enc.SetEscapeHTML(false)
	return enc
}

func (stdJSON) NewDecoder(reader io.Reader) *json.Decoder {
	return json.NewDecoder(reader)
}
