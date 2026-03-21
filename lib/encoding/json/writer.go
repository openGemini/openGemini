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

package json

import (
	"math"
	"strconv"
	"time"

	liftedJson "github.com/openGemini/openGemini/lib/util/lifted/encoding/json"
)

var appendInt64 = appendInt64Default
var appendFloat64 = appendFloat64Default

func appendFloat64Default(dst []byte, v float64) []byte {
	return strconv.AppendFloat(dst, v, 'f', -1, 64)
}

func appendInt64Default(dst []byte, v int64) []byte {
	return strconv.AppendInt(dst, v, 10)
}

type StreamWriter struct {
	buf []byte
}

func NewStreamWriter(buf []byte) *StreamWriter {
	return &StreamWriter{
		buf: buf[:0],
	}
}

func (sw *StreamWriter) Bytes() []byte {
	return sw.buf
}

func (sw *StreamWriter) Reset() {
	sw.buf = sw.buf[:0]
}

func (sw *StreamWriter) WriteObjectStart() {
	sw.buf = append(sw.buf, '{')
}

func (sw *StreamWriter) WriteObjectEnd() {
	sw.buf = append(sw.buf, '}')
}

func (sw *StreamWriter) WriteArrayStart(key string) {
	if key != "" {
		sw.WriteValueString(key)
		sw.WriteColon()
	}
	sw.buf = append(sw.buf, '[')
}

func (sw *StreamWriter) WriteArrayEnd() {
	sw.buf = append(sw.buf, ']')
}

func (sw *StreamWriter) WriteColon() {
	sw.buf = append(sw.buf, ':')
}

func (sw *StreamWriter) WriteComma() {
	sw.buf = append(sw.buf, ',')
}

func (sw *StreamWriter) TrimLeftOne() {
	if len(sw.buf) > 0 {
		sw.buf = sw.buf[:len(sw.buf)-1]
	}
}

func (sw *StreamWriter) WriteValueBool(v bool) {
	if v {
		sw.buf = append(sw.buf, "true"...)
	} else {
		sw.buf = append(sw.buf, "false"...)
	}
}

func (sw *StreamWriter) WriteTimeRFC3339(time time.Time) {
	sw.buf = append(sw.buf, '"')
	sw.buf, _ = time.AppendText(sw.buf)
	sw.buf = append(sw.buf, '"')
}

func (sw *StreamWriter) WriteValueFloat64(v float64) {
	if math.IsNaN(v) {
		sw.WriteValueString("NaN")
		return
	}
	if math.IsInf(v, 1) {
		sw.WriteValueString("+Inf")
		return
	}
	if math.IsInf(v, -1) {
		sw.WriteValueString("-Inf")
		return
	}

	sw.buf = appendFloat64(sw.buf, v)
}

func (sw *StreamWriter) WriteValueInt64(v int64) {
	sw.buf = appendInt64(sw.buf, v)
}

// WriteValueString This method does not perform escaping and is only used for static strings.
func (sw *StreamWriter) WriteValueString(s string) {
	sw.buf = append(sw.buf, '"')
	sw.buf = append(sw.buf, s...)
	sw.buf = append(sw.buf, '"')
}

func (sw *StreamWriter) WriteIntKV(key string, value int64) {
	sw.WriteValueString(key)
	sw.WriteColon()
	sw.WriteValueInt64(value)
	sw.WriteComma()
}

func (sw *StreamWriter) WriteStringKV(key string, value string) {
	sw.WriteValueString(key)
	sw.WriteColon()
	sw.WriteValueStringEscape(value)
	sw.WriteComma()
}

func (sw *StreamWriter) WriteValueStringEscape(v string) {
	sw.buf = liftedJson.AppendString(sw.buf, v, true)
}

func (sw *StreamWriter) WriteNil() {
	sw.buf = append(sw.buf, "null"...)
}
