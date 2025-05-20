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

package compress

import (
	"io"

	"github.com/golang/snappy"
)

type SnappyBlockWriter struct {
	w        io.Writer
	buf      []byte
	compress []byte
}

func NewSnappyBlockWriter(w io.Writer) *SnappyBlockWriter {
	return &SnappyBlockWriter{
		w:        w,
		buf:      make([]byte, 0),
		compress: make([]byte, 0),
	}
}

func (w *SnappyBlockWriter) Reset(writer io.Writer) {
	w.w = writer
	w.buf = w.buf[:0]
}

func (w *SnappyBlockWriter) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return len(w.buf), nil
}

func (w *SnappyBlockWriter) Close() error {
	w.compress = snappy.Encode(w.compress[:0], w.buf)
	_, err := w.w.Write(w.compress)
	return err
}
