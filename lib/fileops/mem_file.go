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

package fileops

import (
	"io"

	"github.com/openGemini/openGemini/lib/errno"
)

type MemFile struct {
	maxSize int64
	size    int64
	data    []byte
}

func NewMemFile(maxSize int64) *MemFile {
	return &MemFile{
		maxSize: maxSize,
		data:    make([]byte, 0, maxSize),
	}
}

func (mf *MemFile) MaxSize() int64 {
	return mf.maxSize
}

func (mf *MemFile) Size() int64 {
	return mf.size
}

func (mf *MemFile) Write(b []byte) {
	end := int64(len(b)) + mf.size
	if end > mf.maxSize {
		return
	}

	mf.data = append(mf.data, b...)
	mf.size = int64(len(mf.data))
}

func (mf *MemFile) ReadAt(dst []byte, ofs int64) (int, error) {
	end := int64(len(dst)) + ofs
	if mf.size < end || end > mf.maxSize {
		return 0, io.EOF
	}

	n := copy(dst, mf.data[ofs:end])
	return n, nil
}

func (mf *MemFile) Load(reader io.ReaderAt, size int) error {
	if cap(mf.data) < size {
		mf.data = make([]byte, size)
		mf.maxSize = int64(size)
	}

	n, err := reader.ReadAt(mf.data[:size], 0)
	if n != size {
		return errno.NewError(errno.ShortRead, n, size)
	}
	mf.size = int64(size)
	return err
}
