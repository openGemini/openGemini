// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"io"
)

type IndexWriter interface {
	Init(name string, lock *string, cacheMeta bool, limitCompact bool)
	Write(p []byte) (int, error)
	Size() int
	BlockSize() int
	CopyTo(to io.Writer) (int, error)
	SwitchMetaBuffer() (int, error)
	MetaDataBlocks(dst [][]byte) [][]byte
	allInBuffer() bool
	Close() error
}

func newIndexWriter() IndexWriter {
	return &indexWriter{}
}

func NewPKIndexWriter(indexName string, cacheMeta bool, limitCompact bool, lockPath *string) IndexWriter {
	w, ok := indexWriterPool.Get().(IndexWriter)
	if !ok || IsInterfaceNil(w) {
		w = newIndexWriter()
	}
	w.Init(indexName, lockPath, cacheMeta, limitCompact)

	return w
}

func tryExpand(dst []byte, estimateLen int) []byte {
	if cap(dst) < estimateLen {
		dst = make([]byte, 0, estimateLen)
	}
	return dst
}
