// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

import "github.com/openGemini/openGemini/lib/fileops"

type MemoryReader interface {
	AppendDataBlock(srcData []byte)
	ReadChunkMetaBlock(metaIdx int, sid uint64, count uint32) []byte
	ReadDataBlock(offset int64, size uint32, dstPtr *[]byte) ([]byte, error)
	CopyBlocks(src MemoryReader)
	LoadIntoMemory(dr fileops.BasicFileReader, tr *Trailer, metaIndexItems []MetaIndex) error
	FreeMemory() int64
	DataInMemory() bool
	MetaInMemory() bool
	ReserveMetaBlock(n int)
	ReserveDataBlock(n int)
	DataBlocks() [][]byte
	MetaBlocks() [][]byte
	SetMetaBlocks(blocks [][]byte)
	Size() int64
	Reset()
}

type memReader struct{}

func (memReader) AppendDataBlock([]byte)                                              {}
func (memReader) ReadChunkMetaBlock(int, uint64, uint32) []byte                       { return nil }
func (memReader) ReadDataBlock(int64, uint32, *[]byte) ([]byte, error)                { return nil, nil }
func (memReader) CopyBlocks(MemoryReader)                                             {}
func (memReader) LoadIntoMemory(fileops.BasicFileReader, *Trailer, []MetaIndex) error { return nil }
func (memReader) FreeMemory() int64                                                   { return 0 }
func (memReader) DataInMemory() bool                                                  { return false }
func (memReader) MetaInMemory() bool                                                  { return false }
func (memReader) ReserveMetaBlock(int)                                                {}
func (memReader) ReserveDataBlock(int)                                                {}
func (memReader) Size() int64                                                         { return 0 }
func (memReader) Reset()                                                              {}
func (memReader) DataBlocks() [][]byte                                                { return nil }
func (memReader) MetaBlocks() [][]byte                                                { return nil }
func (memReader) SetMetaBlocks([][]byte)                                              {}

var (
	emptyMemReader = &memReader{}
)

func NewMemReader() MemoryReader {
	return &memReader{}
}
