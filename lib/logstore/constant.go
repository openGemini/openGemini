/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package logstore

const (
	Prime_64 uint64 = 0x9E3779B185EBCA87
)

const (
	OBSMetaFileName    = "segment.meta"
	OBSVLMFileName     = "segment.vlm"
	OBSContentFileName = "segment.cnt"
	FilterLogName      = "segment.fll"
)

type Constant struct {
	FilterDataMemSize         int64 // single bloomfilter's size
	FilterDataDiskSize        int64 // FilterDataDiskSize = FilterDataMemSize + crc32
	FilterCntPerVerticalGorup int64 // the bloom filter's count in single vertical group
	FilterGroupMaxDiskSize    int64 // the maximum size of row storage before converting to vertical storage

	VerticalPieceMemSize      int64 // single Vertical piece's size in memory, equals 8Byte * FilterCntPerVerticalGorup
	VerticalPieceDiskSize     int64 // VerticalPieceDiskSize = VerticalPieceMemSize + crc32
	VerticalPieceCntPerFilter int64
	VerticalGroupDiskSize     int64
	ScanBlockCnt              int
	MetaDataBatchSize         int
	UnnestMetaDataBatchSize   int

	MaxBlockDataSize       int
	MaxLogSizeLimit        int
	MaxBlockLogStoreNBytes int

	MaxBlockCompressedNBytes int
	MaxBlockStoreNBytes      int

	ContentAppenderBufferSize int
}

func InitConstant(filterDataMemSize, filterCntPerVerticalGorup int64, scanBlockCnt, metaDataBatchSize, unnestMetaDataBatchSize int,
	maxBlockDataSize, maxBlockCompressedNBytes, ContentAppenderBufferSize int) *Constant {
	c := &Constant{}
	c.FilterDataMemSize = filterDataMemSize
	c.FilterDataDiskSize = c.FilterDataMemSize + 4
	c.FilterCntPerVerticalGorup = filterCntPerVerticalGorup
	c.FilterGroupMaxDiskSize = c.FilterCntPerVerticalGorup * c.FilterDataDiskSize

	c.ScanBlockCnt = scanBlockCnt
	c.MetaDataBatchSize = metaDataBatchSize
	c.UnnestMetaDataBatchSize = unnestMetaDataBatchSize

	c.VerticalPieceMemSize = 8 * c.FilterCntPerVerticalGorup
	c.VerticalPieceDiskSize = c.VerticalPieceMemSize + 4
	c.VerticalPieceCntPerFilter = c.FilterDataMemSize / 8
	c.VerticalGroupDiskSize = c.VerticalPieceDiskSize * c.VerticalPieceCntPerFilter

	c.MaxBlockDataSize = maxBlockDataSize
	c.MaxLogSizeLimit = 10 * c.MaxBlockDataSize

	c.MaxBlockCompressedNBytes = maxBlockCompressedNBytes
	c.MaxBlockStoreNBytes = 4 + c.MaxBlockCompressedNBytes + 4

	c.ContentAppenderBufferSize = ContentAppenderBufferSize

	return c
}

var LogStoreConstantV0 *Constant // version[0, 1]
var LogStoreConstantV2 *Constant // version[2, ...]

func init() {
	LogStoreConstantV0 = InitConstant(32*1024+64, 1024, 32, 128, 32, 1024*1024, 3158128,
		4*1024*1024)
	LogStoreConstantV2 = InitConstant(256*1024+64, 128, 16, 16, 4, 1024*1024*8, 25264577,
		32*1024*1024)
}

func GetConstant(version uint32) *Constant {
	if version <= 1 {
		return LogStoreConstantV0
	} else { // version >= 2
		return LogStoreConstantV2
	}
}
