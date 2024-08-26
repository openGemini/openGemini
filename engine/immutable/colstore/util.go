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

package colstore

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	primaryKeyMagic            string = "COLX"
	fileMagicSize              int    = len(primaryKeyMagic)
	version                    uint32 = 0
	headerSize                 int    = fileMagicSize + util.Uint32SizeBytes
	accMetaSize                int    = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
	CoarseIndexFragment        int    = 8
	MinRowsForSeek             int    = 0
	IndexFileSuffix            string = ".idx"
	MinMaxIndexFileSuffix      string = ".mm"
	SetIndexFileSuffix         string = ".set"
	BloomFilterIndexFileSuffix string = ".bf"
	TextIndexDataFileSuffix    string = ".pos" // posting list
	TextIndexHeadFileSuffix    string = ".bh"  // block header
	TextIndexPartFileSuffix    string = ".ph"  // part header
	DefaultTCLocation          int8   = -1
	crcSize                    uint32 = 4
	pKMetaItemSize             int    = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
)

const (
	TextIndexData = iota
	TextIndexHead
	TextIndexPart
	TextIndexMax
)

func AppendPKIndexSuffix(dataPath string) string {
	indexPath := dataPath + IndexFileSuffix
	return indexPath
}

func AppendSecondaryIndexSuffix(dataPath string, fieldName string, indexType index.IndexType, fileType int) string {
	var indexFileSuffix string
	switch indexType {
	case index.MinMax:
		indexFileSuffix = MinMaxIndexFileSuffix
	case index.Set:
		indexFileSuffix = SetIndexFileSuffix
	case index.BloomFilter, index.BloomFilterFullText:
		indexFileSuffix = BloomFilterIndexFileSuffix
	case index.Text:
		if fileType == TextIndexData {
			indexFileSuffix = TextIndexDataFileSuffix
		} else if fileType == TextIndexHead {
			indexFileSuffix = TextIndexHeadFileSuffix
		} else if fileType == TextIndexPart {
			indexFileSuffix = TextIndexPartFileSuffix
		} else {
			panic(fmt.Sprintf("unsupported index file type{%d} for text index", fileType))
		}
	default:
		panic(fmt.Sprintf("unsupported the secondary index: %d", indexType))
	}
	indexPath := dataPath + "." + fieldName + indexFileSuffix
	return indexPath
}
