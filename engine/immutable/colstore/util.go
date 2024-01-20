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

package colstore

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	primaryKeyMagic            string = "COLX"
	fileMagicSize              int    = len(primaryKeyMagic)
	version                    uint32 = 0
	headerSize                 int    = fileMagicSize + util.Uint32SizeBytes
	accMetaSize                int    = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
	RowsNumPerFragment         int    = util.RowsNumPerFragment
	CoarseIndexFragment        int    = 8
	MinRowsForSeek             int    = 0
	IndexFileSuffix            string = ".idx"
	MinMaxIndexFileSuffix      string = ".mm"
	SetIndexFileSuffix         string = ".set"
	BloomFilterIndexFileSuffix string = ".bf"
	DefaultTCLocation          int8   = -1
	crcSize                    uint32 = 4
	pKMetaItemSize             int    = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
)

var (
	MinMaxIndex              = "minmax"
	SetIndex                 = "set"
	BloomFilterIndex         = "bloomfilter"
	BloomFilterFullTextIndex = logstore.BloomFilterFullText
)

func AppendPKIndexSuffix(dataPath string) string {
	indexPath := dataPath + IndexFileSuffix
	return indexPath
}

func AppendSKIndexSuffix(dataPath string, fieldName string, indexName string) string {
	var indexFileSuffix string
	switch indexName {
	case MinMaxIndex:
		indexFileSuffix = MinMaxIndexFileSuffix
	case SetIndex:
		indexFileSuffix = SetIndexFileSuffix
	case BloomFilterIndex:
		indexFileSuffix = BloomFilterIndexFileSuffix
	default:
		panic(fmt.Sprintf("unsupported the skip index: %s", indexName))
	}
	indexPath := dataPath + "." + fieldName + indexFileSuffix
	return indexPath
}
