// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package index

import (
	"fmt"
	"strings"
)

type IndexType int

func (t IndexType) String() string {
	if t >= TypeEnd || t < MergeSet {
		return ""
	}
	return TypeToName[t]
}

func (t IndexType) FileExtension() string {
	if t >= TypeEnd || t < MergeSet {
		return ""
	}
	return FileExtensions[t]
}

const (
	MergeSet IndexType = iota
	Text
	Field
	TimeCluster
	BloomFilter
	BloomFilterFullText
	MinMax
	Set
	Spatial
	BloomFilterIp
	BloomFilterUniversal
	AggMap // only for column store, and no need to create manually
	TypeEnd
)

const (
	MergeSetIndex             = "mergeset"
	TextIndex                 = "text"
	FieldIndex                = "field"
	TimeClusterIndex          = "timecluster"
	BloomFilterIndex          = "bloomfilter"
	BloomFilterFullTextIndex  = "bloomfilter_fulltext"
	BloomFilterUniversalIndex = "bloomfilter_universal"
	MinMaxIndex               = "minmax"
	SetIndex                  = "set"
	BloomFilterIpIndex        = "bloomfilter_ip"
	SpatialIndex              = "spatial"
)

var (
	NameToType = map[string]IndexType{
		MergeSetIndex:             MergeSet,
		TextIndex:                 Text,
		FieldIndex:                Field,
		TimeClusterIndex:          TimeCluster,
		BloomFilterIndex:          BloomFilter,
		BloomFilterFullTextIndex:  BloomFilterFullText,
		BloomFilterUniversalIndex: BloomFilterUniversal,
		MinMaxIndex:               MinMax,
		SetIndex:                  Set,
		BloomFilterIpIndex:        BloomFilterIp,
		SpatialIndex:              Spatial,
	}
	TypeToName     = [TypeEnd]string{}
	FileExtensions = [TypeEnd]string{
		BloomFilter:         ".bf",
		BloomFilterFullText: ".bf",
		MinMax:              ".mm",
		Set:                 ".set",
		Spatial:             ".spi",
		BloomFilterIp:       ".bf",
		AggMap:              ".am",
	}
)

func init() {
	for k, v := range NameToType {
		TypeToName[v] = k
	}
}

type ContextKey string

const (
	QueryIndexState ContextKey = "QueryIndexState"
)

func GetIndexTypeByName(name string) (IndexType, error) {
	index, ok := NameToType[strings.ToLower(name)]
	if !ok {
		return MergeSet, fmt.Errorf("invalid index name %s", name)
	}
	return index, nil
}

func GetIndexNameByType(id IndexType) string {
	return id.String()
}
