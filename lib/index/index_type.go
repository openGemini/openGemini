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

const (
	MergeSet IndexType = iota
	Text
	Field
	TimeCluster
	BloomFilter
	BloomFilterFullText
	MinMax
	Set
	IndexTypeAll
	BloomFilterIp
)

const (
	MergeSetIndex            = "mergeset"
	TextIndex                = "text"
	FieldIndex               = "field"
	TimeClusterIndex         = "timecluster"
	BloomFilterIndex         = "bloomfilter"
	BloomFilterFullTextIndex = "bloomfilter_fulltext"
	MinMaxIndex              = "minmax"
	SetIndex                 = "set"
	BloomFilterIpIndex       = "bloomfilter_ip"
)

var (
	IndexNameToType = map[string]IndexType{
		MergeSetIndex:            MergeSet,
		TextIndex:                Text,
		FieldIndex:               Field,
		TimeClusterIndex:         TimeCluster,
		BloomFilterIndex:         BloomFilter,
		BloomFilterFullTextIndex: BloomFilterFullText,
		MinMaxIndex:              MinMax,
		SetIndex:                 Set,
		BloomFilterIpIndex:       BloomFilterIp,
	}
	IndexTypeToName = map[IndexType]string{
		MergeSet:            MergeSetIndex,
		Text:                TextIndex,
		Field:               FieldIndex,
		TimeCluster:         TimeClusterIndex,
		BloomFilter:         BloomFilterIndex,
		BloomFilterFullText: BloomFilterFullTextIndex,
		MinMax:              MinMaxIndex,
		Set:                 SetIndex,
		BloomFilterIp:       BloomFilterIpIndex,
	}
)

type ContextKey string

const (
	QueryIndexState ContextKey = "QueryIndexState"
)

func GetIndexTypeByName(name string) (IndexType, error) {
	index, ok := IndexNameToType[strings.ToLower(name)]
	if !ok {
		return MergeSet, fmt.Errorf("invalid index name %s", name)
	}
	return index, nil
}

func GetIndexNameByType(id IndexType) (string, error) {
	name, ok := IndexTypeToName[id]
	if !ok {
		return "", fmt.Errorf("invalid index type %d", id)
	}
	return name, nil
}
