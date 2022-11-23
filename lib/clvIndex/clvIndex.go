/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
/*
	This module is the specific architecture design of CLV index.
	The key of indexTreeMap structure is a column of a table, and value is its corresponding dictionary and index.
	IndexType is the index type. DicAndIndex contains specific dictionaries and indexes
	The CreateCLVIndex function is used to create a dictionary based on log information, and then create an index using the dictionary.
	The CLVSearch function is used to query the index according to the table name, column name and query options, and get the result set containing the ID and timestamp.
*/
package clvIndex

import (
	"github.com/openGemini/openGemini/lib/utils"
)

type CLVIndex struct {
	indexTreeMap map[MeasurementAndFieldKey]*CLVIndexNode
	indexType    CLVIndexType
}

func NewCLVIndex(indexType CLVIndexType) *CLVIndex {
	return &CLVIndex{
		indexTreeMap: make(map[MeasurementAndFieldKey]*CLVIndexNode),
		indexType:    indexType,
	}
}

/*
	There are two types of dictionaries, CLVC and CLVL. CLVC is a configuration dictionary based on a batch of data, and CLVL is a learning dictionary based on a batch of data and query load.
*/

type CLVDicType int32

const (
	CLVC CLVDicType = 0
	CLVL CLVDicType = 1
)

/*
	There are two types of indexes, namely VGRAM and VTOKEN.
	The former is an index item constructed according to character division, and is aligned with the NGram tokenizer of ES;
	the latter is an index item constructed based on word division, which is based on the standard segmentation of ES.
*/

type CLVIndexType int32

const (
	VGRAM  CLVIndexType = 0
	VTOKEN CLVIndexType = 1
)

type MeasurementAndFieldKey struct {
	measurementName string
	fieldKey        string
}

func NewMeasurementAndFieldKey(measurementName string, fieldKey string) MeasurementAndFieldKey {
	return MeasurementAndFieldKey{
		measurementName: measurementName,
		fieldKey:        fieldKey,
	}
}

func (clvIndex *CLVIndex) CreateCLVIndex(log string, tsid uint64, timeStamp int64, measurement string, fieldName string) {
	measurementAndFieldKey := NewMeasurementAndFieldKey(measurement, fieldName)
	if _, ok := clvIndex.indexTreeMap[measurementAndFieldKey]; !ok {
		var dic *CLVDictionary
		//todo 赋值给dic
		//dic =
		clvIndex.indexTreeMap[measurementAndFieldKey] = NewCLVIndexNode(clvIndex.indexType, dic) //Start with the configuration dictionary
		//clvIndex.indexTreeMap[measurementAndFieldKey].dic.CreateDictionaryIfNotExists(log, tsid, timeStamp, indexType)
	} /*else { //Later, a learning dictionary was used to index the log of this table.
		clvIndex.indexTreeMap[measurementAndFieldKey].dicType = CLVL //todo
	}*/
	clvIndex.indexTreeMap[measurementAndFieldKey].CreateCLVIndexIfNotExists(log, tsid, timeStamp)
}

func (clvIndex *CLVIndex) CLVSearch(measurementName string, fieldKey string, queryType QuerySearch, queryStr string) []utils.SeriesId {
	var res []utils.SeriesId
	option := NewQueryOption(measurementName, fieldKey, queryType, queryStr)
	measurementAndFieldKey := NewMeasurementAndFieldKey(measurementName, fieldKey)
	if _, ok := clvIndex.indexTreeMap[measurementAndFieldKey]; ok {
		indexType := clvIndex.indexType
		dic := clvIndex.indexTreeMap[measurementAndFieldKey].dic
		index := clvIndex.indexTreeMap[measurementAndFieldKey]
		res = CLVSearchIndex(indexType, dic.DicType, option, dic, index)
	} else {
		res = nil
	}
	return res
}
