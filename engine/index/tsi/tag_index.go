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

package tsi

import "sync"

type IndexRecord struct {
	TagCol *TagCol
	Err    error
}

type TagCol struct {
	Wg       *sync.WaitGroup
	Val      []byte
	Mst, Key []byte
	Err      error
}

var IndexTagColsPool sync.Pool

type IndexTagCols []TagCol

func GetIndexTagCols() *IndexTagCols {
	tagCols := IndexTagColsPool.Get()
	if tagCols == nil {
		return &IndexTagCols{}
	}
	return tagCols.(*IndexTagCols)
}

func PutIndexTagCols(tagCols *IndexTagCols) {
	tagCols.reset()
	*tagCols = (*tagCols)[:0]
	IndexTagColsPool.Put(tagCols)
}

func (itc *IndexTagCols) reset() {
	for i := range *itc {
		(*itc)[i].Key = (*itc)[i].Key[:0]
		(*itc)[i].Mst = (*itc)[i].Mst[:0]
		(*itc)[i].Val = (*itc)[i].Val[:0]
		(*itc)[i].Err = nil
	}
}
