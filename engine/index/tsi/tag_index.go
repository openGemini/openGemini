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

var IndexRecordPool sync.Pool

func GetIndexRecord() *IndexRecord {
	rec := IndexRecordPool.Get()
	if rec == nil {
		return &IndexRecord{
			TagCol: &TagCol{},
		}
	}
	return rec.(*IndexRecord)
}

func PutIndexRecord(rec *IndexRecord) {
	rec.reset()
	IndexRecordPool.Put(rec)
}

func (rec *IndexRecord) reset() {
	rec.TagCol.Key = rec.TagCol.Key[:0]
	rec.TagCol.Mst = rec.TagCol.Mst[:0]
	rec.TagCol.Val = rec.TagCol.Val[:0]
	rec.TagCol.Err = nil
}
