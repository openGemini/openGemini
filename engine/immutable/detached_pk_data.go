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

package immutable

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
)

const PKDataLimitNum = 16

type DetachedPKDataReader struct {
	r        fileops.BasicFileReader
	metaInfo *colstore.DetachedPKMetaInfo
	orderMap map[int64]int
}

func NewDetachedPKDataReader(path string, opts *obs.ObsOptions) (*DetachedPKDataReader, error) {
	fd, err := obs.OpenObsFile(path, PrimaryKeyFile, opts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedPKDataReader{r: dr, orderMap: make(map[int64]int)}, nil
}

func (reader *DetachedPKDataReader) SetPkMetaInfo(metaInfo *colstore.DetachedPKMetaInfo) {
	reader.metaInfo = metaInfo
}

func (reader *DetachedPKDataReader) Read(offset, length []int64, metas []*colstore.DetachedPKMeta) ([]*colstore.DetachedPKData, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, PKDataLimitNum)
	pkItems := make([]*colstore.DetachedPKData, len(offset))
	for i, of := range offset {
		reader.orderMap[of] = i
	}
	i := 0
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		pkItems[i] = &colstore.DetachedPKData{Offset: r.Offset}
		_, err := pkItems[i].Unmarshal(r.Content, metas[reader.orderMap[r.Offset]], reader.metaInfo)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	sort.Slice(pkItems, func(i, j int) bool {
		return pkItems[i].Offset < pkItems[j].Offset
	})
	return pkItems, nil
}
